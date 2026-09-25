/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.NgrrdUri;
import dev.nishisan.utils.oss.format.DefinitionHash;

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Cache thread-safe de {@link NgrrdHandle} abertos localmente, por
 * {@code seriesKey}, sobre um único {@link BlobVolume}. Fecha handles ociosos
 * ({@link #closeIdle()}) e limita quantos ficam abertos ao mesmo tempo
 * ({@link #evictIfOverLimit()}, LRU), evitando que um storage node com milhares
 * de séries mantenha todas em memória o tempo todo.
 *
 * <h2>Desenho de concorrência (revisão pós-Refuter)</h2>
 *
 * <p>Cada série tem uma {@link HandleEntry} própria, com seu {@link ReentrantLock}
 * — o lock <strong>vive e morre com a entrada</strong>: quando ela é removida do
 * mapa, o lock não é mais referenciado por ninguém (resolve o crescimento sem
 * limite de um mapa de locks separado). Nenhum código deste registro segura
 * dois locks de entrada ao mesmo tempo — as operações de eviction usam
 * {@link ReentrantLock#tryLock()} (nunca bloqueiam) e processam uma entrada por
 * vez, o que elimina por construção o deadlock AB-BA entre {@link #open} (que
 * evict{@code a} depois de abrir) e {@link #closeIdle()}/{@link #evictIfOverLimit()}
 * (que fecham outras entradas).</p>
 *
 * <p>{@link #withHandle} é o único caminho para <strong>usar</strong> um handle
 * (escrever, ler, checkpoint): trava a entrada, confere que ela ainda é a
 * corrente no mapa, executa a operação e destrava — enquanto essa trava está
 * com alguém, nenhum {@code closeIdle}/{@code evict}/{@code close}/
 * {@code markMigrating} consegue fechar a mesma entrada (todos tentam o mesmo
 * lock antes de fechar). Isso elimina o bug em que {@code checkpoint()} podia
 * responder OK sem fazer nada porque o {@code NgrrdWriter} subjacente já tinha
 * sido fechado por outra thread ({@code NgrrdWriter.sync()} é um no-op
 * silencioso quando fechado — só {@code write()} lança "Writer já fechado").
 * {@link StorageRequestHandler} não recebe mais {@link NgrrdHandle} cru: toda
 * operação passa por {@link #withHandle}.</p>
 */
public final class SeriesHandleRegistry implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(SeriesHandleRegistry.class.getName());

    private final BlobVolume volume;
    private final String volumeName;
    private final Duration idleTtl;
    private final int maxOpenHandles;
    private final Object[] operationLocks = java.util.stream.IntStream.range(0, 256).mapToObj(i -> new Object()).toArray();
    /** Lock identity shared by OPEN, metadata inspection and migration; use {@code CoordinationLocks.acquire}. */
    public Object operationLock(String key) { return operationLocks[Math.floorMod(key.hashCode(), operationLocks.length)]; }
    private final Clock clock;

    private final ConcurrentMap<String, HandleEntry> entries = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> hashBySeriesKey = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DefinitionRecord> definitionByHash = new ConcurrentHashMap<>();
    private final Set<String> migrating = ConcurrentHashMap.newKeySet();
    private final Set<String> copying = ConcurrentHashMap.newKeySet();
    /**
     * Séries fechadas por {@link #close(String)} explícito do cliente —
     * {@link #reopenIfKnown} não as reabre sozinho (ao contrário de um
     * fechamento por ociosidade/LRU); só um novo {@link #open} limpa a marca.
     */
    private final Set<String> closedByClient = ConcurrentHashMap.newKeySet();
    /**
     * Séries esquecidas por {@link #forget(String)} — a origem de uma migração concluída
     * ({@code MIGRATE_FINISH}) marca aqui a chave para fechar em definitivo a corrida "réplica local
     * atrasada": enquanto marcada, quem decide se um {@code OPEN} pode (re)criar a série neste nó não é
     * mais a réplica LOCAL do catálogo (que ainda pode dizer {@code ACTIVE(self)} por um instante depois
     * do FINISH) nem o {@code placementHint} do cliente, e sim uma confirmação forte do líder — ver
     * {@code StorageRequestHandler#ownership}. Só {@link #open} limpa a marca (a mesma reabertura
     * legítima que já limpa {@link #closedByClient}), nunca {@link #reopenIfKnown} sozinho.
     */
    private final Set<String> forgotten = ConcurrentHashMap.newKeySet();

    public SeriesHandleRegistry(BlobVolume volume, String volumeName, Duration idleTtl, int maxOpenHandles,
            Clock clock) {
        this.volume = Objects.requireNonNull(volume, "volume");
        this.volumeName = Objects.requireNonNull(volumeName, "volumeName");
        this.idleTtl = Objects.requireNonNull(idleTtl, "idleTtl");
        if (maxOpenHandles <= 0) {
            throw new IllegalArgumentException("maxOpenHandles deve ser > 0: " + maxOpenHandles);
        }
        this.maxOpenHandles = maxOpenHandles;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Abre (ou confirma já aberta, atualizando {@code lastAccess}) a série.
     * Limpa a marca de {@link #close fechado pelo cliente}, se houver — um novo
     * {@code open} é o único jeito de reabrir uma série fechada explicitamente.
     *
     * @throws IllegalStateException se a série está marcada como
     *                                 {@link #markMigrating migrating}
     */
    public NgrrdHandle open(String seriesKey, String yaml, Ngrrd.OpenOptions options) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        Objects.requireNonNull(yaml, "yaml");
        Objects.requireNonNull(options, "options");
        if (migrating.contains(seriesKey)) {
            throw new IllegalStateException("MIGRATING");
        }
        NgrrdHandle handle;
        for (;;) {
            HandleEntry entry = entries.computeIfAbsent(seriesKey, key -> new HandleEntry());
            entry.lock.lock();
            try {
                // A2 (TOCTOU): entre o computeIfAbsent acima e a aquisição do lock, outra thread pode
                // ter fechado e removido esta MESMA entrada (ex.: close(key)) e uma terceira pode já
                // ter inserido uma entrada NOVA para a chave. Sem esta reconferência, seguiríamos
                // travando/operando numa entrada "fantasma" — já fora do mapa — e devolveríamos
                // `entry.handle` já fechado por quem a removeu, como se fosse um handle válido.
                if (entries.get(seriesKey) != entry) {
                    continue;
                }
                if (migrating.contains(seriesKey)) {
                    entries.remove(seriesKey, entry);
                    throw new IllegalStateException("MIGRATING");
                }
                if (entry.handle == null) {
                    try {
                        entry.handle = Ngrrd.open(volume, NgrrdUri.of(volumeName, seriesKey), yaml, options);
                    } catch (RuntimeException e) {
                        entries.remove(seriesKey, entry);
                        throw e;
                    }
                    cacheDefinition(seriesKey, yaml, options);
                }
                closedByClient.remove(seriesKey);
                // Reabertura legítima: quem chama open() já passou pela checagem forte de dono em
                // StorageRequestHandler#ownership (isForgotten força placementStrong antes de chegar
                // aqui) — é o único lugar que limpa a marca de esquecida.
                forgotten.remove(seriesKey);
                entry.touch(clock.millis());
                handle = entry.handle;
                break;
            } finally {
                entry.lock.unlock();
            }
        }
        evictIfOverLimit();
        return handle;
    }

    /**
     * Executa {@code fn} sobre o handle aberto de {@code seriesKey}, com a
     * garantia de que nenhum {@code close}/{@code closeIdle}/{@code evict}
     * concorrente fecha o handle por baixo enquanto {@code fn} roda — ambos
     * disputam o mesmo {@link ReentrantLock} da entrada. Único caminho seguro
     * para write/read/checkpoint/flush; use no lugar de expor o
     * {@link NgrrdHandle} bruto ao chamador.
     *
     * <p>{@code fn} roda inteiro dentro do lock da entrada — não pode ser
     * {@code null} (contrato, não conveniência: um {@code fn} nulo indicaria um
     * chamador quebrado, não "nada a fazer"), nem deve bloquear por conta
     * própria (ex.: esperar outra thread) sob pena de segurar o lock da série
     * além do necessário.</p>
     *
     * @throws NullPointerException se {@code seriesKey} ou {@code fn} forem {@code null}
     * @return o resultado de {@code fn}, ou {@link Optional#empty()} se a série
     *         não está aberta (nunca foi, foi fechada, ou está migrando)
     */
    public <R> Optional<R> withHandle(String seriesKey, Function<NgrrdHandle, R> fn) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        Objects.requireNonNull(fn, "fn");
        if (isMigrationFrozen(seriesKey)) {
            return Optional.empty();
        }
        HandleEntry entry = entries.get(seriesKey);
        if (entry == null) {
            return Optional.empty();
        }
        entry.lock.lock();
        try {
            // Reconfere sob o lock: a entrada pode ter sido removida (fechada) ou substituída
            // (fechada e reaberta por outra thread) entre o `entries.get` acima e agora.
            if (entries.get(seriesKey) != entry || entry.handle == null) {
                return Optional.empty();
            }
            entry.touch(clock.millis());
            return Optional.ofNullable(fn.apply(entry.handle));
        } finally {
            entry.lock.unlock();
        }
    }

    /**
     * Handle já aberto para {@code seriesKey}, para inspeção/testes — atualiza
     * {@code lastAccess}. <strong>Não use o resultado para operar sobre o
     * handle</strong> (a referência pode ser fechada por outra thread logo em
     * seguida); operações passam por {@link #withHandle}.
     */
    public Optional<NgrrdHandle> existing(String seriesKey) {
        return withHandle(seriesKey, handle -> handle);
    }

    /** Indica se a série está aberta agora, sem expor o handle (usado pelas checagens de dono). */
    public boolean isOpen(String seriesKey) {
        return withHandle(seriesKey, handle -> Boolean.TRUE).isPresent();
    }

    /**
     * Reabre a série se o cache de definições ainda conhece seu YAML/opções
     * (foi aberta antes e fechada por ociosidade/LRU) — dispensa o chamador de
     * reenviar a definição. {@link Optional#empty()} se a série nunca foi
     * aberta neste processo, está {@link #markMigrating migrating}, ou foi
     * {@link #close fechada explicitamente pelo cliente} (essa só volta com um
     * novo {@link #open}).
     */
    public Optional<NgrrdHandle> reopenIfKnown(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        if (isMigrationFrozen(seriesKey) || closedByClient.contains(seriesKey)) {
            return Optional.empty();
        }
        String hash = hashBySeriesKey.get(seriesKey);
        DefinitionRecord definition = hash != null ? definitionByHash.get(hash) : null;
        if (definition == null && !entries.containsKey(seriesKey)) {
            return Optional.empty();
        }

        NgrrdHandle handle = null;
        for (;;) {
            HandleEntry entry = entries.computeIfAbsent(seriesKey, key -> new HandleEntry());
            entry.lock.lock();
            try {
                // A2 (TOCTOU): mesma reconferência de open() — a entrada pode ter sido trocada entre
                // o computeIfAbsent e o lock.
                if (entries.get(seriesKey) != entry) {
                    continue;
                }
                if (isMigrationFrozen(seriesKey) || closedByClient.contains(seriesKey)) {
                    // Só remove se ainda vazia E ainda a corrente — nunca apaga um handle que outra
                    // thread já tenha aberto de verdade nesta mesma entrada nesse meio-tempo (o bug
                    // que o Refuter pegou: remover incondicionalmente fora do lock podia derrubar do
                    // mapa uma entrada que um open() concorrente acabara de popular, vazando o handle).
                    if (entry.handle == null) {
                        entries.remove(seriesKey, entry);
                    }
                    return Optional.empty();
                }
                if (entry.handle == null) {
                    if (definition == null) {
                        entries.remove(seriesKey, entry);
                        return Optional.empty();
                    }
                    try {
                        entry.handle = Ngrrd.open(volume, NgrrdUri.of(volumeName, seriesKey),
                                definition.yaml(), definition.options());
                    } catch (RuntimeException e) {
                        entries.remove(seriesKey, entry);
                        throw e;
                    }
                }
                entry.touch(clock.millis());
                handle = entry.handle;
                break;
            } finally {
                entry.lock.unlock();
            }
        }
        evictIfOverLimit();
        return Optional.of(handle);
    }

    /**
     * Libera a referência local da série (CLOSE do cliente): checkpoint+close
     * do handle, se aberto, e remove a entrada — marca a série como fechada
     * pelo cliente ({@link #reopenIfKnown} deixa de reabri-la sozinha).
     * Idempotente.
     */
    public void close(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        HandleEntry entry = entries.get(seriesKey);
        if (entry == null) {
            return;
        }
        entry.lock.lock();
        try {
            if (entries.remove(seriesKey, entry)) {
                closeQuietly(seriesKey, entry.handle);
                closedByClient.add(seriesKey);
            }
        } finally {
            entry.lock.unlock();
        }
    }

    /**
     * Libera a referência local da série SEM marcar {@link #closedByClient}: checkpoint+close do
     * handle, se aberto, e remove a entrada — {@link #reopenIfKnown} continua livre para reabri-la
     * depois. Usado pela migração (M3): o destino chama antes de gravar a imagem recebida (garante
     * que nenhum handle antigo desta série sobrevive à substituição do arquivo por baixo); a origem
     * chama após {@code MIGRATE_FINISH} apagar a cópia local. Idempotente.
     *
     * <p>Diferente de {@link #close(String)} (fechamento explícito do cliente, que bloqueia
     * reaberturas automáticas): aqui a série pode voltar a ser aberta livremente — o que a impede,
     * durante a migração, é {@link #markMigrating}, não esta chamada.</p>
     */
    public void discard(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        HandleEntry entry = entries.get(seriesKey);
        if (entry == null) {
            return;
        }
        entry.lock.lock();
        try {
            if (entries.remove(seriesKey, entry)) {
                closeQuietly(seriesKey, entry.handle);
            }
        } finally {
            entry.lock.unlock();
        }
    }

    /**
     * Como {@link #discard(String)} e, além disso, <strong>esquece a definição</strong> da série:
     * depois desta chamada {@link #reopenIfKnown} não consegue mais reabri-la sozinho — só um
     * {@link #open} explícito (com a definição vinda do cliente, e portanto com o dono confirmado por
     * {@code placementStrong}) traz a série de volta a este nó. Idempotente.
     *
     * <p>Defeito que isto corrige (M3, mesma família da seção 0 da spec — recriação de série vazia no
     * nó errado): a origem chamava {@link #discard} depois de {@code MIGRATE_FINISH} apagar a imagem
     * local, mas {@code discard} só solta o handle e mantém o YAML/opções em cache. Uma escrita
     * atrasada do cliente que chegasse à origem logo depois (com a réplica local do catálogo ainda
     * dizendo {@code ACTIVE} aqui, e sem a marca de {@code migrating}, já limpa pelo FINISH) passava
     * pelo teste de dono e caía na auto-cura de {@code StorageRequestHandler}, que chamava
     * {@code reopenIfKnown} — e este RECRIAVA o arquivo da série, vazio, no dono antigo. O resultado é
     * uma órfã permanente: reproduzido por {@code RebalanceClusterTest} ("imagem ... não deveria mais
     * existir no dono antigo"). Marcar a série como esquecida fecha o caminho na raiz.</p>
     */
    public void forget(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        // Reaproveita o mesmo bloqueio de reabertura automática de close(): o conjunto é consultado
        // DENTRO do lock da entrada por open()/reopenIfKnown, então não há janela em que uma
        // reabertura concorrente escape. hashBySeriesKey só é limpo por último — definitionByHash é
        // compartilhado por todas as séries do mesmo YAML e nunca pode ser removido por uma delas.
        closedByClient.add(seriesKey);
        // Marca separada de closedByClient: além de bloquear reopenIfKnown, isForgotten é consultada
        // por StorageRequestHandler#ownership para nunca confiar na réplica local (nem no hint) de
        // dono enquanto esta série não for reaberta com confirmação forte do líder.
        forgotten.add(seriesKey);
        discard(seriesKey);
        hashBySeriesKey.remove(seriesKey);
    }

    /**
     * Indica se {@code seriesKey} está {@link #forget esquecida} neste nó — só {@link #open} (uma
     * reabertura legítima, já validada por confirmação forte do dono) limpa a marca.
     */
    public boolean isForgotten(String seriesKey) {
        return forgotten.contains(seriesKey);
    }

    /**
     * Fecha handles ociosos há mais de {@code idleTtl}. Candidatos são
     * escolhidos por um snapshot ordenado por {@code lastAccess}, sem lock;
     * cada um só é fechado se {@link ReentrantLock#tryLock()} conseguir a
     * entrada na hora — se estiver ocupada (ex.: um {@code checkpoint} em
     * andamento via {@link #withHandle}), pula para a próxima em vez de
     * bloquear. Retorna quantos foram fechados de fato.
     */
    public int closeIdle() {
        long now = clock.millis();
        long ttlMillis = idleTtl.toMillis();
        int closedCount = 0;
        for (AccessSnapshot candidate : snapshotByLastAccess()) {
            String seriesKey = candidate.seriesKey();
            HandleEntry entry = candidate.entry();
            if (now - entry.lastAccessMs < ttlMillis) {
                continue;
            }
            if (!entry.lock.tryLock()) {
                continue;
            }
            try {
                if (entries.get(seriesKey) != entry || entry.handle == null) {
                    continue;
                }
                if (now - entry.lastAccessMs < ttlMillis) {
                    continue;
                }
                closeQuietly(seriesKey, entry.handle);
                entries.remove(seriesKey, entry);
                closedCount++;
            } finally {
                entry.lock.unlock();
            }
        }
        return closedCount;
    }

    /**
     * Fecha o(s) handle(s) menos recentemente acessado(s) até respeitar
     * {@code maxOpenHandles}. Mesmo protocolo não bloqueante de
     * {@link #closeIdle()}: um candidato ocupado é pulado, nunca esperado.
     */
    public void evictIfOverLimit() {
        while (entries.size() > maxOpenHandles) {
            boolean evictedOne = false;
            for (AccessSnapshot candidate : snapshotByLastAccess()) {
                if (entries.size() <= maxOpenHandles) {
                    return;
                }
                String seriesKey = candidate.seriesKey();
                HandleEntry entry = candidate.entry();
                if (!entry.lock.tryLock()) {
                    continue;
                }
                try {
                    if (entries.get(seriesKey) != entry || entry.handle == null) {
                        continue;
                    }
                    closeQuietly(seriesKey, entry.handle);
                    entries.remove(seriesKey, entry);
                    evictedOne = true;
                } finally {
                    entry.lock.unlock();
                }
            }
            if (!evictedOne) {
                // Todas as candidatas estavam ocupadas agora — desiste até a próxima chamada
                // (após cada open/reopenIfKnown) em vez de girar (busy-loop) esperando.
                return;
            }
        }
    }

    /** Quantidade de séries com handle aberto agora. */
    public int openCount() {
        return (int) entries.values().stream().filter(entry -> entry.handle != null).count();
    }

    /** Chaves das séries com handle aberto agora. */
    public Set<String> openSeries() {
        return entries.entrySet().stream()
                .filter(entry -> entry.getValue().handle != null)
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Marca a série como em migração: faz checkpoint+close do handle aberto (se
     * houver) e impede reaberturas — {@link #open} passa a lançar
     * {@link IllegalStateException}; {@link #withHandle}/{@link #reopenIfKnown}
     * passam a devolver {@link Optional#empty()}. Na cópia online, este bloqueio
     * só começa na troca final de dono; uma falha no checkpoint impede o commit.
     */
    public void markMigrating(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        migrating.add(seriesKey);
        copying.remove(seriesKey);
        HandleEntry entry = entries.get(seriesKey);
        if (entry == null) {
            return;
        }
        entry.lock.lock();
        try {
            // An acknowledged write must reach the image before migration closes its writer.
            // A failed checkpoint leaves the handle available for a safe abort/retry.
            if (entries.get(seriesKey) == entry && entry.handle != null) { entry.handle.checkpoint(); }
            if (entries.remove(seriesKey, entry)) {
                closeQuietly(seriesKey, entry.handle);
            }
        } finally {
            entry.lock.unlock();
        }
    }

    /** Remove a marca de migração, permitindo {@link #open} novamente. */
    public void clearMigrating(String seriesKey) {
        migrating.remove(seriesKey);
        copying.remove(seriesKey);
    }

    /** Marks an online copy; existing same-geometry handles remain writable until markMigrating. */
    public void beginMigrationCopy(String seriesKey) {
        migrating.add(seriesKey);
        copying.add(seriesKey);
    }

    /** True only for the short cutover, not the initial online copy. */
    public boolean isMigrationFrozen(String seriesKey) {
        return migrating.contains(seriesKey) && !copying.contains(seriesKey);
    }

    /** Whether this source still accepts writes while copying an image. */
    public boolean isCopying(String seriesKey) { return copying.contains(seriesKey); }

    /** Checkpoints and snapshots one series under its write lock, then immediately releases writers. */
    public byte[] migrationSnapshot(String seriesKey, java.util.function.Supplier<byte[]> image) {
        for (;;) {
            HandleEntry entry = entries.computeIfAbsent(seriesKey, key -> new HandleEntry());
            entry.lock.lock();
            try {
                if (entries.get(seriesKey) != entry) { continue; }
                if (entry.handle != null) { entry.handle.checkpoint(); }
                return image.get();
            } finally {
                if (entry.handle == null) { entries.remove(seriesKey, entry); }
                entry.lock.unlock();
            }
        }
    }

    /** Indica se a série está marcada como em migração. */
    public boolean isMigrating(String seriesKey) {
        return migrating.contains(seriesKey);
    }

    /**
     * YAML cached by an earlier open in this process, if any. This is an in-memory convenience;
     * persisted or newly migrated series may exist without an entry in this cache.
     */
    public Optional<String> cachedYaml(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        String hash = hashBySeriesKey.get(seriesKey);
        DefinitionRecord definition = hash != null ? definitionByHash.get(hash) : null;
        return definition != null ? Optional.of(definition.yaml()) : Optional.empty();
    }

    /** Fecha (checkpoint+close) todos os handles abertos. */
    @Override
    public void close() {
        for (String seriesKey : List.copyOf(entries.keySet())) {
            HandleEntry entry = entries.get(seriesKey);
            if (entry == null) {
                continue;
            }
            entry.lock.lock();
            try {
                if (entries.remove(seriesKey, entry) && entry.handle != null) {
                    closeQuietly(seriesKey, entry.handle);
                }
            } finally {
                entry.lock.unlock();
            }
        }
    }

    /**
     * Congela os horários antes de ordenar: ler lastAccessMs durante a comparação
     * viola a transitividade quando withHandle/open toca a entrada em paralelo.
     * O snapshot é aproximado; identidade, uso e TTL são reconferidos sob o lock
     * da entrada antes do fechamento.
     */
    private List<AccessSnapshot> snapshotByLastAccess() {
        List<AccessSnapshot> snapshot = new ArrayList<>(entries.size());
        entries.forEach((key, entry) -> snapshot.add(new AccessSnapshot(key, entry, entry.lastAccessMs)));
        snapshot.sort(Comparator.comparingLong(AccessSnapshot::lastAccessMs));
        return snapshot;
    }

    private record AccessSnapshot(String seriesKey, HandleEntry entry, long lastAccessMs) {
    }

    private void cacheDefinition(String seriesKey, String yaml, Ngrrd.OpenOptions options) {
        String hash = DefinitionHash.hex(yaml);
        hashBySeriesKey.put(seriesKey, hash);
        definitionByHash.put(hash, new DefinitionRecord(yaml, options));
    }

    /**
     * Fecha um handle com checkpoint prévio. {@code NgrrdHandle.close()} já
     * força durabilidade internamente (materializa o CDP em progresso antes de
     * encerrar a thread do writer — ver {@code NgrrdWriter.close()}/{@code
     * checkpointAndForce()}), então o {@code checkpoint()} explícito aqui é
     * redundante em condições normais; mantido por clareza de intenção e como
     * rede de segurança. Chamado sempre com a entrada travada.
     */
    private void closeQuietly(String seriesKey, NgrrdHandle handle) {
        if (handle == null) {
            return;
        }
        try {
            handle.checkpoint();
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Falha no checkpoint final da série " + seriesKey + " antes de fechar", e);
        }
        try {
            handle.close();
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Falha ao fechar handle da série " + seriesKey, e);
        }
    }

    /** Definição cacheada (YAML + opções de abertura) para reabertura via {@link #reopenIfKnown}. */
    private record DefinitionRecord(String yaml, Ngrrd.OpenOptions options) {
    }

    /**
     * Entrada por série: o {@link ReentrantLock} vive e morre com ela — nunca é
     * compartilhado entre chaves nem sobrevive à remoção da entrada do mapa.
     * {@code handle} é {@code null} só na janela entre a criação da entrada
     * (via {@code computeIfAbsent}) e a conclusão da abertura sob o lock; todo
     * código que lê {@code handle} fora desse lock trata {@code null} como
     * "ainda não pronta" (equivalente a não aberta).
     */
    private static final class HandleEntry {
        private final ReentrantLock lock = new ReentrantLock();
        private volatile NgrrdHandle handle;
        private volatile long lastAccessMs;

        void touch(long now) {
            this.lastAccessMs = now;
        }
    }
}
