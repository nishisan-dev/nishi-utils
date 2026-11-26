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
import dev.nishisan.utils.oss.NgrrdHandle.CheckpointTiming;
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.NgrrdUri;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.format.DefinitionHash;
import dev.nishisan.utils.oss.writer.WriterAbandonedException;

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
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
 *
 * <h2>Séries sujas (8.14.0)</h2>
 *
 * <p>Cada entrada guarda {@code writeSeq} (escritas admitidas, incrementado sob o lock da série por
 * {@link SeriesAccess#recordWrite()}) e {@code checkpointedSeq} (a maior {@code writeSeq} já coberta
 * por um checkpoint concluído com sucesso). A série está suja enquanto {@code writeSeq >
 * checkpointedSeq}. Todo checkpoint feito por este registro (remoto via {@link
 * SeriesAccess#checkpointAsync()}, local via {@link #tryCheckpointAsync}, de fechamento e de migração)
 * captura a {@code writeSeq} sob o lock, no enfileiramento, e só a grava como {@code checkpointedSeq}
 * quando conclui com sucesso. Uma entrada nova nasce limpa: o fechamento faz checkpoint+close antes de
 * remover a entrada.</p>
 *
 * <h2>Índice incremental de séries sujas (8.15.0)</h2>
 *
 * <p>Com o checkpoint local ligado ({@link #trackDirtyIndex}, na montagem do nó), o registro mantém
 * {@code dirtyIndex}, um <strong>superconjunto</strong> das séries sujas, e {@link #dirtySeries()}/{@link
 * #dirtyCount()} deixam de varrer {@code entries}. Cada entrada tem a marca {@code indexed}, escrita só sob o
 * lock dela: {@link SeriesAccess#recordWrite()} liga a marca e insere a chave na primeira escrita depois da
 * última poda. A chave só sai do índice pelo helper único {@link #pruneIndex}, chamado sem nenhum lock de
 * entrada tomado: ele remove a chave e, sob o lock da entrada corrente, a devolve se ela estiver suja (ou
 * desliga a marca se estiver limpa). A poda roda quando o ciclo encontra a série limpa, ausente ou fechada
 * ({@link #tryCheckpointAsync}) e nos seis caminhos que tiram uma entrada do mapa ({@link #close()}, {@link
 * #discard}, {@link #closeIdle()}, {@link #evictIfOverLimit()}, {@link #closeForDeletion} e {@link
 * #markMigrating}). Nunca em {@code markCheckpointed}, que roda fora do lock e perderia uma escrita
 * concorrente. Por isso uma série checkpointada no ciclo N só sai do índice quando o ciclo N+1 a encontra
 * limpa ou quando ela fecha: {@link #dirtyCount()} é um limite superior atrasado em um ciclo.</p>
 *
 * <p>Com o checkpoint local desligado (o padrão), nenhum índice é mantido e as duas consultas varrem
 * {@code entries}, como antes.</p>
 */
public final class SeriesHandleRegistry implements Closeable {

    private SeriesLifecycleService lifecycle;
    public void lifecycle(SeriesLifecycleService value) { this.lifecycle = value; }
    public SeriesLifecycleService lifecycle() { return lifecycle; }
    public void invalidateOwnership(String key) { fireOwnershipChange(key); }
    /** Deterministic unique stripe ordering avoids cross-series deadlocks for grouped receipts. */
    public java.util.List<Object> operationLocks(java.util.Collection<String> keys) {
        return keys.stream().map(key -> Math.floorMod(key.hashCode(), operationLocks.length)).distinct().sorted()
                .map(index -> operationLocks[index]).toList();
    }

    private static final Logger LOGGER = Logger.getLogger(SeriesHandleRegistry.class.getName());

    /** Janela dos WARNINGs agrupados de falha de fechamento: no máximo um por janela, por registro. */
    private static final long CLOSE_FAILURE_LOG_INTERVAL_MS = 10_000L;

    /**
     * Espaçamento mínimo entre tentativas de fechar a mesma série nas varreduras ({@link #closeIdle()} e {@link
     * #evictIfOverLimit()}) depois de um close que falhou. Sem ele, k séries presas custariam k vezes o teto do
     * fechamento a cada varredura (e a cada abertura de outra série). Os caminhos explícitos ({@link #discard},
     * {@link #markMigrating}, {@link #close()}) não respeitam o backoff.
     */
    static final long CLOSE_RETRY_BACKOFF_MS = 60_000L;

    /**
     * Teto da espera, com a série travada, pelo checkpoint que o registro faz antes de fechar o handle ({@link
     * #closeQuietly}), no corte da migração ({@link #markMigrating}) e no snapshot da cópia online ({@link
     * #migrationSnapshot}). Igual ao teto do writer para os forces em voo no Shutdown ({@code
     * NgrrdWriter.SHUTDOWN_FORCE_WAIT_SECONDS}, 20 s): um force lento, mas que anda, termina dentro dele; um force
     * preso não segura o lock da série para sempre. Estourado, o fechamento segue para o {@code close()} do handle,
     * que aplica o teto do writer e lança {@link WriterAbandonedException} se o force continuar preso. No pior caso,
     * fechar uma série presa custa este teto mais o do writer.
     */
    static final Duration CLOSE_CHECKPOINT_WAIT = Duration.ofSeconds(20);

    /** Teto de chaves de cada LRU usado para classificar {@code NOT_OPEN} por causa. */
    static final int NOT_OPEN_CAUSE_LRU_LIMIT = 65_536;

    /** Teto de chaves do LRU de séries com writer abandonado à espera do primeiro checkpoint remoto. */
    static final int ABANDONED_WRITER_LRU_LIMIT = 65_536;

    /**
     * Causa de uma resposta {@code NOT_OPEN}. A classificação é aproximada: os conjuntos que a sustentam são LRUs
     * limitados e, estourado o teto, uma chave antiga cai em {@link #RESTART}.
     */
    public enum NotOpenCause {
        /** Definição desconhecida neste processo: reinício do nó ou série nunca aberta aqui. */
        RESTART("notOpen.restart"),
        /** A série foi fechada explicitamente por um cliente ({@code CLOSE}). */
        CLIENT_CLOSED("notOpen.clientClosed"),
        /** A série foi esquecida (origem) ou recebida por migração (destino) e ainda não foi reaberta. */
        MIGRATED("notOpen.migrated"),
        /** A definição é conhecida, mas a reabertura automática falhou. */
        REOPEN_FAILED("notOpen.reopenFailed"),
        /** A série está congelada por uma migração em curso. */
        FROZEN("notOpen.frozen");

        private final String metricKey;

        NotOpenCause(String metricKey) {
            this.metricKey = metricKey;
        }

        /** Chave em {@code lifecycleMetrics}. */
        public String metricKey() {
            return metricKey;
        }
    }

    private static Map<NotOpenCause, AtomicLong> newNotOpenCounters() {
        Map<NotOpenCause, AtomicLong> counters = new EnumMap<>(NotOpenCause.class);
        for (NotOpenCause cause : NotOpenCause.values()) {
            counters.put(cause, new AtomicLong());
        }
        return counters;
    }

    /** Conjunto de chaves com teto fixo e despejo LRU (ordem de acesso), sob lock próprio. */
    static final class BoundedKeySet {
        private final Object lock = new Object();
        private final LinkedHashMap<String, Boolean> keys;

        BoundedKeySet(int limit) {
            this.keys = new LinkedHashMap<>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > limit;
                }
            };
        }

        void add(String key) {
            synchronized (lock) {
                keys.put(key, Boolean.TRUE);
            }
        }

        /** Remove a chave; devolve {@code true} se ela estava no conjunto. */
        boolean remove(String key) {
            synchronized (lock) {
                return keys.remove(key) != null;
            }
        }

        boolean contains(String key) {
            synchronized (lock) {
                return keys.get(key) != null;
            }
        }

        int size() {
            synchronized (lock) {
                return keys.size();
            }
        }
    }

    private final BlobVolume volume;
    private final String volumeName;
    private final Duration idleTtl;
    /** Fator {@code k} do TTL por série ({@link IdleTtlPolicy}); 0 = só {@link #idleTtl}. */
    private final int idleTtlStepFactor;
    /**
     * Entradas abertas cujo TTL efetivo é maior que o global ({@code handles.idleTtlExtended}). Sobe na abertura e
     * desce em {@link #removeEntry}, ambos sob o lock da entrada: sem varredura.
     */
    private final AtomicLong idleTtlExtended = new AtomicLong();
    /** Handles fechados por ociosidade ({@code handles.idleClosed}). */
    private final AtomicLong idleClosed = new AtomicLong();
    /** Handles despejados por {@code maxOpenHandles} ({@code handles.evicted}). */
    private final AtomicLong evicted = new AtomicLong();
    /** Reaberturas automáticas que de fato abriram o handle ({@code handles.selfHealed}). */
    private final AtomicLong selfHealed = new AtomicLong();
    /** Entradas removidas por writer abandonado no fechamento ({@code handles.writerAbandoned}). */
    private final AtomicLong writerAbandoned = new AtomicLong();
    /**
     * Checkpoints/flushes remotos recusados por serem os primeiros depois do abandono do writer da série
     * ({@code handles.abandonedCheckpointRejected}).
     */
    private final AtomicLong abandonedCheckpointRejected = new AtomicLong();
    /**
     * Séries cujo writer foi abandonado no fechamento e que ainda não tiveram um checkpoint remoto recusado ({@link
     * #consumeAbandonedCheckpoint}). LRU limitado: estourado o teto, a chave mais antiga sai e o checkpoint seguinte
     * dela responde normalmente.
     */
    private final BoundedKeySet abandonedSinceCheckpoint = new BoundedKeySet(ABANDONED_WRITER_LRU_LIMIT);
    /** Espera efetiva pelo checkpoint de fechamento, em ns ({@link #CLOSE_CHECKPOINT_WAIT}); só testes a reduzem. */
    private volatile long closeCheckpointWaitNanos = CLOSE_CHECKPOINT_WAIT.toNanos();
    /**
     * Instante (ms do relógio) do último WARNING agrupado de falha de fechamento; {@code -1} = nunca. Sob {@link
     * #closeFailureLogLock}.
     */
    private long lastCloseFailureLogMs = -1;
    /** Falhas de fechamento logadas em FINE desde o último WARNING agrupado. Sob {@link #closeFailureLogLock}. */
    private long suppressedCloseFailures;
    /** Guarda {@link #lastCloseFailureLogMs} e {@link #suppressedCloseFailures}. */
    private final Object closeFailureLogLock = new Object();
    /** Respostas {@code NOT_OPEN} por causa ({@code notOpen.*}); a soma é o total de {@code NOT_OPEN}. */
    private final Map<NotOpenCause, AtomicLong> notOpenByCause = newNotOpenCounters();
    /** Séries fechadas explicitamente por um cliente ({@link #close}); LRU limitado, classificação aproximada. */
    private final BoundedKeySet recentlyClosedByClient = new BoundedKeySet(NOT_OPEN_CAUSE_LRU_LIMIT);
    /** Séries cuja imagem acabou de ser recebida por migração neste nó; LRU limitado, classificação aproximada. */
    private final BoundedKeySet recentlyMigratedIn = new BoundedKeySet(NOT_OPEN_CAUSE_LRU_LIMIT);
    /** Parses de {@code baseStepSec} feitos por este registro; um por hash de definição. */
    private final AtomicLong baseStepParses = new AtomicLong();
    private final int maxOpenHandles;
    /**
     * Stripes de operação. Criados por {@link dev.nishisan.utils.oss.cluster.rpc.CoordinationLocks#newStripe()}:
     * o {@code CoordinationLocks.acquire} trava direto o lock embutido em cada um, sem passar pelo mapa
     * fraco global. Nenhum código faz {@code synchronized} sobre eles; todo acesso é por
     * {@code CoordinationLocks.acquire}.
     */
    private final Object[] operationLocks = java.util.stream.IntStream.range(0, 256)
            .mapToObj(i -> dev.nishisan.utils.oss.cluster.rpc.CoordinationLocks.newStripe()).toArray();
    /** Lock identity shared by OPEN, metadata inspection and migration; use {@code CoordinationLocks.acquire}. */
    public Object operationLock(String key) { return operationLocks[Math.floorMod(key.hashCode(), operationLocks.length)]; }
    private final Clock clock;

    private final ConcurrentMap<String, HandleEntry> entries = new ConcurrentHashMap<>();
    /**
     * Índice incremental de séries sujas: superconjunto das sujas, mantido só com {@link #dirtyIndexTracked}.
     * Entra por {@link SeriesAccess#recordWrite()}, sai só por {@link #pruneIndex}. Ver o Javadoc da classe.
     */
    private final Set<String> dirtyIndex = ConcurrentHashMap.newKeySet();
    /** Liga o {@link #dirtyIndex}; definido na montagem do nó, antes da primeira abertura de série. */
    private volatile boolean dirtyIndexTracked;
    /**
     * Definição (hash do YAML) de cada série aberta neste processo e ainda não fechada pelo cliente nem
     * esquecida — é o que permite a {@link #reopenIfKnown} reabrir sem o cliente reenviar o YAML. A
     * PRESENÇA da chave aqui é também a autorização de reabertura automática: {@link #close(String)} e
     * {@link #forget(String)} removem a chave, e só um novo {@link #open} a devolve. Assim o fechamento
     * explícito não deixa marca nenhuma por série (issue #174: um conjunto de "fechadas pelo cliente"
     * crescia sem limite).
     */
    private final ConcurrentMap<String, String> hashBySeriesKey = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, DefinitionRecord> definitionByHash = new ConcurrentHashMap<>();
    private final Set<String> migrating = ConcurrentHashMap.newKeySet();
    private final Set<String> copying = ConcurrentHashMap.newKeySet();
    /**
     * Séries esquecidas por {@link #forget(String)} — a origem de uma migração concluída
     * ({@code MIGRATE_FINISH}) marca aqui a chave para fechar em definitivo a corrida "réplica local
     * atrasada": enquanto marcada, quem decide se um {@code OPEN} pode (re)criar a série neste nó não é
     * mais a réplica LOCAL do catálogo (que ainda pode dizer {@code ACTIVE(self)} por um instante depois
     * do FINISH) nem o {@code placementHint} do cliente, e sim uma confirmação forte do líder — ver
     * {@code StorageRequestHandler#ownership}. {@link #open} limpa a marca (a mesma reabertura
     * legítima que volta a cachear a definição), nunca {@link #reopenIfKnown} sozinho; e ela perde a
     * validade quando a réplica local converge para outro dono ({@link #pruneForgotten}).
     */
    private final Set<String> forgotten = ConcurrentHashMap.newKeySet();
    /** Ver {@link #addOwnershipChangeListener}. */
    private final List<Consumer<String>> ownershipChangeListeners = new CopyOnWriteArrayList<>();

    /** Registro sem extensão do TTL por série ({@code idleTtlStepFactor = 0}): só {@code idleTtl} vale. */
    public SeriesHandleRegistry(BlobVolume volume, String volumeName, Duration idleTtl, int maxOpenHandles,
            Clock clock) {
        this(volume, volumeName, idleTtl, 0, maxOpenHandles, clock);
    }

    /**
     * @param idleTtlStepFactor fator {@code k} de {@link IdleTtlPolicy#effectiveIdleTtlMs}: o TTL de cada série é
     *                          {@code max(idleTtl, k × baseStepSec)}; {@code 0} usa só {@code idleTtl}
     */
    public SeriesHandleRegistry(BlobVolume volume, String volumeName, Duration idleTtl, int idleTtlStepFactor,
            int maxOpenHandles, Clock clock) {
        if (idleTtlStepFactor < 0) {
            throw new IllegalArgumentException("idleTtlStepFactor deve ser >= 0: " + idleTtlStepFactor);
        }
        this.idleTtlStepFactor = idleTtlStepFactor;
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
     * Liga ou desliga o índice incremental de séries sujas. O nó liga quando o checkpoint local está ligado
     * ({@code ngrrd.checkpoint.enabled}), porque é o ciclo dele que poda o índice. Desligado (o padrão),
     * {@link SeriesAccess#recordWrite()} não toca índice nenhum e {@link #dirtySeries()}/{@link #dirtyCount()}
     * varrem as entradas.
     *
     * @throws IllegalStateException se já houver entrada aberta: as escritas anteriores não estariam no índice
     */
    void trackDirtyIndex(boolean enabled) {
        if (!entries.isEmpty()) {
            throw new IllegalStateException("trackDirtyIndex só pode ser definido antes da primeira abertura de série");
        }
        dirtyIndexTracked = enabled;
        dirtyIndex.clear();
    }

    /** Indica se o índice incremental de séries sujas está ligado (ver {@link #trackDirtyIndex}). */
    boolean dirtyIndexTracked() {
        return dirtyIndexTracked;
    }

    /**
     * Registra quem precisa saber que a posse local de uma série pode ter mudado — hoje, o cache de
     * confirmações do líder do {@code StorageRequestHandler} (issue #177), que descarta a entrada da
     * série. Disparado por {@link #beginMigrationCopy}, {@link #markMigrating}, {@link #clearMigrating},
     * {@link #forget} e {@link #discard} (e, por consequência, {@link #close(String)}), sempre, mesmo
     * quando a série não tinha handle aberto.
     *
     * <p><strong>Contrato:</strong> todo caminho futuro deste registro que mova a posse de uma série (início
     * ou fim de migração, esquecimento, substituição da imagem) precisa disparar este aviso — senão uma
     * confirmação em cache pode sobreviver à mudança por até o prazo do cache.</p>
     *
     * @param listener recebe o {@code seriesKey}; uma exceção dele é logada e não interrompe a operação
     */
    public void addOwnershipChangeListener(Consumer<String> listener) {
        ownershipChangeListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    private void fireOwnershipChange(String seriesKey) {
        for (Consumer<String> listener : ownershipChangeListeners) {
            try {
                listener.accept(seriesKey);
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "Falha num listener de mudança de posse da série " + seriesKey, e);
            }
        }
    }

    /**
     * Abre (ou confirma já aberta, atualizando {@code lastAccess}) a série e
     * cacheia sua definição — um novo {@code open} é o único jeito de reabrir
     * uma série {@link #close fechada pelo cliente} ou {@link #forget esquecida}.
     *
     * <p>A recusa por migração nunca fecha nem descarta um handle já aberto: se a marca chega entre a
     * checagem inicial e o lock da entrada, a entrada viva fica no mapa para {@link #markMigrating} ou para a
     * cópia online ({@link #beginMigrationCopy}) tratarem.</p>
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
                    // A migração começou entre a checagem fora do lock e aqui. Só sai do mapa a entrada vazia
                    // (criada por este open() ou por outro caminho que ainda não abriu o handle). Com handle
                    // aberto, a entrada fica para quem marcou a migração: markMigrating faz checkpoint, close e
                    // poda no caminho dele, e na cópia online (beginMigrationCopy) o handle segue gravável até
                    // o corte. Removê-la aqui vazaria o writer, esconderia a entrada do markMigrating (a escrita
                    // confirmada poderia não chegar à imagem) e deixaria reopenIfKnown abrir um segundo handle.
                    if (entry.handle == null) {
                        removeEntry(seriesKey, entry);
                    }
                    throw new IllegalStateException("MIGRATING");
                }
                if (entry.handle == null) {
                    try {
                        entry.handle = Ngrrd.open(volume, NgrrdUri.of(volumeName, seriesKey), yaml, options);
                    } catch (RuntimeException e) {
                        removeEntry(seriesKey, entry);
                        throw e;
                    }
                    applyIdleTtl(entry, cacheDefinition(seriesKey, yaml, options));
                }
                // Reabertura legítima: quem chama open() já passou pela checagem forte de dono em
                // StorageRequestHandler#ownership (isForgotten força placementStrong antes de chegar
                // aqui) — é o único lugar que limpa a marca de esquecida.
                forgotten.remove(seriesKey);
                recentlyClosedByClient.remove(seriesKey);
                recentlyMigratedIn.remove(seriesKey);
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
        Objects.requireNonNull(fn, "fn");
        return withSeries(seriesKey, access -> fn.apply(access.handle()));
    }

    /**
     * Como {@link #withHandle}, mas entrega um {@link SeriesAccess}: além do handle, registra escritas
     * admitidas ({@link SeriesAccess#recordWrite()}) e enfileira checkpoints que marcam a série como
     * limpa ao concluir ({@link SeriesAccess#checkpointAsync()}). Mesmas garantias de lock, mesma
     * renovação de {@code lastAccess}.
     *
     * @throws NullPointerException se {@code seriesKey} ou {@code fn} forem {@code null}
     * @return o resultado de {@code fn}, ou {@link Optional#empty()} se a série não está aberta
     */
    public <R> Optional<R> withSeries(String seriesKey, Function<SeriesAccess, R> fn) {
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
            return Optional.ofNullable(fn.apply(
                    new SeriesAccess(seriesKey, entry, dirtyIndexTracked ? dirtyIndex : null)));
        } finally {
            entry.lock.unlock();
        }
    }

    /**
     * Tenta enfileirar um checkpoint local da série, sem nunca esperar o lock dela: usa
     * {@link ReentrantLock#tryLock()} e <strong>não</strong> renova {@code lastAccess} (um checkpoint de
     * fundo não pode adiar o fechamento por ociosidade). Sob o lock, reconfere que a entrada ainda é a
     * corrente, que não está congelada por migração e que está suja; só então captura a
     * {@code writeSeq} e chama {@link NgrrdHandle#checkpointAsyncTimed()} (a medida de fila e de force vai
     * na future). O lock é solto antes de devolver: quem chama espera a future fora dele. Callbacks
     * encadeados na future podem rodar na thread do pool de force: nunca devem bloquear.
     *
     * <p>Com o índice de sujas ligado, {@link CheckpointAttempt.Outcome#CLEAN} e entrada ausente ou fechada
     * podam a chave ({@link #pruneIndex}) depois de soltar o lock. {@link CheckpointAttempt.Outcome#BUSY} não
     * poda, e {@link CheckpointAttempt.Outcome#UNAVAILABLE} por congelamento de migração também não: a
     * migração pode abortar e a série voltar a aceitar escrita, ainda suja.</p>
     *
     * @return o desfecho; a future só existe em {@link CheckpointAttempt.Outcome#STARTED}
     * @throws RuntimeException se o próprio enfileiramento falhar (raro: pool de writers recusando tarefa)
     */
    public CheckpointAttempt tryCheckpointAsync(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        HandleEntry entry = entries.get(seriesKey);
        if (entry == null) {
            pruneIndex(seriesKey);
            return CheckpointAttempt.UNAVAILABLE;
        }
        if (!entry.lock.tryLock()) {
            return CheckpointAttempt.BUSY;
        }
        CheckpointAttempt attempt;
        boolean prune = false;
        try {
            if (entries.get(seriesKey) != entry || entry.handle == null) {
                prune = true;
                attempt = CheckpointAttempt.UNAVAILABLE;
            } else if (isMigrationFrozen(seriesKey)) {
                // Congelada por migração conferida só aqui, sob o lock: é a checagem que vale contra um
                // markMigrating concorrente (que também trava a entrada antes de fechá-la).
                attempt = CheckpointAttempt.UNAVAILABLE;
            } else if (!entry.dirty()) {
                prune = true;
                attempt = CheckpointAttempt.CLEAN;
            } else {
                attempt = new CheckpointAttempt(CheckpointAttempt.Outcome.STARTED, checkpointAsyncTimedLocked(entry));
            }
        } finally {
            entry.lock.unlock();
        }
        if (prune) {
            pruneIndex(seriesKey);
        }
        return attempt;
    }

    /** Indica se a série está aberta e com escritas admitidas ainda não cobertas por checkpoint. */
    public boolean isDirty(String seriesKey) {
        HandleEntry entry = entries.get(seriesKey);
        return entry != null && entry.handle != null && entry.dirty();
    }

    /**
     * Instantâneo das séries candidatas a checkpoint. Com o índice de sujas ligado ({@link #trackDirtyIndex}),
     * é uma cópia do índice, sem varrer as entradas: um <strong>superconjunto</strong> das sujas, que pode
     * trazer séries já limpas (checkpointadas desde a última poda) ou já fechadas. Desligado, varre as
     * entradas e devolve exatamente as abertas e sujas agora (ver {@link #isDirty}).
     */
    public Set<String> dirtySeries() {
        if (dirtyIndexTracked) {
            return Set.copyOf(dirtyIndex);
        }
        return entries.entrySet().stream()
                .filter(entry -> entry.getValue().handle != null && entry.getValue().dirty())
                .map(Map.Entry::getKey)
                .collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Quantidade de séries sujas (instantâneo, sem lock). Com o índice de sujas ligado, é o tamanho do índice:
     * um limite superior atrasado em um ciclo (uma série checkpointada só sai quando o ciclo seguinte a
     * encontra limpa, ou quando fecha). Desligado, varre as entradas e conta as abertas e sujas agora.
     */
    public int dirtyCount() {
        if (dirtyIndexTracked) {
            return dirtyIndex.size();
        }
        return (int) entries.values().stream().filter(entry -> entry.handle != null && entry.dirty()).count();
    }

    /**
     * Helper único de poda do índice de sujas. Chamado <strong>sem</strong> nenhum lock de entrada tomado (o
     * registro nunca segura dois locks de entrada): remove a chave e, sob o lock da entrada corrente, a devolve
     * se a entrada estiver indexada e suja, ou desliga a marca {@code indexed} se estiver limpa. Uma escrita
     * que se indexou antes da remoção é vista na reconferência, sob o mesmo lock que ela usou; uma escrita
     * posterior encontra {@code indexed == false} e se insere sozinha. Nada se perde.
     *
     * <p>Usa {@code tryLock}: se a entrada corrente estiver ocupada, devolve a chave ao índice sem esperar (o
     * índice continua superconjunto, e o próximo ciclo poda de novo). Assim a poda nunca bloqueia a thread do
     * ciclo, a de status ({@link #closeIdle()}) nem quem fecha a série. No-op com o índice desligado. Visível
     * para testes.</p>
     */
    void pruneIndex(String seriesKey) {
        if (!dirtyIndexTracked) {
            return;
        }
        dirtyIndex.remove(seriesKey);
        HandleEntry current = entries.get(seriesKey);
        if (current == null) {
            return;
        }
        if (!current.lock.tryLock()) {
            dirtyIndex.add(seriesKey);
            return;
        }
        try {
            if (current.indexed) {
                if (current.dirty()) {
                    dirtyIndex.add(seriesKey);
                } else {
                    current.indexed = false;
                }
            }
        } finally {
            current.lock.unlock();
        }
    }

    /**
     * Enfileira o checkpoint com a entrada travada pelo chamador: captura a {@code writeSeq} agora e a
     * grava como {@code checkpointedSeq} só no sucesso. A future devolvida conclui depois dessa marcação.
     */
    private static CompletableFuture<Void> checkpointAsyncLocked(HandleEntry entry) {
        long seq = entry.writeSeq;
        return entry.handle.checkpointAsync().whenComplete((ignored, failure) -> {
            if (failure == null) {
                entry.markCheckpointed(seq);
            }
        });
    }

    /** Como {@link #checkpointAsyncLocked}, mas pelo {@link NgrrdHandle#checkpointAsyncTimed()}: a future traz a medida. */
    private static CompletableFuture<CheckpointTiming> checkpointAsyncTimedLocked(HandleEntry entry) {
        long seq = entry.writeSeq;
        return entry.handle.checkpointAsyncTimed().whenComplete((timing, failure) -> {
            if (failure == null) {
                entry.markCheckpointed(seq);
            }
        });
    }

    /**
     * Checkpoint com a entrada travada e espera limitada por {@link #CLOSE_CHECKPOINT_WAIT}: enfileira pelo {@link
     * #checkpointAsyncLocked} (que marca a série como limpa até a seq capturada, só no sucesso) e espera a future.
     * Nunca usa o {@code checkpoint()} síncrono do handle, que espera sem prazo e, com um force destacado preso,
     * não volta mais (o Sync encadeia no force em voo).
     *
     * @throws CheckpointWaitTimeoutException se o checkpoint não concluiu no teto; a future segue pendente e a série
     *                                        continua suja
     * @throws RuntimeException               a mesma falha que o checkpoint lançaria
     */
    private void awaitCheckpointLocked(String seriesKey, HandleEntry entry) {
        CompletableFuture<Void> checkpoint = checkpointAsyncLocked(entry);
        long waitNanos = closeCheckpointWaitNanos;
        try {
            checkpoint.get(waitNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            throw new CheckpointWaitTimeoutException("checkpoint da série " + seriesKey + " não concluiu em "
                    + TimeUnit.NANOSECONDS.toMillis(waitNanos) + " ms (force preso?)");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("checkpoint da série " + seriesKey + " falhou", cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("espera do checkpoint da série " + seriesKey + " interrompida", e);
        }
    }

    /** Reduz o teto da espera pelo checkpoint de fechamento ({@link #CLOSE_CHECKPOINT_WAIT}). Só para testes. */
    void closeCheckpointWaitForTests(Duration wait) {
        this.closeCheckpointWaitNanos = wait.toNanos();
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
     *
     * <p><strong>Reabertura automática NUNCA cria.</strong> O {@link DefinitionRecord} cacheado é
     * compartilhado por hash da definição YAML, não por {@code seriesKey} — duas séries com o mesmo
     * YAML dividem a mesma entrada em {@code definitionByHash}, então {@code createIfMissing} do
     * {@link DefinitionRecord} reflete apenas a ÚLTIMA série que passou por {@link #open} com aquele
     * YAML, não necessariamente esta. Por isso a reabertura aqui sempre força
     * {@code definition.options().withCreateIfMissing(false)}, ignorando o valor cacheado: se o objeto
     * físico sumiu entre o fechamento por ociosidade/LRU e esta tentativa de auto-cura, {@code
     * Ngrrd.open} lança {@link SeriesNotFoundException}, tratada aqui como {@link Optional#empty()}
     * (o chamador em {@code StorageRequestHandler} responde {@code NOT_OPEN}) em vez de propagada —
     * só um novo {@link #open} explícito do cliente, que decide {@code createIfMissing} por si,
     * recria a série.</p>
     */
    public Optional<NgrrdHandle> reopenIfKnown(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        if (isMigrationFrozen(seriesKey)) {
            return Optional.empty();
        }
        if (!hashBySeriesKey.containsKey(seriesKey) && !entries.containsKey(seriesKey)) {
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
                // A definição é lida sob o lock: um close()/forget() concorrente a remove ANTES de
                // procurar a entrada, então ou ele já a removeu (e aqui não se reabre) ou ainda vai achar
                // e fechar a entrada que este método abrir — a série nunca termina aberta depois dele. Ler
                // aqui (e não antes do laço) também garante reabrir com a definição corrente, não com a
                // de um open() anterior substituída no meio do caminho por outro YAML.
                String hash = hashBySeriesKey.get(seriesKey);
                if (isMigrationFrozen(seriesKey) || hash == null) {
                    // Só remove se ainda vazia E ainda a corrente — nunca apaga um handle que outra
                    // thread já tenha aberto de verdade nesta mesma entrada nesse meio-tempo (o bug
                    // que o Refuter pegou: remover incondicionalmente fora do lock podia derrubar do
                    // mapa uma entrada que um open() concorrente acabara de popular, vazando o handle).
                    if (entry.handle == null) {
                        removeEntry(seriesKey, entry);
                    }
                    return Optional.empty();
                }
                if (entry.handle == null) {
                    DefinitionRecord definition = definitionByHash.get(hash);
                    if (definition == null) {
                        removeEntry(seriesKey, entry);
                        return Optional.empty();
                    }
                    try {
                        entry.handle = Ngrrd.open(volume, NgrrdUri.of(volumeName, seriesKey),
                                definition.yaml(), definition.options().withCreateIfMissing(false));
                        applyIdleTtl(entry, definition);
                        selfHealed.incrementAndGet();
                    } catch (SeriesNotFoundException e) {
                        // Reabertura automática nunca cria: o objeto sumiu entre o fechamento por
                        // ociosidade/LRU e esta tentativa de auto-cura — devolve vazio (o chamador decide
                        // NOT_OPEN) em vez de propagar; só um novo open() explícito do cliente recria.
                        removeEntry(seriesKey, entry);
                        return Optional.empty();
                    } catch (RuntimeException e) {
                        removeEntry(seriesKey, entry);
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
     * Libera a referência local da série (CLOSE do cliente): descarta a
     * definição em cache — {@link #reopenIfKnown} deixa de reabri-la sozinha,
     * mesmo que o handle já tenha sido fechado antes por ociosidade/LRU — e faz
     * checkpoint+close do handle, se aberto. Idempotente.
     */
    public void close(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        // Primeiro a definição, depois a entrada: ver a reconferência sob o lock em reopenIfKnown.
        hashBySeriesKey.remove(seriesKey);
        recentlyClosedByClient.add(seriesKey);
        discard(seriesKey);
    }

    /**
     * Registra que a imagem da série acabou de ser recebida por migração neste nó (destino, no
     * {@code MIGRATE_COMMIT}): enquanto ninguém a reabrir, um {@code NOT_OPEN} é classificado como
     * {@link NotOpenCause#MIGRATED}. LRU limitado; {@link #open} remove a chave.
     */
    public void noteMigratedIn(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        recentlyMigratedIn.add(seriesKey);
    }

    /**
     * Classifica por que a série responde {@code NOT_OPEN} agora. Ordem: congelada, esquecida/migrada, fechada por
     * cliente, definição conhecida (reabertura falhou), senão {@link NotOpenCause#RESTART}. Aproximada: os LRUs
     * têm teto de {@value #NOT_OPEN_CAUSE_LRU_LIMIT} chaves e, estourado, a causa cai em {@code RESTART}.
     */
    public NotOpenCause notOpenCause(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        if (isMigrationFrozen(seriesKey)) {
            return NotOpenCause.FROZEN;
        }
        if (forgotten.contains(seriesKey) || recentlyMigratedIn.contains(seriesKey)) {
            return NotOpenCause.MIGRATED;
        }
        if (recentlyClosedByClient.contains(seriesKey)) {
            return NotOpenCause.CLIENT_CLOSED;
        }
        if (hashBySeriesKey.containsKey(seriesKey)) {
            return NotOpenCause.REOPEN_FAILED;
        }
        return NotOpenCause.RESTART;
    }

    /** Classifica a causa do {@code NOT_OPEN} e a conta em {@code notOpen.<causa>}. */
    public NotOpenCause recordNotOpen(String seriesKey) {
        NotOpenCause cause = notOpenCause(seriesKey);
        notOpenByCause.get(cause).incrementAndGet();
        return cause;
    }

    /**
     * Consome a marca de writer abandonado da série, para o primeiro {@code CHECKPOINT}/{@code FLUSH} remoto
     * depois do abandono: devolve {@code true} (e conta em {@code handles.abandonedCheckpointRejected}) se a série
     * estava marcada, e a marca sai, de modo que o checkpoint seguinte responde normalmente. Quem recebe
     * {@code true} tem de responder falha: amostras confirmadas desde o último checkpoint concluído podem ter sido
     * perdidas no abandono, e reabrir a série do disco e responder OK seria um falso positivo de durabilidade.
     *
     * <p>Chame com o lock da série tomado (dentro de {@link #withSeries}): a marca é gravada sob o lock da entrada
     * abandonada, antes de ela sair do mapa, e assim um checkpoint concorrente ao fechamento sempre a enxerga.</p>
     */
    public boolean consumeAbandonedCheckpoint(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        if (!abandonedSinceCheckpoint.remove(seriesKey)) {
            return false;
        }
        abandonedCheckpointRejected.incrementAndGet();
        return true;
    }

    /**
     * Libera a referência local da série SEM descartar a definição em cache: checkpoint+close do
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
        fireOwnershipChange(seriesKey);
        HandleEntry entry = entries.get(seriesKey);
        if (entry == null) {
            return;
        }
        boolean removed = false;
        WriterAbandonedException abandoned = null;
        entry.lock.lock();
        try {
            if (entries.get(seriesKey) == entry) {
                abandoned = closeQuietly(seriesKey, entry, true, CloseLog.DIRECT);
                removed = removeEntry(seriesKey, entry);
            }
        } finally {
            entry.lock.unlock();
        }
        if (removed) {
            pruneIndex(seriesKey);
        }
        rethrowAbandoned(abandoned);
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
    /** Remove the handle only after deletion shutdown is confirmed; never checkpoint it. */
    public void closeForDeletion(String seriesKey) {
        HandleEntry entry = entries.get(seriesKey);
        if (entry != null) {
            boolean removed = false;
            entry.lock.lock();
            try {
                if (entries.get(seriesKey) == entry) {
                    if (entry.handle != null) entry.handle.closeForDeletion();
                    removed = removeEntry(seriesKey, entry);
                }
            } finally { entry.lock.unlock(); }
            if (removed) {
                pruneIndex(seriesKey);
            }
        }
        hashBySeriesKey.remove(seriesKey);
        // A série apagada não tem mais amostras a proteger: uma recriação com a mesma chave não herda a recusa.
        abandonedSinceCheckpoint.remove(seriesKey);
        forgotten.add(seriesKey);
        fireOwnershipChange(seriesKey);
    }

    public void forget(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        // isForgotten é consultada por StorageRequestHandler#ownership para nunca confiar na réplica
        // local (nem no hint) de dono enquanto esta série não for reaberta com confirmação forte do
        // líder ou a réplica local não convergir para outro dono (ver pruneForgotten).
        forgotten.add(seriesKey);
        fireOwnershipChange(seriesKey);
        // Mesmo bloqueio de reabertura automática de close(): a definição sai antes da entrada e é
        // reconferida DENTRO do lock por reopenIfKnown, então nenhuma reabertura concorrente escapa.
        // Só o vínculo série→hash sai — definitionByHash é compartilhado por todas as séries do mesmo
        // YAML e nunca pode ser removido por uma delas.
        hashBySeriesKey.remove(seriesKey);
        discard(seriesKey);
    }

    /**
     * Indica se {@code seriesKey} está {@link #forget esquecida} neste nó. A marca sai com {@link #open}
     * (uma reabertura legítima, já validada por confirmação forte do dono) ou quando a réplica local do
     * catálogo converge para outro dono ({@link #dropForgotten}/{@link #pruneForgotten}).
     */
    public boolean isForgotten(String seriesKey) {
        return forgotten.contains(seriesKey);
    }

    /**
     * Descarta a marca de {@link #forget esquecida} de {@code seriesKey}, sem devolver a definição nem
     * reabrir nada. Para quem já viu a réplica local do catálogo apontar outro dono: a marca só existe
     * para não confiar numa réplica que ainda diga {@code ACTIVE(self)} logo após o {@code FINISH}.
     * Idempotente.
     */
    public void dropForgotten(String seriesKey) {
        forgotten.remove(seriesKey);
    }

    /**
     * Descarta as marcas de {@link #forget esquecida} para as quais {@code ownedElsewhere} responde
     * {@code true} (tipicamente: a réplica local do catálogo já mostra outro dono). Sem esta varredura,
     * uma série migrada para fora e nunca mais pedida a este nó deixaria a marca em memória para sempre
     * (issue #174).
     *
     * @return quantas marcas foram descartadas
     */
    public int pruneForgotten(Predicate<String> ownedElsewhere) {
        Objects.requireNonNull(ownedElsewhere, "ownedElsewhere");
        int pruned = 0;
        for (String seriesKey : forgotten) {
            if (ownedElsewhere.test(seriesKey) && forgotten.remove(seriesKey)) {
                pruned++;
            }
        }
        return pruned;
    }

    /** Quantidade de séries marcadas como {@link #forget esquecidas} agora. */
    public int forgottenCount() {
        return forgotten.size();
    }

    /**
     * Fecha handles ociosos há mais do TTL efetivo da própria série
     * ({@code max(idleTtl, k × baseStepSec)}, ver {@link IdleTtlPolicy}); uma entrada sem TTL próprio cai no
     * {@code idleTtl} global. O despejo por {@code maxOpenHandles} não muda. Candidatos são
     * escolhidos por um snapshot ordenado por {@code lastAccess}, sem lock;
     * cada um só é fechado se {@link ReentrantLock#tryLock()} conseguir a
     * entrada na hora — se estiver ocupada (ex.: um {@code checkpoint} em
     * andamento via {@link #withHandle}), pula para a próxima em vez de
     * bloquear. Retorna quantos foram fechados de fato.
     */
    public int closeIdle() {
        long now = clock.millis();
        long globalTtlMillis = idleTtl.toMillis();
        int closedCount = 0;
        for (AccessSnapshot candidate : snapshotByLastAccess()) {
            String seriesKey = candidate.seriesKey();
            HandleEntry entry = candidate.entry();
            if (now - entry.lastAccessMs < entry.effectiveIdleTtlMs(globalTtlMillis)) {
                continue;
            }
            if (!entry.lock.tryLock()) {
                continue;
            }
            try {
                if (entries.get(seriesKey) != entry || entry.handle == null) {
                    continue;
                }
                if (now - entry.lastAccessMs < entry.effectiveIdleTtlMs(globalTtlMillis)) {
                    continue;
                }
                if (now < entry.closeRetryNotBeforeMs) {
                    // O close desta série falhou há pouco: espera o backoff em vez de pagar o teto de novo.
                    continue;
                }
                try {
                    if (closeQuietly(seriesKey, entry, true, CloseLog.GROUPED) == null) {
                        idleClosed.incrementAndGet();
                        closedCount++;
                    }
                } catch (RuntimeException e) {
                    // Falha de UMA série não aborta a varredura: a entrada fica e só volta a ser tentada depois do
                    // backoff.
                    entry.closeRetryNotBeforeMs = clock.millis() + CLOSE_RETRY_BACKOFF_MS;
                    continue;
                }
                removeEntry(seriesKey, entry);
            } finally {
                entry.lock.unlock();
            }
            // Só chega aqui quem fechou e removeu a entrada (os demais saem por continue).
            pruneIndex(seriesKey);
        }
        flushSuppressedCloseFailures();
        return closedCount;
    }

    /**
     * Fecha o(s) handle(s) menos recentemente acessado(s) até respeitar
     * {@code maxOpenHandles}. Mesmo protocolo não bloqueante de
     * {@link #closeIdle()}: um candidato ocupado é pulado, nunca esperado.
     */
    public void evictIfOverLimit() {
        // Entradas cujo close falhou nesta chamada: continuam no mapa, mas não contam para o limite nem são
        // tentadas de novo. Sem isso, uma série presa faria o despejo cair na série recém-aberta (a mais nova).
        Set<HandleEntry> failed = new HashSet<>();
        while (entries.size() - failed.size() > maxOpenHandles) {
            boolean evictedOne = false;
            for (AccessSnapshot candidate : snapshotByLastAccess()) {
                if (entries.size() - failed.size() <= maxOpenHandles) {
                    return;
                }
                String seriesKey = candidate.seriesKey();
                HandleEntry entry = candidate.entry();
                if (failed.contains(entry) || !entry.lock.tryLock()) {
                    continue;
                }
                try {
                    if (entries.get(seriesKey) != entry || entry.handle == null) {
                        continue;
                    }
                    long now = clock.millis();
                    if (now < entry.closeRetryNotBeforeMs) {
                        // Em backoff depois de um close que falhou: conta como falha desta chamada, sem nova tentativa.
                        failed.add(entry);
                        continue;
                    }
                    try {
                        if (closeQuietly(seriesKey, entry, true, CloseLog.GROUPED) == null) {
                            evicted.incrementAndGet();
                        }
                    } catch (RuntimeException e) {
                        // Esta roda dentro do open de OUTRA série: a falha desta não pode se propagar a ele.
                        entry.closeRetryNotBeforeMs = clock.millis() + CLOSE_RETRY_BACKOFF_MS;
                        failed.add(entry);
                        continue;
                    }
                    removeEntry(seriesKey, entry);
                    evictedOne = true;
                } finally {
                    entry.lock.unlock();
                }
                // Só chega aqui quem fechou e removeu a entrada (os demais saem por continue).
                pruneIndex(seriesKey);
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
     *
     * <p>A espera pelo checkpoint do corte tem o teto {@link #CLOSE_CHECKPOINT_WAIT}. Uma falha do checkpoint
     * mantém a entrada (o handle segue disponível para o aborto). Estourado o teto (force preso), a migração também
     * aborta, mas o handle é fechado antes: com o force ainda preso, o {@code close()} lança {@link
     * WriterAbandonedException}, a entrada sai como abandonada e é essa exceção que volta; se o {@code close()}
     * concluir, a entrada sai normalmente e volta a {@link CheckpointWaitTimeoutException}.</p>
     */
    public void markMigrating(String seriesKey) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        migrating.add(seriesKey);
        copying.remove(seriesKey);
        fireOwnershipChange(seriesKey);
        HandleEntry entry = entries.get(seriesKey);
        if (entry == null) {
            return;
        }
        boolean removed = false;
        WriterAbandonedException abandoned = null;
        CheckpointWaitTimeoutException timedOut = null;
        entry.lock.lock();
        try {
            // Uma escrita confirmada precisa chegar à imagem antes de a migração fechar o writer.
            // Um checkpoint que falha deixa o handle disponível para abortar ou tentar de novo com segurança.
            if (entries.get(seriesKey) == entry && entry.handle != null) {
                try {
                    awaitCheckpointLocked(seriesKey, entry);
                } catch (CheckpointWaitTimeoutException e) {
                    // Force preso: segue para o close(), que aplica o teto do writer; a migração aborta de todo jeito.
                    timedOut = e;
                }
            }
            if (entries.get(seriesKey) == entry) {
                // O checkpoint do corte já foi feito (ou estourou o teto): o close() vai direto, sem nova espera.
                abandoned = closeQuietly(seriesKey, entry, false, CloseLog.DIRECT);
                removed = removeEntry(seriesKey, entry);
            }
        } finally {
            entry.lock.unlock();
        }
        if (removed) {
            pruneIndex(seriesKey);
        }
        if (abandoned != null && timedOut != null) {
            // Estourou o teto do checkpoint e o close abandonou o writer: o abandono volta, sem perder o estouro.
            abandoned.addSuppressed(timedOut);
        }
        rethrowAbandoned(abandoned);
        if (timedOut != null) {
            throw timedOut;
        }
    }

    /** Remove a marca de migração, permitindo {@link #open} novamente. */
    public void clearMigrating(String seriesKey) {
        migrating.remove(seriesKey);
        copying.remove(seriesKey);
        fireOwnershipChange(seriesKey);
    }

    /** Marca uma cópia online; handles existentes de mesma geometria continuam graváveis até o {@link #markMigrating}. */
    public void beginMigrationCopy(String seriesKey) {
        migrating.add(seriesKey);
        copying.add(seriesKey);
        fireOwnershipChange(seriesKey);
    }

    /** Verdadeiro só durante a troca curta de dono, não na cópia online inicial. */
    public boolean isMigrationFrozen(String seriesKey) {
        return migrating.contains(seriesKey) && !copying.contains(seriesKey);
    }

    /** Indica se esta origem ainda aceita escritas enquanto copia uma imagem. */
    public boolean isCopying(String seriesKey) { return copying.contains(seriesKey); }

    /**
     * Faz o checkpoint e o snapshot de uma série sob o lock de escrita da entrada e libera os writers em seguida.
     *
     * <p>A espera pelo checkpoint tem o teto {@link #CLOSE_CHECKPOINT_WAIT}: estourado, lança {@link
     * CheckpointWaitTimeoutException} (a migração aborta) e mantém o handle; quem fecha a série presa é o caminho de
     * fechamento.</p>
     */
    public byte[] migrationSnapshot(String seriesKey, java.util.function.Supplier<byte[]> image) {
        for (;;) {
            HandleEntry entry = entries.computeIfAbsent(seriesKey, key -> new HandleEntry());
            entry.lock.lock();
            try {
                if (entries.get(seriesKey) != entry) { continue; }
                if (entry.handle != null) { awaitCheckpointLocked(seriesKey, entry); }
                return image.get();
            } finally {
                if (entry.handle == null) { removeEntry(seriesKey, entry); }
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

    /**
     * Fecha (checkpoint+close) todos os handles abertos, mesmo que alguns falhem.
     *
     * @throws IllegalStateException no fim, se alguma série falhou: agrega como {@code suppressed} as falhas comuns
     *                               (a entrada fica), os abandonos de writer ({@link WriterAbandonedException}; a
     *                               entrada sai) e a falha do journal
     */
    @Override
    public void close() {
        RuntimeException failure = null;
        for (String seriesKey : List.copyOf(entries.keySet())) {
            HandleEntry entry = entries.get(seriesKey);
            if (entry == null) {
                continue;
            }
            boolean removed = false;
            entry.lock.lock();
            try {
                if (entries.get(seriesKey) == entry && entry.handle != null) {
                    WriterAbandonedException abandoned = closeQuietly(seriesKey, entry, true, CloseLog.GROUPED);
                    removed = removeEntry(seriesKey, entry);
                    if (abandoned != null) {
                        // A entrada abandonada já saiu (SEVERE e métrica em closeQuietly); o abandono volta também
                        // no agregado, para quem fecha o registro saber que houve perda possível.
                        if (failure == null) {
                            failure = new IllegalStateException("Falha ao fechar uma ou mais séries do registry");
                        }
                        failure.addSuppressed(abandoned);
                    }
                }
            } catch (RuntimeException e) {
                // Continua fechando as demais séries; as falhas voltam agregadas no fim.
                if (failure == null) {
                    failure = new IllegalStateException("Falha ao fechar uma ou mais séries do registry");
                }
                failure.addSuppressed(e);
            } finally {
                entry.lock.unlock();
            }
            if (removed) {
                pruneIndex(seriesKey);
            }
        }
        if (lifecycle != null) {
            try { lifecycle.journal().close(); }
            catch (java.io.IOException e) {
                java.io.UncheckedIOException io = new java.io.UncheckedIOException(e);
                if (failure != null) {
                    failure.addSuppressed(io);
                } else {
                    throw io;
                }
            }
        }
        if (failure != null) {
            throw failure;
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

    /**
     * Guarda a definição da série e devolve o registro corrente do hash. O {@code baseStepSec} é calculado uma
     * única vez por hash: um segundo {@code open} do mesmo YAML não reparseia. As opções continuam sendo as da
     * última abertura; como o registro é imutável, uma opção diferente troca o registro reaproveitando o
     * {@code baseStepSec} já calculado.
     */
    private DefinitionRecord cacheDefinition(String seriesKey, String yaml, Ngrrd.OpenOptions options) {
        String hash = DefinitionHash.hex(yaml);
        hashBySeriesKey.put(seriesKey, hash);
        DefinitionRecord record = definitionByHash.computeIfAbsent(hash,
                h -> new DefinitionRecord(yaml, options, parseBaseStepSec(yaml)));
        if (!record.options().equals(options)) {
            record = definitionByHash.compute(hash, (h, current) -> current == null
                    ? new DefinitionRecord(yaml, options, parseBaseStepSec(yaml))
                    : new DefinitionRecord(current.yaml(), options, current.baseStepSec()));
        }
        return record;
    }

    /**
     * {@code spec.time.baseStepSec} da definição, ou {@code 0} se não puder ser lido (sem extensão do TTL). O YAML
     * já foi aceito por {@link Ngrrd#open}, então a falha aqui não é esperada.
     */
    private int parseBaseStepSec(String yaml) {
        baseStepParses.incrementAndGet();
        try {
            return NgrrdYamlLoader.parse(yaml, System::getenv).spec().time().baseStepSec();
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING,
                    "Não foi possível ler spec.time.baseStepSec; o TTL de ociosidade fica no global", e);
            return 0;
        }
    }

    /** Número de parses de {@code baseStepSec} feitos por este registro (um por hash de definição). */
    long baseStepParseCount() {
        return baseStepParses.get();
    }

    /**
     * Preenche {@link HandleEntry#idleTtlMs} de uma entrada recém-aberta pelo helper único e mantém o contador
     * {@code handles.idleTtlExtended}. Chamado sob o lock da entrada, logo depois de abrir o handle.
     */
    private void applyIdleTtl(HandleEntry entry, DefinitionRecord definition) {
        entry.idleTtlMs = IdleTtlPolicy.effectiveIdleTtlMs(idleTtl, idleTtlStepFactor, definition.baseStepSec());
        if (entry.idleTtlMs > idleTtl.toMillis() && !entry.extendedCounted) {
            entry.extendedCounted = true;
            idleTtlExtended.incrementAndGet();
        }
    }

    /**
     * Único ponto que tira uma entrada do mapa: mantém o contador {@code handles.idleTtlExtended}. Chamado sob o
     * lock da entrada.
     */
    private boolean removeEntry(String seriesKey, HandleEntry entry) {
        boolean removed = entries.remove(seriesKey, entry);
        if (removed && entry.extendedCounted) {
            entry.extendedCounted = false;
            idleTtlExtended.decrementAndGet();
        }
        return removed;
    }

    /**
     * Métricas de ciclo de vida dos handles, para {@code NodeMetricsSnapshot.lifecycleMetrics}:
     * {@code handles.idleTtlExtended} = entradas abertas cujo TTL efetivo é maior que o global;
     * {@code handles.idleClosed}, {@code handles.evicted} e {@code handles.selfHealed} = churn de fechar e reabrir;
     * {@code handles.writerAbandoned} = entradas removidas porque o writer foi abandonado no fechamento (force preso);
     * {@code handles.abandonedCheckpointRejected} = checkpoints/flushes remotos recusados por serem os primeiros
     * depois de um abandono;
     * {@code notOpen.<causa>} = respostas {@code NOT_OPEN} por {@link NotOpenCause}.
     */
    public Map<String, Long> handleMetrics() {
        Map<String, Long> metrics = new LinkedHashMap<>();
        metrics.put("handles.idleTtlExtended", idleTtlExtended.get());
        metrics.put("handles.idleClosed", idleClosed.get());
        metrics.put("handles.evicted", evicted.get());
        metrics.put("handles.selfHealed", selfHealed.get());
        metrics.put("handles.writerAbandoned", writerAbandoned.get());
        metrics.put("handles.abandonedCheckpointRejected", abandonedCheckpointRejected.get());
        for (NotOpenCause cause : NotOpenCause.values()) {
            metrics.put(cause.metricKey(), notOpenByCause.get(cause).get());
        }
        return metrics;
    }

    /** Tamanho atual dos LRUs de causa (fechadas por cliente, recebidas por migração); para testes. */
    int[] notOpenCauseSetSizes() {
        return new int[] {recentlyClosedByClient.size(), recentlyMigratedIn.size()};
    }

    /**
     * Fecha um handle com checkpoint prévio. {@code NgrrdHandle.close()} já
     * força durabilidade internamente (materializa o CDP em progresso antes de
     * encerrar a thread do writer — ver {@code NgrrdWriter.close()}/{@code
     * checkpointAndForce()}), então o checkpoint explícito aqui é
     * redundante em condições normais; mantido porque marca a série como limpa e porque, ao contrário do
     * checkpoint final do {@code close()} (que o writer só registra no stderr), uma falha dele aparece no log do
     * registro. Chamado sempre com a entrada travada.
     *
     * <p>A espera pelo checkpoint prévio tem o teto {@link #CLOSE_CHECKPOINT_WAIT}: com um force destacado preso, o
     * Sync encadeia no force em voo e nunca concluiria. Estourado o teto, ou falhando o checkpoint, a falha só é
     * logada: quem pediu o fechamento — inclusive um {@code CLOSE} remoto — não recebe o erro, e o {@code close()}
     * do handle segue, com o teto do writer.</p>
     *
     * <p>Se o {@code close()} lançar {@link WriterAbandonedException} (force preso além do teto do writer), o
     * writer não volta mais: a exceção é pegajosa e a entrada nunca fecharia. Aqui ela é registrada (SEVERE,
     * marcador {@code NGRRD_WRITER_ABANDONED}), contada em {@code handles.writerAbandoned} e
     * <strong>devolvida</strong> (sem lançar), para o chamador remover a entrada pelo caminho normal
     * ({@link #removeEntry}) sem marcar a série como limpa; a próxima abertura reabre do disco. Qualquer outra
     * falha do {@code close()} é logada (ver {@code log}) e propagada, e a entrada permanece.</p>
     *
     * @param checkpointFirst {@code false} quando o chamador já fez o checkpoint (ou já estourou o teto dele), como
     *                        o corte da migração: o {@code close()} vai direto, sem uma segunda espera
     * @param log             como logar as falhas de checkpoint e de fechamento: agrupadas por janela nas varreduras
     *                        e no fechamento do registro, diretas nos caminhos explícitos de uma série
     * @return {@code null} se fechou normalmente (ou não havia handle); a exceção de abandono, se for o caso
     */
    private WriterAbandonedException closeQuietly(String seriesKey, HandleEntry entry, boolean checkpointFirst,
            CloseLog log) {
        NgrrdHandle handle = entry.handle;
        if (handle == null) {
            return null;
        }
        if (checkpointFirst) {
            try {
                awaitCheckpointLocked(seriesKey, entry);
            } catch (RuntimeException e) {
                logCloseFailure(log, "Falha no checkpoint final da série " + seriesKey + " antes de fechar", e);
            }
        }
        try {
            handle.close();
            return null;
        } catch (WriterAbandonedException e) {
            writerAbandoned.incrementAndGet();
            // Marcada antes de a entrada sair (ainda sob o lock dela): um checkpoint remoto que esperava esse lock
            // reconfere a marca sob o lock da entrada nova e não escapa da recusa.
            abandonedSinceCheckpoint.add(seriesKey);
            LOGGER.log(Level.SEVERE, "NGRRD_WRITER_ABANDONED seriesKey=" + seriesKey
                    + " amostras desde o último checkpoint podem ter sido perdidas; a entrada foi removida", e);
            return e;
        } catch (RuntimeException e) {
            logCloseFailure(log, "Falha ao fechar handle da série " + seriesKey, e);
            throw e;
        }
    }

    /** Como {@link #closeQuietly} loga as falhas de checkpoint e de fechamento de uma série. */
    private enum CloseLog {
        /**
         * Varreduras ({@link #closeIdle()}, {@link #evictIfOverLimit()}) e {@link #close()}: no máximo um WARNING por
         * janela, com a contagem das suprimidas (ver {@link #logCloseFailure}).
         */
        GROUPED,
        /** Caminhos explícitos de uma série ({@link #discard}, {@link #markMigrating}): sempre WARNING. */
        DIRECT
    }

    /** Lança o abandono devolvido por {@link #closeQuietly}, depois de a entrada sair e do unlock. */
    private static void rethrowAbandoned(WriterAbandonedException abandoned) {
        if (abandoned != null) {
            throw abandoned;
        }
    }

    /**
     * Loga uma falha de checkpoint ou de fechamento de uma série. {@link CloseLog#DIRECT} sai sempre em WARNING. Em
     * {@link CloseLog#GROUPED}, sai no máximo um WARNING (com stack) a cada {@link #CLOSE_FAILURE_LOG_INTERVAL_MS}
     * para todo o registro; as demais da janela saem em FINE e são contadas, e o WARNING seguinte diz quantas foram
     * suprimidas desde o anterior. Assim, com muitas séries presas falhando a cada varredura, o log não inunda e
     * as que não aparecem por nome continuam visíveis na contagem.
     */
    private void logCloseFailure(CloseLog log, String message, RuntimeException failure) {
        if (log == CloseLog.DIRECT) {
            LOGGER.log(Level.WARNING, message, failure);
            return;
        }
        long now = clock.millis();
        long suppressed;
        long sinceLastMs;
        synchronized (closeFailureLogLock) {
            if (lastCloseFailureLogMs >= 0 && now - lastCloseFailureLogMs < CLOSE_FAILURE_LOG_INTERVAL_MS) {
                suppressedCloseFailures++;
                suppressed = -1;
                sinceLastMs = 0;
            } else {
                suppressed = suppressedCloseFailures;
                sinceLastMs = lastCloseFailureLogMs >= 0 ? now - lastCloseFailureLogMs : 0;
                suppressedCloseFailures = 0;
                lastCloseFailureLogMs = now;
            }
        }
        if (suppressed < 0) {
            LOGGER.log(Level.FINE, message, failure);
        } else if (suppressed > 0) {
            LOGGER.log(Level.WARNING, message + " (" + suppressed + " falhas de fechamento suprimidas nos últimos "
                    + TimeUnit.MILLISECONDS.toSeconds(sinceLastMs) + " s)", failure);
        } else {
            LOGGER.log(Level.WARNING, message, failure);
        }
    }

    /**
     * Emite o WARNING das falhas de fechamento ainda suprimidas quando a janela de log já venceu e nenhuma nova falha
     * apareceu para carregar a contagem. Chamado no fim de cada {@link #closeIdle()}, para que a contagem das
     * suprimidas não se perca quando as séries presas entram em backoff e a varredura seguinte passa sem falhar.
     */
    private void flushSuppressedCloseFailures() {
        long now = clock.millis();
        long suppressed;
        long sinceLastMs;
        synchronized (closeFailureLogLock) {
            if (suppressedCloseFailures == 0 || lastCloseFailureLogMs < 0
                    || now - lastCloseFailureLogMs < CLOSE_FAILURE_LOG_INTERVAL_MS) {
                return;
            }
            suppressed = suppressedCloseFailures;
            sinceLastMs = now - lastCloseFailureLogMs;
            suppressedCloseFailures = 0;
            lastCloseFailureLogMs = now;
        }
        LOGGER.log(Level.WARNING, suppressed + " falhas de fechamento suprimidas nos últimos "
                + TimeUnit.MILLISECONDS.toSeconds(sinceLastMs) + " s");
    }

    /**
     * O checkpoint feito pelo registro com a série travada não concluiu no teto {@link #CLOSE_CHECKPOINT_WAIT}
     * (tipicamente, force destacado preso). A série continua suja.
     */
    static final class CheckpointWaitTimeoutException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        CheckpointWaitTimeoutException(String message) {
            super(message);
        }
    }

    /** Definição cacheada (YAML + opções de abertura) para reabertura via {@link #reopenIfKnown}. */
    private record DefinitionRecord(String yaml, Ngrrd.OpenOptions options, int baseStepSec) {
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
        /** Escritas admitidas; só muda sob {@link #lock}, lida sem lock pelos instantâneos. */
        private volatile long writeSeq;
        /** Maior {@link #writeSeq} já coberta por um checkpoint concluído com sucesso. */
        private final AtomicLong checkpointedSeq = new AtomicLong();
        /**
         * A chave desta entrada foi inserida no índice de sujas e ainda não foi podada como limpa. Escrita só
         * sob {@link #lock} (por {@link SeriesAccess#recordWrite()} e {@link #pruneIndex}).
         */
        private volatile boolean indexed;
        /**
         * TTL de ociosidade efetivo desta série, em ms ({@link IdleTtlPolicy}); {@code 0} = não preenchido, vale o
         * global. Escrito só sob {@link #lock}, na abertura.
         */
        private volatile long idleTtlMs;
        /** Esta entrada está contada em {@code handles.idleTtlExtended}. Só muda sob {@link #lock}. */
        private boolean extendedCounted;
        /**
         * Antes deste instante (ms do relógio), {@link #closeIdle()} e {@link #evictIfOverLimit()} não tentam fechar
         * a entrada de novo: o último close delas falhou ({@link #CLOSE_RETRY_BACKOFF_MS}). Lido e escrito só sob
         * {@link #lock}.
         */
        private long closeRetryNotBeforeMs;

        long effectiveIdleTtlMs(long globalMs) {
            return idleTtlMs > 0 ? idleTtlMs : globalMs;
        }

        void touch(long now) {
            this.lastAccessMs = now;
        }

        boolean dirty() {
            return writeSeq > checkpointedSeq.get();
        }

        void markCheckpointed(long seq) {
            checkpointedSeq.accumulateAndGet(seq, Math::max);
        }
    }

    /**
     * Acesso a uma série aberta, entregue por {@link #withSeries} e válido só dentro dele, com o lock
     * da série na thread chamadora. Usar fora disso lança {@link IllegalStateException}.
     */
    public static final class SeriesAccess {
        private final String seriesKey;
        private final HandleEntry entry;
        /** Índice de sujas do registro, ou {@code null} com o índice desligado. */
        private final Set<String> dirtyIndex;

        private SeriesAccess(String seriesKey, HandleEntry entry, Set<String> dirtyIndex) {
            this.seriesKey = seriesKey;
            this.entry = entry;
            this.dirtyIndex = dirtyIndex;
        }

        /** Handle da série; o mesmo contrato de {@link #withHandle}. */
        public NgrrdHandle handle() {
            requireHeld();
            return entry.handle;
        }

        /**
         * Registra uma escrita admitida (já enfileirada no writer): a série passa a estar suja. Com o índice de
         * sujas ligado, a primeira escrita desde a última poda liga a marca {@code indexed} da entrada e insere
         * a chave no índice, nessa ordem e sob o lock da série.
         */
        public void recordWrite() {
            requireHeld();
            entry.writeSeq++;
            if (dirtyIndex != null && !entry.indexed) {
                entry.indexed = true;
                dirtyIndex.add(seriesKey);
            }
        }

        /**
         * Enfileira {@link NgrrdHandle#checkpointAsync()} e devolve sem esperar. A {@code writeSeq} é
         * capturada agora, sob o lock; no sucesso ela vira a {@code checkpointedSeq} da série antes de a
         * future devolvida concluir. Espere a future depois de soltar o lock (e o stripe).
         */
        public CompletableFuture<Void> checkpointAsync() {
            requireHeld();
            return checkpointAsyncLocked(entry);
        }

        private void requireHeld() {
            if (!entry.lock.isHeldByCurrentThread()) {
                throw new IllegalStateException("SeriesAccess usado fora de withSeries");
            }
        }
    }

    /**
     * Desfecho de {@link #tryCheckpointAsync}.
     *
     * @param outcome o que aconteceu
     * @param future  future do checkpoint enfileirado, com a medida de espera em fila e de force
     *                ({@link CheckpointTiming}); {@code null} fora de {@link Outcome#STARTED}
     */
    public record CheckpointAttempt(Outcome outcome, CompletableFuture<CheckpointTiming> future) {

        static final CheckpointAttempt BUSY = new CheckpointAttempt(Outcome.BUSY, null);
        static final CheckpointAttempt CLEAN = new CheckpointAttempt(Outcome.CLEAN, null);
        static final CheckpointAttempt UNAVAILABLE = new CheckpointAttempt(Outcome.UNAVAILABLE, null);

        /** Desfechos possíveis. */
        public enum Outcome {
            /** Checkpoint enfileirado; espere {@link CheckpointAttempt#future()}. */
            STARTED,
            /** Lock da série ocupado agora; tente no próximo ciclo. */
            BUSY,
            /** Série aberta, mas sem escrita nova desde o último checkpoint. */
            CLEAN,
            /** Série fechada, ausente ou congelada por migração. */
            UNAVAILABLE
        }
    }
}
