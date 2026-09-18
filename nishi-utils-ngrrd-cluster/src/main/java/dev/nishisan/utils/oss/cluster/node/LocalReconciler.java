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

import dev.nishisan.utils.ngrid.cluster.coordination.LeadershipListener;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.PlaceRequest;
import dev.nishisan.utils.oss.cluster.protocol.PlaceResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsResponse;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reconcilia o volume local de um storage node com o catálogo do cluster: adota séries presentes no
 * volume mas ausentes do catálogo (também o caminho de <b>migração do ngrrd single-node</b> — apontar
 * um storage node novo para um volume blob existente adota todas as séries), apaga cópias órfãs
 * (placement {@code ACTIVE} confirmado noutro dono há mais de {@code orphanGrace}), e reporta séries
 * {@code ACTIVE} no catálogo local mas ausentes do volume (nunca inventa dados).
 *
 * <h2>Salvaguardas contra deleção indevida (ALTO-1/ALTO-2 do Refuter)</h2>
 * <ul>
 *   <li>Uma chave com {@link SeriesHandleRegistry#isOpen} ou {@link SeriesHandleRegistry#isMigrating}
 *       nunca é tocada neste ciclo — nem adotada, nem apagada.</li>
 *   <li>Antes de apagar, a decisão é sempre revalidada com {@link CatalogView#placementStrong} (não a
 *       visão em lote/eventual do início do ciclo); se divergir (já não {@code ACTIVE} noutro dono),
 *       nada é apagado.</li>
 *   <li>Mesmo com o placement forte confirmando {@code ACTIVE} noutro dono há mais de
 *       {@code orphanGrace}, só apaga se esse dono confirmar, via {@link Commands#SERIES_EXISTS}, que
 *       de fato possui o objeto — qualquer falha/timeout do RPC é tratada como "não confirmado" (não
 *       apaga).</li>
 *   <li>O primeiro ciclo depois de {@link #start()} (ou da construção, para quem chama
 *       {@link #reconcileOnce()} diretamente) nunca apaga nada — só adota e conta; um segundo ciclo é
 *       necessário para qualquer deleção.</li>
 *   <li>Antes de adotar (chave ausente do catálogo), o estado do PRÓPRIO nó é confirmado com leitura
 *       FORTE ({@link CatalogView#nodeStatusStrong}) — um nó novo, sem status algum ainda publicado
 *       ({@link Optional#empty()}), conta como elegível (mesmo critério de default do
 *       {@code NodeStatusReporter}); só {@link NodeState#DRAINING}/{@link NodeState#DRAINED} (ou a
 *       ausência de um líder alcançável para responder) bloqueiam. Quando bloqueado, a chave só é
 *       contada como {@code unplaced} — nenhum {@code PLACE} é enviado — e entra num conjunto isento do
 *       ramo de deleção, para nunca ser apagada por engano caso o catálogo mais tarde mostre um dono
 *       aparentemente órfão. A isenção não é permanente de verdade: uma adoção bem-sucedida <em>mais
 *       tarde</em> (self voltou a {@code ACTIVE}) remove a chave do conjunto, tornando-a elegível de
 *       novo ao GC de órfã caso uma migração legítima a mova para fora depois.</li>
 * </ul>
 *
 * <p>Executa no start do nó (após {@link #awaitCatalogStable} — líder eleito, uma leitura forte bem
 * sucedida e a réplica local estável por 2 ticks seguidos ou até 30 s), a cada {@code reconcileInterval}
 * (agendamento próprio), e imediatamente ao este nó virar líder (só reconcilia o próprio volume — nunca
 * o de outro nó).</p>
 *
 * <p>Reconhece objetos de série pela convenção de nomeação configurada no nó
 * ({@link StorageNodeConfig#seriesObjectPrefix()} — {@code {prefix}/<seriesKey>.ngrr}, ver
 * {@link SeriesObjectKeys}) — qualquer outro prefixo no volume (ex.: snapshots de schema) é ignorado
 * silenciosamente, por não ser um objeto de série.</p>
 */
public final class LocalReconciler implements Closeable, LeadershipListener {

    private static final Logger LOGGER = Logger.getLogger(LocalReconciler.class.getName());

    /** Teto de adoções (chamadas {@code PLACE}) por ciclo — seção 2 da spec do M4. */
    private static final int MAX_ADOPTIONS_PER_CYCLE = 200;
    private static final int PLACE_ATTEMPTS = 5;
    private static final Duration PLACE_BACKOFF = Duration.ofMillis(200L);

    private static final int STABLE_TICKS_REQUIRED = 2;
    private static final Duration STABLE_POLL_INTERVAL = Duration.ofMillis(200L);
    private static final Duration STABLE_AWAIT_TIMEOUT = Duration.ofSeconds(30L);
    /** MÉDIO-7: chave sintética usada só para confirmar que uma leitura FORTE ao líder é possível. */
    private static final String STABLE_PROBE_KEY = "__ngrrd_reconciler_probe__";

    /** Resultado de um ciclo de reconciliação — ver Javadoc da classe. */
    public record ReconcileReport(int adopted, int orphansDeleted, int unplaced, int missing, long durationMs) {

        static final ReconcileReport EMPTY = new ReconcileReport(0, 0, 0, 0, 0L);
    }

    private final BlobVolume volume;
    private final CatalogView catalog;
    private final ClusterRpc rpc;
    private final SeriesHandleRegistry registry;
    private final String self;
    private final String seriesObjectPrefix;
    private final Duration orphanGrace;
    private final Duration reconcileInterval;
    private final BooleanSupplier leaderSupplier;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;

    private final AtomicBoolean running = new AtomicBoolean(false);
    /** ALTO-1(d): nenhum ciclo apaga nada antes do segundo — protege o primeiro reporte após um restart. */
    private volatile boolean firstCycleDone = false;
    /**
     * ALTO-2: chaves cuja adoção foi adiada porque {@code self} não estava confirmadamente
     * {@code ACTIVE} (ou não havia líder) — isentas do ramo de deleção pelo resto da vida desta
     * instância, mesmo que uma leitura eventual mais tarde pareça indicar uma órfã. Também usado para
     * qualquer chave que o líder recusou adotar em {@code self} (ex.: nó DRAINING no momento do PLACE).
     */
    private final Set<String> unplacedExempt = ConcurrentHashMap.newKeySet();
    private volatile ReconcileReport lastReport = ReconcileReport.EMPTY;
    private volatile ScheduledFuture<?> periodicTask;
    /** MÉDIO-C: sinaliza {@link #awaitCatalogStable} a sair imediatamente quando {@link #close()} é chamado. */
    private volatile boolean closed = false;

    public LocalReconciler(BlobVolume volume, CatalogView catalog, ClusterRpc rpc, SeriesHandleRegistry registry,
            String self, String seriesObjectPrefix, Duration orphanGrace, Duration reconcileInterval,
            BooleanSupplier leaderSupplier, Clock clock) {
        this.volume = Objects.requireNonNull(volume, "volume");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.rpc = Objects.requireNonNull(rpc, "rpc");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.self = Objects.requireNonNull(self, "self");
        this.seriesObjectPrefix = Objects.requireNonNull(seriesObjectPrefix, "seriesObjectPrefix");
        this.orphanGrace = Objects.requireNonNull(orphanGrace, "orphanGrace");
        this.reconcileInterval = Objects.requireNonNull(reconcileInterval, "reconcileInterval");
        this.leaderSupplier = Objects.requireNonNull(leaderSupplier, "leaderSupplier");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "ngrrd-local-reconciler");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Snapshot do último ciclo concluído (para {@code NodeMetricsSnapshot}). */
    public ReconcileReport lastReport() {
        return lastReport;
    }

    /** Agenda a primeira reconciliação (após a convergência do catálogo) e as seguintes a cada {@code reconcileInterval}. */
    public void start() {
        scheduler.execute(() -> {
            awaitCatalogStable();
            // MÉDIO-C: se close() já rodou enquanto isto esperava em awaitCatalogStable, não continua —
            // nem para um ciclo inicial, nem tentando agendar no scheduler já encerrado (o que só
            // produziria um RejectedExecutionException sem função nenhuma).
            if (closed) {
                return;
            }
            runOnceSafely();
            if (closed) {
                return;
            }
            try {
                periodicTask = scheduler.scheduleWithFixedDelay(this::runOnceSafely, reconcileInterval.toMillis(),
                        reconcileInterval.toMillis(), TimeUnit.MILLISECONDS);
            } catch (RuntimeException e) {
                LOGGER.log(Level.FINE, "Não foi possível agendar o ciclo periódico do reconciliador do nó "
                        + self + " (fechado?)", e);
            }
        });
    }

    @Override
    public void onLeaderChanged(NodeId newLeader) {
        if (leaderSupplier.getAsBoolean()) {
            scheduler.execute(this::runOnceSafely);
        }
    }

    private void runOnceSafely() {
        // m4: nunca deixa uma tarefa periódica/disparada por evento escapar com Throwable não tratado
        // (mesmo padrão de NodeStatusReporter#tick / MigrationCoordinator#onLeaderChanged).
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            lastReport = reconcileOnce();
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "Falha inesperada num ciclo de reconciliação local", t);
        } finally {
            running.set(false);
        }
    }

    /**
     * Executa um ciclo de reconciliação síncrono e devolve o relatório — método público principal para
     * testes; a agenda de produção passa por {@link #start()}. Marca o fim do "primeiro ciclo" (ALTO-1
     * d) incondicionalmente ao retornar, mesmo se este ciclo não apagou nada por outro motivo.
     */
    public ReconcileReport reconcileOnce() {
        long startedAt = clock.millis();
        boolean allowDelete = firstCycleDone;
        int adopted = 0;
        int orphansDeleted = 0;
        int unplaced = 0;
        int missing = 0;

        Map<String, SeriesPlacement> placements = catalog.placementsLocal();
        List<String> seriesKeys = seriesKeysInVolume();
        Boolean selfActiveStrong = null; // calculado sob demanda, uma vez, só se alguma chave precisar

        int adoptionsThisCycle = 0;
        for (String seriesKey : seriesKeys) {
            if (registry.isOpen(seriesKey) || registry.isMigrating(seriesKey)) {
                // ALTO-1(a): handle aberto ou em migração — nunca adota nem apaga neste ciclo.
                continue;
            }
            SeriesPlacement placement = placements.get(seriesKey);
            if (placement == null) {
                if (selfActiveStrong == null) {
                    selfActiveStrong = isSelfActiveStrong();
                }
                if (!selfActiveStrong) {
                    unplaced++;
                    unplacedExempt.add(seriesKey);
                    LOGGER.log(Level.WARNING, "RECONCILE_UNPLACED série=" + seriesKey
                            + " — self não confirmado ACTIVE (ou sem líder); adoção adiada");
                    continue;
                }
                if (adoptionsThisCycle >= MAX_ADOPTIONS_PER_CYCLE) {
                    continue;
                }
                adoptionsThisCycle++;
                SeriesPlacement result = placeSelf(seriesKey);
                if (result == null) {
                    // Líder indisponível/instável — nem adotado nem unplaced; o próximo ciclo tenta de novo.
                    continue;
                }
                if (result.isOwnedBy(self)) {
                    adopted++;
                    // MÉDIO-A do Refuter: uma adoção bem-sucedida agora reabilita a chave para o ramo
                    // de deleção — ela pode ter ficado isenta num ciclo anterior (ex.: self estava
                    // DRAINING então) e, uma vez adotada de verdade, uma migração LEGÍTIMA para fora
                    // dela mais tarde precisa poder acionar o GC de órfã normalmente.
                    unplacedExempt.remove(seriesKey);
                } else {
                    // O líder não aceitou self como candidato (ex.: DRAINING no instante do PLACE): a
                    // cópia local vira órfã de migração, mas NÃO é apagada — nem agora nem depois.
                    unplaced++;
                    unplacedExempt.add(seriesKey);
                    LOGGER.log(Level.WARNING, "RECONCILE_UNPLACED série=" + seriesKey + " dono=self "
                            + "candidato recusado pelo líder; colocada em " + result.ownerNodeId());
                }
                continue;
            }
            if (placement.state() == PlacementState.MIGRATING) {
                // Origem ou destino de uma migração em curso — o coordenador resolve, nada a fazer aqui.
                continue;
            }
            if (placement.isOwnedBy(self)) {
                // ACTIVE em self: já correto.
                continue;
            }
            // Candidata a órfã (ACTIVE noutro dono, segundo a visão em lote do início do ciclo).
            if (unplacedExempt.contains(seriesKey)) {
                continue;
            }
            if (!allowDelete) {
                // ALTO-1(d): primeiro ciclo desta instância — nunca apaga, só adoção/contagem.
                continue;
            }
            // ALTO-1(b): revalida com leitura FORTE, não a visão em lote já obsoleta por definição (o
            // ciclo pode levar tempo, e o próprio ato de reconciliar não pode confiar em cache próprio).
            Optional<SeriesPlacement> strongOpt = safePlacementStrong(seriesKey);
            if (strongOpt.isEmpty()) {
                continue;
            }
            SeriesPlacement strong = strongOpt.get();
            if (strong.state() != PlacementState.ACTIVE || strong.isOwnedBy(self)) {
                // Divergiu da visão em lote (já não é mais uma órfã, ou voltou a ser nossa) — não apaga.
                continue;
            }
            long ageMs = clock.millis() - strong.updatedAtEpochMs();
            if (ageMs <= orphanGrace.toMillis()) {
                continue;
            }
            // ALTO-1(c): só apaga se o dono forte confirmar que de fato possui o objeto.
            if (!confirmExistsAtOwner(strong.ownerNodeId(), seriesKey)) {
                continue;
            }
            volume.storage().delete(objectKey(seriesKey));
            orphansDeleted++;
            LOGGER.log(Level.INFO, "NGRRD_RECONCILE_ORPHAN_DELETED série=" + seriesKey + " dono=" + strong.ownerNodeId());
        }

        // Catálogo ACTIVE em self, mas ausente do volume: nunca inventa dados, só loga e conta.
        for (Map.Entry<String, SeriesPlacement> entry : placements.entrySet()) {
            SeriesPlacement placement = entry.getValue();
            if (placement.state() == PlacementState.ACTIVE && placement.isOwnedBy(self)
                    && !volume.storage().exists(objectKey(entry.getKey()))) {
                missing++;
                LOGGER.log(Level.SEVERE, "MISSING_SERIES série=" + entry.getKey() + " — ACTIVE no catálogo local "
                        + "mas ausente do volume");
            }
        }

        firstCycleDone = true;
        long durationMs = clock.millis() - startedAt;
        ReconcileReport report = new ReconcileReport(adopted, orphansDeleted, unplaced, missing, durationMs);
        LOGGER.log(Level.INFO, () -> "NGRRD_RECONCILE nodeId=" + self + " adopted=" + report.adopted()
                + " orphansDeleted=" + report.orphansDeleted() + " unplaced=" + report.unplaced()
                + " missing=" + report.missing() + " durationMs=" + report.durationMs());
        return report;
    }

    /**
     * ALTO-2: confirma, com leitura FORTE (round-trip ao líder), se {@code self} pode adotar séries
     * agora — nunca a réplica local. MÉDIO-A do Refuter: um nó novo, ainda sem entrada alguma em
     * {@code ngrrd.nodes} ({@link Optional#empty()}), conta como elegível — é exatamente o mesmo
     * critério que {@code NodeStatusReporter#report()} usa para o próprio default (nenhum histórico
     * ainda não é o mesmo que "não pode adotar"; um nó legitimamente novo tem de conseguir adotar as
     * séries do seu próprio volume). Só {@link NodeState#DRAINING}/{@link NodeState#DRAINED} bloqueiam
     * de fato. Qualquer falha (tipicamente sem líder eleito) é tratada como "não elegível", nunca lança.
     */
    private boolean isSelfActiveStrong() {
        try {
            Optional<StorageNodeStatus> status = catalog.nodeStatusStrong(self);
            return status.isEmpty() || status.get().state() == NodeState.ACTIVE;
        } catch (RuntimeException e) {
            LOGGER.log(Level.FINE, "Leitura forte do próprio estado (" + self + ") falhou (sem líder?); "
                    + "adoção adiada neste ciclo", e);
            return false;
        }
    }

    /** {@link CatalogView#placementStrong}, mas nunca lança — falha vira {@link Optional#empty()} (não apaga). */
    private Optional<SeriesPlacement> safePlacementStrong(String seriesKey) {
        try {
            return catalog.placementStrong(seriesKey);
        } catch (RuntimeException e) {
            LOGGER.log(Level.FINE, "placementStrong falhou para " + seriesKey + " — não apagando por precaução", e);
            return Optional.empty();
        }
    }

    /**
     * ALTO-1(c): pergunta a {@code ownerNodeId} (o dono forte) se ele possui fisicamente o objeto da
     * série. Qualquer falha/timeout do RPC devolve {@code false} — "não confirmado" nunca autoriza apagar.
     */
    private boolean confirmExistsAtOwner(String ownerNodeId, String seriesKey) {
        try {
            SeriesExistsResponse response = rpc.call(NodeId.of(ownerNodeId), Commands.SERIES_EXISTS,
                    new SeriesExistsRequest(seriesKey), SeriesExistsResponse.class);
            return response.exists();
        } catch (RuntimeException e) {
            LOGGER.log(Level.FINE, "SERIES_EXISTS falhou para " + seriesKey + " em " + ownerNodeId
                    + " — não apagando por precaução (timeout/indisponibilidade)", e);
            return false;
        }
    }

    /** Chaves de série presentes no volume, decodificadas pela convenção de {@link #seriesObjectPrefix}. */
    private List<String> seriesKeysInVolume() {
        return volume.storage().list(SeriesObjectKeys.prefixWithSlash(seriesObjectPrefix)).stream()
                .map(objectKey -> SeriesObjectKeys.seriesKeyOf(objectKey, seriesObjectPrefix))
                .flatMap(Optional::stream)
                .toList();
    }

    private String objectKey(String seriesKey) {
        return SeriesObjectKeys.objectKey(seriesObjectPrefix, seriesKey);
    }

    /**
     * Pede ao líder para colocar {@code seriesKey} com {@code preferredOwnerNodeId=self} (adoção).
     * Retenta um número limitado de vezes com backoff curto em {@code NOT_LEADER}/indisponibilidade de
     * transporte; devolve {@code null} se não resolveu dentro do orçamento — o próximo ciclo tenta de
     * novo, nunca lança.
     */
    private SeriesPlacement placeSelf(String seriesKey) {
        NodeId leaderHint = null;
        for (int attempt = 1; attempt <= PLACE_ATTEMPTS; attempt++) {
            NodeId leader = leaderHint != null ? leaderHint : rpc.leaderId().orElse(null);
            leaderHint = null;
            if (leader == null) {
                sleepQuietly(PLACE_BACKOFF);
                continue;
            }
            PlaceResponse response;
            try {
                response = rpc.call(leader, Commands.PLACE, new PlaceRequest(seriesKey, null, self),
                        PlaceResponse.class);
            } catch (NgrrdClusterException e) {
                LOGGER.log(Level.FINE, "Falha de transporte ao adotar " + seriesKey + " (tentativa " + attempt + ")", e);
                sleepQuietly(PLACE_BACKOFF);
                continue;
            }
            switch (response.status()) {
                case OK -> {
                    return response.placement();
                }
                case NOT_LEADER -> {
                    if (response.leaderNodeId() != null) {
                        leaderHint = NodeId.of(response.leaderNodeId());
                    }
                    sleepQuietly(PLACE_BACKOFF);
                }
                default -> {
                    LOGGER.log(Level.WARNING, "PLACE de " + seriesKey + " (adoção) falhou: " + response.status()
                            + (response.message() != null ? " (" + response.message() + ")" : ""));
                    return null;
                }
            }
        }
        LOGGER.log(Level.WARNING, "Adoção de " + seriesKey + " não resolvida após " + PLACE_ATTEMPTS
                + " tentativas (líder indisponível) — tentando de novo no próximo ciclo");
        return null;
    }

    /**
     * MÉDIO-7: espera {@code placementsLocal()} ficar estável (mesmo tamanho) por
     * {@value #STABLE_TICKS_REQUIRED} verificações seguidas — cada uma delas exigindo TAMBÉM um líder
     * presente e uma leitura FORTE bem-sucedida (qualquer chave; usa {@link #STABLE_PROBE_KEY}, que não
     * existe de verdade — só interessa que o round-trip não lance) — ou até
     * {@value #STABLE_AWAIT_TIMEOUT}, o que vier primeiro. Combinado com ALTO-1(d) (primeiro ciclo nunca
     * apaga), garante que o primeiro {@code reconcileOnce()} de produção só roda depois de confirmar
     * conectividade real com o líder, não apenas um {@code placementsLocal()} vazio por coincidência.
     * Best-effort: nunca lança, mesmo se a malha nunca estabilizar (o primeiro ciclo roda de qualquer
     * forma com o que houver, e os seguintes reconvergem).
     *
     * <p>MÉDIO-C do Refuter: o laço também checa {@link #closed} e a interrupção da própria thread a
     * cada volta — {@link #close()} chamado enquanto este método está preso aqui (ex.: sem líder algum
     * na malha) precisa sair em bem menos que {@value #STABLE_AWAIT_TIMEOUT}, sem fazer mais nenhuma
     * chamada a {@link #rpc}/{@link #catalog}.</p>
     */
    private void awaitCatalogStable() {
        long deadline = clock.millis() + STABLE_AWAIT_TIMEOUT.toMillis();
        int stableTicks = 0;
        int lastSize = -1;
        while (!closed && !Thread.currentThread().isInterrupted() && clock.millis() < deadline) {
            boolean leaderPresent = rpc.leaderId().isPresent();
            boolean strongOk = leaderPresent && strongProbeSucceeds();
            int size = catalog.placementsLocal().size();
            if (leaderPresent && strongOk && size == lastSize) {
                stableTicks++;
                if (stableTicks >= STABLE_TICKS_REQUIRED) {
                    return;
                }
            } else if (leaderPresent && strongOk) {
                lastSize = size;
                stableTicks = 1;
            } else {
                lastSize = -1;
                stableTicks = 0;
            }
            sleepQuietly(STABLE_POLL_INTERVAL);
        }
    }

    private boolean strongProbeSucceeds() {
        try {
            catalog.placementStrong(STABLE_PROBE_KEY);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(Math.max(1L, duration.toMillis()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * MÉDIO-C do Refuter: {@code closed=true} primeiro (checado a cada volta de
     * {@link #awaitCatalogStable}), depois {@code shutdownNow()} — não a parada graciosa usada por
     * {@code NodeStatusReporter} — porque este scheduler nunca segura um recurso de I/O do volume
     * durante o próprio {@code sleepQuietly}/espera de rede de {@link #awaitCatalogStable} ou
     * {@link #placeSelf}; interromper a thread aí é seguro e é o que garante saída em bem menos de 1 s
     * mesmo com a malha sem líder algum, em vez de esperar o {@link #STABLE_AWAIT_TIMEOUT} inteiro
     * (30 s por padrão).
     */
    @Override
    public void close() {
        closed = true;
        ScheduledFuture<?> task = periodicTask;
        if (task != null) {
            task.cancel(false);
        }
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.log(Level.WARNING, "Scheduler do reconciliador local do nó " + self + " não parou em 5s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
