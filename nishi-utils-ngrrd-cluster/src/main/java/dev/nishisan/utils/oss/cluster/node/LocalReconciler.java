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
import dev.nishisan.utils.oss.cluster.catalog.CatalogView;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
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
 * Reconciles the local volume with authoritative placements. Unplaced data is quarantined
 * and preserved until an explicit administrator action. It never automatically adopts it.
 * Stale migration copies require a confirmed live owner, a grace interval and two cycles
 * before collection. Open handles and local migrations are preserved.
 */
public final class LocalReconciler implements Closeable, LeadershipListener {

    private static final Logger LOGGER = Logger.getLogger(LocalReconciler.class.getName());

    /** Teto de adoções (chamadas {@code PLACE}) por ciclo — seção 2 da spec do M4. */

    private static final int STABLE_TICKS_REQUIRED = 2;
    private static final Duration STABLE_POLL_INTERVAL = Duration.ofMillis(200L);
    private static final Duration STABLE_AWAIT_TIMEOUT = Duration.ofSeconds(30L);
    /** MÉDIO-7: chave sintética usada só para confirmar que uma leitura FORTE ao líder é possível. */
    private static final String STABLE_PROBE_KEY = "__ngrrd_reconciler_probe__";

    /** Resultado de um ciclo de reconciliação — ver Javadoc da classe. */
    public record ReconcileReport(int adopted, int orphansDeleted, int unplaced, int missing, long durationMs,
            int forgottenPruned) {

        static final ReconcileReport EMPTY = new ReconcileReport(0, 0, 0, 0, 0L, 0);

        /** Assinatura anterior à 8.6.0, sem {@code forgottenPruned} (zerado). */
        public ReconcileReport(int adopted, int orphansDeleted, int unplaced, int missing, long durationMs) {
            this(adopted, orphansDeleted, unplaced, missing, durationMs, 0);
        }
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
    /** Unplaced objects stay exempt; explicit adoption clears this after the durable state becomes ACTIVE. */
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
        int adopted = 0; // Kept in the public report for compatibility; adoption is administrative.
        int orphansDeleted = 0;
        int unplaced = 0;
        int missing = 0;

        Map<String, SeriesPlacement> placements = catalog.placementsLocal();
        List<String> seriesKeys = seriesKeysInVolume();
        for (String seriesKey : seriesKeys) {
            if (registry.isMigrating(seriesKey)) {
                // Preserve local migrations; unplaced open handles are still quarantined below.
                continue;
            }
            SeriesPlacement placement = placements.get(seriesKey);
            if (placement == null) {
                // A lost/restored catalog must never turn surviving data into automatic deletion.
                // Failure of the strong read leaves the object untouched and unknown.
                try {
                    Optional<SeriesPlacement> strong = catalog.placementStrong(seriesKey);
                    if (strong.isEmpty()) {
                        unplaced++;
                        unplacedExempt.add(seriesKey);
                        if (registry.lifecycle() != null) registry.lifecycle().inspect(seriesKey);
                        else LOGGER.warning("NGRRD_SERIES_QUARANTINED série=" + seriesKey + " nó=" + self
                                + " ação='ngrrd-admin reconcile " + self + " --adopt'");
                        continue;
                    }
                    placement = strong.get();
                } catch (RuntimeException e) {
                    LOGGER.log(Level.FINE, "Unknown placement; preserving " + seriesKey, e);
                    continue;
                }
            }
            if (registry.lifecycle() != null) {
                var state = registry.lifecycle().journal().get(seriesKey);
                if (state != null && state.phase() == SeriesLifecycleJournal.Phase.ACTIVE
                        && !state.generationId().equals(placement.generationId())) {
                    try { registry.lifecycle().inspect(seriesKey); }
                    catch (RuntimeException unknown) { continue; }
                    state = registry.lifecycle().journal().get(seriesKey);
                }
                if (state != null && state.phase() == SeriesLifecycleJournal.Phase.ACTIVE) unplacedExempt.remove(seriesKey);
            }
            if (registry.isOpen(seriesKey) || placement.deletion() != null
                    || (registry.lifecycle() != null && registry.lifecycle().journal().get(seriesKey) != null
                    && registry.lifecycle().journal().get(seriesKey).phase() == SeriesLifecycleJournal.Phase.QUARANTINED)) continue;
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
                // First cycle never collects migration remnants.
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

        // Issue #174: marcas de esquecida de séries cuja réplica local já mostra outro dono não protegem
        // mais nada (ver SeriesHandleRegistry#pruneForgotten) — sem placement local não há sinal de
        // convergência e a marca fica. Lê a réplica na hora, não o snapshot do início do ciclo: uma série
        // que saiu deste nó durante um ciclo longo não pode perder a marca por uma foto anterior.
        int forgottenPruned = registry.pruneForgotten(seriesKey -> catalog.placementLocal(seriesKey)
                .filter(placement -> !placement.isOwnedBy(self))
                .isPresent());

        firstCycleDone = true;
        long durationMs = clock.millis() - startedAt;
        ReconcileReport report = new ReconcileReport(adopted, orphansDeleted, unplaced, missing, durationMs,
                forgottenPruned);
        LOGGER.log(Level.INFO, () -> "NGRRD_RECONCILE nodeId=" + self + " adopted=" + report.adopted()
                + " orphansDeleted=" + report.orphansDeleted() + " unplaced=" + report.unplaced()
                + " missing=" + report.missing() + " forgottenPruned=" + report.forgottenPruned()
                + " durationMs=" + report.durationMs());
        return report;
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
     * uma consulta ao líder; interromper a thread aí é seguro e é o que garante saída em bem menos de 1 s
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
