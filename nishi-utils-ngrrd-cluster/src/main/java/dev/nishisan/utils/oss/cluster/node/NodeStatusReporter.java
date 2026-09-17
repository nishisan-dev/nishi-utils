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
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.metrics.BlobVolumeStats;

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Publica periodicamente o {@link StorageNodeStatus} deste nó no catálogo, a
 * partir de {@link BlobVolume#stats()}, e agenda {@link SeriesHandleRegistry#closeIdle()}
 * no mesmo tick. Preserva {@link NodeState#DRAINING}/{@link NodeState#DRAINED}
 * já registrados (não reescreve para {@link NodeState#ACTIVE} um nó em
 * drenagem — decisão do {@code AdminService}, fora deste marco).
 *
 * <p>Implementa {@link LeadershipListener} para reportar imediatamente assim que
 * este nó percebe uma troca de líder — o líder novo só teria a visão deste nó
 * pelo próximo tick regular (até {@code interval} de distância) sem isso, o
 * que é exatamente a janela que fazia {@code LeastLoadedPlacementPolicy}
 * descartar nós legítimos por status "velho" logo após um handoff (achado
 * F2 do Debugger). Falha ao publicar não espera o próximo tick: retenta com
 * backoff curto (200 ms → 2 s).</p>
 */
public final class NodeStatusReporter implements Closeable, LeadershipListener {

    private static final Logger LOGGER = Logger.getLogger(NodeStatusReporter.class.getName());
    private static final long RETRY_BACKOFF_MIN_MS = 200L;
    private static final long RETRY_BACKOFF_MAX_MS = 2_000L;

    private final CatalogService catalog;
    private final BlobVolume volume;
    private final SeriesHandleRegistry registry;
    private final String nodeId;
    private final long capacityBytes;
    private final Duration interval;
    private final Clock clock;
    private final ScheduledExecutorService scheduler;

    private volatile ScheduledFuture<?> task;
    /** Retentativa de publicação em voo (backoff), separada de {@link #task} — o tick periódico continua existindo. */
    private volatile ScheduledFuture<?> pendingRetry;

    public NodeStatusReporter(CatalogService catalog, BlobVolume volume, SeriesHandleRegistry registry,
            String nodeId, long capacityBytes, Duration interval, Clock clock) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.volume = Objects.requireNonNull(volume, "volume");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.capacityBytes = capacityBytes;
        this.interval = Objects.requireNonNull(interval, "interval");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "ngrrd-status-reporter");
            thread.setDaemon(true);
            return thread;
        });
    }

    /** Agenda o primeiro reporte imediatamente e os seguintes a cada {@code interval}. */
    public void start() {
        task = scheduler.scheduleWithFixedDelay(this::tick, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * m5: captura {@link Throwable}, não só {@link RuntimeException} — um
     * {@code ScheduledExecutorService} para de agendar novas execuções de uma
     * tarefa periódica se ela escapar com qualquer exceção não capturada
     * (inclusive um {@link Error}), então uma falha aqui não pode derrubar o
     * scheduler silenciosamente.
     */
    private void tick() {
        reportWithRetry(1);
        try {
            registry.closeIdle();
        } catch (Throwable e) {
            LOGGER.log(Level.SEVERE, "Falha ao fechar handles ociosos do nó " + nodeId, e);
        }
    }

    /**
     * Publica o status; em falha, reagenda uma nova tentativa com backoff curto
     * (200 ms → 2 s, dobrando a cada tentativa) em vez de esperar o próximo
     * tick regular — uma falha de publicação não deve deixar o líder sem um
     * status fresco deste nó por até {@code interval} inteiro.
     */
    private void reportWithRetry(int attempt) {
        try {
            report();
        } catch (Throwable e) {
            long backoffMs = Math.min(RETRY_BACKOFF_MIN_MS << Math.min(attempt - 1, 4), RETRY_BACKOFF_MAX_MS);
            LOGGER.log(Level.SEVERE, "Falha ao reportar status do nó " + nodeId + " (tentativa " + attempt
                    + "); retentando em " + backoffMs + " ms", e);
            try {
                pendingRetry = scheduler.schedule(() -> reportWithRetry(attempt + 1), backoffMs, TimeUnit.MILLISECONDS);
            } catch (RuntimeException scheduleFailure) {
                LOGGER.log(Level.SEVERE, "Não foi possível reagendar o reporte de status do nó " + nodeId
                        + " após falha; scheduler provavelmente já foi encerrado", scheduleFailure);
            }
        }
    }

    /**
     * Reporta imediatamente ao perceber uma troca de líder — sem esperar o próximo tick regular.
     * Best-effort: se o scheduler já estiver encerrado (nó fechando), a exceção é apenas logada.
     */
    @Override
    public void onLeaderChanged(NodeId newLeader) {
        try {
            scheduler.execute(() -> reportWithRetry(1));
        } catch (RuntimeException e) {
            LOGGER.log(Level.FINE, "Reporte imediato de status ignorado (scheduler encerrado?) no nó " + nodeId, e);
        }
    }

    private void report() {
        BlobVolumeStats stats = volume.stats();
        long seriesCount = stats.catalogEntryCount();
        long usedBytes = sum(stats.shardUsedBytes());
        NodeState state = catalog.nodeStatusLocal(nodeId).map(StorageNodeStatus::state).orElse(NodeState.ACTIVE);
        long now = clock.millis();
        catalog.putNodeStatus(new StorageNodeStatus(nodeId, state, seriesCount, usedBytes, capacityBytes, now));
    }

    private static long sum(long[] values) {
        long total = 0L;
        for (long value : values) {
            total += value;
        }
        return total;
    }

    @Override
    public void close() {
        ScheduledFuture<?> current = task;
        if (current != null) {
            current.cancel(false);
        }
        ScheduledFuture<?> retry = pendingRetry;
        if (retry != null) {
            retry.cancel(false);
        }
        scheduler.shutdownNow();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.log(Level.WARNING, "Scheduler do status reporter do nó " + nodeId + " não parou em 5s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
