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
import dev.nishisan.utils.oss.cluster.metrics.BlobVolumeSummary;
import dev.nishisan.utils.oss.cluster.metrics.NgrrdClusterMetricsListener;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationExecutor;
import dev.nishisan.utils.oss.metrics.BlobVolumeStats;

import java.io.Closeable;
import java.time.Clock;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
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
 *
 * <p>M2: a cada tick, também monta um {@link NodeMetricsSnapshot} completo (via
 * {@link #metricsSnapshot()} — reaproveitado tanto aqui quanto por
 * {@code ngrrd.admin.metrics} para uma consulta pontual), loga a linha de marcador
 * {@code NGRRD_NODE_STATUS} (padrão de log marker do projeto — Docker ITs futuros podem
 * depender dela, não renomear) e, se configurado, notifica
 * {@link NgrrdClusterMetricsListener#onNodeMetrics}.</p>
 */
public final class NodeStatusReporter implements Closeable, LeadershipListener {

    private static final Logger LOGGER = Logger.getLogger(NodeStatusReporter.class.getName());
    private static final long RETRY_BACKOFF_MIN_MS = 200L;
    private static final long RETRY_BACKOFF_MAX_MS = 2_000L;
    /**
     * M3 — teto de tentativas por rodada de publicação. Ver {@link #reportGeneration} para o porquê
     * de o valor ser baixo: esgotado o teto, a rodada desiste e o próximo tick periódico
     * (a cada {@code interval}) tenta de novo, em vez de manter uma cadeia de retentativas viva.
     */
    private static final int MAX_REPORT_ATTEMPTS = 3;
    /** M3: prazo após o qual entradas terminais de {@link MigrationExecutor} são varridas a cada tick. */
    private static final Duration MIGRATION_STATE_TTL = Duration.ofMinutes(10);

    private final CatalogService catalog;
    private final BlobVolume volume;
    private final SeriesHandleRegistry registry;
    private final String nodeId;
    private final long capacityBytes;
    private final Duration interval;
    private final Clock clock;
    private final Supplier<StorageRequestHandler.StorageHandlerMetrics> handlerMetricsSupplier;
    private final BooleanSupplier leaderSupplier;
    private final NgrrdClusterMetricsListener metricsListener;
    /** {@code null} nos testes que não montam o executor de migração (M3) — métricas ficam 0/0. */
    private final MigrationExecutor migrationExecutor;
    /**
     * Prazo usado por {@link MigrationExecutor#healStuckMigrations} a cada tick — mesmo
     * {@code migrationTimeout} de {@code StorageNodeConfig}/{@code MigrationCoordinator}; irrelevante
     * quando {@link #migrationExecutor} é {@code null}.
     */
    private final Duration migrationTimeout;
    private final ScheduledExecutorService scheduler;

    private volatile ScheduledFuture<?> task;
    /** Retentativa de publicação em voo (backoff), separada de {@link #task} — o tick periódico continua existindo. */
    private volatile ScheduledFuture<?> pendingRetry;

    /**
     * Geração da rodada de publicação corrente. Cada tick periódico e cada {@link #onLeaderChanged}
     * incrementam este contador e carregam a geração resultante pela cadeia de retentativas; uma
     * retentativa cuja geração ficou para trás simplesmente desiste.
     *
     * <p>Defeito que isto corrige (M3, causa raiz da não convergência da malha de 3 nós): antes,
     * cada tick que falhava iniciava uma cadeia de retentativas <em>própria</em> e <em>ilimitada</em>
     * (200 ms → 2 s, para sempre), e {@link #onLeaderChanged} iniciava mais uma. Numa troca de líder
     * as publicações falham em todos os nós, então as cadeias se acumulavam — dezenas delas, cada uma
     * republicando a cada 2 s. Cada publicação é um {@code DistributedMap.put} roteado ao líder
     * adotado localmente; quando esse nó responde "não sou o líder", o core registra a recusa e
     * reavalia a liderança (escape de impasse D9 do {@code ClusterCoordinator}), o que troca o líder,
     * o que dispara {@link #onLeaderChanged} em todos os nós, o que cria mais cadeias — realimentação
     * positiva que impedia a malha de assentar. Medido por A/B com 3 nós: NGrid puro e NGrid com os
     * mapas do catálogo declarados convergem em ≤ 3,6 s; com uma publicação de status a cada 2 s por
     * nó (sem retentativa) os piores casos vão a 12-15 s; com as cadeias acumuladas, passava de 150 s
     * sem convergir.</p>
     */
    private final AtomicLong reportGeneration = new AtomicLong();

    /**
     * Taxa de {@code samples/s} do log marker é calculada ENTRE ticks — só {@link #tick()} (sempre
     * a mesma thread única do {@link #scheduler}) lê/escreve estes dois campos, então não precisam
     * de sincronização própria.
     */
    private long lastSamplesWritten = -1L;
    private long lastSamplesAtEpochMs;

    public NodeStatusReporter(CatalogService catalog, BlobVolume volume, SeriesHandleRegistry registry,
            String nodeId, long capacityBytes, Duration interval, Clock clock,
            Supplier<StorageRequestHandler.StorageHandlerMetrics> handlerMetricsSupplier,
            BooleanSupplier leaderSupplier, NgrrdClusterMetricsListener metricsListener) {
        // migrationTimeout não importa aqui: sem migrationExecutor, healStuckMigrations nunca roda.
        this(catalog, volume, registry, nodeId, capacityBytes, interval, clock, handlerMetricsSupplier,
                leaderSupplier, metricsListener, null, MIGRATION_STATE_TTL);
    }

    public NodeStatusReporter(CatalogService catalog, BlobVolume volume, SeriesHandleRegistry registry,
            String nodeId, long capacityBytes, Duration interval, Clock clock,
            Supplier<StorageRequestHandler.StorageHandlerMetrics> handlerMetricsSupplier,
            BooleanSupplier leaderSupplier, NgrrdClusterMetricsListener metricsListener,
            MigrationExecutor migrationExecutor, Duration migrationTimeout) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.volume = Objects.requireNonNull(volume, "volume");
        this.registry = Objects.requireNonNull(registry, "registry");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.capacityBytes = capacityBytes;
        this.interval = Objects.requireNonNull(interval, "interval");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.handlerMetricsSupplier = Objects.requireNonNull(handlerMetricsSupplier, "handlerMetricsSupplier");
        this.leaderSupplier = Objects.requireNonNull(leaderSupplier, "leaderSupplier");
        this.metricsListener = metricsListener;
        this.migrationExecutor = migrationExecutor;
        this.migrationTimeout = Objects.requireNonNull(migrationTimeout, "migrationTimeout");
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
        reportWithRetry(1, reportGeneration.incrementAndGet());
        try {
            registry.closeIdle();
        } catch (Throwable e) {
            LOGGER.log(Level.SEVERE, "Falha ao fechar handles ociosos do nó " + nodeId, e);
        }
        if (migrationExecutor != null) {
            try {
                migrationExecutor.sweepExpiredStates(MIGRATION_STATE_TTL);
            } catch (Throwable e) {
                LOGGER.log(Level.SEVERE, "Falha ao varrer migrações expiradas do nó " + nodeId, e);
            }
            try {
                migrationExecutor.healStuckMigrations(migrationTimeout);
            } catch (Throwable e) {
                LOGGER.log(Level.SEVERE, "Falha ao autocurar migrações presas do nó " + nodeId, e);
            }
        }
        try {
            publishMetrics();
        } catch (Throwable e) {
            LOGGER.log(Level.SEVERE, "Falha ao publicar métricas do nó " + nodeId, e);
        }
    }

    /**
     * Monta o {@link NodeMetricsSnapshot}, loga a linha {@code NGRRD_NODE_STATUS} e notifica
     * {@link #metricsListener}, se configurado.
     */
    private void publishMetrics() {
        NodeMetricsSnapshot snapshot = metricsSnapshot();
        double samplesPerSecond = computeSamplesPerSecond(snapshot.samplesWritten(), snapshot.capturedAtEpochMs());
        LOGGER.info(String.format(Locale.ROOT,
                "NGRRD_NODE_STATUS nodeId=%s leader=%s series=%d usedBytes=%d openHandles=%d samples/s=%.1f "
                        + "writeBatchP99us=%d checkpointP99us=%d readP99us=%d",
                snapshot.nodeId(), snapshot.leader(), snapshot.seriesCount(), snapshot.usedBytes(),
                snapshot.openHandles(), samplesPerSecond, snapshot.writeBatchLatency().p99Micros(),
                snapshot.checkpointLatency().p99Micros(), snapshot.readLatency().p99Micros()));
        if (metricsListener != null) {
            metricsListener.onNodeMetrics(snapshot);
        }
    }

    /**
     * Amostras/s entre este tick e o anterior — primeiro tick sempre devolve {@code 0.0} (não há
     * "anterior" ainda para comparar). Só chamado por {@link #tick()}, sempre na mesma thread única
     * do {@link #scheduler}, então {@link #lastSamplesWritten}/{@link #lastSamplesAtEpochMs} não
     * precisam de sincronização própria.
     */
    private double computeSamplesPerSecond(long samplesWritten, long nowEpochMs) {
        if (lastSamplesWritten < 0) {
            lastSamplesWritten = samplesWritten;
            lastSamplesAtEpochMs = nowEpochMs;
            return 0.0;
        }
        long elapsedMs = nowEpochMs - lastSamplesAtEpochMs;
        double rate = elapsedMs > 0 ? (samplesWritten - lastSamplesWritten) * 1_000.0 / elapsedMs : 0.0;
        lastSamplesWritten = samplesWritten;
        lastSamplesAtEpochMs = nowEpochMs;
        return Math.max(0.0, rate);
    }

    /**
     * Snapshot completo das métricas operacionais deste nó, montado sob demanda — usado tanto pelo
     * tick periódico quanto por uma consulta pontual de {@code ngrrd.admin.metrics}. Não depende do
     * catálogo replicado {@code ngrrd.nodes} (que pode estar levemente atrasado): lê
     * {@link BlobVolume#stats()} e o {@link SeriesHandleRegistry} diretamente, a mesma fonte usada
     * por {@link #report()}.
     */
    public NodeMetricsSnapshot metricsSnapshot() {
        BlobVolumeStats stats = volume.stats();
        StorageRequestHandler.StorageHandlerMetrics handlerMetrics = handlerMetricsSupplier.get();
        MigrationExecutor.ExecutorMetrics migrationMetrics = migrationExecutor != null
                ? migrationExecutor.metricsSnapshot()
                : new MigrationExecutor.ExecutorMetrics(0L, 0L);
        return new NodeMetricsSnapshot(
                nodeId,
                clock.millis(),
                leaderSupplier.getAsBoolean(),
                stats.catalogEntryCount(),
                sum(stats.shardUsedBytes()),
                capacityBytes,
                registry.openCount(),
                handlerMetrics.writeBatches(),
                handlerMetrics.samplesWritten(),
                handlerMetrics.samplesFailed(),
                handlerMetrics.checkpoints(),
                handlerMetrics.flushes(),
                handlerMetrics.reads(),
                handlerMetrics.writeBatchLatency(),
                handlerMetrics.checkpointLatency(),
                handlerMetrics.readLatency(),
                handlerMetrics.errorsByStatus(),
                BlobVolumeSummary.from(stats),
                migrationMetrics.migrationsIn(),
                migrationMetrics.migrationsOut());
    }

    /**
     * Publica o status; em falha, reagenda uma nova tentativa com backoff curto
     * (200 ms → 2 s, dobrando a cada tentativa) em vez de esperar o próximo
     * tick regular — uma falha de publicação não deve deixar o líder sem um
     * status fresco deste nó por até {@code interval} inteiro.
     *
     * <p>A rodada desiste ao atingir {@value #MAX_REPORT_ATTEMPTS} tentativas, e qualquer tentativa
     * de uma geração superada por um tick (ou troca de líder) mais recente sai sem publicar: no
     * máximo UMA cadeia de retentativas fica viva por vez. Ver {@link #reportGeneration} para a
     * realimentação positiva que isso corrige.</p>
     */
    private void reportWithRetry(int attempt, long generation) {
        if (generation != reportGeneration.get()) {
            // Superada por um tick periódico ou uma troca de líder mais recente: aquela rodada
            // publica o status mais novo, republicar o antigo aqui só somaria pressão no líder.
            return;
        }
        try {
            report();
        } catch (Throwable e) {
            if (attempt >= MAX_REPORT_ATTEMPTS) {
                // Falhar aqui é esperado durante uma eleição (o líder adotado localmente recusa a
                // escrita, ou ainda está em catch-up): WARNING sem stack trace, e o próximo tick
                // periódico tenta de novo com um status mais fresco.
                LOGGER.log(Level.WARNING, () -> "Status do nó " + nodeId + " não publicado após "
                        + attempt + " tentativas (" + e + "); nova tentativa no próximo tick");
                return;
            }
            long backoffMs = Math.min(RETRY_BACKOFF_MIN_MS << Math.min(attempt - 1, 4), RETRY_BACKOFF_MAX_MS);
            LOGGER.log(Level.FINE, "Falha ao reportar status do nó " + nodeId + " (tentativa " + attempt
                    + "); retentando em " + backoffMs + " ms", e);
            try {
                pendingRetry = scheduler.schedule(() -> reportWithRetry(attempt + 1, generation),
                        backoffMs, TimeUnit.MILLISECONDS);
            } catch (RuntimeException scheduleFailure) {
                LOGGER.log(Level.FINE, "Não foi possível reagendar o reporte de status do nó " + nodeId
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
            long generation = reportGeneration.incrementAndGet();
            scheduler.execute(() -> reportWithRetry(1, generation));
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
        // (Refuter r2, BAIXO) shutdown() graciosa, não shutdownNow(): um tick em andamento nesta thread
        // pode estar no meio de uma operação de I/O sobre o BlobVolume (ex.: registry.closeIdle() ou
        // publishMetrics() tocando um FileChannel) — interrompê-la à força fecha esse canal por baixo
        // (ClosedByInterruptException) e loga SEVERE, mesmo o volume ainda sendo perfeitamente válido
        // (NgrrdStorageNode.close() chama isto ANTES de fechar o volume, de propósito — ver seu
        // Javadoc). Esperar o tick terminar sozinho evita o barulho; só recorre a shutdownNow() como
        // último caso, se a espera graciosa estourar o prazo.
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.log(Level.WARNING, "Scheduler do status reporter do nó " + nodeId + " não parou em 5s "
                        + "de forma graciosa — forçando");
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
