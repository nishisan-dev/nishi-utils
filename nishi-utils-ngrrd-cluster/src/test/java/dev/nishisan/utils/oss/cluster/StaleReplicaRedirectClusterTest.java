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

package dev.nishisan.utils.oss.cluster;

import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.cluster.api.ClientMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import dev.nishisan.utils.oss.cluster.node.StorageRequestHandler;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator.MigrationOutcome;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator.MigrationResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Issue #177 fim a fim: o destino de uma migração com a réplica local do catálogo atrasada não pode prender
 * as escritas de uma série em ingestão contínua. 3 storage nodes reais + 1 cliente real; o
 * {@link StorageRequestHandler} do destino C enxerga, só para a série S, uma réplica congelada (gancho de
 * teste {@link NgrrdClusterTestHarness#start(Path, int, java.util.function.Consumer,
 * java.util.function.IntFunction, java.util.function.IntFunction)}); o resto do nó — executor de
 * migração, reconciliador — lê a réplica real.
 *
 * <p>Antes da correção, C respondia pela réplica congelada ({@code WRONG_OWNER(A)} ou {@code MIGRATING})
 * enquanto a origem A, já esquecida, apontava C: a série ficava presa até a réplica de C convergir — aqui,
 * para sempre. Com a confirmação no líder, C descobre que é o dono e aceita as escritas.</p>
 *
 * <p>C nunca é o líder: o líder responde pela própria réplica, que é a autoritativa.</p>
 */
@Timeout(value = 240, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class StaleReplicaRedirectClusterTest {

    private static final int STORAGE_NODE_COUNT = 3;
    private static final long START = 1_747_339_200_000L;
    private static final Duration WRITE_INTERVAL = Duration.ofMillis(10);
    /** Prazo, a partir do fim da migração, para a série voltar a confirmar escritas no destino. */
    private static final Duration CONVERGENCE_DEADLINE = Duration.ofSeconds(15);
    /** Ingestão mantida depois do fim da migração, com o destino ainda enxergando a réplica congelada. */
    private static final Duration INGESTION_AFTER_MIGRATION = Duration.ofSeconds(2);
    /**
     * Teto de retentativas {@code WRONG_OWNER} do cliente: o salto legítimo origem → destino custa poucas
     * (os lotes em voo no instante do flip); o pingue-pongue do defeito gera uma a cada ciclo, sem fim.
     */
    private static final long MAX_WRONG_OWNER_RETRIES = 10;
    private static final String YAML = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata: {name: stale-replica-redirect}
            spec:
              time: {baseStepSec: 1}
              identity:
                seriesKeyTemplate: "sensor:{id}"
                tags: [{name: id}]
              dataSources:
                - {name: value, type: GAUGE, heartbeatSec: 10}
              archives:
                rras:
                  - {name: raw, stepSec: 1, rows: 32768, cf: [AVERAGE], xff: 0.5}
              storage:
                backend: blob
                objectNaming: {scheme: deterministic, seriesPrefix: series, schemaPrefix: schema}
            """;

    /** Réplica congelada por nó: {@code nodeId → (seriesKey → placement)} visto pelo handler de storage. */
    private final Map<String, Map<String, SeriesPlacement>> frozenByNode = new ConcurrentHashMap<>();
    /** Série e nó cuja réplica é congelada em {@code MIGRATING} no {@code beforeComplete} (caso 2). */
    private final AtomicReference<String> freezeMigratingSeries = new AtomicReference<>();
    private final AtomicReference<String> freezeMigratingAtNode = new AtomicReference<>();
    private final AtomicReference<NgrrdClusterTestHarness> harnessRef = new AtomicReference<>();

    @Test
    void destinoComReplicaActiveDaOrigemCongeladaNaoPrendeEscritas(@TempDir Path base) throws Exception {
        runMigrationWithStaleDestination(base, false);
    }

    @Test
    void destinoComReplicaMigratingCongeladaNaoPrendeEscritas(@TempDir Path base) throws Exception {
        runMigrationWithStaleDestination(base, true);
    }

    /**
     * @param freezeMigrating {@code false}: a réplica de C fica em {@code ACTIVE(A)} desde antes da migração;
     *                        {@code true}: fica no {@code MIGRATING(A→C)} capturado logo antes do flip
     */
    private void runMigrationWithStaleDestination(Path base, boolean freezeMigrating) throws Exception {
        try (NgrrdClusterTestHarness harness = NgrrdClusterTestHarness.start(base, STORAGE_NODE_COUNT,
                builder -> builder.rebalanceEnabled(false).migrationStatusPollInterval(Duration.ofMillis(50)),
                this::migratingSnapshotHooks, this::frozenReplicaDecorator)) {
            harnessRef.set(harness);
            harness.awaitNodeStatuses(STORAGE_NODE_COUNT);
            try (NgrrdClusterClient client = harness.connectClient(builder -> builder
                    .batchMaxDelay(Duration.ofMillis(20))
                    .requestTimeout(Duration.ofSeconds(5))
                    .retryTimeout(Duration.ofSeconds(30))
                    .closeTimeout(Duration.ofSeconds(10)))) {
                NgrrdHandle handle = client.open(YAML, Map.of("id", "stale-replica"));
                String seriesKey = handle.seriesKey();
                NgrrdStorageNode leader = harness.leaderNode();
                String source = leader.catalog().placementStrong(seriesKey).orElseThrow().ownerNodeId();
                NgrrdStorageNode destination = harness.nodes().stream()
                        .filter(node -> !node.nodeId().equals(source) && !node.nodeId().equals(leader.nodeId()))
                        .findFirst()
                        .orElseThrow();
                String target = destination.nodeId();

                AtomicInteger written = new AtomicInteger();
                AtomicBoolean producing = new AtomicBoolean(true);
                ExecutorService producerPool = Executors.newSingleThreadExecutor();
                try {
                    Future<?> producer = producerPool.submit(() -> {
                        while (producing.get()) {
                            int sequence = written.incrementAndGet();
                            handle.write("value", new Sample(START + sequence * 1000L, sequence));
                            LockSupport.parkNanos(WRITE_INTERVAL.toNanos());
                        }
                    });
                    handle.checkpoint();
                    harness.awaitCatalogReplicaCaughtUp(target);

                    if (freezeMigrating) {
                        freezeMigratingAtNode.set(target);
                        freezeMigratingSeries.set(seriesKey);
                    } else {
                        SeriesPlacement beforeMigration = destination.catalog().placementLocal(seriesKey)
                                .orElseThrow(() -> new AssertionError("réplica de " + target + " sem " + seriesKey));
                        assertTrue(beforeMigration.state() == PlacementState.ACTIVE
                                && beforeMigration.isOwnedBy(source), "placement inesperado: " + beforeMigration);
                        freeze(target, seriesKey, beforeMigration);
                    }

                    MigrationResult result = harness.leaderNode().migrationCoordinator()
                            .migrate(seriesKey, source, target)
                            .get(60, TimeUnit.SECONDS);
                    long migratedAtNanos = System.nanoTime();
                    assertEquals(MigrationOutcome.COMPLETED, result.outcome(), result.reason());
                    SeriesPlacement frozen = frozenByNode.getOrDefault(target, Map.of()).get(seriesKey);
                    assertNotNull(frozen, "a réplica de " + target + " deveria estar congelada");
                    assertEquals(freezeMigrating ? PlacementState.MIGRATING : PlacementState.ACTIVE, frozen.state(),
                            "réplica congelada: " + frozen);
                    assertTrue(frozen.isOwnedBy(source), "réplica congelada: " + frozen);

                    Thread.sleep(INGESTION_AFTER_MIGRATION.toMillis());
                    producing.set(false);
                    producer.get(5, TimeUnit.SECONDS);

                    long remainingNanos = migratedAtNanos + CONVERGENCE_DEADLINE.toNanos() - System.nanoTime();
                    try {
                        CompletableFuture.runAsync(handle::checkpoint)
                                .get(Math.max(remainingNanos, 0L), TimeUnit.NANOSECONDS);
                    } catch (TimeoutException e) {
                        fail("checkpoint não concluiu em " + CONVERGENCE_DEADLINE + " após a migração — "
                                + describe(client.metrics(), destination));
                    }

                    ClientMetricsSnapshot metrics = client.metrics();
                    assertEquals(0L, metrics.samplesFailed(), describe(metrics, destination));
                    assertEquals(written.get(), metrics.samplesEnqueued(), describe(metrics, destination));
                    assertEquals(metrics.samplesEnqueued(), metrics.samplesSent(),
                            "toda amostra enfileirada deveria estar confirmada — " + describe(metrics, destination));
                    long wrongOwnerRetries = metrics.retriesByStatus().getOrDefault(SeriesStatus.WRONG_OWNER, 0L);
                    assertTrue(wrongOwnerRetries <= MAX_WRONG_OWNER_RETRIES,
                            "retentativas WRONG_OWNER demais: " + describe(metrics, destination));
                    assertTrue(destination.metricsSnapshot().redirectOverrides() >= 1,
                            "o destino deveria ter corrigido a réplica pela resposta do líder — "
                                    + describe(metrics, destination));
                    assertTrue(destination.registry().isOpen(seriesKey), "a série deveria estar aberta no destino");
                    System.out.printf("STALE_REPLICA_REDIRECT mode=%s samples=%d wrongOwnerRetries=%d"
                                    + " migratingRetries=%d ownerLookups=%d redirectCycles=%d redirectOverrides=%d%n",
                            freezeMigrating ? "MIGRATING" : "ACTIVE", written.get(), wrongOwnerRetries,
                            metrics.retriesByStatus().getOrDefault(SeriesStatus.MIGRATING, 0L),
                            metrics.ownerLookups(), metrics.redirectCycles(),
                            destination.metricsSnapshot().redirectOverrides());

                    assertAllSamplesStored(handle, written.get());
                } finally {
                    producing.set(false);
                    producerPool.shutdownNow();
                }
            }
        }
    }

    /** Toda amostra {@code 1..count} está na série, em ordem (a amostra {@code count + 1} fecha o último passo). */
    private static void assertAllSamplesStored(NgrrdHandle handle, int count) {
        handle.write("value", new Sample(START + (count + 1) * 1000L, count + 1));
        handle.checkpoint();
        SeriesResult result = handle.read("value", new ViewQuery(Duration.ofSeconds(count + 2), 1,
                ConsolidationFunction.AVERAGE, count + 2), START + (count + 2) * 1000L);
        List<Double> values = result.points().stream()
                .filter(point -> point.tsEpochMs() >= START + 1000L && point.tsEpochMs() <= START + count * 1000L)
                .map(point -> point.value())
                .toList();
        assertEquals(IntStream.rangeClosed(1, count).mapToObj(n -> (double) n).toList(), values,
                "amostras ausentes/fora de ordem em " + handle.seriesKey());
    }

    private static String describe(ClientMetricsSnapshot metrics, NgrrdStorageNode destination) {
        return "cliente: enqueued=" + metrics.samplesEnqueued() + " sent=" + metrics.samplesSent() + " failed="
                + metrics.samplesFailed() + " retries=" + metrics.retriesByStatus() + " ownerLookups="
                + metrics.ownerLookups() + " redirectCycles=" + metrics.redirectCycles() + "; destino "
                + destination.nodeId() + ": redirectConfirmations="
                + destination.metricsSnapshot().redirectConfirmations() + " redirectOverrides="
                + destination.metricsSnapshot().redirectOverrides() + " redirectConfirmationFailures="
                + destination.metricsSnapshot().redirectConfirmationFailures();
    }

    private void freeze(String nodeId, String seriesKey, SeriesPlacement placement) {
        frozenByNode.computeIfAbsent(nodeId, id -> new ConcurrentHashMap<>()).put(seriesKey, placement);
    }

    /**
     * Hooks do nó {@code index}: no {@code beforeComplete} — com o catálogo do líder (este nó) ainda em
     * {@code MIGRATING(A→C)} — congela esse placement na visão do handler de storage de C (caso 2).
     */
    private MigrationCoordinator.MigrationHooks migratingSnapshotHooks(int index) {
        return new MigrationCoordinator.MigrationHooks() {
            @Override
            public void beforeComplete(String migrationId) {
                String seriesKey = freezeMigratingSeries.get();
                String nodeId = freezeMigratingAtNode.get();
                if (seriesKey == null || nodeId == null) {
                    return;
                }
                Optional<SeriesPlacement> placement = harnessRef.get().nodes().get(index).catalog()
                        .placementLocal(seriesKey);
                if (placement.isPresent() && placement.get().state() == PlacementState.MIGRATING
                        && migrationId.equals(placement.get().migrationId())) {
                    freeze(nodeId, seriesKey, placement.get());
                }
            }
        };
    }

    /**
     * Decorador do nó {@code index} ({@code storage-<index>}): réplica congelada só para as séries dele em
     * {@link #frozenByNode}.
     */
    private UnaryOperator<StorageRequestHandler.PlacementLookup> frozenReplicaDecorator(int index) {
        String nodeId = "storage-" + index;
        return real -> new FrozenReplicaLookup(real, frozenByNode.computeIfAbsent(nodeId,
                id -> new ConcurrentHashMap<>()));
    }

    /**
     * {@link StorageRequestHandler.PlacementLookup} que devolve em {@link #placementLocal} o placement
     * congelado da série, quando houver; toda consulta ao líder e o estado de liderança vêm do adaptador real.
     */
    private static final class FrozenReplicaLookup implements StorageRequestHandler.PlacementLookup {
        private final StorageRequestHandler.PlacementLookup real;
        private final Map<String, SeriesPlacement> frozen;

        FrozenReplicaLookup(StorageRequestHandler.PlacementLookup real, Map<String, SeriesPlacement> frozen) {
            this.real = real;
            this.frozen = frozen;
        }

        @Override
        public Optional<SeriesPlacement> placementLocal(String seriesKey) {
            SeriesPlacement placement = frozen.get(seriesKey);
            return placement != null ? Optional.of(placement) : real.placementLocal(seriesKey);
        }

        @Override
        public Optional<SeriesPlacement> placementStrong(String seriesKey) {
            return real.placementStrong(seriesKey);
        }

        @Override
        public Map<String, SeriesPlacement> placementsAtLeader(Collection<String> seriesKeys, Duration maxWait) {
            return real.placementsAtLeader(seriesKeys, maxWait);
        }

        @Override
        public boolean localIsAuthoritative() {
            return real.localIsAuthoritative();
        }

        @Override
        public boolean leaderKnown() {
            return real.leaderKnown();
        }
    }
}
