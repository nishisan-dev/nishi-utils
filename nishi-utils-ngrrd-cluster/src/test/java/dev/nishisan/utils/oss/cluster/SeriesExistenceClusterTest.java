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

import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataPoint;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.api.SeriesInfo;
import dev.nishisan.utils.oss.cluster.api.SeriesVerification;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import dev.nishisan.utils.oss.cluster.node.StorageNodeConfig;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.NodeStatusView;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator.MigrationHooks;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator.MigrationOutcome;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator.MigrationResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Existência de séries, abertura sem criar (handle somente leitura), verificação física e migração num
 * cluster real in-process: 2 storage nodes + clientes transparentes. O rebalanceamento automático fica
 * desligado para que os placements gravados à mão no catálogo não sejam movidos por baixo do teste.
 */
@Timeout(value = 240, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SeriesExistenceClusterTest {

    private static final long START = 1_747_339_200_000L;
    private static final long STEP_MS = 1_000L;
    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(90);
    private static final int PHASE_BASELINE = 0;
    private static final int PHASE_MIGRATION = 1;
    private static final int PHASE_AFTER = 2;
    private static final Ngrrd.OpenOptions READ_ONLY = Ngrrd.OpenOptions.defaults().withCreateIfMissing(false);
    private static final String YAML = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata: {name: series-existence}
            spec:
              time: {baseStepSec: 1}
              identity:
                seriesKeyTemplate: "sensor:{id}"
                tags: [{name: id}]
              dataSources:
                - {name: value, type: GAUGE, heartbeatSec: 10}
              archives:
                rras:
                  - {name: raw, stepSec: 1, rows: 8192, cf: [AVERAGE], xff: 0.5}
              storage:
                backend: blob
                objectNaming: {scheme: deterministic, seriesPrefix: series, schemaPrefix: schema}
            """;

    private static final MigrationHooks NO_OP_HOOKS = new MigrationHooks() {
    };

    private NgrrdClusterTestHarness harness;

    @AfterEach
    void tearDown() {
        if (harness != null) {
            harness.close();
        }
    }

    @Test
    void serieExistenteExisteEAbreSemCriar(@TempDir Path base) throws Exception {
        NgrrdClusterClient writer = startCluster(base, builder -> { });
        NgrrdHandle writable = writer.open(YAML, tags("existing"));
        String key = writable.seriesKey();
        writeSequence(writable, 1, 20);
        writable.checkpoint();

        assertTrue(writer.exists(key));
        String owner = harness.leaderNode().catalog().placementStrong(key).orElseThrow().ownerNodeId();
        SeriesInfo info = writer.find(key).orElseThrow();
        assertEquals(owner, info.ownerNodeId());
        assertEquals(PlacementState.ACTIVE, info.state());
        assertTrue(nodeById(owner).volume().storage().exists(objectKey(key)),
                "o objeto de " + key + " deveria existir no dono " + owner);

        // Segundo cliente, sem handle nem cache da série: abre pela réplica fria do catálogo.
        NgrrdClusterClient reader = connectClient(builder -> { });
        assertTrue(reader.exists(key));
        assertEquals(owner, reader.find(key).orElseThrow().ownerNodeId());
        assertEquals(SeriesVerification.PRESENT, reader.verify(List.of(key)).get(key));

        NgrrdHandle readOnly = reader.open(YAML, tags("existing"), READ_ONLY);
        assertEquals(key, readOnly.seriesKey());
        assertEquals(sequence(1, 19), readValues(readOnly, 19));
        assertThrows(IllegalStateException.class,
                () -> readOnly.write("value", new Sample(START + 21 * STEP_MS, 21)));
        assertThrows(IllegalStateException.class, readOnly::checkpoint);

        // O handle somente leitura continua acompanhando a série depois de novas escritas do gravável.
        writeSequence(writable, 21, 30);
        writable.checkpoint();
        awaitCondition("leitura sem criar reflete as escritas novas",
                () -> readValues(readOnly, 29).equals(sequence(1, 29)));
        // O close do somente leitura é local: não fecha a série no dono, e o gravável segue escrevendo.
        readOnly.close();
        assertTrue(nodeById(owner).registry().isOpen(key));
        writeSequence(writable, 31, 40);
        writable.checkpoint();
        assertEquals(sequence(1, 39), readValues(writable, 39));
    }

    @Test
    void serieInexistenteNaoCriaNada(@TempDir Path base) throws Exception {
        NgrrdClusterClient client = startCluster(base, builder -> { });
        String key = "sensor:ghost";

        assertFalse(client.exists(key));
        assertTrue(client.find(key).isEmpty());

        SeriesNotFoundException notFound = assertThrows(SeriesNotFoundException.class,
                () -> client.open(YAML, tags("ghost"), READ_ONLY));
        assertEquals(key, notFound.seriesKey());
        assertEquals(SeriesNotFoundException.Reason.NOT_PLACED, notFound.reason());

        assertTrue(harness.leaderNode().catalog().placementStrong(key).isEmpty(),
                "o open sem criar não pode ter posicionado a série no líder");
        for (NgrrdStorageNode node : harness.nodes()) {
            assertTrue(node.catalog().placementLocal(key).isEmpty(),
                    "placement inesperado de " + key + " na réplica de " + node.nodeId());
            assertFalse(node.volume().storage().exists(objectKey(key)),
                    "objeto de " + key + " criado em " + node.nodeId());
            assertFalse(node.registry().isOpen(key), key + " aberta em " + node.nodeId());
        }
        assertEquals(SeriesVerification.NOT_PLACED, client.verify(List.of(key)).get(key));
        assertFalse(client.exists(key));
    }

    @Test
    void serieEmMigracaoContaComoExistente(@TempDir Path base) throws Exception {
        // Placement MIGRATING gravado à mão no catálogo não se sustenta: o líder recém-eleito varre o
        // catálogo por ~10 s (resumeInFlight) e aborta migrações que não conduz. A série fica em MIGRATING
        // de verdade segurando uma migração real logo antes do cutover.
        CutoverGate gate = new CutoverGate();
        NgrrdClusterClient writer = startCluster(base, storage -> { }, gate, builder -> { });
        NgrrdHandle writable = writer.open(YAML, tags("migrating"));
        String key = writable.seriesKey();
        writeSequence(writable, 1, 20);
        writable.checkpoint();
        NgrrdClusterClient reader = connectClient(builder -> { });

        String owner = harness.leaderNode().catalog().placementStrong(key).orElseThrow().ownerNodeId();
        String target = otherNodeId(owner);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<MigrationResult> migration =
                    harness.leaderNode().migrationCoordinator().migrate(key, owner, target);
            assertTrue(gate.awaitHeld(), "a migração não chegou ao cutover");

            awaitCondition("réplica do cliente vê a série em MIGRATING", () -> writer.find(key)
                    .map(info -> info.state() == PlacementState.MIGRATING).orElse(false));
            assertTrue(writer.exists(key));
            SeriesInfo info = writer.find(key).orElseThrow();
            assertEquals(PlacementState.MIGRATING, info.state());
            assertEquals(owner, info.ownerNodeId());
            assertEquals(target, info.targetNodeId());
            assertTrue(reader.exists(key), "MIGRATING conta como existente também para o segundo cliente");
            assertEquals(PlacementState.MIGRATING, reader.find(key).orElseThrow().state());

            Future<NgrrdHandle> pendingOpen = executor.submit(() -> reader.open(YAML, tags("migrating"), READ_ONLY));
            // Asserção negativa: o open sem criar espera a migração em vez de concluir (ou falhar) na hora.
            assertThrows(TimeoutException.class, () -> pendingOpen.get(1, TimeUnit.SECONDS),
                    "o open sem criar não pode concluir enquanto a série está em MIGRATING");

            long releasedAt = System.nanoTime();
            gate.release();
            MigrationResult result = migration.get(60, TimeUnit.SECONDS);
            assertEquals(MigrationOutcome.COMPLETED, result.outcome(), result.reason());
            NgrrdHandle readOnly = pendingOpen.get(60, TimeUnit.SECONDS);
            System.out.printf("SERIES_EXISTENCE_MIGRATING_OPEN openAfterCutoverReleaseMs=%.1f%n",
                    (System.nanoTime() - releasedAt) / 1_000_000.0);
            assertEquals(sequence(1, 19), readValues(readOnly, 19));
            assertEquals(target, harness.leaderNode().catalog().placementStrong(key).orElseThrow().ownerNodeId());
        } finally {
            gate.release();
            executor.shutdownNow();
        }
    }

    @Test
    void placementSemArquivo(@TempDir Path base) throws Exception {
        NgrrdClusterClient client = startCluster(base, builder -> { });
        String key = "sensor:placed-without-file";
        NgrrdStorageNode storageX = harness.nodes().get(1);
        harness.leaderNode().catalog().putPlacement(key,
                SeriesPlacement.active(storageX.nodeId(), System.currentTimeMillis()));

        assertTrue(client.exists(key), "exists só olha o catálogo: placement sem arquivo conta como existente");
        assertEquals(storageX.nodeId(), client.find(key).orElseThrow().ownerNodeId());
        assertEquals(SeriesVerification.MISSING_ON_OWNER, client.verify(List.of(key)).get(key));

        SeriesNotFoundException notFound = assertThrows(SeriesNotFoundException.class,
                () -> client.open(YAML, tags("placed-without-file"), READ_ONLY));
        assertEquals(key, notFound.seriesKey());
        assertEquals(SeriesNotFoundException.Reason.MISSING_ON_OWNER, notFound.reason());

        for (NgrrdStorageNode node : harness.nodes()) {
            assertFalse(node.volume().storage().exists(objectKey(key)),
                    "o open sem criar não pode ter criado " + key + " em " + node.nodeId());
            assertFalse(node.registry().isOpen(key), key + " aberta em " + node.nodeId());
        }
        SeriesPlacement placement = harness.leaderNode().catalog().placementStrong(key).orElseThrow();
        assertEquals(storageX.nodeId(), placement.ownerNodeId());
        assertEquals(PlacementState.ACTIVE, placement.state());
    }

    @Test
    void existsEmLoteGrandePagina(@TempDir Path base) throws Exception {
        NgrrdClusterClient client = startCluster(base, builder -> builder.catalogLookupBatchSize(2000));
        NgrrdHandle existing = client.open(YAML, tags("batch-existing"));
        existing.checkpoint();

        List<String> absentKeys = IntStream.range(0, 5000).mapToObj(i -> "sensor:absent-" + i).toList();
        Map<String, Boolean> absent = client.exists(absentKeys);
        assertEquals(5000, absent.size());
        assertTrue(absent.values().stream().noneMatch(Boolean::booleanValue),
                "chave inexistente respondida como existente");

        List<String> mixed = new ArrayList<>(absentKeys);
        mixed.add(2500, existing.seriesKey());
        Map<String, Boolean> mixedResult = client.exists(mixed);
        assertEquals(5001, mixedResult.size());
        assertTrue(mixedResult.get(existing.seriesKey()));
        assertEquals(1L, mixedResult.values().stream().filter(Boolean::booleanValue).count());

        Map<String, SeriesVerification> verified = client.verify(mixed);
        assertEquals(5001, verified.size());
        assertEquals(SeriesVerification.PRESENT, verified.get(existing.seriesKey()));
        assertEquals(5000L, verified.values().stream().filter(v -> v == SeriesVerification.NOT_PLACED).count());
    }

    @Test
    void migracaoComLeituraSemCriarDuranteIngestao(@TempDir Path base) throws Exception {
        // Chunks de 8 KiB a 32 KiB/s: a cópia dura alguns segundos com ingestão e leituras em curso. O compasso
        // de banda só atrasa o chunk SEGUINTE, então com um único chunk a cópia seria praticamente instantânea.
        NgrrdClusterClient writer = startCluster(base,
                storage -> storage.migrationChunkBytes(8L * 1024L).migrationBytesPerSecond(32L * 1024L),
                NO_OP_HOOKS,
                builder -> builder.batchMaxSamples(50).batchMaxDelay(Duration.ofMillis(20)));
        NgrrdHandle writable = writer.open(YAML, tags("moving"));
        String key = writable.seriesKey();
        writeSequence(writable, 1, 50);
        writable.checkpoint();

        NgrrdClusterClient reader = connectClient(builder -> { });
        NgrrdHandle readOnly = reader.open(YAML, tags("moving"), READ_ONLY);
        assertEquals(sequence(1, 49), readValues(readOnly, 49));

        String source = harness.leaderNode().catalog().placementStrong(key).orElseThrow().ownerNodeId();
        String target = otherNodeId(source);

        AtomicInteger written = new AtomicInteger(50);
        AtomicInteger phase = new AtomicInteger(PHASE_BASELINE);
        AtomicBoolean running = new AtomicBoolean(true);
        LatencyTracker writeLatency = new LatencyTracker();
        LatencyTracker checkpointLatency = new LatencyTracker();
        Collection<Throwable> ingestionFailures = new ConcurrentLinkedQueue<>();
        Collection<Throwable> readFailures = new ConcurrentLinkedQueue<>();
        AtomicInteger successfulReads = new AtomicInteger();
        AtomicInteger readsDuringMigration = new AtomicInteger();

        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> producer = workers.submit(() -> ingest(writable, written, phase, running, writeLatency,
                    checkpointLatency, ingestionFailures));
            Future<?> readLoop = workers.submit(() -> {
                while (running.get()) {
                    try {
                        readValues(readOnly, written.get());
                        successfulReads.incrementAndGet();
                        if (phase.get() == PHASE_MIGRATION) {
                            readsDuringMigration.incrementAndGet();
                        }
                    } catch (RuntimeException e) {
                        readFailures.add(e);
                    }
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                }
            });

            awaitCondition("ingestão de base antes da migração", () -> written.get() >= 150);
            phase.set(PHASE_MIGRATION);
            long migrationStarted = System.nanoTime();
            CompletableFuture<MigrationResult> migration =
                    harness.leaderNode().migrationCoordinator().migrate(key, source, target);
            MigrationResult result = migration.get(60, TimeUnit.SECONDS);
            long migrationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - migrationStarted);
            assertEquals(MigrationOutcome.COMPLETED, result.outcome(), result.reason());
            phase.set(PHASE_AFTER);
            int writtenAtCutover = written.get();

            awaitCondition("ingestão continua depois da troca de dono", () -> written.get() >= writtenAtCutover + 150);
            running.set(false);
            producer.get(30, TimeUnit.SECONDS);
            readLoop.get(30, TimeUnit.SECONDS);

            assertTrue(ingestionFailures.isEmpty(), "falhas na ingestão: " + ingestionFailures);
            assertTrue(readFailures.stream().noneMatch(SeriesNotFoundException.class::isInstance),
                    "leitura sem criar viu a série como inexistente durante a migração: " + readFailures);
            assertTrue(readFailures.isEmpty(), "falhas de leitura durante a migração: " + readFailures);
            assertTrue(readsDuringMigration.get() > 0, "nenhuma leitura sem criar concluída durante a migração");

            int count = written.get();
            writable.write("value", new Sample(START + (count + 1) * STEP_MS, count + 1));
            writable.checkpoint();
            writer.flushAll();
            assertEquals(0L, writer.metrics().samplesFailed());

            assertEquals(target, harness.leaderNode().catalog().placementStrong(key).orElseThrow().ownerNodeId());
            NgrrdStorageNode newOwner = nodeById(target);
            assertTrue(newOwner.volume().storage().exists(objectKey(key)), "objeto ausente no novo dono " + target);
            assertEquals(sequence(1, count), readValues(writable, count),
                    "amostras perdidas/reordenadas na migração");
            awaitCondition("leitura sem criar reflete todas as amostras no novo dono",
                    () -> readValues(readOnly, count).equals(sequence(1, count)));
            assertTrue(newOwner.registry().isOpen(key), "a série deveria estar aberta no novo dono " + target);

            System.out.printf("SERIES_EXISTENCE_MIGRATION migrationMs=%d samples=%d reads=%d readsDuringMigration=%d"
                            + " writeMaxBaselineMs=%.1f writeMaxMigrationMs=%.1f writeMaxAfterMs=%.1f"
                            + " checkpointMaxBaselineMs=%.1f checkpointMaxMigrationMs=%.1f checkpointMaxAfterMs=%.1f%n",
                    migrationMs, count, successfulReads.get(), readsDuringMigration.get(),
                    writeLatency.maxMs(PHASE_BASELINE), writeLatency.maxMs(PHASE_MIGRATION),
                    writeLatency.maxMs(PHASE_AFTER), checkpointLatency.maxMs(PHASE_BASELINE),
                    checkpointLatency.maxMs(PHASE_MIGRATION), checkpointLatency.maxMs(PHASE_AFTER));
        } finally {
            running.set(false);
            workers.shutdownNow();
        }
    }

    /**
     * Ingestão a ~50 amostras/s pelo handle gravável, com checkpoint a cada 10 amostras; mede a latência
     * de cada escrita e de cada checkpoint por fase (antes, durante e depois da migração).
     */
    private static void ingest(NgrrdHandle writable, AtomicInteger written, AtomicInteger phase, AtomicBoolean running,
            LatencyTracker writeLatency, LatencyTracker checkpointLatency, Collection<Throwable> failures) {
        long pacingStart = System.nanoTime();
        int produced = 0;
        while (running.get()) {
            int sequence = written.get() + 1;
            try {
                // A fase é a do início da operação: um checkpoint que atravessa o cutover conta na migração.
                int writePhase = phase.get();
                long before = System.nanoTime();
                writable.write("value", new Sample(START + sequence * STEP_MS, sequence));
                writeLatency.record(writePhase, System.nanoTime() - before);
                written.set(sequence);
                if (sequence % 10 == 0) {
                    int checkpointPhase = phase.get();
                    long checkpointBefore = System.nanoTime();
                    writable.checkpoint();
                    checkpointLatency.record(checkpointPhase, System.nanoTime() - checkpointBefore);
                }
            } catch (RuntimeException e) {
                failures.add(e);
                return;
            }
            produced++;
            long remaining = pacingStart + produced * TimeUnit.MILLISECONDS.toNanos(20) - System.nanoTime();
            if (remaining > 0) {
                LockSupport.parkNanos(remaining);
            }
        }
    }

    /**
     * Segura a migração logo antes do flip do catálogo para {@code ACTIVE(dst)}: a série fica em
     * {@code MIGRATING} até {@link #release()}. Instalado em todos os nós porque o líder só é conhecido
     * depois da eleição; só o coordenador do líder conduz migrações.
     */
    private static final class CutoverGate implements MigrationHooks {
        private final CountDownLatch held = new CountDownLatch(1);
        private final CountDownLatch released = new CountDownLatch(1);

        @Override
        public void beforeComplete(String migrationId) {
            held.countDown();
            try {
                released.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        boolean awaitHeld() throws InterruptedException {
            return held.await(60, TimeUnit.SECONDS);
        }

        void release() {
            released.countDown();
        }
    }

    /** Maior latência observada por fase, em nanos. */
    private static final class LatencyTracker {
        private final AtomicLong[] maxByPhase = {new AtomicLong(), new AtomicLong(), new AtomicLong()};

        void record(int phase, long nanos) {
            maxByPhase[phase].accumulateAndGet(nanos, Math::max);
        }

        double maxMs(int phase) {
            return maxByPhase[phase].get() / 1_000_000.0;
        }
    }

    /**
     * Sobe 2 storage nodes (rebalanceamento automático desligado), conecta o primeiro cliente e espera as
     * capacidades dos storages ficarem visíveis antes de devolvê-lo.
     */
    private NgrrdClusterClient startCluster(Path base, Consumer<NgrrdClusterConfig.Builder> customize)
            throws Exception {
        return startCluster(base, storage -> { }, NO_OP_HOOKS, customize);
    }

    private NgrrdClusterClient startCluster(Path base, Consumer<StorageNodeConfig.Builder> storage,
            MigrationHooks hooks, Consumer<NgrrdClusterConfig.Builder> customize) throws Exception {
        harness = NgrrdClusterTestHarness.start(base, 2, builder -> storage.accept(builder.rebalanceEnabled(false)),
                index -> hooks);
        harness.awaitNodeStatuses(2);
        return connectClient(customize);
    }

    private NgrrdClusterClient connectClient(Consumer<NgrrdClusterConfig.Builder> customize) {
        NgrrdClusterClient client = harness.connectClient(builder -> customize.accept(builder
                .requestTimeout(Duration.ofSeconds(5))
                .retryTimeout(Duration.ofSeconds(30))
                .closeTimeout(Duration.ofSeconds(15))));
        awaitCapabilitiesAdvertised(client);
        return client;
    }

    /**
     * Espera o status de todos os storages, com as capacidades novas, estar replicado em todos os storage
     * nodes e visível ao cliente pelo líder — sem isso, {@code exists}/{@code open} sem criar podem falhar
     * com {@code UNSUPPORTED_BY_NODE} por status ainda não publicado.
     */
    private void awaitCapabilitiesAdvertised(NgrrdClusterClient client) {
        int expected = harness.nodes().size();
        awaitCondition("capacidades dos storages visíveis a todos os nós e ao cliente", () -> {
            for (NgrrdStorageNode node : harness.nodes()) {
                Collection<StorageNodeStatus> statuses = node.catalog().nodesLocal();
                if (statuses.size() != expected
                        || !statuses.stream().allMatch(SeriesExistenceClusterTest::advertisesAll)) {
                    return false;
                }
            }
            AdminStatusResponse status;
            try {
                status = client.clusterStatus();
            } catch (NgrrdClusterException e) {
                return false;
            }
            return status.nodes().size() == expected
                    && status.nodes().stream()
                            .map(NodeStatusView::status)
                            .allMatch(SeriesExistenceClusterTest::advertisesAll);
        });
    }

    private static boolean advertisesAll(StorageNodeStatus status) {
        return StorageCapabilities.ALL.stream().allMatch(status::advertises);
    }

    private NgrrdStorageNode nodeById(String nodeId) {
        return harness.nodes().stream()
                .filter(node -> node.nodeId().equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("storage node desconhecido: " + nodeId));
    }

    private String otherNodeId(String nodeId) {
        return harness.nodes().stream()
                .map(NgrrdStorageNode::nodeId)
                .filter(id -> !id.equals(nodeId))
                .findFirst()
                .orElseThrow();
    }

    private static Map<String, String> tags(String id) {
        return Map.of("id", id);
    }

    private static String objectKey(String seriesKey) {
        return "series/" + seriesKey + ".ngrr";
    }

    /** Escreve as amostras {@code from..to} (valor igual à sequência), uma por segundo a partir de {@link #START}. */
    private static void writeSequence(NgrrdHandle handle, int from, int to) {
        for (int sequence = from; sequence <= to; sequence++) {
            handle.write("value", new Sample(START + sequence * STEP_MS, sequence));
        }
    }

    private static List<Double> sequence(int from, int to) {
        return IntStream.rangeClosed(from, to).mapToObj(n -> (double) n).toList();
    }

    /** Valores lidos nos buckets das amostras {@code 1..count}. */
    private static List<Double> readValues(NgrrdHandle handle, int count) {
        ViewQuery query = new ViewQuery(Duration.ofSeconds(count + 2L), 1, ConsolidationFunction.AVERAGE, count + 2);
        return handle.read("value", query, START + (count + 2L) * STEP_MS).points().stream()
                .filter(point -> point.tsEpochMs() >= START + STEP_MS && point.tsEpochMs() <= START + count * STEP_MS)
                .map(DataPoint::value)
                .toList();
    }

    private static void awaitCondition(String description, BooleanSupplier condition) {
        long deadline = System.nanoTime() + AWAIT_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            if (Thread.currentThread().isInterrupted()) {
                fail("interrompido aguardando: " + description);
            }
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo (" + AWAIT_TIMEOUT + "): " + description);
        }
    }
}
