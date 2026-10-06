/*
 *  Copyright (C) 2020-2026 Lucas Nishimura <lucas.nishimura at gmail.com>
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
package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HandbackCompletePayload;
import dev.nishisan.utils.ngrid.common.HandbackGrantPayload;
import dev.nishisan.utils.ngrid.common.HandbackRequestPayload;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Lado incumbente do handback (8.11.2, H1 da revisão): o vetor congelado do GRANT só traz os tópicos que
 * este nó SERVE (handler registrado), inclusive os de fronteira zero. Um tópico que só existe no
 * {@code sequence-state.dat} (mapa removido da configuração) fica de fora: o candidato o exigiria, este nó
 * não poderia servir o seu snapshot e o handback ficaria em ciclo (INSTALLING até o prazo, abort,
 * cooldown, de novo), com a produção congelada a cada tentativa. No COMPLETE, um tópico fora do vetor
 * congelado nunca é reancorado.
 */
class HandbackGrantExcludesUnservedTopicsTest {

    private static final String OFFSETS = "map:_ngrid-queue-offsets";
    private static final String CATALOG = "map:ngrrd.catalog";
    private static final String EMPTY = "map:fresh-empty";
    private static final String REMOVED = "map:removed-from-config";
    private static final long OFFSETS_W = 40L;
    private static final long CATALOG_W = 300L;
    private static final long REMOVED_DISK = 10L;
    private static final NodeId LOCAL = NodeId.of("storage-079");
    private static final NodeId CANDIDATE = NodeId.of("storage-217");

    private Path tempDir;
    private ScheduledExecutorService scheduler;
    private final List<LogRecord> severe = new CopyOnWriteArrayList<>();
    // Referência forte: o LogManager guarda os loggers por referência fraca.
    private final Logger managerLogger = Logger.getLogger(ReplicationManager.class.getName());
    private Handler captureHandler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("handback-grant-unserved");
        scheduler = Executors.newScheduledThreadPool(2);
        captureHandler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLevel().intValue() >= Level.SEVERE.intValue()) {
                    severe.add(record);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        managerLogger.addHandler(captureHandler);
    }

    @AfterEach
    void tearDown() {
        managerLogger.removeHandler(captureHandler);
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void grantSoNomeiaTopicosServidosEOCompleteNaoReancoraTopicoForaDoVetor() throws Exception {
        Map<String, Long> state = new HashMap<>();
        state.put(OFFSETS, OFFSETS_W + 1L);
        state.put(CATALOG, CATALOG_W + 1L);
        state.put(REMOVED, REMOVED_DISK + 1L); // só no disco: handler nunca registrado
        state.put("_topic:" + OFFSETS, OFFSETS_W);
        state.put("_topic:" + CATALOG, CATALOG_W);
        state.put("_topic:" + REMOVED, REMOVED_DISK);
        state.put("_global", CATALOG_W);
        writeSequenceState(state);

        ScriptedTransport transport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1, Set.of(), 50),
                List.of());
        ClusterCoordinator coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(5),
                        Duration.ofSeconds(60), 1, null).withPairMode(true),
                scheduler);
        ReplicationManager manager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .strictConsistency(false)
                        .leaderLocalApply(false)
                        .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                        .operationTimeout(Duration.ofSeconds(5))
                        .affinityHandbackMode(true)
                        .handoverMaxDuration(Duration.ofSeconds(30))
                        .dataDirectory(tempDir)
                        .build());
        try {
            manager.registerHandler(OFFSETS, new SnapshotHandler());
            manager.registerHandler(CATALOG, new SnapshotHandler());
            manager.registerHandler(EMPTY, new SnapshotHandler()); // servido, fronteira zero
            manager.start();
            coordinator.start();
            awaitCondition(() -> coordinator.isLeader() && !manager.isLeaderSyncing(), 15_000,
                    "o incumbente deve liderar sozinho");
            assertEquals(REMOVED_DISK, frontier(manager, REMOVED), "o tópico só de disco está nas fronteiras");

            transport.connect(new NodeInfo(CANDIDATE, "127.0.0.1", 2, Set.of(), 100));
            for (int i = 0; i < 5; i++) {
                candidateHeartbeat(transport, false, 0L);
                Thread.sleep(50);
            }
            transport.deliver(ClusterMessage.request(MessageType.HANDBACK_REQUEST, "handback", CANDIDATE, LOCAL,
                    new HandbackRequestPayload(CANDIDATE, 0L, CATALOG_W)));
            ClusterMessage grant = awaitSent(transport, MessageType.HANDBACK_GRANT, 10_000);
            HandbackGrantPayload grantPayload = grant.payload(HandbackGrantPayload.class);
            assertEquals(Map.of(OFFSETS, OFFSETS_W, CATALOG, CATALOG_W, EMPTY, 0L), grantPayload.frozenByTopic(),
                    "o GRANT congela só os tópicos servidos (com o de fronteira zero), sem o tópico só de disco");

            // O candidato instala exatamente o vetor congelado e conclui; um candidato antigo (ou um
            // vetor forjado) que incluísse o tópico só de disco não pode reancorá-lo.
            long newEpoch = grantPayload.leaderEpoch() + 1L;
            Map<String, Long> cutover = new HashMap<>(grantPayload.frozenByTopic());
            cutover.put(REMOVED, REMOVED_DISK - 3L);
            transport.deliver(ClusterMessage.request(MessageType.HANDBACK_COMPLETE, "handback", CANDIDATE, LOCAL,
                    new HandbackCompletePayload(CATALOG_W, newEpoch, cutover)));
            awaitCondition(() -> {
                candidateHeartbeat(transport, true, newEpoch);
                return !coordinator.isLeader()
                        && CANDIDATE.equals(coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null));
            }, 10_000, "o incumbente deve ser rebaixado para o candidato");

            assertEquals(REMOVED_DISK, frontier(manager, REMOVED), "tópico fora do vetor congelado: não reancora");
            assertEquals(CATALOG_W, frontier(manager, CATALOG));
            assertEquals(OFFSETS_W, frontier(manager, OFFSETS));
            assertEquals(0L, frontier(manager, EMPTY));
            List<String> markers = severe.stream()
                    .map(LogRecord::getMessage)
                    .filter(m -> m != null && m.contains(ReplicationManager.HANDBACK_VECTOR_MISMATCH_MARKER))
                    .toList();
            assertEquals(1, markers.size(), "um marcador para o tópico fora do vetor: " + markers);
            assertTrue(markers.get(0).contains("topic=" + REMOVED) && markers.get(0).contains("not served"),
                    "marcador: " + markers.get(0));
            assertFalse(manager.isHandbackInProgress());
        } finally {
            try {
                manager.close();
            } catch (Exception ignored) {
                // teardown best-effort
            }
            try {
                coordinator.close();
            } catch (Exception ignored) {
                // teardown best-effort
            }
        }
    }

    // ---- apoio ----

    private static void candidateHeartbeat(ScriptedTransport transport, boolean leader, long epoch) {
        transport.deliver(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", CANDIDATE, null,
                HeartbeatPayload.now(0L, epoch, leader)));
    }

    private static long frontier(ReplicationManager manager, String topic) {
        return manager.appliedFrontiers().frontier(topic);
    }

    private static ClusterMessage awaitSent(ScriptedTransport transport, MessageType type, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            List<ClusterMessage> sent = transport.sentOfType(type);
            if (!sent.isEmpty()) {
                return sent.get(0);
            }
            Thread.sleep(20);
        }
        throw new AssertionError("o nó não enviou " + type + " em " + timeoutMs + " ms");
    }

    private static void awaitCondition(BooleanSupplier condition, long timeoutMs, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        fail(message);
    }

    private void writeSequenceState(Map<String, Long> state) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(
                Files.newOutputStream(tempDir.resolve("sequence-state.dat")))) {
            oos.writeObject(new HashMap<>(state));
        }
    }

    /** Handler que serve um snapshot qualquer. */
    private static final class SnapshotHandler implements ReplicationHandler {
        @Override
        public void apply(UUID operationId, Object payload) {
        }

        @Override
        public Object getSnapshot() {
            return new byte[] {1};
        }
    }
}
