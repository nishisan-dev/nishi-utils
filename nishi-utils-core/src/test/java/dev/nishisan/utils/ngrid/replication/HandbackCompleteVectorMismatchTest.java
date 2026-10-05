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
import java.io.ObjectInputStream;
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
 * Lado incumbente do handback (8.11.2): o {@code HANDBACK_COMPLETE} traz, para um tópico, um cutover
 * diferente da fronteira que este nó congelou no GRANT — o candidato não instalou o snapshot deste nó
 * para esse tópico (no incidente do CTP, a fronteira de disco do candidato para {@code ngrrd.nodes}).
 * O incumbente não pode rebaixar: fronteira, contador persistido e cursor ficam; os tópicos coerentes
 * reancoram normalmente; o marcador {@code NGRID_HANDBACK_VECTOR_MISMATCH} é emitido.
 */
class HandbackCompleteVectorMismatchTest {

    private static final String OFFSETS = "map:_ngrid-queue-offsets";
    private static final String CATALOG = "map:ngrrd.catalog";
    private static final String NODES = "map:ngrrd.nodes";
    private static final long OFFSETS_W = 40L;
    private static final long CATALOG_W = 300L;
    private static final long NODES_W = 76L;
    private static final long NODES_STALE = 72L;
    private static final NodeId LOCAL = NodeId.of("storage-079");
    private static final NodeId CANDIDATE = NodeId.of("storage-217");

    private Path tempDir;
    private ScheduledExecutorService scheduler;
    private final List<LogRecord> severe = new CopyOnWriteArrayList<>();
    private Handler captureHandler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("handback-vector-mismatch");
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
        Logger.getLogger(ReplicationManager.class.getName()).addHandler(captureHandler);
    }

    @AfterEach
    void tearDown() {
        Logger.getLogger(ReplicationManager.class.getName()).removeHandler(captureHandler);
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void incumbenteNaoRebaixaTopicoCujoCutoverDifereDaFronteiraCongelada() throws Exception {
        Map<String, Long> state = new HashMap<>();
        state.put(OFFSETS, OFFSETS_W + 1L);
        state.put(CATALOG, CATALOG_W + 1L);
        state.put(NODES, NODES_W + 1L);
        state.put("_topic:" + OFFSETS, OFFSETS_W);
        state.put("_topic:" + CATALOG, CATALOG_W);
        state.put("_topic:" + NODES, NODES_W);
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
            manager.registerHandler(NODES, new SnapshotHandler());
            manager.start();
            coordinator.start();
            awaitCondition(() -> coordinator.isLeader() && !manager.isLeaderSyncing(), 15_000,
                    "o incumbente deve liderar sozinho");

            transport.connect(new NodeInfo(CANDIDATE, "127.0.0.1", 2, Set.of(), 100));
            for (int i = 0; i < 5; i++) {
                candidateHeartbeat(transport, false, 0L);
                Thread.sleep(50);
            }
            awaitCondition(coordinator::isLeader, 5_000, "o candidato atrasado não pode tomar a liderança");
            transport.deliver(ClusterMessage.request(MessageType.HANDBACK_REQUEST, "handback", CANDIDATE, LOCAL,
                    new HandbackRequestPayload(CANDIDATE, 0L, CATALOG_W)));
            ClusterMessage grant = awaitSent(transport, MessageType.HANDBACK_GRANT, 10_000);
            HandbackGrantPayload grantPayload = grant.payload(HandbackGrantPayload.class);
            assertEquals(Map.of(OFFSETS, OFFSETS_W, CATALOG, CATALOG_W, NODES, NODES_W), grantPayload.frozenByTopic(),
                    "o GRANT congela as três fronteiras");
            // COMPLETE com `nodes` abaixo do congelado: o candidato não instalou o snapshot deste nó para ele.
            long newEpoch = grantPayload.leaderEpoch() + 1L;
            transport.deliver(ClusterMessage.request(MessageType.HANDBACK_COMPLETE, "handback", CANDIDATE, LOCAL,
                    new HandbackCompletePayload(OFFSETS_W, newEpoch,
                            Map.of(OFFSETS, OFFSETS_W, CATALOG, CATALOG_W, NODES, NODES_STALE))));
            awaitCondition(() -> {
                candidateHeartbeat(transport, true, newEpoch);
                return !coordinator.isLeader()
                        && CANDIDATE.equals(coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null));
            }, 10_000, "o incumbente deve ser rebaixado para o candidato");

            assertEquals(NODES_W, frontier(manager, NODES), "a fronteira de nodes não pode ser rebaixada");
            assertEquals(NODES_W, manager.getRelayStreamCursor(NODES),
                    "o cursor de nodes fica na fronteira congelada, não no valor divergente");
            assertEquals(CATALOG_W, frontier(manager, CATALOG), "os tópicos coerentes seguem no valor congelado");
            assertEquals(OFFSETS_W, frontier(manager, OFFSETS));
            assertFalse(manager.getTopicReplicationStatuses().get(NODES).relayPendingBootstrap(),
                    "a divergência não arma bootstrap no incumbente");

            List<LogRecord> mismatches = severe.stream()
                    .filter(r -> r.getMessage() != null
                            && r.getMessage().contains(ReplicationManager.HANDBACK_VECTOR_MISMATCH_MARKER))
                    .toList();
            assertEquals(1, mismatches.size(), "um marcador por tópico divergente: " + mismatches);
            String message = mismatches.get(0).getMessage();
            assertTrue(message.contains("topic=" + NODES) && message.contains("cutover=" + NODES_STALE)
                    && message.contains("frozen=" + NODES_W), "marcador: " + message);
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
        Map<String, Long> persisted = readSequenceState();
        assertEquals(NODES_W, persisted.get("_topic:" + NODES),
                "o contador persistido de nodes não pode receber o valor divergente");
        assertEquals(NODES_W + 1L, persisted.get(NODES), "a fronteira persistida de nodes fica no congelado");
    }

    // ---- apoio ----

    private static void candidateHeartbeat(ScriptedTransport transport, boolean leader, long epoch) {
        transport.deliver(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", CANDIDATE, null,
                HeartbeatPayload.now(0L, epoch, leader)));
    }

    private static long frontier(ReplicationManager manager, String topic) {
        return manager.getTopicReplicationStatuses().get(topic).nextExpectedSequence() - 1L;
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

    @SuppressWarnings("unchecked")
    private Map<String, Long> readSequenceState() throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(
                Files.newInputStream(tempDir.resolve("sequence-state.dat")))) {
            return (Map<String, Long>) ois.readObject();
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
