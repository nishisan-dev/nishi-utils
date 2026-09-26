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

package dev.nishisan.utils.ngrid.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.RelayStreamBatchPayload;
import dev.nishisan.utils.ngrid.common.SyncResponsePayload;

/**
 * Revisão #178, onda C (replicação): C1 cadeia de snapshot só do líder acordado e em ordem; C4 nó
 * parado rejeita escritas; C5 entrada envenenada do relay é dead-lettered em vez de travar o tópico;
 * C6 recusa do líder com backoff e cooldown de snapshot.
 */
class RelayStreamRobustnessTest {

    private static final String TOPIC = "t";
    private static final NodeId FOLLOWER = NodeId.of("aaa-follower");
    private static final NodeId LEADER = NodeId.of("zzz-leader");
    private static final NodeId IMPOSTER = NodeId.of("mmm-imposter");
    private static final long EPOCH = 1L;

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("relay-robustness");
        scheduler = Executors.newScheduledThreadPool(3);
    }

    @AfterEach
    void tearDown() {
        ReplicationManager.RELAY_DEAD_LETTER_THRESHOLD = 200;
        scheduler.shutdownNow();
    }

    /** C1: chunk de outro nó ou fora da cadeia é descartado; a cadeia recomeça do chunk 0. */
    @Test
    @Timeout(30)
    void snapshotChunksOutsideTheLeaderChainAreIgnored() throws Exception {
        try (Follower f = new Follower(tempDir, false)) {
            f.awaitAgreedLeader();
            f.deliverSyncChunk(LEADER, 10L, 0, true);
            f.awaitInstalls(1, 5_000);
            // Chunk 1 do impostor: ignorado, e a cadeia é abandonada.
            f.deliverSyncChunk(IMPOSTER, 10L, 1, true);
            Thread.sleep(300);
            assertEquals(1, f.installs.get(), "chunk de quem não é o líder acordado não é instalado");
            // Chunk 1 do líder DEPOIS do abandono: também fora da cadeia (esperado chunk 0).
            f.deliverSyncChunk(LEADER, 10L, 1, true);
            Thread.sleep(300);
            assertEquals(1, f.installs.get(), "chunk fora da ordem da cadeia não é instalado");
            // Recomeço legítimo: chunk 0 e 1 do líder, em ordem.
            f.deliverSyncChunk(LEADER, 10L, 0, true);
            f.awaitInstalls(2, 5_000);
            f.deliverSyncChunk(LEADER, 10L, 1, false);
            f.awaitInstalls(3, 5_000);
        }
    }

    /** C4: depois de stop() nenhuma escrita é aceita, mesmo que o coordinator ainda diga líder. */
    @Test
    @Timeout(30)
    void stoppedManagerRejectsWrites() throws Exception {
        NodeInfo leaderNode = new NodeInfo(LEADER, "127.0.0.1", 0);
        RecordingTransport transport = new RecordingTransport(leaderNode, List.of());
        ClusterCoordinator coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(),
                scheduler);
        coordinator.start();
        ReplicationManager manager = new ReplicationManager(transport, coordinator, ReplicationConfig.builder(1)
                .strictConsistency(false).leaderLocalApply(false)
                .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                .joinPeerDiscoveryWindow(Duration.ZERO)
                .operationTimeout(Duration.ofSeconds(5)).dataDirectory(tempDir).build());
        manager.registerHandler(TOPIC, (operationId, payload) -> {
        });
        manager.start();
        try {
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline && (!coordinator.isLeader() || manager.isLeaderSyncing())) {
                Thread.sleep(25);
            }
            manager.replicate(TOPIC, "ok".getBytes(StandardCharsets.UTF_8)).get();
            manager.stop();
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> manager.replicate(TOPIC, "late".getBytes(StandardCharsets.UTF_8)));
            assertTrue(e.getMessage().contains("shutting down"), e.getMessage());
        } finally {
            manager.close();
            coordinator.close();
        }
    }

    /** C5: uma entrada cujo apply sempre falha é dead-lettered após N falhas e o tópico segue. */
    @Test
    @Timeout(30)
    void poisonRelayEntryIsDeadLetteredAndTheTopicResumes() throws Exception {
        ReplicationManager.RELAY_DEAD_LETTER_THRESHOLD = 5;
        try (Follower f = new Follower(tempDir, true)) {
            f.awaitAgreedLeader();
            f.deliverBatch(new RelayStreamBatchPayload(TOPIC, 1L,
                    List.of(frame(1, "op-1"), frame(2, "poison"), frame(3, "op-3")), 3L, 1L, false));
            f.awaitApplied(2, 15_000);
            assertEquals(List.of("op-1", "op-3"), f.applied, "o envenenado é pulado, o resto aplica");
            assertEquals(1L, f.manager.getDeadLetteredCount());
            assertEquals(4L, f.manager.getTopicReplicationStatuses().get(TOPIC).nextExpectedSequence());
            Path deadLetterDir = tempDir.resolve("relay").resolve("dead-letter").resolve(TOPIC);
            try (var files = Files.list(deadLetterDir)) {
                assertEquals(1L, files.count(), "o frame envenenado foi guardado em disco");
            }
        }
    }

    /** C6: recusas do líder são re-tentadas com backoff, e o snapshot não é pedido em rajada. */
    @Test
    @Timeout(30)
    void leaderRefusalBacksOffAndSnapshotRequestsAreSpaced() throws Exception {
        try (Follower f = new Follower(tempDir, false)) {
            f.awaitAgreedLeader();
            f.transport.sent.clear();
            // Cada FETCH do seguidor é respondido com leaderUnavailable, como um eleito que defere.
            long until = System.currentTimeMillis() + 1_500;
            int refusals = 0;
            while (System.currentTimeMillis() < until) {
                int fetches = f.countSent(MessageType.RELAY_STREAM_FETCH);
                if (fetches > refusals) {
                    refusals = fetches;
                    f.deliverBatch(new RelayStreamBatchPayload(TOPIC, 1L, List.of(), 0L, 0L, false, true));
                }
                Thread.sleep(10);
            }
            int fetchesUnderRefusal = f.countSent(MessageType.RELAY_STREAM_FETCH);
            assertTrue(fetchesUnderRefusal <= 8, "backoff exponencial: poucos FETCH em 1,5 s (=" + fetchesUnderRefusal + ")");

            // needSnapshot em rajada: um único SYNC_REQUEST dentro do cooldown.
            f.transport.sent.clear();
            for (int i = 0; i < 5; i++) {
                f.deliverBatch(new RelayStreamBatchPayload(TOPIC, 1L, List.of(), 100L, 50L, true));
                Thread.sleep(20);
            }
            Thread.sleep(200);
            assertEquals(1, f.countSent(MessageType.SYNC_REQUEST), "snapshot pedido uma vez por janela de cooldown");
        }
    }

    private static byte[] frame(long seq, String payload) {
        return RelayEntryCodec.encode(new RelayEntry(EPOCH, seq, TOPIC, UUID.randomUUID(),
                payload.getBytes(StandardCharsets.UTF_8)));
    }

    private final class Follower implements AutoCloseable {
        final RecordingTransport transport;
        final ClusterCoordinator coordinator;
        final ReplicationManager manager;
        final List<String> applied = new CopyOnWriteArrayList<>();
        final AtomicInteger installs = new AtomicInteger();

        Follower(Path dataDir, boolean poisonFails) {
            NodeInfo followerNode = new NodeInfo(FOLLOWER, "127.0.0.1", 1);
            NodeInfo leaderNode = new NodeInfo(LEADER, "127.0.0.1", 2);
            this.transport = new RecordingTransport(followerNode, List.of(leaderNode));
            this.coordinator = new ClusterCoordinator(transport,
                    ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(60),
                            Duration.ofSeconds(60), 2, null),
                    scheduler);
            coordinator.start();
            this.manager = new ReplicationManager(transport, coordinator,
                    ReplicationConfig.builder(1)
                            .strictConsistency(false)
                            .leaderLocalApply(false)
                            .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                            .operationTimeout(Duration.ofSeconds(5))
                            .relayStreamPollInterval(Duration.ofMillis(50))
                            .dataDirectory(dataDir)
                            .build());
            manager.registerHandler(TOPIC, new ReplicationHandler() {
                @Override
                public void apply(UUID operationId, Object payload) {
                    String value = new String((byte[]) payload, StandardCharsets.UTF_8);
                    if (poisonFails && "poison".equals(value)) {
                        throw new IllegalStateException("apply envenenado");
                    }
                    applied.add(value);
                }

                @Override
                public void installSnapshot(Object snapshot) {
                    installs.incrementAndGet();
                }
            });
            manager.start();
        }

        void awaitAgreedLeader() throws InterruptedException {
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                coordinator.onMessage(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", LEADER, null,
                        HeartbeatPayload.now(-1L, EPOCH)));
                if (LEADER.equals(coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null))) {
                    return;
                }
                Thread.sleep(50);
            }
            fail("o líder acordado não convergiu para " + LEADER);
        }

        void deliverBatch(RelayStreamBatchPayload batch) {
            transport.deliverToListeners(ClusterMessage.request(MessageType.RELAY_STREAM_BATCH, "stream",
                    LEADER, FOLLOWER, batch));
        }

        void deliverSyncChunk(NodeId from, long watermark, int chunk, boolean hasMore) {
            transport.deliverToListeners(ClusterMessage.request(MessageType.SYNC_RESPONSE, "sync",
                    from, FOLLOWER, new SyncResponsePayload(TOPIC, watermark, chunk, hasMore, new byte[0])));
        }

        void awaitInstalls(int target, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (installs.get() < target && System.currentTimeMillis() < deadline) {
                Thread.sleep(25);
            }
            assertEquals(target, installs.get(), "instalações de snapshot");
        }

        void awaitApplied(int target, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (applied.size() < target && System.currentTimeMillis() < deadline) {
                Thread.sleep(25);
            }
            assertTrue(applied.size() >= target, "aplicadas=" + applied);
        }

        int countSent(MessageType type) {
            int n = 0;
            for (ClusterMessage m : transport.sent) {
                if (m.type() == type) {
                    n++;
                }
            }
            return n;
        }

        @Override
        public void close() {
            try {
                manager.close();
            } catch (Exception ignored) {
            }
            try {
                coordinator.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static final class RecordingTransport implements Transport {
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final ConcurrentHashMap<NodeId, Boolean> connected = new ConcurrentHashMap<>();
        final List<ClusterMessage> sent = new CopyOnWriteArrayList<>();

        RecordingTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            for (NodeInfo p : peers) {
                connected.put(p.nodeId(), true);
            }
        }

        void deliverToListeners(ClusterMessage message) {
            for (TransportListener l : listeners) {
                l.onMessage(message);
            }
        }

        @Override public void start() { }
        @Override public NodeInfo local() { return local; }
        @Override public Collection<NodeInfo> peers() {
            List<NodeInfo> all = new ArrayList<>();
            all.add(local);
            all.addAll(peers);
            return all;
        }
        @Override public void addPeer(NodeInfo peer) { peers.add(peer); connected.put(peer.nodeId(), true); }
        @Override public void addListener(TransportListener l) { listeners.add(l); }
        @Override public void removeListener(TransportListener l) { listeners.remove(l); }
        @Override public void broadcast(ClusterMessage m) { sent.add(m); }
        @Override public void send(ClusterMessage m) { sent.add(m); }
        @Override public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage m) {
            sent.add(m);
            CompletableFuture<ClusterMessage> f = new CompletableFuture<>();
            f.completeExceptionally(new UnsupportedOperationException("not used"));
            return f;
        }
        @Override public boolean isConnected(NodeId nodeId) { return Boolean.TRUE.equals(connected.get(nodeId)); }
        @Override public boolean isReachable(NodeId nodeId) { return isConnected(nodeId); }
        @Override public void close() throws IOException { }
    }
}
