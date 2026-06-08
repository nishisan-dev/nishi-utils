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

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.RelayStreamBatchPayload;
import dev.nishisan.utils.ngrid.common.RelayStreamFetchPayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre as correções dos comentários P1 da review:
 * <ol>
 *   <li>o follower descarta um {@code RELAY_STREAM_BATCH} de um líder NÃO acordado (fetch em voo que
 *       atravessa uma troca de liderança);</li>
 *   <li>em RELAY_STREAM, uma falha de append no op-log (binlog) FALHA o write e reusa a sequência —
 *       sem hole permanente e sem ackar um registro ausente da fonte do stream.</li>
 * </ol>
 */
class RelayStreamReviewFixesTest {

    private static final String TOPIC = "cardinal-state";
    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("relay-stream-review");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void followerIgnoresBatchFromNonAgreedLeader() throws Exception {
        NodeId leader = NodeId.of("zzz-leader");
        NodeId follower = NodeId.of("aaa-follower");
        NodeId imposter = NodeId.of("mmm-other");
        NodeInfo leaderNode = new NodeInfo(leader, "127.0.0.1", 0);
        NodeInfo followerNode = new NodeInfo(follower, "127.0.0.1", 0);

        RecordingTransport transport = new RecordingTransport(followerNode, List.of(leaderNode));
        ClusterCoordinator coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(),
                scheduler);
        coordinator.start();
        transport.simulatePeerConnected(leaderNode); // leader = max NodeId -> agreed leader

        ReplicationManager rm = new ReplicationManager(transport, coordinator, ReplicationConfig.builder(1)
                .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                .operationTimeout(Duration.ofSeconds(5)).dataDirectory(tempDir).build());
        rm.registerHandler(TOPIC, (operationId, payload) -> {
        });
        rm.start();
        try {
            byte[] frame1 = RelayEntryCodec.encode(new RelayEntry(1L, 1L, TOPIC, UUID.randomUUID(), bytes("op-1")));

            // Batch from the IMPOSTER (not the agreed leader): must be ignored — cursor stays at 0.
            transport.deliver(ClusterMessage.request(MessageType.RELAY_STREAM_BATCH, "stream", imposter, follower,
                    new RelayStreamBatchPayload(TOPIC, 1L, List.of(frame1), 1L, 1L, false)));
            Thread.sleep(200);
            assertEquals(0L, rm.getRelayStreamCursor(TOPIC),
                    "a batch from a non-agreed leader must not advance the cursor");

            // Same batch from the AGREED leader: accepted — cursor advances to 1.
            transport.deliver(ClusterMessage.request(MessageType.RELAY_STREAM_BATCH, "stream", leader, follower,
                    new RelayStreamBatchPayload(TOPIC, 1L, List.of(frame1), 1L, 1L, false)));
            long deadline = System.currentTimeMillis() + 3000;
            while (rm.getRelayStreamCursor(TOPIC) < 1L && System.currentTimeMillis() < deadline) {
                Thread.sleep(25);
            }
            assertEquals(1L, rm.getRelayStreamCursor(TOPIC),
                    "a batch from the agreed leader must advance the cursor");
        } finally {
            rm.close();
            coordinator.close();
        }
    }

    @Test
    void failedOpLogAppendFailsWriteAndDoesNotBurnSequence() throws Exception {
        NodeId leader = NodeId.of("zzz-leader");
        NodeId peer = NodeId.of("aaa-peer");
        NodeInfo leaderNode = new NodeInfo(leader, "127.0.0.1", 0);
        NodeInfo peerNode = new NodeInfo(peer, "127.0.0.1", 0);

        RecordingTransport transport = new RecordingTransport(leaderNode, List.of(peerNode));
        ClusterCoordinator coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(),
                scheduler);
        coordinator.start();
        transport.simulatePeerConnected(peerNode);

        ReplicationManager rm = new ReplicationManager(transport, coordinator, ReplicationConfig.builder(1)
                .strictConsistency(false).leaderLocalApply(false)
                .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                .operationTimeout(Duration.ofSeconds(5)).dataDirectory(tempDir).build());
        // Handler whose encodePayload throws for a "POISON" payload — simulates an op-log append failure
        // on the emission path (appendStreamOpLog catches it and returns false).
        rm.registerHandler(TOPIC, new ReplicationHandler() {
            @Override
            public void apply(UUID operationId, Object payload) {
            }

            @Override
            public byte[] encodePayload(Object payload) {
                byte[] b = (byte[]) payload;
                if (new String(b, StandardCharsets.UTF_8).startsWith("POISON")) {
                    throw new RuntimeException("simulated op-log append failure");
                }
                return b;
            }

            @Override
            public Object decodePayload(byte[] payloadBytes) {
                return payloadBytes;
            }
        });
        rm.start();
        try {
            rm.replicate(TOPIC, bytes("OK1")).get(2, TimeUnit.SECONDS); // seq 1

            // The poisoned write must FAIL (not be acked) — the binlog append is load-bearing.
            ExecutionException ex = assertThrows(ExecutionException.class,
                    () -> rm.replicate(TOPIC, bytes("POISON")).get(2, TimeUnit.SECONDS));
            assertTrue(ex.getMessage() == null || ex.getMessage().contains("op-log")
                    || ex.getCause() != null, "poisoned write should fail with the append error");

            rm.replicate(TOPIC, bytes("OK2")).get(2, TimeUnit.SECONDS); // reuses seq 2 (not burned)
            Thread.sleep(150);

            // The op-log is contiguous [1, 2] with NO hole at the burned sequence — verified by serving
            // the stream fetch.
            transport.clearSent();
            transport.deliver(ClusterMessage.request(MessageType.RELAY_STREAM_FETCH, "stream", peer, leader,
                    new RelayStreamFetchPayload(TOPIC, 1L, 10)));
            RelayStreamBatchPayload served = awaitBatch(transport);
            assertEquals(2, served.frames().size(), "op-log must hold exactly [1, 2] (POISON rolled back)");
            assertEquals(1L, RelayEntryCodec.decode(served.frames().get(0)).sequence());
            assertEquals(2L, RelayEntryCodec.decode(served.frames().get(1)).sequence());
        } finally {
            rm.close();
            coordinator.close();
        }
    }

    private static RelayStreamBatchPayload awaitBatch(RecordingTransport transport) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            for (ClusterMessage m : transport.sent()) {
                if (m.type() == MessageType.RELAY_STREAM_BATCH) {
                    return m.payload(RelayStreamBatchPayload.class);
                }
            }
            Thread.sleep(25);
        }
        throw new AssertionError("leader did not serve a RELAY_STREAM_BATCH");
    }

    private static final class RecordingTransport implements Transport {
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final ConcurrentHashMap<NodeId, Boolean> connected = new ConcurrentHashMap<>();
        private final List<ClusterMessage> sent = new CopyOnWriteArrayList<>();

        RecordingTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            for (NodeInfo p : peers) {
                connected.put(p.nodeId(), true);
            }
        }

        @Override
        public void start() {
        }

        @Override
        public NodeInfo local() {
            return local;
        }

        @Override
        public Collection<NodeInfo> peers() {
            List<NodeInfo> all = new ArrayList<>();
            all.add(local);
            all.addAll(peers);
            return all;
        }

        @Override
        public void addListener(TransportListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeListener(TransportListener listener) {
            listeners.remove(listener);
        }

        @Override
        public void broadcast(ClusterMessage message) {
            sent.add(message);
        }

        @Override
        public void send(ClusterMessage message) {
            sent.add(message);
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            sent.add(message);
            CompletableFuture<ClusterMessage> f = new CompletableFuture<>();
            f.completeExceptionally(new UnsupportedOperationException("not used"));
            return f;
        }

        @Override
        public boolean isConnected(NodeId nodeId) {
            return Boolean.TRUE.equals(connected.get(nodeId));
        }

        @Override
        public boolean isReachable(NodeId nodeId) {
            return isConnected(nodeId);
        }

        @Override
        public void addPeer(NodeInfo peer) {
        }

        void simulatePeerConnected(NodeInfo peer) {
            connected.put(peer.nodeId(), true);
            for (TransportListener l : listeners) {
                l.onPeerConnected(peer);
            }
        }

        void deliver(ClusterMessage message) {
            for (TransportListener l : listeners) {
                l.onMessage(message);
            }
        }

        List<ClusterMessage> sent() {
            return List.copyOf(sent);
        }

        void clearSent() {
            sent.clear();
        }

        @Override
        public void close() throws IOException {
        }
    }
}
