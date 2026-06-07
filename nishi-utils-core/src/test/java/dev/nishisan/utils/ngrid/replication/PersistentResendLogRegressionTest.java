package dev.nishisan.utils.ngrid.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.ReplicationAckPayload;
import dev.nishisan.utils.ngrid.common.ReplicationPayload;
import dev.nishisan.utils.ngrid.common.SequenceResendRequestPayload;
import dev.nishisan.utils.ngrid.common.SequenceResendResponsePayload;

/**
 * Regression for #127: under high production the leader's resend op-log window collapsed because
 * the count cap (heap memory bound) won over the temporal window, so a follower's bootstrap-gap
 * resend hit already-evicted sequences → snapshot fallback in a loop (the #124 death spiral).
 *
 * <p>This reproduces the mechanism deterministically with a tiny count cap and a large temporal
 * window: with the in-heap-only op-log, the early sequences are gone and the leader reports them
 * missing (forcing snapshot); with the disk-backed op-log ({@code persistentResendLog}), the same
 * sequences are still served from disk and the resend succeeds — no fallback.
 */
class PersistentResendLogRegressionTest {

    private static final String TOPIC = "cardinal-state";
    private static final NodeInfo LEADER = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
    private static final NodeInfo PEER = new NodeInfo(NodeId.of("aaa-peer"), "127.0.0.1", 0);

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("persistent-resend-test");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    void heapOnlyOpLogReportsBootstrapGapAsMissing() throws Exception {
        SequenceResendResponsePayload response = runResendAfterHeapEviction(false);
        assertTrue(response.operations().isEmpty(),
                "heap-only op-log must NOT serve the evicted bootstrap gap");
        assertEquals(2, response.missingSequences().size(),
                "the evicted early sequences are reported missing → snapshot fallback (the #124 spiral)");
    }

    @Test
    void persistentOpLogServesBootstrapGapFromDisk() throws Exception {
        SequenceResendResponsePayload response = runResendAfterHeapEviction(true);
        assertEquals(2, response.operations().size(),
                "disk-backed op-log must serve the bootstrap gap from the time-governed window");
        assertTrue(response.missingSequences().isEmpty(),
                "no missing sequences → no snapshot fallback, spiral eliminated");
        List<Long> seqs = new ArrayList<>();
        response.operations().forEach(op -> seqs.add(op.sequence()));
        assertEquals(List.of(1L, 2L), seqs);
        // Payload integrity: the disk round-trip must reproduce the committed bytes.
        assertEquals("data-1", new String((byte[]) response.operations().get(0).data(), StandardCharsets.UTF_8));
    }

    private SequenceResendResponsePayload runResendAfterHeapEviction(boolean persistent) throws Exception {
        RecordingTransport transport = new RecordingTransport(LEADER, List.of(PEER));
        ClusterCoordinator coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(),
                scheduler);
        coordinator.start();
        transport.simulatePeerConnected(PEER);
        assertTrue(coordinator.isLeader(), "zzz-leader must be elected leader");

        ReplicationManager leader = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(2)
                        .operationTimeout(Duration.ofSeconds(5))
                        .dataDirectory(tempDir)
                        // Tiny count cap (memory bound) + large temporal window: the exact regime where
                        // the count cap collapses the heap window under load.
                        .replicationLogRetention(3)
                        .replicationLogRetentionTime(Duration.ofMinutes(30))
                        .persistentResendLog(persistent)
                        .build());
        leader.registerHandler(TOPIC, (operationId, payload) -> {
        });
        leader.start();

        try {
            // Commit 8 ops (seq 1..8). The heap keeps only the last 3 (6,7,8); seq 1..5 are evicted
            // from heap but remain on disk within the 30-min window when persistence is on.
            for (int i = 1; i <= 8; i++) {
                replicateAndCommit(leader, transport, ("data-" + i).getBytes(StandardCharsets.UTF_8));
            }
            // Indexing runs just after the operation future completes (on the commit thread), so await
            // it settling before probing the op-log. Heap-only caps at the count retention (3).
            awaitLogSize(leader, persistent ? 8 : 3);

            // A catching-up follower asks for the bootstrap gap [1..2] (evicted from the heap cache).
            transport.clearSent();
            SequenceResendRequestPayload resend = new SequenceResendRequestPayload(TOPIC, 1, 2);
            ClusterMessage request = ClusterMessage.request(MessageType.SEQUENCE_RESEND_REQUEST, "resend",
                    PEER.nodeId(), LEADER.nodeId(), resend);
            transport.deliverToListeners(request);

            ClusterMessage response = awaitMessage(transport, MessageType.SEQUENCE_RESEND_RESPONSE);
            return response.payload(SequenceResendResponsePayload.class);
        } finally {
            leader.close();
            coordinator.close();
        }
    }

    private void replicateAndCommit(ReplicationManager leader, RecordingTransport transport, byte[] data)
            throws Exception {
        int before = transport.countReplicationRequests(TOPIC);
        CompletableFuture<?> future = leader.replicate(TOPIC, data);
        ClusterMessage request = awaitReplicationRequest(transport, before + 1);
        ReplicationPayload payload = request.payload(ReplicationPayload.class);
        ClusterMessage ack = ClusterMessage.request(MessageType.REPLICATION_ACK, "ack",
                PEER.nodeId(), LEADER.nodeId(), new ReplicationAckPayload(payload.operationId(), true));
        transport.deliverToListeners(ack);
        future.get(2, TimeUnit.SECONDS);
    }

    private ClusterMessage awaitReplicationRequest(RecordingTransport transport, int minCount) throws Exception {
        long deadline = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < deadline) {
            List<ClusterMessage> reqs = transport.getSentMessages().stream()
                    .filter(m -> m.type() == MessageType.REPLICATION_REQUEST && TOPIC.equals(m.qualifier()))
                    .toList();
            if (reqs.size() >= minCount) {
                return reqs.get(reqs.size() - 1);
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Expected REPLICATION_REQUEST was not sent");
    }

    private void awaitLogSize(ReplicationManager leader, int expected) throws Exception {
        long deadline = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < deadline) {
            if (leader.getReplicationLogSize(TOPIC) >= expected) {
                return;
            }
            Thread.sleep(10);
        }
        assertEquals(expected, leader.getReplicationLogSize(TOPIC),
                "op-log size must reflect the active tier");
    }

    private ClusterMessage awaitMessage(RecordingTransport transport, MessageType type) throws Exception {
        long deadline = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < deadline) {
            for (ClusterMessage m : transport.getSentMessages()) {
                if (m.type() == type) {
                    return m;
                }
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Expected message of type " + type + " was not sent");
    }

    /** Minimal in-memory transport that records sent frames and delivers inbound frames to listeners. */
    private static final class RecordingTransport implements Transport {
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final ConcurrentHashMap<NodeId, Boolean> connected = new ConcurrentHashMap<>();
        private final List<ClusterMessage> sent = new CopyOnWriteArrayList<>();

        private RecordingTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            peers.forEach(p -> connected.put(p.nodeId(), true));
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

        @Override
        public void close() throws IOException {
        }

        void simulatePeerConnected(NodeInfo peer) {
            connected.put(peer.nodeId(), true);
            listeners.forEach(l -> l.onPeerConnected(peer));
        }

        void deliverToListeners(ClusterMessage message) {
            listeners.forEach(l -> l.onMessage(message));
        }

        List<ClusterMessage> getSentMessages() {
            return List.copyOf(sent);
        }

        int countReplicationRequests(String topic) {
            return (int) sent.stream()
                    .filter(m -> m.type() == MessageType.REPLICATION_REQUEST && topic.equals(m.qualifier()))
                    .count();
        }

        void clearSent() {
            sent.clear();
        }
    }
}
