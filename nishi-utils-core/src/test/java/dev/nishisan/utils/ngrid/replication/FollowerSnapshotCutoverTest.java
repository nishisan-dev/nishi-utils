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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.ReplicationPayload;
import dev.nishisan.utils.ngrid.common.SequenceResendResponsePayload;
import dev.nishisan.utils.ngrid.common.SyncResponsePayload;

class FollowerSnapshotCutoverTest {

    private static final String TOPIC = "cardinal-state";

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("snapshot-cutover-test");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void relayModeSnapshotCutoverDiscardsOldRelayPrefixAndAppliesTail() throws Exception {
        NodeInfo leaderNode = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        NodeInfo followerNode = new NodeInfo(NodeId.of("aaa-follower"), "127.0.0.1", 0);

        RecordingTransport transport = new RecordingTransport(followerNode, List.of(leaderNode));
        ClusterCoordinator coordinator = new ClusterCoordinator(
                transport, ClusterCoordinatorConfig.defaults(), scheduler);
        coordinator.start();
        transport.simulatePeerConnected(leaderNode);

        RecordingHandler handler = new RecordingHandler();
        ReplicationManager follower = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(2)
                        .dataDirectory(tempDir)
                        .followerIngestMode(FollowerIngestMode.RELAY_LOG)
                        .relayApplyBatchSize(4)
                        .build());
        follower.registerHandler(TOPIC, handler);
        follower.start();

        try {
            deliverSyncResponse(transport, leaderNode, followerNode, 5L);
            await(() -> follower.getTopicReplicationStatuses().get(TOPIC).nextExpectedSequence() == 6L,
                    5_000, "snapshot cutover did not anchor nextExpected");

            for (long seq = 1; seq <= 7; seq++) {
                deliverReplication(transport, leaderNode, followerNode, seq, "tail-" + seq);
            }

            await(() -> handler.applied().equals(List.of("tail-6", "tail-7")),
                    5_000, "post-watermark relay tail was not applied");

            ReplicationManager.TopicReplicationStatus status = follower.getTopicReplicationStatuses().get(TOPIC);
            assertEquals(8L, status.nextExpectedSequence());
            assertEquals(0L, status.relayBacklog());
            assertEquals(0L, status.relayHeadSequence());
            assertEquals(0L, follower.getGapsDetected(),
                    "stale relay prefix must not be counted as gaps after snapshot cutover");
            assertFalse(transport.getSentMessages().stream()
                    .anyMatch(m -> m.type() == MessageType.SEQUENCE_RESEND_REQUEST),
                    "stale relay prefix must not request resend");
        } finally {
            follower.close();
            coordinator.close();
        }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void relayModeResendResponseBuffersAtReorderHeadAndUnblocksFutureRelayEntry() throws Exception {
        NodeInfo leaderNode = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        NodeInfo followerNode = new NodeInfo(NodeId.of("aaa-follower"), "127.0.0.1", 0);

        RecordingTransport transport = new RecordingTransport(followerNode, List.of(leaderNode));
        ClusterCoordinator coordinator = new ClusterCoordinator(
                transport, ClusterCoordinatorConfig.defaults(), scheduler);
        coordinator.start();
        transport.simulatePeerConnected(leaderNode);

        RecordingHandler handler = new RecordingHandler();
        ReplicationManager follower = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(2)
                        .dataDirectory(tempDir)
                        .followerIngestMode(FollowerIngestMode.RELAY_LOG)
                        .relayApplyBatchSize(4)
                        .build());
        follower.registerHandler(TOPIC, handler);
        follower.start();

        try {
            deliverSyncResponse(transport, leaderNode, followerNode, 0L);
            await(() -> follower.getTopicReplicationStatuses().get(TOPIC).nextExpectedSequence() == 1L,
                    5_000, "snapshot cutover did not initialize nextExpected");

            deliverReplication(transport, leaderNode, followerNode, 3L, "future-3");

            await(() -> transport.getSentMessages().stream()
                    .anyMatch(m -> m.type() == MessageType.SEQUENCE_RESEND_REQUEST),
                    5_000, "relay gap did not request resend");

            SequenceResendResponsePayload responsePayload = new SequenceResendResponsePayload(
                    TOPIC,
                    List.of(
                            replicationPayload(1L, "head-1"),
                            replicationPayload(2L, "head-2")),
                    List.of());
            ClusterMessage response = ClusterMessage.request(
                    MessageType.SEQUENCE_RESEND_RESPONSE,
                    "resend",
                    leaderNode.nodeId(),
                    followerNode.nodeId(),
                    responsePayload);
            transport.deliverToListeners(response);

            await(() -> handler.applied().equals(List.of("head-1", "head-2", "future-3")),
                    5_000, "resent relay prefix did not unblock the future relay entry");

            assertEquals(4L, follower.getTopicReplicationStatuses().get(TOPIC).nextExpectedSequence());
            assertEquals(0L, follower.getTopicReplicationStatuses().get(TOPIC).relayBacklog());
            assertEquals(0L, follower.getSnapshotFallbackCount());
            assertEquals(1L, transport.getSentMessages().stream()
                    .filter(m -> m.type() == MessageType.SEQUENCE_RESEND_REQUEST)
                    .count(), "resend response should unblock the original gap without overlapping requests");
        } finally {
            follower.close();
            coordinator.close();
        }
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    void staleMissingResendResponseAfterFrontierAdvanceDoesNotTriggerSnapshot() throws Exception {
        NodeInfo leaderNode = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        NodeInfo followerNode = new NodeInfo(NodeId.of("aaa-follower"), "127.0.0.1", 0);

        RecordingTransport transport = new RecordingTransport(followerNode, List.of(leaderNode));
        ClusterCoordinator coordinator = new ClusterCoordinator(
                transport, ClusterCoordinatorConfig.defaults(), scheduler);
        coordinator.start();
        transport.simulatePeerConnected(leaderNode);

        ReplicationManager follower = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(2)
                        .dataDirectory(tempDir)
                        .resendTimeout(Duration.ofSeconds(5))
                        .build());
        follower.registerHandler(TOPIC, (operationId, payload) -> {
        });
        follower.start();

        try {
            deliverReplication(transport, leaderNode, followerNode, 1L, "applied");
            await(() -> follower.getTopicReplicationStatuses().get(TOPIC).nextExpectedSequence() == 2L,
                    5_000, "frontier did not advance before stale resend response");

            SequenceResendResponsePayload staleResponse = new SequenceResendResponsePayload(
                    TOPIC, List.of(), List.of(1L));
            ClusterMessage response = ClusterMessage.request(
                    MessageType.SEQUENCE_RESEND_RESPONSE,
                    "resend",
                    leaderNode.nodeId(),
                    followerNode.nodeId(),
                    staleResponse);
            transport.deliverToListeners(response);
            Thread.sleep(250);

            assertEquals(0L, follower.getSnapshotFallbackCount(),
                    "stale missing sequence below nextExpected must not force snapshot fallback");
            assertFalse(transport.getSentMessages().stream()
                    .anyMatch(m -> m.type() == MessageType.SYNC_REQUEST),
                    "stale missing sequence below nextExpected must not request sync");
        } finally {
            follower.close();
            coordinator.close();
        }
    }

    private static void deliverSyncResponse(RecordingTransport transport, NodeInfo leaderNode, NodeInfo followerNode,
            long watermark) {
        SyncResponsePayload payload = new SyncResponsePayload(
                TOPIC,
                watermark,
                0,
                false,
                "snapshot".getBytes(StandardCharsets.UTF_8));
        ClusterMessage response = ClusterMessage.request(
                MessageType.SYNC_RESPONSE,
                TOPIC,
                leaderNode.nodeId(),
                followerNode.nodeId(),
                payload);
        transport.deliverToListeners(response);
    }

    private static void deliverReplication(RecordingTransport transport, NodeInfo leaderNode, NodeInfo followerNode,
            long sequence, String value) {
        ReplicationPayload payload = replicationPayload(sequence, value);
        ClusterMessage message = ClusterMessage.request(
                MessageType.REPLICATION_REQUEST,
                TOPIC,
                leaderNode.nodeId(),
                followerNode.nodeId(),
                payload);
        transport.deliverToListeners(message);
    }

    private static ReplicationPayload replicationPayload(long sequence, String value) {
        return new ReplicationPayload(
                UUID.randomUUID(),
                sequence,
                1L,
                TOPIC,
                value.getBytes(StandardCharsets.UTF_8));
    }

    private static void await(Condition condition, long timeoutMs, String failureMessage) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.matches()) {
                return;
            }
            Thread.sleep(25);
        }
        assertTrue(condition.matches(), failureMessage);
    }

    @FunctionalInterface
    private interface Condition {
        boolean matches() throws Exception;
    }

    private static final class RecordingHandler implements ReplicationHandler {
        private final List<String> applied = new CopyOnWriteArrayList<>();
        private volatile boolean installed;

        @Override
        public void apply(UUID operationId, Object payload) {
            applied.add(new String((byte[]) payload, StandardCharsets.UTF_8));
        }

        @Override
        public void resetState() {
            applied.clear();
            installed = false;
        }

        @Override
        public void installSnapshot(Object snapshot) {
            installed = true;
        }

        List<String> applied() {
            if (!installed) {
                return List.of();
            }
            return List.copyOf(applied);
        }
    }

    private static final class RecordingTransport implements Transport {
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final ConcurrentHashMap<NodeId, Boolean> connected = new ConcurrentHashMap<>();
        private final List<ClusterMessage> sent = new CopyOnWriteArrayList<>();

        private RecordingTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            peers.forEach(peer -> connected.put(peer.nodeId(), true));
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
            CompletableFuture<ClusterMessage> future = new CompletableFuture<>();
            future.completeExceptionally(new UnsupportedOperationException("not used"));
            return future;
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
            listeners.forEach(listener -> listener.onPeerConnected(peer));
        }

        void deliverToListeners(ClusterMessage message) {
            listeners.forEach(listener -> listener.onMessage(message));
        }

        List<ClusterMessage> getSentMessages() {
            return List.copyOf(sent);
        }
    }
}
