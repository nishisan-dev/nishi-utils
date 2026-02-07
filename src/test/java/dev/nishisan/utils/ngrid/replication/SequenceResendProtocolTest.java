package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the Sequence Resend Protocol (Phase 3 - Intelligent Sequence
 * Gap Recovery).
 *
 * Uses a RecordingTransport to verify:
 * - Small gaps trigger SEQUENCE_RESEND_REQUEST instead of full snapshot sync.
 * - Leader responds with operations from its replication log.
 * - Leader responds with missingSequences when log is empty.
 * - Follower applies resent operations and advances its sequence.
 * - Gaps exceeding the threshold trigger direct snapshot fallback.
 * - Metrics are correctly updated.
 *
 * <p>
 * <strong>Important test patterns:</strong>
 * </p>
 * <ul>
 * <li>Leader election: ClusterCoordinator elects the node with the HIGHEST
 * NodeId (lexicographic max).</li>
 * <li>Gap detection: checkForMissingSequences needs SEQUENCE_WAIT_TIMEOUT (1s)
 * to expire.
 * A second replication request must arrive AFTER 1s to re-trigger the
 * check.</li>
 * </ul>
 */
class SequenceResendProtocolTest {

    private static final String TOPIC = "test-topic";
    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("resend-protocol-test");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    // ────────────────────────────────────────────────
    // Test 1: Small gap sends SEQUENCE_RESEND_REQUEST
    // ────────────────────────────────────────────────

    @Test
    void testResendRequestSentOnSmallGap() throws Exception {
        // follower = "aaa-follower" (lower ID → not leader)
        // leader = "zzz-leader" (higher ID → becomes leader)
        NodeInfo leaderNode = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        NodeInfo followerNode = new NodeInfo(NodeId.of("aaa-follower"), "127.0.0.1", 0);

        RecordingTransport followerTransport = new RecordingTransport(followerNode, List.of(leaderNode));
        ClusterCoordinator followerCoordinator = new ClusterCoordinator(
                followerTransport, ClusterCoordinatorConfig.defaults(), scheduler);
        followerCoordinator.start();
        followerTransport.simulatePeerConnected(leaderNode);

        // Confirm follower is NOT leader
        assertFalse(followerCoordinator.isLeader(), "aaa-follower should NOT be leader");

        ReplicationManager followerManager = new ReplicationManager(followerTransport, followerCoordinator,
                ReplicationConfig.builder(2)
                        .operationTimeout(Duration.ofSeconds(5))
                        .dataDirectory(tempDir)
                        .resendGapThreshold(10)
                        .resendTimeout(Duration.ofSeconds(2))
                        .build());
        followerManager.registerHandler(TOPIC, (operationId, payload) -> {
        });
        followerManager.start();

        try {
            // Send seq=2 when follower expects seq=1 → gap of 1
            UUID opId2 = UUID.randomUUID();
            ReplicationPayload payloadSeq2 = new ReplicationPayload(opId2, 2, 1, TOPIC, "data-2");
            ClusterMessage msg2 = ClusterMessage.request(
                    MessageType.REPLICATION_REQUEST, null,
                    leaderNode.nodeId(), followerNode.nodeId(), payloadSeq2);
            followerTransport.deliverToListeners(msg2);

            // Wait > SEQUENCE_WAIT_TIMEOUT (1s)
            Thread.sleep(1200);

            // Send another message (seq=3) to re-trigger checkForMissingSequences after
            // timeout
            UUID opId3 = UUID.randomUUID();
            ReplicationPayload payloadSeq3 = new ReplicationPayload(opId3, 3, 1, TOPIC, "data-3");
            ClusterMessage msg3 = ClusterMessage.request(
                    MessageType.REPLICATION_REQUEST, null,
                    leaderNode.nodeId(), followerNode.nodeId(), payloadSeq3);
            followerTransport.deliverToListeners(msg3);

            // Small wait for processing
            Thread.sleep(300);

            List<ClusterMessage> sentMessages = followerTransport.getSentMessages();
            boolean hasResendRequest = sentMessages.stream()
                    .anyMatch(m -> m.type() == MessageType.SEQUENCE_RESEND_REQUEST);
            boolean hasSyncRequest = sentMessages.stream()
                    .anyMatch(m -> m.type() == MessageType.SYNC_REQUEST);

            assertTrue(hasResendRequest, "Should have sent SEQUENCE_RESEND_REQUEST for small gap");
            assertFalse(hasSyncRequest, "Should NOT have sent SYNC_REQUEST for small gap");

            // Verify the resend request payload
            ClusterMessage resendReq = sentMessages.stream()
                    .filter(m -> m.type() == MessageType.SEQUENCE_RESEND_REQUEST)
                    .findFirst().orElseThrow();
            SequenceResendRequestPayload reqPayload = resendReq.payload(SequenceResendRequestPayload.class);
            assertEquals(TOPIC, reqPayload.topic());
            assertEquals(1, reqPayload.fromSequence());

            // Verify metrics
            assertTrue(followerManager.getGapsDetected() > 0, "gapsDetected should be > 0");

        } finally {
            followerManager.close();
            followerCoordinator.close();
        }
    }

    // ────────────────────────────────────────────────
    // Test 2: Leader handles resend request correctly
    // ────────────────────────────────────────────────

    @Test
    void testLeaderRespondsToResendRequestWithOperations() throws Exception {
        // leader = "zzz-leader" (highest → elected leader)
        // peer = "aaa-peer" (lowest → follower)
        NodeInfo leaderNode = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        NodeInfo peerNode = new NodeInfo(NodeId.of("aaa-peer"), "127.0.0.1", 0);

        RecordingTransport leaderTransport = new RecordingTransport(leaderNode, List.of(peerNode));
        ClusterCoordinator leaderCoordinator = new ClusterCoordinator(
                leaderTransport, ClusterCoordinatorConfig.defaults(), scheduler);
        leaderCoordinator.start();
        leaderTransport.simulatePeerConnected(peerNode);

        assertTrue(leaderCoordinator.isLeader(), "zzz-leader should be elected as leader");

        ReplicationManager leaderManager = new ReplicationManager(leaderTransport, leaderCoordinator,
                ReplicationConfig.builder(2)
                        .operationTimeout(Duration.ofSeconds(5))
                        .dataDirectory(tempDir)
                        .build());
        leaderManager.registerHandler(TOPIC, (operationId, payload) -> {
        });
        leaderManager.start();

        try {
            // Replicate an operation to populate the replication log
            leaderManager.replicate(TOPIC, "test-data");
            Thread.sleep(200);

            // Commit the operation: emulate ACK from peer so leader marks it COMMITTED.
            ClusterMessage replicationRequest = leaderTransport.getSentMessages().stream()
                    .filter(m -> m.type() == MessageType.REPLICATION_REQUEST && TOPIC.equals(m.qualifier()))
                    .findFirst()
                    .orElseThrow();
            ReplicationPayload replicated = replicationRequest.payload(ReplicationPayload.class);
            ClusterMessage ack = ClusterMessage.request(
                    MessageType.REPLICATION_ACK,
                    "ack",
                    peerNode.nodeId(),
                    leaderNode.nodeId(),
                    new ReplicationAckPayload(replicated.operationId(), true));
            leaderTransport.deliverToListeners(ack);
            Thread.sleep(150);

            // Now simulate a SEQUENCE_RESEND_REQUEST from the follower
            SequenceResendRequestPayload resendRequest = new SequenceResendRequestPayload(TOPIC, 1, 1);
            ClusterMessage request = ClusterMessage.request(
                    MessageType.SEQUENCE_RESEND_REQUEST, "resend",
                    peerNode.nodeId(), leaderNode.nodeId(), resendRequest);
            leaderTransport.deliverToListeners(request);

            Thread.sleep(200);

            List<ClusterMessage> sentMessages = leaderTransport.getSentMessages();
            Optional<ClusterMessage> resendResponse = sentMessages.stream()
                    .filter(m -> m.type() == MessageType.SEQUENCE_RESEND_RESPONSE)
                    .findFirst();

            assertTrue(resendResponse.isPresent(), "Leader should have sent SEQUENCE_RESEND_RESPONSE");

            SequenceResendResponsePayload responsePayload = resendResponse.get()
                    .payload(SequenceResendResponsePayload.class);
            assertEquals(TOPIC, responsePayload.topic());
            assertFalse(responsePayload.operations().isEmpty(), "Response should contain operations");
            assertTrue(responsePayload.missingSequences().isEmpty(), "Response should have no missing sequences");

            ReplicationPayload resentOp = responsePayload.operations().get(0);
            assertEquals(1, resentOp.sequence());
            assertEquals(TOPIC, resentOp.topic());

        } finally {
            leaderManager.close();
            leaderCoordinator.close();
        }
    }

    // ────────────────────────────────────────────────
    // Test 3: Leader responds with missingSequences when log is empty
    // ────────────────────────────────────────────────

    @Test
    void testLeaderRespondsMissingSequencesWhenLogIsEmpty() throws Exception {
        NodeInfo leaderNode = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        NodeInfo peerNode = new NodeInfo(NodeId.of("aaa-peer"), "127.0.0.1", 0);

        RecordingTransport leaderTransport = new RecordingTransport(leaderNode, List.of(peerNode));
        ClusterCoordinator leaderCoordinator = new ClusterCoordinator(
                leaderTransport, ClusterCoordinatorConfig.defaults(), scheduler);
        leaderCoordinator.start();
        leaderTransport.simulatePeerConnected(peerNode);

        assertTrue(leaderCoordinator.isLeader(), "zzz-leader should be elected as leader");

        ReplicationManager leaderManager = new ReplicationManager(leaderTransport, leaderCoordinator,
                ReplicationConfig.builder(2)
                        .operationTimeout(Duration.ofSeconds(5))
                        .dataDirectory(tempDir)
                        .build());
        leaderManager.registerHandler(TOPIC, (operationId, payload) -> {
        });
        leaderManager.start();

        try {
            // Send a resend request for sequences the leader doesn't have (no replication
            // done yet)
            SequenceResendRequestPayload resendRequest = new SequenceResendRequestPayload(TOPIC, 1, 5);
            ClusterMessage request = ClusterMessage.request(
                    MessageType.SEQUENCE_RESEND_REQUEST, "resend",
                    peerNode.nodeId(), leaderNode.nodeId(), resendRequest);
            leaderTransport.deliverToListeners(request);

            Thread.sleep(200);

            List<ClusterMessage> sentMessages = leaderTransport.getSentMessages();
            Optional<ClusterMessage> resendResponse = sentMessages.stream()
                    .filter(m -> m.type() == MessageType.SEQUENCE_RESEND_RESPONSE)
                    .findFirst();

            assertTrue(resendResponse.isPresent(), "Leader should have sent SEQUENCE_RESEND_RESPONSE");

            SequenceResendResponsePayload responsePayload = resendResponse.get()
                    .payload(SequenceResendResponsePayload.class);
            assertTrue(responsePayload.operations().isEmpty(), "Response should have no operations");
            assertEquals(5, responsePayload.missingSequences().size(),
                    "All 5 requested sequences should be reported as missing");

        } finally {
            leaderManager.close();
            leaderCoordinator.close();
        }
    }

    // ────────────────────────────────────────────────
    // Test 4: Large gap goes directly to snapshot sync
    // ────────────────────────────────────────────────

    @Test
    void testDirectSnapshotOnLargeGap() throws Exception {
        NodeInfo leaderNode = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        NodeInfo followerNode = new NodeInfo(NodeId.of("aaa-follower"), "127.0.0.1", 0);

        RecordingTransport followerTransport = new RecordingTransport(followerNode, List.of(leaderNode));
        ClusterCoordinator followerCoordinator = new ClusterCoordinator(
                followerTransport, ClusterCoordinatorConfig.defaults(), scheduler);
        followerCoordinator.start();
        followerTransport.simulatePeerConnected(leaderNode);

        assertFalse(followerCoordinator.isLeader(), "aaa-follower should NOT be leader");

        ReplicationManager followerManager = new ReplicationManager(followerTransport, followerCoordinator,
                ReplicationConfig.builder(2)
                        .operationTimeout(Duration.ofSeconds(5))
                        .dataDirectory(tempDir)
                        .resendGapThreshold(5) // Very small threshold
                        .build());
        followerManager.registerHandler(TOPIC, (operationId, payload) -> {
        });
        followerManager.start();

        try {
            // Send seq=20 when expecting seq=1 → gap=19 > threshold=5
            UUID opId = UUID.randomUUID();
            ReplicationPayload payload = new ReplicationPayload(opId, 20, 1, TOPIC, "data-20");
            ClusterMessage msg = ClusterMessage.request(
                    MessageType.REPLICATION_REQUEST, null,
                    leaderNode.nodeId(), followerNode.nodeId(), payload);
            followerTransport.deliverToListeners(msg);

            // Wait > SEQUENCE_WAIT_TIMEOUT (1s)
            Thread.sleep(1200);

            // Re-trigger gap check: send yet another higher sequence
            UUID opId2 = UUID.randomUUID();
            ReplicationPayload payload2 = new ReplicationPayload(opId2, 21, 1, TOPIC, "data-21");
            ClusterMessage msg2 = ClusterMessage.request(
                    MessageType.REPLICATION_REQUEST, null,
                    leaderNode.nodeId(), followerNode.nodeId(), payload2);
            followerTransport.deliverToListeners(msg2);

            Thread.sleep(300);

            List<ClusterMessage> sentMessages = followerTransport.getSentMessages();
            boolean hasResendRequest = sentMessages.stream()
                    .anyMatch(m -> m.type() == MessageType.SEQUENCE_RESEND_REQUEST);
            boolean hasSyncRequest = sentMessages.stream()
                    .anyMatch(m -> m.type() == MessageType.SYNC_REQUEST);

            assertFalse(hasResendRequest, "Should NOT have sent SEQUENCE_RESEND_REQUEST for large gap");
            assertTrue(hasSyncRequest, "Should have sent SYNC_REQUEST for large gap (direct fallback)");

            assertTrue(followerManager.getSnapshotFallbackCount() > 0,
                    "snapshotFallbackCount should be > 0 for large gap");

        } finally {
            followerManager.close();
            followerCoordinator.close();
        }
    }

    // ────────────────────────────────────────────────
    // Test 5: Follower applies resent operations successfully
    // ────────────────────────────────────────────────

    @Test
    void testFollowerAppliesResentOperations() throws Exception {
        NodeInfo leaderNode = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        NodeInfo followerNode = new NodeInfo(NodeId.of("aaa-follower"), "127.0.0.1", 0);

        RecordingTransport followerTransport = new RecordingTransport(followerNode, List.of(leaderNode));
        ClusterCoordinator followerCoordinator = new ClusterCoordinator(
                followerTransport, ClusterCoordinatorConfig.defaults(), scheduler);
        followerCoordinator.start();
        followerTransport.simulatePeerConnected(leaderNode);

        AtomicInteger applyCount = new AtomicInteger(0);
        List<String> appliedPayloads = new CopyOnWriteArrayList<>();

        ReplicationManager followerManager = new ReplicationManager(followerTransport, followerCoordinator,
                ReplicationConfig.builder(2)
                        .operationTimeout(Duration.ofSeconds(5))
                        .dataDirectory(tempDir)
                        .resendGapThreshold(10)
                        .build());
        followerManager.registerHandler(TOPIC, (operationId, payload) -> {
            applyCount.incrementAndGet();
            appliedPayloads.add(payload.toString());
        });
        followerManager.start();

        try {
            // Send seq=3 when follower expects seq=1 → gap of 2
            UUID opId3 = UUID.randomUUID();
            ReplicationPayload payloadSeq3 = new ReplicationPayload(opId3, 3, 1, TOPIC, "data-3");
            ClusterMessage msg3 = ClusterMessage.request(
                    MessageType.REPLICATION_REQUEST, null,
                    leaderNode.nodeId(), followerNode.nodeId(), payloadSeq3);
            followerTransport.deliverToListeners(msg3);

            // Wait > SEQUENCE_WAIT_TIMEOUT to let gap detection trigger
            Thread.sleep(1200);

            // Send seq=4 to re-trigger checkForMissingSequences after wait timeout
            UUID opId4 = UUID.randomUUID();
            ReplicationPayload payloadSeq4 = new ReplicationPayload(opId4, 4, 1, TOPIC, "data-4");
            ClusterMessage msg4 = ClusterMessage.request(
                    MessageType.REPLICATION_REQUEST, null,
                    leaderNode.nodeId(), followerNode.nodeId(), payloadSeq4);
            followerTransport.deliverToListeners(msg4);
            Thread.sleep(200);

            // Now simulate the leader responding with the missing operations (seq=1, seq=2)
            UUID opId1 = UUID.randomUUID();
            UUID opId2 = UUID.randomUUID();
            List<ReplicationPayload> resentOps = List.of(
                    new ReplicationPayload(opId1, 1, 1, TOPIC, "data-1"),
                    new ReplicationPayload(opId2, 2, 1, TOPIC, "data-2"));

            SequenceResendResponsePayload responsePayload = new SequenceResendResponsePayload(
                    TOPIC, resentOps, List.of());
            ClusterMessage response = ClusterMessage.request(
                    MessageType.SEQUENCE_RESEND_RESPONSE, "resend",
                    leaderNode.nodeId(), followerNode.nodeId(), responsePayload);
            followerTransport.deliverToListeners(response);

            // Wait for processing
            Thread.sleep(1000);

            // Verify all 4 operations were applied (seq=1, seq=2 from resend, seq=3 and
            // seq=4 from buffer)
            assertEquals(4, applyCount.get(),
                    "All 4 operations (2 resent + 2 buffered) should have been applied");
            assertTrue(appliedPayloads.contains("data-1"), "data-1 should have been applied");
            assertTrue(appliedPayloads.contains("data-2"), "data-2 should have been applied");
            assertTrue(appliedPayloads.contains("data-3"), "data-3 should have been applied");
            assertTrue(appliedPayloads.contains("data-4"), "data-4 should have been applied");

            // Verify resend success metric
            assertEquals(1, followerManager.getResendSuccessCount(), "resendSuccessCount should be 1");

        } finally {
            followerManager.close();
            followerCoordinator.close();
        }
    }

    // ────────────────────────────────────────────────
    // Test 6: Metrics are correctly updated
    // ────────────────────────────────────────────────

    @Test
    void testMetricsUpdated() throws Exception {
        NodeInfo leaderNode = new NodeInfo(NodeId.of("zzz-leader"), "127.0.0.1", 0);
        NodeInfo followerNode = new NodeInfo(NodeId.of("aaa-follower"), "127.0.0.1", 0);

        RecordingTransport followerTransport = new RecordingTransport(followerNode, List.of(leaderNode));
        ClusterCoordinator followerCoordinator = new ClusterCoordinator(
                followerTransport, ClusterCoordinatorConfig.defaults(), scheduler);
        followerCoordinator.start();
        followerTransport.simulatePeerConnected(leaderNode);

        ReplicationManager followerManager = new ReplicationManager(followerTransport, followerCoordinator,
                ReplicationConfig.builder(2)
                        .operationTimeout(Duration.ofSeconds(5))
                        .dataDirectory(tempDir)
                        .resendGapThreshold(5)
                        .build());
        followerManager.registerHandler(TOPIC, (operationId, payload) -> {
        });
        followerManager.start();

        try {
            // Initial metrics should be zero
            assertEquals(0, followerManager.getGapsDetected());
            assertEquals(0, followerManager.getResendSuccessCount());
            assertEquals(0, followerManager.getSnapshotFallbackCount());
            assertEquals(0.0, followerManager.getAverageConvergenceTimeMs());

            // Trigger a large gap (> threshold) to bump snapshotFallbackCount
            UUID opId = UUID.randomUUID();
            ReplicationPayload payload = new ReplicationPayload(opId, 100, 1, TOPIC, "data-100");
            ClusterMessage msg = ClusterMessage.request(
                    MessageType.REPLICATION_REQUEST, null,
                    leaderNode.nodeId(), followerNode.nodeId(), payload);
            followerTransport.deliverToListeners(msg);

            // Wait > SEQUENCE_WAIT_TIMEOUT
            Thread.sleep(1200);

            // Re-trigger gap check
            UUID opId2 = UUID.randomUUID();
            ReplicationPayload payload2 = new ReplicationPayload(opId2, 101, 1, TOPIC, "data-101");
            ClusterMessage msg2 = ClusterMessage.request(
                    MessageType.REPLICATION_REQUEST, null,
                    leaderNode.nodeId(), followerNode.nodeId(), payload2);
            followerTransport.deliverToListeners(msg2);

            Thread.sleep(300);

            assertTrue(followerManager.getGapsDetected() > 0, "gapsDetected should be incremented");
            assertTrue(followerManager.getSnapshotFallbackCount() > 0,
                    "snapshotFallbackCount should be incremented for large gap");

        } finally {
            followerManager.close();
            followerCoordinator.close();
        }
    }

    // ────────────────────────────────────────────────
    // RecordingTransport: captures sent messages & delivers to listeners
    // ────────────────────────────────────────────────

    private static final class RecordingTransport implements Transport {
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listenersSet = new CopyOnWriteArraySet<>();
        private final ConcurrentHashMap<NodeId, Boolean> connected = new ConcurrentHashMap<>();
        private final List<ClusterMessage> sentMessages = new CopyOnWriteArrayList<>();

        private RecordingTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            for (NodeInfo peer : peers) {
                connected.put(peer.nodeId(), true);
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
            listenersSet.add(listener);
        }

        @Override
        public void removeListener(TransportListener listener) {
            listenersSet.remove(listener);
        }

        @Override
        public void broadcast(ClusterMessage message) {
            sentMessages.add(message);
        }

        @Override
        public void send(ClusterMessage message) {
            sentMessages.add(message);
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            sentMessages.add(message);
            CompletableFuture<ClusterMessage> f = new CompletableFuture<>();
            f.completeExceptionally(new UnsupportedOperationException("Not used in this test"));
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
            for (TransportListener l : listenersSet) {
                l.onPeerConnected(peer);
            }
        }

        void deliverToListeners(ClusterMessage message) {
            for (TransportListener l : listenersSet) {
                l.onMessage(message);
            }
        }

        List<ClusterMessage> getSentMessages() {
            return List.copyOf(sentMessages);
        }

        @Override
        public void close() throws IOException {
        }
    }
}
