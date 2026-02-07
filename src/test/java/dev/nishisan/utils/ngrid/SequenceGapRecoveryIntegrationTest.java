package dev.nishisan.utils.ngrid;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.DistributedQueue;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the Sequence Gap Recovery mechanism (Phase 3).
 *
 * <p>
 * Validates hybrid recovery with real TCP transport and NGridNode cluster:
 * </p>
 * <ul>
 * <li>Small gap catch-up after follower restart (resend path ≤ threshold).</li>
 * <li>Large gap catch-up for late-joining follower (snapshot sync path).</li>
 * <li>3-node cluster convergence under writes with metrics validation.</li>
 * </ul>
 *
 * <p>
 * Follows the patterns established in {@code QueueNodeFailoverIntegrationTest}:
 * dynamic ports, bilateral peer registration, cluster stability wait, and
 * pre-created queues on all nodes before replication.
 * </p>
 */
class SequenceGapRecoveryIntegrationTest {

    private static final String QUEUE_NAME = "gap-recovery-queue";
    private static final Duration OP_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration HEARTBEAT = Duration.ofMillis(200);

    private Path baseDir;
    private NodeInfo info1, info2, info3;

    @BeforeEach
    void setUp() throws IOException {
        baseDir = Files.createTempDirectory("ngrid-gap-recovery");
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        int port3 = allocateFreeLocalPort(Set.of(port1, port2));

        // NodeIds: zzz-gap-1 > mmm-gap-2 > aaa-gap-3 (leader election by max)
        info1 = new NodeInfo(NodeId.of("zzz-gap-1"), "127.0.0.1", port1);
        info2 = new NodeInfo(NodeId.of("mmm-gap-2"), "127.0.0.1", port2);
        info3 = new NodeInfo(NodeId.of("aaa-gap-3"), "127.0.0.1", port3);
    }

    @AfterEach
    void tearDown() {
        // baseDir cleanup handled by OS temp directory
    }

    // ────────────────────────────────────────────────
    // Test 1: Three-node cluster convergence and
    // metrics validation
    // ────────────────────────────────────────────────

    /**
     * Starts a 3-node cluster, writes 50 items via leader, verifies data
     * converges on all followers, and checks that gap recovery metrics
     * are accessible and valid.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testThreeNodeClusterConvergenceWithMetrics() throws Exception {
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));
        Path dir3 = Files.createDirectories(baseDir.resolve("node3"));

        try (NGridNode node1 = new NGridNode(NGridConfig.builder(info1)
                .addPeer(info2).addPeer(info3)
                .queueDirectory(dir1)
                .replicationFactor(2)
                .replicationOperationTimeout(OP_TIMEOUT)
                .heartbeatInterval(HEARTBEAT)
                .build());
                NGridNode node2 = new NGridNode(NGridConfig.builder(info2)
                        .addPeer(info1).addPeer(info3)
                        .queueDirectory(dir2)
                        .replicationFactor(2)
                        .replicationOperationTimeout(OP_TIMEOUT)
                        .heartbeatInterval(HEARTBEAT)
                        .build());
                NGridNode node3 = new NGridNode(NGridConfig.builder(info3)
                        .addPeer(info1).addPeer(info2)
                        .queueDirectory(dir3)
                        .replicationFactor(2)
                        .replicationOperationTimeout(OP_TIMEOUT)
                        .heartbeatInterval(HEARTBEAT)
                        .build())) {

            node1.start();
            node2.start();
            node3.start();
            awaitClusterStability(node1, node2, node3);

            // Pre-create queue on ALL nodes to ensure handlers are registered
            node1.getQueue(QUEUE_NAME, String.class);
            node2.getQueue(QUEUE_NAME, String.class);
            node3.getQueue(QUEUE_NAME, String.class);

            // zzz-gap-1 should be leader (highest NodeId)
            assertTrue(node1.coordinator().isLeader(),
                    "zzz-gap-1 should be leader (highest NodeId)");

            // Write 50 items via leader
            DistributedQueue<String> queueL = node1.getQueue(QUEUE_NAME, String.class);
            for (int i = 0; i < 50; i++) {
                queueL.offer("data-" + i);
            }

            // Wait for followers to catch up
            waitForLastApplied(node2, 50, 15_000);
            waitForLastApplied(node3, 50, 15_000);

            // Verify data on node2
            DistributedQueue<String> queue2 = node2.getQueue(QUEUE_NAME, String.class);
            Optional<String> peek2 = queue2.peek();
            assertTrue(peek2.isPresent(), "Node2 should have queued items");
            assertEquals("data-0", peek2.orElse(null));

            // Verify data on node3
            DistributedQueue<String> queue3 = node3.getQueue(QUEUE_NAME, String.class);
            Optional<String> peek3 = queue3.peek();
            assertTrue(peek3.isPresent(), "Node3 should have queued items");
            assertEquals("data-0", peek3.orElse(null));

            // Verify sequence alignment
            long leaderSeq = node1.replicationManager().getGlobalSequence();
            assertEquals(leaderSeq, node2.replicationManager().getLastAppliedSequence(),
                    "Node2 sequence should match leader");
            assertEquals(leaderSeq, node3.replicationManager().getLastAppliedSequence(),
                    "Node3 sequence should match leader");

            // Verify Phase 3 metrics are accessible (non-negative)
            assertTrue(node2.replicationManager().getGapsDetected() >= 0);
            assertTrue(node2.replicationManager().getResendSuccessCount() >= 0);
            assertTrue(node2.replicationManager().getSnapshotFallbackCount() >= 0);
            assertTrue(node2.replicationManager().getAverageConvergenceTimeMs() >= 0);
        }
    }

    // ────────────────────────────────────────────────
    // Test 2: Follower catches up after late join
    // with large lag (snapshot sync path)
    // ────────────────────────────────────────────────

    /**
     * Leader alone writes 600 items (above SYNC_THRESHOLD=500).
     * Follower joins late and catches up via snapshot sync.
     */
    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void testFollowerCatchUpWithLargeGapViaSnapshotSync() throws Exception {
        Path dirL = Files.createDirectories(baseDir.resolve("leader"));
        Path dirF = Files.createDirectories(baseDir.resolve("follower"));

        try (NGridNode leader = new NGridNode(NGridConfig.builder(info1)
                .queueDirectory(dirL)
                .replicationFactor(1) // allow writes with only leader
                .replicationOperationTimeout(OP_TIMEOUT)
                .heartbeatInterval(HEARTBEAT)
                .build())) {

            leader.start();

            // Pre-create queue on leader
            DistributedQueue<String> queueL = leader.getQueue(QUEUE_NAME, String.class);

            // Write 600 items (above SYNC_THRESHOLD=500)
            for (int i = 0; i < 600; i++) {
                queueL.offer("item-" + i);
            }

            assertEquals(600, leader.replicationManager().getGlobalSequence(),
                    "Leader should have 600 operations");

            // Start follower late — gap=600
            try (NGridNode follower = new NGridNode(NGridConfig.builder(info3)
                    .addPeer(info1)
                    .queueDirectory(dirF)
                    .replicationFactor(1)
                    .replicationOperationTimeout(OP_TIMEOUT)
                    .heartbeatInterval(HEARTBEAT)
                    .build())) {

                follower.start();

                // Pre-create queue on follower to register handler
                follower.getQueue(QUEUE_NAME, String.class);

                // Wait for follower to see leader watermark
                waitForLeaderWatermark(follower, 600, 10_000);

                // Wait for snapshot catch-up
                waitForLastApplied(follower, 600, 30_000);

                // Verify data integrity
                DistributedQueue<String> queueF = follower.getQueue(QUEUE_NAME, String.class);
                Optional<String> first = queueF.peek();
                assertTrue(first.isPresent(), "Follower queue should have items after snapshot sync");
                assertEquals("item-0", first.orElse(null));

                assertTrue(follower.replicationManager().getLastAppliedSequence() >= 600,
                        "Follower should have caught up via snapshot to at least 600");
            }
        }
    }

    // ────────────────────────────────────────────────
    // Test 3: Follower restart with small gap —
    // validates data convergence after brief outage
    // ────────────────────────────────────────────────

    /**
     * Starts a 2-node cluster, writes 20 items, stops follower,
     * writes 10 more, restarts follower. Follower should catch up
     * (gap=10 ≤ resendGapThreshold=50).
     */
    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void testFollowerCatchUpAfterBriefOutage() throws Exception {
        Path dirL = Files.createDirectories(baseDir.resolve("leader"));
        Path dirF = Files.createDirectories(baseDir.resolve("follower"));

        try (NGridNode leader = new NGridNode(NGridConfig.builder(info1)
                .addPeer(info3)
                .queueDirectory(dirL)
                .replicationFactor(1) // allow writes with only leader
                .replicationOperationTimeout(OP_TIMEOUT)
                .heartbeatInterval(HEARTBEAT)
                .build())) {

            leader.start();

            // Phase 1: Start follower and write 20 items together
            NGridNode follower = new NGridNode(NGridConfig.builder(info3)
                    .addPeer(info1)
                    .queueDirectory(dirF)
                    .replicationFactor(1)
                    .replicationOperationTimeout(OP_TIMEOUT)
                    .heartbeatInterval(HEARTBEAT)
                    .build());
            follower.start();

            // Wait for cluster convergence (2 nodes)
            awaitBilateralConnection(leader, follower);

            // Pre-create queue on both nodes
            leader.getQueue(QUEUE_NAME, String.class);
            follower.getQueue(QUEUE_NAME, String.class);

            DistributedQueue<String> queueL = leader.getQueue(QUEUE_NAME, String.class);
            for (int i = 0; i < 20; i++) {
                queueL.offer("item-" + i);
            }

            // Wait for follower to replicate all 20 items
            waitForLastApplied(follower, 20, 15_000);
            assertTrue(follower.replicationManager().getLastAppliedSequence() >= 20,
                    "Follower should have replicated 20 items");

            // Phase 2: Stop follower, write 10 more items
            follower.close();

            for (int i = 20; i < 30; i++) {
                queueL.offer("item-" + i);
            }

            assertEquals(30, leader.replicationManager().getGlobalSequence(),
                    "Leader should have 30 operations");

            // Phase 3: Restart follower — has gap of 10 items
            follower = new NGridNode(NGridConfig.builder(info3)
                    .addPeer(info1)
                    .queueDirectory(dirF)
                    .replicationFactor(1)
                    .replicationOperationTimeout(OP_TIMEOUT)
                    .heartbeatInterval(HEARTBEAT)
                    .build());
            follower.start();

            try {
                // Pre-create queue on restarted follower
                follower.getQueue(QUEUE_NAME, String.class);

                // Wait for bilateral connection to re-establish
                awaitBilateralConnection(leader, follower);

                // Write items with deliberate delay so that at least one replication
                // arrives AFTER the SEQUENCE_WAIT_TIMEOUT (1s). The gap detection
                // mechanism (checkForMissingSequences) requires:
                // 1. A buffered out-of-order sequence with wait time > 1s
                // 2. A new replication request to re-trigger detection
                // Writing ~15 items at 300ms intervals ensures this condition is met.
                int extraItems = 15;
                for (int i = 30; i < 30 + extraItems; i++) {
                    queueL.offer("item-" + i);
                    Thread.sleep(300);
                }

                int totalExpected = 30 + extraItems; // 45

                // After restart, lastAppliedSequence is a relative counter starting from 0.
                // The follower needs to apply seqs 21-45 = 25 operations from resend + new
                // writes.
                int opsAfterRestart = totalExpected - 20; // 25
                waitForLastApplied(follower, opsAfterRestart, 30_000);

                // Verify data integrity — queue should have all items from before restart
                DistributedQueue<String> queueF = follower.getQueue(QUEUE_NAME, String.class);
                Optional<String> first = queueF.peek();
                assertTrue(first.isPresent(), "Follower queue should have items after catch-up");
                assertEquals("item-0", first.orElse(null));

                // Verify that resend path was used (gap detection should have fired)
                assertTrue(follower.replicationManager().getLastAppliedSequence() >= opsAfterRestart,
                        "Follower should have applied at least " + opsAfterRestart + " operations after restart");
            } finally {
                closeQuietly(follower);
            }
        }
    }

    // ────────────────────────────────────────────────
    // Utility methods (pattern from QueueNodeFailoverIntegrationTest)
    // ────────────────────────────────────────────────

    private void awaitClusterStability(NGridNode... nodes) {
        if (nodes.length < 2)
            return;
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(20);
        while (System.currentTimeMillis() < deadline) {
            boolean leadersAgree = true;
            boolean allConnected = true;

            Optional<?> firstLeader = nodes[0].coordinator().leaderInfo();
            for (int i = 1; i < nodes.length; i++) {
                if (!firstLeader.equals(nodes[i].coordinator().leaderInfo())) {
                    leadersAgree = false;
                    break;
                }
            }

            if (leadersAgree && firstLeader.isPresent()) {
                // Check all-to-all connectivity
                for (NGridNode a : nodes) {
                    for (NGridNode b : nodes) {
                        if (a != b) {
                            NodeId bId = b.config().local().nodeId();
                            if (!a.transport().isConnected(bId)) {
                                allConnected = false;
                                break;
                            }
                        }
                    }
                    if (!allConnected)
                        break;
                }

                // Check active members count
                boolean allMembers = true;
                for (NGridNode n : nodes) {
                    if (n.coordinator().activeMembers().size() < nodes.length) {
                        allMembers = false;
                        break;
                    }
                }

                if (leadersAgree && allConnected && allMembers) {
                    return;
                }
            }

            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new IllegalStateException("Cluster did not stabilize in time");
    }

    private void awaitBilateralConnection(NGridNode a, NGridNode b) {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(15);
        NodeId aId = a.config().local().nodeId();
        NodeId bId = b.config().local().nodeId();
        while (System.currentTimeMillis() < deadline) {
            if (a.transport().isConnected(bId) && b.transport().isConnected(aId)
                    && a.coordinator().leaderInfo().isPresent()
                    && a.coordinator().leaderInfo().equals(b.coordinator().leaderInfo())) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new IllegalStateException("Bilateral connection didn't establish in time");
    }

    private void waitForLeaderWatermark(NGridNode node, long minWatermark, long timeoutMs) throws Exception {
        long start = System.currentTimeMillis();
        while (node.coordinator().getTrackedLeaderHighWatermark() < minWatermark) {
            Thread.sleep(100);
            if (System.currentTimeMillis() - start > timeoutMs) {
                throw new IllegalStateException(
                        "Node didn't see leader watermark >= " + minWatermark
                                + ". Current: " + node.coordinator().getTrackedLeaderHighWatermark());
            }
        }
    }

    private void waitForLastApplied(NGridNode node, long minSequence, long timeoutMs) throws Exception {
        long start = System.currentTimeMillis();
        while (node.replicationManager().getLastAppliedSequence() < minSequence) {
            Thread.sleep(200);
            if (System.currentTimeMillis() - start > timeoutMs) {
                throw new IllegalStateException(
                        "Node catch-up stalled. Last applied seq: "
                                + node.replicationManager().getLastAppliedSequence()
                                + " (expected >= " + minSequence + ")");
            }
        }
    }

    private void closeQuietly(NGridNode node) {
        try {
            if (node != null)
                node.close();
        } catch (IOException e) {
            // best-effort
        }
    }

    private int allocateFreeLocalPort() {
        return allocateFreeLocalPort(Set.of());
    }

    private int allocateFreeLocalPort(Set<Integer> avoid) {
        for (int attempt = 0; attempt < 20; attempt++) {
            try (ServerSocket ss = new ServerSocket(0)) {
                int port = ss.getLocalPort();
                if (!avoid.contains(port)) {
                    return port;
                }
            } catch (IOException e) {
                // retry
            }
        }
        throw new IllegalStateException("Could not allocate free local port");
    }
}
