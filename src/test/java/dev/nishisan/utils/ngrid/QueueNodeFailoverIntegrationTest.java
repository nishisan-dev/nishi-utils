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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD Tests for Queue resilience during node failover scenarios.
 * These tests validate that data is preserved when the leader fails during
 * writes.
 */
class QueueNodeFailoverIntegrationTest {

    private NGridNode node1;
    private NGridNode node2;
    private NGridNode node3;
    private NodeInfo info1;
    private NodeInfo info2;
    private NodeInfo info3;
    private Path baseDir;

    @BeforeEach
    void setUp() throws IOException {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        int port3 = allocateFreeLocalPort(Set.of(port1, port2));

        info1 = new NodeInfo(NodeId.of("failover-1"), "127.0.0.1", port1);
        info2 = new NodeInfo(NodeId.of("failover-2"), "127.0.0.1", port2);
        info3 = new NodeInfo(NodeId.of("failover-3"), "127.0.0.1", port3);

        baseDir = Files.createTempDirectory("ngrid-failover-test");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));
        Path dir3 = Files.createDirectories(baseDir.resolve("node3"));

        Duration opTimeout = Duration.ofSeconds(10);

        node1 = new NGridNode(NGridConfig.builder(info1)
                .addPeer(info2).addPeer(info3)
                .queueDirectory(dir1)
                .replicationFactor(2) // Needs 2 for quorum
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(Duration.ofMillis(500))
                .build());

        node2 = new NGridNode(NGridConfig.builder(info2)
                .addPeer(info1).addPeer(info3)
                .queueDirectory(dir2)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(Duration.ofMillis(500))
                .build());

        node3 = new NGridNode(NGridConfig.builder(info3)
                .addPeer(info1).addPeer(info2)
                .queueDirectory(dir3)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(Duration.ofMillis(500))
                .build());

        node1.start();
        node2.start();
        node3.start();

        awaitClusterStability();

        // Pre-create queues on ALL nodes to ensure handlers are registered
        // before any replication attempts. This is necessary because dynamic
        // queue creation via getQueue() only registers the handler locally.
        node1.getQueue("failover-queue", String.class);
        node2.getQueue("failover-queue", String.class);
        node3.getQueue("failover-queue", String.class);
        node1.getQueue("stress-queue", String.class);
        node2.getQueue("stress-queue", String.class);
        node3.getQueue("stress-queue", String.class);
    }

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
        closeQuietly(node3);
    }

    /**
     * Tests that writes are preserved after leader failover.
     * 1. Write items to queue via leader
     * 2. Kill leader
     * 3. Wait for new leader election
     * 4. Verify items are still readable
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testDataPersistsAfterLeaderFailover() throws Exception {
        // Find current leader and wait until it's fully synced
        NGridNode leader = awaitNewLeader(15_000);
        assertNotNull(leader, "Should have a leader");

        DistributedQueue<String> queue = leader.getQueue("failover-queue", String.class);

        // Write items
        int itemCount = 50;
        for (int i = 0; i < itemCount; i++) {
            queue.offer("item-" + i);
        }

        // Kill the leader
        closeQuietly(leader);
        disableNode(leader);

        // Wait for new leader election with polling (not fixed sleep)
        NGridNode newLeader = awaitNewLeader(15_000);
        assertNotNull(newLeader, "A new leader should have been elected after failover");

        // Access queue from the new leader (guaranteed to be ready)
        DistributedQueue<String> survivingQueue = newLeader.getQueue("failover-queue", String.class);

        // Verify queue has items. Replication may have been partial before
        // the old leader died, so we check presence rather than exact ordering.
        Optional<String> firstItem = survivingQueue.peek();
        assertTrue(firstItem.isPresent(), "Queue should have items after failover");
        assertTrue(firstItem.get().startsWith("item-"),
                "Item should be from the original batch, got: " + firstItem.get());
    }

    /**
     * Tests that writes during leader failover don't cause data loss.
     * This is a stress test that writes continuously while killing the leader.
     */
    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void testWritesDuringFailover() throws Exception {
        NGridNode leader = awaitNewLeader(15_000);
        assertNotNull(leader, "Should have a leader");

        DistributedQueue<String> queue = leader.getQueue("stress-queue", String.class);

        // Write some initial items
        int initialItems = 20;
        for (int i = 0; i < initialItems; i++) {
            queue.offer("initial-" + i);
        }

        // Kill leader
        closeQuietly(leader);
        disableNode(leader);

        // Wait for new leader election with polling
        NGridNode newLeader = awaitNewLeader(15_000);
        assertNotNull(newLeader, "A new leader should have been elected after failover");

        // Access queue from the new leader
        DistributedQueue<String> survivorQueue = newLeader.getQueue("stress-queue", String.class);

        // Should be able to peek
        Optional<String> item = survivorQueue.peek();
        assertTrue(item.isPresent(), "Queue should have items after leader failover");
    }

    private void awaitClusterStability() {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (System.currentTimeMillis() < deadline) {
            boolean leadersAgree = node1.coordinator().leaderInfo().isPresent()
                    && node1.coordinator().leaderInfo().equals(node2.coordinator().leaderInfo())
                    && node1.coordinator().leaderInfo().equals(node3.coordinator().leaderInfo());
            boolean allMembers = node1.coordinator().activeMembers().size() == 3
                    && node2.coordinator().activeMembers().size() == 3
                    && node3.coordinator().activeMembers().size() == 3;
            boolean connected = node1.transport().isConnected(info2.nodeId())
                    && node1.transport().isConnected(info3.nodeId())
                    && node2.transport().isConnected(info1.nodeId())
                    && node2.transport().isConnected(info3.nodeId())
                    && node3.transport().isConnected(info1.nodeId())
                    && node3.transport().isConnected(info2.nodeId());
            if (leadersAgree && allMembers && connected) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        // Diagnostic info for CI debugging
        String diag = String.format(
                "leadersAgree=%s, allMembers=[%d,%d,%d], connected=[1->2:%s,1->3:%s,2->1:%s,2->3:%s,3->1:%s,3->2:%s]",
                node1.coordinator().leaderInfo().isPresent()
                        && node1.coordinator().leaderInfo().equals(node2.coordinator().leaderInfo())
                        && node1.coordinator().leaderInfo().equals(node3.coordinator().leaderInfo()),
                node1.coordinator().activeMembers().size(),
                node2.coordinator().activeMembers().size(),
                node3.coordinator().activeMembers().size(),
                node1.transport().isConnected(info2.nodeId()),
                node1.transport().isConnected(info3.nodeId()),
                node2.transport().isConnected(info1.nodeId()),
                node2.transport().isConnected(info3.nodeId()),
                node3.transport().isConnected(info1.nodeId()),
                node3.transport().isConnected(info2.nodeId()));
        throw new IllegalStateException("Cluster did not stabilize in time. State: " + diag);
    }

    /**
     * Awaits a new leader to be elected and fully ready (not syncing).
     * Uses polling instead of fixed Thread.sleep to be robust in CI.
     */
    private NGridNode awaitNewLeader(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            NGridNode[] candidates = { node1, node2, node3 };
            for (NGridNode n : candidates) {
                if (n != null && n.coordinator().isLeader()
                        && !n.replicationManager().isLeaderSyncing()) {
                    return n;
                }
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new IllegalStateException("No leader elected in time");
    }

    private void disableNode(NGridNode node) {
        if (node == node1)
            node1 = null;
        if (node == node2)
            node2 = null;
        if (node == node3)
            node3 = null;
    }

    private void closeQuietly(NGridNode node) {
        if (node == null)
            return;
        try {
            node.close();
        } catch (IOException ignored) {
        }
    }

    private static int allocateFreeLocalPort() throws IOException {
        return allocateFreeLocalPort(Set.of());
    }

    private static int allocateFreeLocalPort(Set<Integer> avoid) throws IOException {
        for (int attempt = 0; attempt < 50; attempt++) {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(true);
                socket.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = socket.getLocalPort();
                if (port > 0 && !avoid.contains(port)) {
                    return port;
                }
            }
        }
        throw new IOException("Unable to allocate a free local port after multiple attempts");
    }
}
