package dev.nishisan.utils.ngrid.map;

import dev.nishisan.utils.ngrid.ClusterTestUtils;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.structures.Consistency;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.map.NMapPersistenceMode;
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
 * TDD Tests for DistributedMap resilience during leader failover.
 * <p>
 * These tests expose known bugs in the NMap layer:
 * <ul>
 *   <li>Retry too short in {@code invokeLeader()} (3 attempts × 100ms = 300ms)</li>
 *   <li>STRONG reads fail during failover with {@code IllegalStateException}</li>
 *   <li>Leader does not validate lease before accepting writes</li>
 *   <li>No {@code keySet()} exposed on the public API</li>
 * </ul>
 */
class MapNodeFailoverIntegrationTest {

    private NGridNode node1;
    private NGridNode node2;
    private NGridNode node3;
    private NodeInfo info1;
    private NodeInfo info2;
    private NodeInfo info3;

    @BeforeEach
    void setUp() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        int port3 = allocateFreeLocalPort(Set.of(port1, port2));

        info1 = new NodeInfo(NodeId.of("map-fo-1"), "127.0.0.1", port1);
        info2 = new NodeInfo(NodeId.of("map-fo-2"), "127.0.0.1", port2);
        info3 = new NodeInfo(NodeId.of("map-fo-3"), "127.0.0.1", port3);

        Path baseDir = Files.createTempDirectory("ngrid-map-failover");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));
        Path dir3 = Files.createDirectories(baseDir.resolve("node3"));

        Duration opTimeout = Duration.ofSeconds(10);

        node1 = new NGridNode(NGridConfig.builder(info1)
                .addPeer(info2).addPeer(info3)
                .queueDirectory(dir1)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(Duration.ofMillis(500))
                .mapDirectory(dir1.resolve("maps"))
                .mapPersistenceMode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .build());

        node2 = new NGridNode(NGridConfig.builder(info2)
                .addPeer(info1).addPeer(info3)
                .queueDirectory(dir2)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(Duration.ofMillis(500))
                .mapDirectory(dir2.resolve("maps"))
                .mapPersistenceMode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .build());

        node3 = new NGridNode(NGridConfig.builder(info3)
                .addPeer(info1).addPeer(info2)
                .queueDirectory(dir3)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(Duration.ofMillis(500))
                .mapDirectory(dir3.resolve("maps"))
                .mapPersistenceMode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .build());

        node1.start();
        node2.start();
        node3.start();

        ClusterTestUtils.awaitClusterConsensus(node1, node2, node3);

        // Pre-create maps on ALL nodes so replication handlers are registered
        node1.getMap("failover-map", String.class, String.class);
        node2.getMap("failover-map", String.class, String.class);
        node3.getMap("failover-map", String.class, String.class);
    }

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
        closeQuietly(node3);
    }

    /**
     * Validates that map entries survive leader failover.
     * <p>
     * Scenario:
     * <ol>
     *   <li>Put 50 key-value pairs via leader</li>
     *   <li>Kill leader (close)</li>
     *   <li>Wait for new leader election</li>
     *   <li>Get entries from new leader — should return the values</li>
     * </ol>
     * <p>
     * Exposes: Retry too short in {@code invokeLeader()} — follower attempting
     * to read from new leader may fail if election takes > 300ms.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void shouldRecoverMapPutAfterLeaderFailover() throws Exception {
        // Leader = node-3 (highest ID by deterministic election)
        NGridNode leader = awaitReadyLeader(15_000);
        assertNotNull(leader, "Should have a leader");

        DistributedMap<String, String> leaderMap = leader.getMap("failover-map", String.class, String.class);

        // Populate map
        int entryCount = 50;
        for (int i = 0; i < entryCount; i++) {
            leaderMap.put("key-" + i, "value-" + i);
        }

        // Verify pre-condition: data is replicated to followers
        NGridNode follower = findFollower();
        assertNotNull(follower, "Should have a follower");
        // Give replication a moment to propagate
        Thread.sleep(500);

        // Kill leader
        closeQuietly(leader);
        disableNode(leader);

        // Await new leader
        NGridNode newLeader = awaitNewLeader(15_000);
        assertNotNull(newLeader, "A new leader should be elected after failover");

        // Validate entries are accessible from the new leader
        DistributedMap<String, String> survivorMap = newLeader.getMap("failover-map", String.class, String.class);
        Optional<String> recovered = survivorMap.get("key-0");
        assertTrue(recovered.isPresent(), "Map entry should survive leader failover");
        assertEquals("value-0", recovered.get());

        // Spot-check another entry
        Optional<String> recovered25 = survivorMap.get("key-25");
        assertTrue(recovered25.isPresent(), "Map entry key-25 should survive failover");
        assertEquals("value-25", recovered25.get());
    }

    /**
     * Validates that STRONG reads recover after leader failover.
     * <p>
     * Scenario:
     * <ol>
     *   <li>Populate map via leader</li>
     *   <li>Kill leader</li>
     *   <li>Wait for new leader election</li>
     *   <li>{@code get("key-0", Consistency.STRONG)} on a follower should return
     *       the value without throwing {@code IllegalStateException}</li>
     * </ol>
     * <p>
     * Exposes: STRONG reads route to leader via {@code invokeLeader()}, which
     * throws after only 3 retries × 100ms when no leader is available.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void shouldRecoverMapGetStrongAfterLeaderFailover() throws Exception {
        NGridNode leader = awaitReadyLeader(15_000);
        assertNotNull(leader, "Should have a leader");

        DistributedMap<String, String> leaderMap = leader.getMap("failover-map", String.class, String.class);
        leaderMap.put("strong-key", "strong-value");

        // Let replication settle
        Thread.sleep(500);

        // Kill leader
        closeQuietly(leader);
        disableNode(leader);

        // Await new leader
        NGridNode newLeader = awaitNewLeader(15_000);
        assertNotNull(newLeader, "A new leader should be elected");

        // Find a non-leader survivor to test STRONG routing
        NGridNode follower = findFollower();
        if (follower == null) {
            // If only one node left, use the leader itself
            follower = newLeader;
        }

        DistributedMap<String, String> followerMap = follower.getMap("failover-map", String.class, String.class);

        // This should NOT throw; it should route to the new leader
        Optional<String> result = assertDoesNotThrow(
                () -> followerMap.get("strong-key", Consistency.STRONG),
                "STRONG read should succeed after leader failover, not throw IllegalStateException");
        assertTrue(result.isPresent(), "STRONG read should return the value");
        assertEquals("strong-value", result.get());
    }

    /**
     * Validates that an isolated leader (expired lease) rejects writes.
     * <p>
     * Scenario:
     * <ol>
     *   <li>Cluster with short lease (2s) and fast heartbeat (300ms)</li>
     *   <li>Kill the two followers (isolating the leader)</li>
     *   <li>Wait for lease to expire (~3s)</li>
     *   <li>{@code put()} on the isolated leader should fail</li>
     * </ol>
     * <p>
     * Exposes: {@code put()} checks {@code coordinator.isLeader()} but NOT
     * {@code coordinator.hasValidLease()}, allowing writes on an isolated leader.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void shouldRejectWriteOnExpiredLease() throws Exception {
        NGridNode leader = awaitReadyLeader(15_000);
        assertNotNull(leader, "Should have a leader");

        DistributedMap<String, String> leaderMap = leader.getMap("failover-map", String.class, String.class);

        // Verify leader is functional
        leaderMap.put("pre-isolation", "works");

        // Kill both followers to isolate the leader
        NGridNode[] allNodes = {node1, node2, node3};
        for (NGridNode n : allNodes) {
            if (n != null && n != leader) {
                closeQuietly(n);
                disableNode(n);
            }
        }

        // Wait for lease to expire and leader to step down
        // Heartbeat interval = 500ms, default lease = 3 × heartbeatTimeout
        // heartbeatTimeout defaults to 3 × heartbeatInterval = 1500ms
        // leaseTimeout defaults to 3 × heartbeatTimeout = 4500ms
        // So we wait ~6s to be safe
        Thread.sleep(6000);

        // The leader should have stepped down by now (lease expired)
        // A put() should fail — either because the leader stepped down
        // (no leader available) or because the lease is expired
        assertThrows(IllegalStateException.class,
                () -> leaderMap.put("post-isolation", "should-fail"),
                "Put on isolated leader with expired lease should throw");
    }

    /**
     * Validates that {@code keySet()} is available on the DistributedMap API.
     * <p>
     * Scenario:
     * <ol>
     *   <li>Put 10 entries via leader</li>
     *   <li>Call {@code keySet()} from a follower</li>
     *   <li>Validate: result contains all 10 keys</li>
     * </ol>
     * <p>
     * Exposes: The DistributedMap does not expose {@code keySet()} at all.
     * This test will not compile until the method is added.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void shouldExposeKeysAcrossCluster() throws Exception {
        NGridNode leader = awaitReadyLeader(15_000);
        assertNotNull(leader, "Should have a leader");

        DistributedMap<String, String> leaderMap = leader.getMap("failover-map", String.class, String.class);

        int keyCount = 10;
        for (int i = 0; i < keyCount; i++) {
            leaderMap.put("ks-key-" + i, "ks-value-" + i);
        }

        // Let replication settle
        Thread.sleep(500);

        // Read keys from a follower (eventual consistency — local replica)
        NGridNode follower = findFollower();
        assertNotNull(follower, "Should have a follower");

        DistributedMap<String, String> followerMap = follower.getMap("failover-map", String.class, String.class);

        Set<String> keys = followerMap.keySet();
        assertNotNull(keys, "keySet() should not return null");
        assertEquals(keyCount, keys.size(),
                "keySet() should contain all " + keyCount + " keys, got: " + keys);

        for (int i = 0; i < keyCount; i++) {
            assertTrue(keys.contains("ks-key-" + i),
                    "keySet() should contain key 'ks-key-" + i + "'");
        }
    }

    // ==================== Utility Methods ====================

    private NGridNode findFollower() {
        NGridNode[] candidates = {node1, node2, node3};
        for (NGridNode n : candidates) {
            if (n != null && !n.coordinator().isLeader()) {
                return n;
            }
        }
        return null;
    }

    private NGridNode awaitReadyLeader(long timeoutMs) {
        NGridNode leader = awaitNewLeader(timeoutMs);
        if (leader == null) {
            throw new IllegalStateException("No ready leader elected in time");
        }
        return leader;
    }

    private NGridNode awaitNewLeader(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            NGridNode[] candidates = {node1, node2, node3};
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
        return null;
    }

    private void disableNode(NGridNode node) {
        if (node == node1) node1 = null;
        if (node == node2) node2 = null;
        if (node == node3) node3 = null;
    }

    private void closeQuietly(NGridNode node) {
        if (node == null) return;
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
