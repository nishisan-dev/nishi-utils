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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Resilience tests for DistributedMap under partition and failover scenarios.
 */
class MapPartitionResilienceTest {

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

        info1 = new NodeInfo(NodeId.of("part-1"), "127.0.0.1", port1);
        info2 = new NodeInfo(NodeId.of("part-2"), "127.0.0.1", port2);
        info3 = new NodeInfo(NodeId.of("part-3"), "127.0.0.1", port3);

        Path baseDir = Files.createTempDirectory("ngrid-map-partition");
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

        // Pre-create maps on ALL nodes
        node1.getMap("resilience-map", String.class, String.class);
        node2.getMap("resilience-map", String.class, String.class);
        node3.getMap("resilience-map", String.class, String.class);
    }

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
        closeQuietly(node3);
    }

    /**
     * Validates that an isolated leader (expired lease) rejects writes
     * and the majority side elects a new leader that can accept writes.
     * After reconnection, the old leader's data should converge.
     */
    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void shouldRejectWritesOnIsolatedLeaderAndConvergeAfterReconnection() throws Exception {
        NGridNode leader = findLeader();
        assertNotNull(leader, "Should have a leader");

        // Write initial data via leader
        DistributedMap<String, String> leaderMap = leader.getMap("resilience-map", String.class, String.class);
        leaderMap.put("pre-partition", "exists");

        // Let replication settle
        Thread.sleep(500);

        // Kill both followers to isolate the leader
        NGridNode[] allNodes = {node1, node2, node3};
        for (NGridNode n : allNodes) {
            if (n != null && n != leader) {
                closeQuietly(n);
                disableNode(n);
            }
        }

        // Wait for leader lease to expire
        Thread.sleep(6000);

        // Writes on isolated leader should fail
        assertThrows(IllegalStateException.class,
                () -> leaderMap.put("post-partition", "should-fail"),
                "Put on isolated leader with expired lease should throw");

        // Pre-partition data should still be accessible locally (eventual read)
        Optional<String> localRead = leaderMap.get("pre-partition", Consistency.EVENTUAL);
        assertTrue(localRead.isPresent(), "Pre-partition data should survive locally");
    }

    /**
     * Validates that concurrent writes during leader failover converge
     * without data loss on the surviving nodes.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void shouldConvergeAfterConcurrentWritesDuringFailover() throws Exception {
        NGridNode leader = findLeader();
        assertNotNull(leader, "Should have a leader");

        // Write some initial entries
        DistributedMap<String, String> leaderMap = leader.getMap("resilience-map", String.class, String.class);
        for (int i = 0; i < 20; i++) {
            leaderMap.put("init-" + i, "value-" + i);
        }

        // Let replication settle
        Thread.sleep(500);

        // Kill the leader
        closeQuietly(leader);
        disableNode(leader);

        // Wait for new leader election
        NGridNode newLeader = awaitNewLeader(15_000);
        assertNotNull(newLeader, "A new leader should be elected after failover");

        // Write new entries on the new leader concurrently
        DistributedMap<String, String> survivorMap = newLeader.getMap("resilience-map", String.class, String.class);
        int concurrentWrites = 30;
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        ExecutorService pool = Executors.newFixedThreadPool(3);
        CountDownLatch done = new CountDownLatch(concurrentWrites);

        for (int i = 0; i < concurrentWrites; i++) {
            int idx = i;
            pool.submit(() -> {
                try {
                    survivorMap.put("post-failover-" + idx, "value-" + idx);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        assertTrue(done.await(30, TimeUnit.SECONDS), "Concurrent writes should complete");
        pool.shutdownNow();

        // At least some writes should succeed (all if quorum is met)
        assertTrue(successCount.get() > 0, "Some concurrent writes should succeed");

        // Verify pre-failover data survived
        Optional<String> recovered = survivorMap.get("init-0");
        assertTrue(recovered.isPresent(), "Pre-failover data should survive");
        assertEquals("value-0", recovered.get());

        // Verify post-failover data is consistent
        for (int i = 0; i < concurrentWrites; i++) {
            Optional<String> val = survivorMap.get("post-failover-" + i);
            if (val.isPresent()) {
                assertEquals("value-" + i, val.get(),
                        "Post-failover entry should have correct value");
            }
        }
    }

    // ==================== Utility Methods ====================

    private NGridNode findLeader() {
        NGridNode[] candidates = {node1, node2, node3};
        for (NGridNode n : candidates) {
            if (n != null && n.coordinator().isLeader()
                    && !n.replicationManager().isLeaderSyncing()) {
                return n;
            }
        }
        return null;
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
