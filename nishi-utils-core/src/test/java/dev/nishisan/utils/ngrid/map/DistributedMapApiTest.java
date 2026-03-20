package dev.nishisan.utils.ngrid.map;

import dev.nishisan.utils.ngrid.ClusterTestUtils;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for new DistributedMap API operations:
 * containsKey, size, isEmpty, putAll.
 */
class DistributedMapApiTest {

    private NGridNode node1;
    private NGridNode node2;
    private NGridNode node3;

    @BeforeEach
    void setUp() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        int port3 = allocateFreeLocalPort(Set.of(port1, port2));

        NodeInfo info1 = new NodeInfo(NodeId.of("api-1"), "127.0.0.1", port1);
        NodeInfo info2 = new NodeInfo(NodeId.of("api-2"), "127.0.0.1", port2);
        NodeInfo info3 = new NodeInfo(NodeId.of("api-3"), "127.0.0.1", port3);

        Path baseDir = Files.createTempDirectory("ngrid-map-api");
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
        node1.getMap("api-test", String.class, String.class);
        node2.getMap("api-test", String.class, String.class);
        node3.getMap("api-test", String.class, String.class);
    }

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
        closeQuietly(node3);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void containsKeyShouldReturnTrueAfterPutAndFalseAfterRemove() {
        NGridNode leader = findLeader();
        assertNotNull(leader, "Should have a leader");

        DistributedMap<String, String> map = leader.getMap("api-test", String.class, String.class);

        assertFalse(map.containsKey("ck-1"), "Key should not exist before put");

        map.put("ck-1", "value-1");
        assertTrue(map.containsKey("ck-1"), "Key should exist after put");

        map.remove("ck-1");
        assertFalse(map.containsKey("ck-1"), "Key should not exist after remove");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void sizeShouldReflectOperations() throws Exception {
        NGridNode leader = findLeader();
        assertNotNull(leader, "Should have a leader");

        DistributedMap<String, String> map = leader.getMap("api-test", String.class, String.class);

        assertEquals(0, map.size(), "Size should be 0 initially");

        map.put("s-1", "v-1");
        map.put("s-2", "v-2");
        map.put("s-3", "v-3");

        assertEquals(3, map.size(), "Size should be 3 after 3 puts");

        // Verify size on follower (eventually consistent)
        NGridNode follower = findFollower();
        assertNotNull(follower, "Should have a follower");
        Thread.sleep(500); // Give replication time to propagate

        DistributedMap<String, String> followerMap = follower.getMap("api-test", String.class, String.class);
        assertEquals(3, followerMap.size(), "Follower size should be 3 after replication");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void isEmptyShouldBeCorrect() {
        NGridNode leader = findLeader();
        assertNotNull(leader, "Should have a leader");

        DistributedMap<String, String> map = leader.getMap("api-test", String.class, String.class);

        assertTrue(map.isEmpty(), "Map should be empty initially");

        map.put("e-1", "v-1");
        assertFalse(map.isEmpty(), "Map should not be empty after put");

        map.remove("e-1");
        assertTrue(map.isEmpty(), "Map should be empty after removing all entries");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void putAllShouldReplicateAllEntries() throws Exception {
        NGridNode leader = findLeader();
        assertNotNull(leader, "Should have a leader");

        DistributedMap<String, String> map = leader.getMap("api-test", String.class, String.class);

        Map<String, String> entries = Map.of(
                "pa-1", "val-1",
                "pa-2", "val-2",
                "pa-3", "val-3",
                "pa-4", "val-4",
                "pa-5", "val-5"
        );
        map.putAll(entries);

        assertEquals(5, map.size(), "Size should be 5 after putAll");

        // Verify all entries are accessible on leader
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            assertEquals(entry.getValue(), map.get(entry.getKey()).orElse(null),
                    "Entry " + entry.getKey() + " should be accessible");
        }

        // Verify replication to follower
        Thread.sleep(500);
        NGridNode follower = findFollower();
        assertNotNull(follower, "Should have a follower");
        DistributedMap<String, String> followerMap = follower.getMap("api-test", String.class, String.class);

        for (Map.Entry<String, String> entry : entries.entrySet()) {
            assertTrue(followerMap.containsKey(entry.getKey()),
                    "Follower should have key " + entry.getKey());
        }
    }

    // ==================== Utility Methods ====================

    private NGridNode findLeader() {
        long deadline = System.currentTimeMillis() + 10_000;
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
                return null;
            }
        }
        return null;
    }

    private NGridNode findFollower() {
        NGridNode[] candidates = {node1, node2, node3};
        for (NGridNode n : candidates) {
            if (n != null && !n.coordinator().isLeader()) {
                return n;
            }
        }
        return null;
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
