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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the DistributedMap API: containsKey, size, isEmpty,
 * putAll and the full {@link java.util.Map} contract (RF1–RF12) — value-returning
 * get/put/remove, replicated reusable clear, values/entrySet snapshots and
 * Map recognition for drop-in usage.
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
            assertEquals(entry.getValue(), map.getOptional(entry.getKey()).orElse(null),
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

    // ==================== java.util.Map contract (RF1–RF12) ====================

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void mapContractReturnsValuesAndNullsRF1toRF5() {
        NGridNode leader = findLeader();
        assertNotNull(leader, "Should have a leader");
        DistributedMap<String, String> map = leader.getMap("api-test", String.class, String.class);

        // RF1: get returns the value or null
        assertNull(map.get("rf-1"), "get on absent key returns null");
        // RF2: put returns the previous value (or null)
        assertNull(map.put("rf-1", "v1"), "put returns null when no previous value");
        assertEquals("v1", map.put("rf-1", "v2"), "put returns the previous value");
        assertEquals("v2", map.get("rf-1"), "get returns the current value");
        // RF4: containsKey(Object)
        assertTrue(map.containsKey("rf-1"));
        // RF3: remove returns the previous value
        assertEquals("v2", map.remove("rf-1"), "remove returns the previous value");
        assertNull(map.get("rf-1"));
        assertFalse(map.containsKey("rf-1"));
        // RF5: size
        map.put("rf-a", "1");
        map.put("rf-b", "2");
        assertEquals(2, map.size());
        // Optional-based variants remain available
        assertEquals(Optional.of("1"), map.getOptional("rf-a"));
        assertEquals(Optional.empty(), map.getOptional("rf-missing"));
    }

    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void clearShouldBeReplicatedAndReusableRF6() throws Exception {
        NGridNode leader = findLeader();
        assertNotNull(leader, "Should have a leader");
        DistributedMap<String, String> map = leader.getMap("api-test", String.class, String.class);

        map.put("c-1", "v1");
        map.put("c-2", "v2");
        assertEquals(2, map.size());

        map.clear();
        assertEquals(0, map.size(), "size must be 0 after clear");
        assertTrue(map.isEmpty(), "map must be empty after clear");

        // RF6: the map stays reusable after clear (persistence engine alive)
        map.put("c-3", "v3");
        assertEquals("v3", map.get("c-3"));
        assertEquals(1, map.size());

        // clear is replicated: followers reflect the empty-then-reuse state
        NGridNode follower = findFollower();
        assertNotNull(follower, "Should have a follower");
        DistributedMap<String, String> followerMap = follower.getMap("api-test", String.class, String.class);
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline
                && !(followerMap.size() == 1 && followerMap.containsKey("c-3"))) {
            Thread.sleep(100);
        }
        assertFalse(followerMap.containsKey("c-1"), "follower must not retain pre-clear keys");
        assertFalse(followerMap.containsKey("c-2"), "follower must not retain pre-clear keys");
        assertTrue(followerMap.containsKey("c-3"), "follower must see the post-clear write");
        assertEquals(1, followerMap.size(), "follower size reflects clear + reuse");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void valuesAndEntrySetReflectLocalReplicaRF7RF8() {
        NGridNode leader = findLeader();
        assertNotNull(leader, "Should have a leader");
        DistributedMap<String, String> map = leader.getMap("api-test", String.class, String.class);

        map.put("k1", "v1");
        map.put("k2", "v2");
        map.put("k3", "v3");

        // RF7: values()
        Collection<String> values = map.values();
        assertEquals(3, values.size());
        assertTrue(values.contains("v1") && values.contains("v2") && values.contains("v3"));

        // RF8: entrySet() + Groovy-style ".each { k, v -> }" iteration
        Map<String, String> collected = new ConcurrentHashMap<>();
        for (Map.Entry<String, String> e : map.entrySet()) {
            collected.put(e.getKey(), e.getValue());
        }
        assertEquals(Map.of("k1", "v1", "k2", "v2", "k3", "v3"), collected);

        // RF: containsValue
        assertTrue(map.containsValue("v2"));
        assertFalse(map.containsValue("nope"));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void readAfterWriteLocalOnLeaderRF10() {
        NGridNode leader = findLeader();
        assertNotNull(leader, "Should have a leader");
        DistributedMap<String, String> map = leader.getMap("api-test", String.class, String.class);

        // RF10: after put(k,v), get(k)/iteration on the same node sees v without
        // waiting for replication.
        for (int i = 0; i < 50; i++) {
            map.put("raw-" + i, "val-" + i);
            assertEquals("val-" + i, map.get("raw-" + i), "read-after-write must be visible locally");
        }
        // iteration on the same pass also sees the writes
        int seen = 0;
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (e.getKey().startsWith("raw-")) {
                seen++;
            }
        }
        assertEquals(50, seen);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void iterationOverSnapshotIsSafeUnderConcurrentWriteRF11() throws Exception {
        NGridNode leader = findLeader();
        assertNotNull(leader, "Should have a leader");
        DistributedMap<String, String> map = leader.getMap("api-test", String.class, String.class);

        for (int i = 0; i < 200; i++) {
            map.put("base-" + i, "v" + i);
        }

        AtomicBoolean failed = new AtomicBoolean(false);
        Thread writer = new Thread(() -> {
            for (int i = 0; i < 200; i++) {
                map.put("hot-" + i, "v" + i);
            }
        });
        writer.start();

        // RF11: iterating the local snapshot must not throw ConcurrentModificationException
        try {
            for (int round = 0; round < 5; round++) {
                int count = 0;
                for (Map.Entry<String, String> e : map.entrySet()) {
                    assertNotNull(e.getKey());
                    count++;
                }
                assertTrue(count > 0);
                for (String v : map.values()) {
                    assertNotNull(v);
                }
            }
        } catch (RuntimeException ex) {
            failed.set(true);
            throw ex;
        } finally {
            writer.join();
        }
        assertFalse(failed.get(), "snapshot iteration must be immune to concurrent writes");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void recognizedAsJavaUtilMapRF12() {
        NGridNode leader = findLeader();
        assertNotNull(leader, "Should have a leader");
        DistributedMap<String, String> distributed = leader.getMap("api-test", String.class, String.class);

        // RF12: must be a java.util.Map so Groovy treats it natively (map[key],
        // map.each, map.containsKey).
        assertInstanceOf(Map.class, distributed, "DistributedMap must be a java.util.Map");

        // Use it purely through the Map<K,V> reference — the "drop-in" contract.
        Map<String, String> map = distributed;
        assertNull(map.put("rf12", "x"));
        assertEquals("x", map.get("rf12"));
        assertTrue(map.containsKey("rf12"));
        assertEquals(1, map.size());
        map.putAll(Map.of("a", "1", "b", "2"));
        assertEquals(3, map.size());
        assertTrue(map.keySet().containsAll(Set.of("rf12", "a", "b")));
        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void dropInReplacementForConcurrentHashMapRF1toRF12() {
        NGridNode leader = findLeader();
        assertNotNull(leader, "Should have a leader");

        // The Cardinal use case: swap ConcurrentHashMap for DistributedMap behind a
        // java.util.Map reference and exercise the same operations the Groovy rules use.
        Map<String, String> reference = new ConcurrentHashMap<>();
        Map<String, String> distributed = leader.getMap("api-test", String.class, String.class);

        for (Map<String, String> m : List.of(reference, distributed)) {
            m.put("id-1", "alpha");
            m.put("id-2", "beta");
            assertEquals("alpha", m.get("id-1"));
            assertTrue(m.containsKey("id-2"));
            assertEquals(2, m.size());

            List<String> vals = new ArrayList<>(m.values());
            assertTrue(vals.contains("alpha") && vals.contains("beta"));

            int each = 0;
            for (Map.Entry<String, String> e : m.entrySet()) {
                assertNotNull(e.getValue());
                each++;
            }
            assertEquals(2, each);

            assertEquals("alpha", m.remove("id-1"));
            assertFalse(m.containsKey("id-1"));

            m.clear();
            assertTrue(m.isEmpty());
            assertEquals(0, m.size());
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
