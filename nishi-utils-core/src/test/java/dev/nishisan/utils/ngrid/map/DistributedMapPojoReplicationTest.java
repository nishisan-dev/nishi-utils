package dev.nishisan.utils.ngrid.map;

import dev.nishisan.utils.ngrid.ClusterTestUtils;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.replication.ReplicationHandler;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests for {@link DistributedMap} with user-defined POJO values.
 *
 * <p>Regression for GitHub issue #82: a {@code DistributedMap<K, POJO>} was returning
 * {@code LinkedHashMap} instead of the original POJO type on followers and after snapshot
 * sync, causing a {@code ClassCastException} at the call site.
 *
 * <p>The POJO used here ({@code TunnelEntryStub}) has <em>no Jackson annotations</em>,
 * exactly mirroring the situation in the ishin-gateway with {@code TunnelRegistryEntry}.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class DistributedMapPojoReplicationTest {

    // -----------------------------------------------------------------------
    // Stub POJO — deliberately has NO Jackson annotations
    // -----------------------------------------------------------------------

    /**
     * Plain POJO without any Jackson annotations. Simulates consumer domain objects
     * such as {@code TunnelRegistryEntry} in ishin-gateway.
     */
    static class TunnelEntryStub {
        private final String host;
        private final int port;
        private final boolean active;

        // Required by the MapReplicationCodec ObjectMapper (no-args constructor + field access)
        @SuppressWarnings("unused")
        TunnelEntryStub() {
            this(null, 0, false);
        }

        TunnelEntryStub(String host, int port, boolean active) {
            this.host = host;
            this.port = port;
            this.active = active;
        }

        String  host()   { return host;   }
        int     port()   { return port;   }
        boolean active() { return active; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TunnelEntryStub that)) return false;
            return port == that.port && active == that.active && Objects.equals(host, that.host);
        }

        @Override public int hashCode() { return Objects.hash(host, port, active); }

        @Override
        public String toString() {
            return "TunnelEntryStub{host='" + host + "', port=" + port + ", active=" + active + '}';
        }
    }

    // -----------------------------------------------------------------------
    // Cluster setup
    // -----------------------------------------------------------------------

    private NGridNode node1;
    private NGridNode node2;
    private NGridNode node3;

    @BeforeEach
    void setUp() throws Exception {
        int port1 = allocateFreeLocalPort();
        int port2 = allocateFreeLocalPort(Set.of(port1));
        int port3 = allocateFreeLocalPort(Set.of(port1, port2));

        NodeInfo info1 = new NodeInfo(NodeId.of("pojo-1"), "127.0.0.1", port1);
        NodeInfo info2 = new NodeInfo(NodeId.of("pojo-2"), "127.0.0.1", port2);
        NodeInfo info3 = new NodeInfo(NodeId.of("pojo-3"), "127.0.0.1", port3);

        Path baseDir = Files.createTempDirectory("ngrid-pojo-replication");
        Path dir1 = Files.createDirectories(baseDir.resolve("node1"));
        Path dir2 = Files.createDirectories(baseDir.resolve("node2"));
        Path dir3 = Files.createDirectories(baseDir.resolve("node3"));

        Duration opTimeout = Duration.ofSeconds(10);

        node1 = new NGridNode(NGridConfig.builder(info1)
                .addPeer(info2).addPeer(info3)
                .dataDirectory(dir1)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(Duration.ofMillis(300))
                .mapDirectory(dir1.resolve("maps"))
                .mapPersistenceMode(NMapPersistenceMode.DISABLED)
                .build());

        node2 = new NGridNode(NGridConfig.builder(info2)
                .addPeer(info1).addPeer(info3)
                .dataDirectory(dir2)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(Duration.ofMillis(300))
                .mapDirectory(dir2.resolve("maps"))
                .mapPersistenceMode(NMapPersistenceMode.DISABLED)
                .build());

        node3 = new NGridNode(NGridConfig.builder(info3)
                .addPeer(info1).addPeer(info2)
                .dataDirectory(dir3)
                .replicationFactor(2)
                .replicationOperationTimeout(opTimeout)
                .heartbeatInterval(Duration.ofMillis(300))
                .mapDirectory(dir3.resolve("maps"))
                .mapPersistenceMode(NMapPersistenceMode.DISABLED)
                .build());

        node1.start();
        node2.start();
        node3.start();

        ClusterTestUtils.awaitClusterConsensus(node1, node2, node3);

        // Pre-register map handlers on all nodes
        node1.getMap("pojo-map", String.class, TunnelEntryStub.class);
        node2.getMap("pojo-map", String.class, TunnelEntryStub.class);
        node3.getMap("pojo-map", String.class, TunnelEntryStub.class);
    }

    @AfterEach
    void tearDown() {
        closeQuietly(node1);
        closeQuietly(node2);
        closeQuietly(node3);
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /**
     * Regression test for issue #82 — replication path.
     * A POJO written on the leader must arrive as the original type on a follower,
     * NOT as {@code LinkedHashMap}.
     */
    @Test
    void pojoValueShouldBePreservedAfterReplication() throws Exception {
        NGridNode leader = requireLeader();
        NGridNode follower = requireFollower();

        DistributedMap<String, TunnelEntryStub> map =
                leader.getMap("pojo-map", String.class, TunnelEntryStub.class);

        TunnelEntryStub expected = new TunnelEntryStub("10.0.0.1", 8080, true);
        map.put("tunnel-1", expected);

        // Give replication time to propagate
        Thread.sleep(700);

        DistributedMap<String, TunnelEntryStub> followerMap =
                follower.getMap("pojo-map", String.class, TunnelEntryStub.class);

        TunnelEntryStub actual = followerMap.get("tunnel-1")
                .orElseThrow(() -> new AssertionError("Key not found on follower after replication"));

        // Before the fix this would throw ClassCastException because 'actual' was LinkedHashMap
        assertInstanceOf(TunnelEntryStub.class, actual,
                "Value on follower must be TunnelEntryStub, not " + actual.getClass().getName());

        assertEquals(expected.host(),   actual.host());
        assertEquals(expected.port(),   actual.port());
        assertEquals(expected.active(), actual.active());
    }

    /**
     * Tests that multiple POJO entries are all preserved after replication.
     */
    @Test
    void multiplePojoEntriesShouldBePreservedAfterReplication() throws Exception {
        NGridNode leader = requireLeader();
        NGridNode follower = requireFollower();

        DistributedMap<String, TunnelEntryStub> map =
                leader.getMap("pojo-map", String.class, TunnelEntryStub.class);

        map.put("t-1", new TunnelEntryStub("10.0.0.1", 8080, true));
        map.put("t-2", new TunnelEntryStub("10.0.0.2", 8081, false));
        map.put("t-3", new TunnelEntryStub("10.0.0.3", 9000, true));

        Thread.sleep(700);

        DistributedMap<String, TunnelEntryStub> followerMap =
                follower.getMap("pojo-map", String.class, TunnelEntryStub.class);

        for (String key : new String[]{"t-1", "t-2", "t-3"}) {
            TunnelEntryStub v = followerMap.get(key)
                    .orElseThrow(() -> new AssertionError("Key " + key + " not found on follower"));
            assertInstanceOf(TunnelEntryStub.class, v,
                    "Value for key '" + key + "' on follower must be TunnelEntryStub, not " + v.getClass().getName());
        }

        // Spot-check values
        TunnelEntryStub t2 = followerMap.get("t-2").orElseThrow();
        assertEquals("10.0.0.2", t2.host());
        assertEquals(8081, t2.port());
        assertFalse(t2.active());
    }

    /**
     * Regression test for issue #82 — snapshot/sync path.
     * Verifies that the {@link MapReplicationCodec} correctly round-trips a
     * {@link MapClusterService} snapshot containing POJO values via
     * {@code encodeSnapshot} / {@code decodeSnapshot}.
     */
    @Test
    void pojoValueShouldBePreservedAfterSnapshotRoundTrip() {
        // Populate a MapClusterService with POJO values using a no-op replication manager
        // so we can test the snapshot codec path in isolation.
        MapClusterService<String, TunnelEntryStub> service = buildInMemoryService();
        service.apply(null, MapReplicationCodec.encode(MapReplicationCommand.put("snap-1",
                new TunnelEntryStub("192.168.1.1", 4443, true))));
        service.apply(null, MapReplicationCodec.encode(MapReplicationCommand.put("snap-2",
                new TunnelEntryStub("192.168.1.2", 9000, false))));

        // Simulate snapshot transfer
        MapClusterService<String, TunnelEntryStub> target = buildInMemoryService();
        target.resetState();

        int chunkIndex = 0;
        while (true) {
            ReplicationHandler.SnapshotChunk chunk = service.getSnapshotChunk(chunkIndex);
            target.installSnapshot(chunk.data());
            if (!chunk.hasMore()) break;
            chunkIndex++;
        }

        // Verify that values on the target service are proper TunnelEntryStub instances
        TunnelEntryStub v1 = target.get("snap-1")
                .orElseThrow(() -> new AssertionError("snap-1 not found after snapshot install"));
        TunnelEntryStub v2 = target.get("snap-2")
                .orElseThrow(() -> new AssertionError("snap-2 not found after snapshot install"));

        assertInstanceOf(TunnelEntryStub.class, v1, "snap-1 must be TunnelEntryStub, not " + v1.getClass());
        assertInstanceOf(TunnelEntryStub.class, v2, "snap-2 must be TunnelEntryStub, not " + v2.getClass());

        assertEquals("192.168.1.1", v1.host());
        assertEquals(4443, v1.port());
        assertTrue(v1.active());

        assertEquals("192.168.1.2", v2.host());
        assertEquals(9000, v2.port());
        assertFalse(v2.active());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Builds an in-memory {@link MapClusterService} backed by a no-op
     * {@link dev.nishisan.utils.ngrid.replication.ReplicationManager} subclass,
     * suitable for isolated unit-style codec validation.
     */
    private MapClusterService<String, TunnelEntryStub> buildInMemoryService() {
        dev.nishisan.utils.ngrid.replication.ReplicationManager noOpManager =
                new dev.nishisan.utils.ngrid.replication.ReplicationManager() {
                    @Override
                    public void registerHandler(String topic,
                            dev.nishisan.utils.ngrid.replication.ReplicationHandler handler) {
                        // no-op for isolated testing
                    }
                };
        return new MapClusterService<>(noOpManager, "map:test-pojo", (Void) null);
    }

    private NGridNode requireLeader() {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            for (NGridNode n : new NGridNode[]{node1, node2, node3}) {
                if (n != null && n.coordinator().isLeader() && !n.replicationManager().isLeaderSyncing()) {
                    return n;
                }
            }
            try { Thread.sleep(200); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for leader");
            }
        }
        fail("No leader elected within timeout");
        return null;
    }

    private NGridNode requireFollower() {
        for (NGridNode n : new NGridNode[]{node1, node2, node3}) {
            if (n != null && !n.coordinator().isLeader()) return n;
        }
        fail("No follower found");
        return null;
    }

    private void closeQuietly(NGridNode node) {
        if (node == null) return;
        try { node.close(); } catch (IOException ignored) {}
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
