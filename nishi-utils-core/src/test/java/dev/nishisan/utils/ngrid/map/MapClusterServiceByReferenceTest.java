package dev.nishisan.utils.ngrid.map;

import dev.nishisan.utils.map.NMapConfig;
import dev.nishisan.utils.map.NMapPersistenceMode;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.replication.ReplicationConfig;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the leader-local by-reference mode of {@link MapClusterService}
 * (issue #115). Uses a single-node "cluster" (quorum = 1, local node is always the
 * leader) so {@code replicate} completes immediately and the local apply runs before
 * {@code put} returns.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class MapClusterServiceByReferenceTest {

    /** Mutable POJO with a public field — mirrors a hot-state DTO mutated in place. */
    static final class MutableBox implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
        public int x;

        @SuppressWarnings("unused")
        MutableBox() {
        }

        MutableBox(int x) {
            this.x = x;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MutableBox b && b.x == x;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(x);
        }
    }

    private InMemoryTransport transport;
    private ScheduledExecutorService coordinatorScheduler;
    private ClusterCoordinator coordinator;
    private ReplicationManager replicationManager;
    private Path replicationDir;
    private final List<MapClusterService<?, ?>> services = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        NodeInfo local = new NodeInfo(NodeId.of("node-1"), "127.0.0.1", 0);
        transport = new InMemoryTransport(local);
        replicationDir = Files.createTempDirectory("byref-replication");
        coordinatorScheduler = Executors.newSingleThreadScheduledExecutor();
        coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), coordinatorScheduler);
        coordinator.start();
        replicationManager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .operationTimeout(Duration.ofSeconds(2))
                        .dataDirectory(replicationDir)
                        .build());
        replicationManager.start();
        awaitLeadership();
    }

    @AfterEach
    void tearDown() throws Exception {
        for (MapClusterService<?, ?> service : services) {
            service.close();
        }
        if (replicationManager != null) {
            replicationManager.close();
        }
        if (coordinator != null) {
            coordinator.close();
        }
        if (coordinatorScheduler != null) {
            coordinatorScheduler.shutdownNow();
        }
        if (transport != null) {
            transport.close();
        }
    }

    private <K, V> MapClusterService<K, V> newService(String topic, boolean byReference) {
        MapClusterService<K, V> service = new MapClusterService<>(replicationManager, topic, null, byReference);
        services.add(service);
        return service;
    }

    // ------------------------------------------------------------------
    // Reference identity (put -> get)
    // ------------------------------------------------------------------

    @Test
    void leaderKeepsOriginalInstanceWhenByReferenceEnabled() {
        MapClusterService<String, MutableBox> map = newService("map:byref-on", true);
        MutableBox value = new MutableBox(1);
        map.put("k", value);
        assertSame(value, map.get("k").orElseThrow(),
                "by-reference leader must return the same instance passed to put");
    }

    @Test
    void leaderStoresDeserializedCopyWhenByReferenceDisabled() {
        MapClusterService<String, MutableBox> map = newService("map:byref-off", false);
        MutableBox value = new MutableBox(1);
        map.put("k", value);
        MutableBox stored = map.get("k").orElseThrow();
        assertNotSame(value, stored, "default mode must store a deserialized copy");
        assertEquals(value, stored, "copy must be equal by content");
    }

    // ------------------------------------------------------------------
    // In-place mutation without a re-put
    // ------------------------------------------------------------------

    @Test
    void inPlaceMutationIsVisibleWhenByReferenceEnabled() {
        MapClusterService<String, MutableBox> map = newService("map:byref-mutate-on", true);
        MutableBox value = new MutableBox(1);
        map.put("k", value);
        value.x = 42; // mutate in place, no explicit re-put
        assertEquals(42, map.get("k").orElseThrow().x,
                "ConcurrentHashMap semantics: in-place mutation must be visible via get");
    }

    @Test
    void inPlaceMutationIsNotVisibleWhenByReferenceDisabled() {
        MapClusterService<String, MutableBox> map = newService("map:byref-mutate-off", false);
        MutableBox value = new MutableBox(1);
        map.put("k", value);
        value.x = 42; // mutate the original; the cache holds a copy
        assertEquals(1, map.get("k").orElseThrow().x,
                "default mode keeps a copy, so in-place mutation must NOT be visible");
    }

    // ------------------------------------------------------------------
    // apply() accepts both a live command and the encoded byte[]
    // ------------------------------------------------------------------

    @Test
    void applyAcceptsLiveCommandAndEncodedBytes() {
        MapClusterService<String, MutableBox> map = newService("map:apply-dual", true);
        MutableBox live = new MutableBox(7);

        // Live command (leader by-reference path): the original instance is stored.
        map.apply(null, MapReplicationCommand.put("live", live));
        assertSame(live, map.get("live").orElseThrow());

        // Encoded byte[] (wire/follower path): a deserialized copy is stored.
        MutableBox onWire = new MutableBox(7);
        map.apply(null, MapReplicationCodec.encode(MapReplicationCommand.put("wire", onWire)));
        MutableBox stored = map.get("wire").orElseThrow();
        assertNotSame(onWire, stored);
        assertEquals(onWire, stored);
    }

    // ------------------------------------------------------------------
    // Persistence + by-reference: WAL captures state-at-put (appendSync)
    // ------------------------------------------------------------------

    @Test
    void persistedByReferenceValueSurvivesReload() throws Exception {
        Path mapsDir = Files.createTempDirectory("byref-maps");
        String topic = "map:byref-persist";
        String mapName = "byref-persist";
        NMapConfig nmapConfig = NMapConfig.builder().mode(NMapPersistenceMode.ASYNC_WITH_FSYNC).build();

        MapClusterService<String, MutableBox> writer = new MapClusterService<>(
                replicationManager, topic, mapsDir, mapName, nmapConfig, true);
        try {
            writer.loadFromDisk(); // opens the WAL for append (mirrors NGridNode)
            MutableBox value = new MutableBox(99);
            writer.put("k", value);
        } finally {
            writer.close();
        }

        // A fresh service backed by the same files must recover the persisted value.
        MapClusterService<String, MutableBox> reader = new MapClusterService<>(
                replicationManager, topic + "-reload", mapsDir, mapName, nmapConfig, true);
        services.add(reader);
        reader.loadFromDisk();
        MutableBox recovered = reader.get("k").orElseThrow(
                () -> new AssertionError("persisted by-reference value was not recovered after reload"));
        assertEquals(99, recovered.x);
    }

    private void awaitLeadership() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (coordinator.isLeader() && coordinator.hasValidLease()) {
                return;
            }
            Thread.sleep(20);
        }
        fail("local node did not become leader in time");
    }

    /**
     * Minimal single-node transport: provides the plumbing for
     * coordinator/replication without delivering messages anywhere.
     */
    private static final class InMemoryTransport implements dev.nishisan.utils.ngrid.cluster.transport.Transport {
        private final NodeInfo local;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();

        private InMemoryTransport(NodeInfo local) {
            this.local = local;
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
            return List.of(local);
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
        }

        @Override
        public void send(ClusterMessage message) {
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            CompletableFuture<ClusterMessage> future = new CompletableFuture<>();
            future.completeExceptionally(new UnsupportedOperationException("not supported"));
            return future;
        }

        @Override
        public boolean isConnected(NodeId nodeId) {
            return true;
        }

        @Override
        public boolean isReachable(NodeId nodeId) {
            return true;
        }

        @Override
        public void addPeer(NodeInfo peer) {
        }

        @Override
        public void close() throws IOException {
            listeners.clear();
        }
    }
}
