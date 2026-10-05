package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.map.*;
import dev.nishisan.utils.ngrid.cluster.coordination.*;
import dev.nishisan.utils.ngrid.common.*;
import dev.nishisan.utils.ngrid.map.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.lang.reflect.Field;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class MapSnapshotDurabilityTest {
    @TempDir Path directory;
    ScheduledExecutorService scheduler;
    ReplicationManager manager;
    ClusterCoordinator coordinator;
    private NMapConfig config() {
        return NMapConfig.builder().mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalTime(Duration.ofMillis(1)).batchTimeout(Duration.ofMillis(5)).build();
    }
    @BeforeEach void setup() {
        scheduler = Executors.newScheduledThreadPool(2);
        ScriptedTransport transport = new ScriptedTransport(new NodeInfo(NodeId.of("local"), "127.0.0.1", 1), List.of());
        coordinator = new ClusterCoordinator(transport, ClusterCoordinatorConfig.defaults(), scheduler);
        manager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1).dataDirectory(directory.resolve("replication")).build());
    }
    @AfterEach void close() throws Exception {
        BlockingRead.release.countDown();
        manager.close();
        coordinator.close();
        scheduler.shutdownNow();
    }
    private MapClusterService<String, Serializable> open() {
        return new MapClusterService<>(manager, "map:catalog", directory, "catalog", config());
    }
    @SuppressWarnings("unchecked") private Map<String, ReplicationHandler> handlers() throws Exception {
        Field field = ReplicationManager.class.getDeclaredField("handlers");
        field.setAccessible(true);
        return (Map<String, ReplicationHandler>) field.get(manager);
    }

    @Test @Timeout(20)
    void handlerIsPublishedOnlyAfterDiskLoadAndRepeatedLoadDoesNotOverwriteSync() throws Exception {
        Path map = directory.resolve("catalog");
        Files.createDirectories(map);
        try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(map.resolve("snapshot.dat")))) {
            out.writeObject(new HashMap<>(Map.of("old", new BlockingRead())));
        }
        BlockingRead.entered = new CountDownLatch(1);
        BlockingRead.release = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<MapClusterService<String, Serializable>> opening = pool.submit(this::open);
            assertTrue(BlockingRead.entered.await(5, TimeUnit.SECONDS));
            assertFalse(handlers().containsKey("map:catalog"), "no handler may race disk hydration");
            BlockingRead.release.countDown();
            MapClusterService<String, Serializable> service = opening.get(5, TimeUnit.SECONDS);
            assertSame(service, handlers().get("map:catalog"));
            service.resetState();
            service.installSnapshot(MapReplicationCodec.encodeSnapshot(new HashMap<>(Map.of("leader", "full-state"))));
            service.onSnapshotInstalled();
            service.onSnapshotCommitted();
            service.loadFromDisk();
            assertEquals(Set.of("leader"), service.keySet());
            service.close();
            MapClusterService<String, Serializable> restarted = open();
            assertEquals(Set.of("leader"), restarted.keySet());
            assertEquals(Optional.of("full-state"), restarted.get("leader"));
            restarted.close();
        } finally {
            BlockingRead.release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void failedDiskLoadNeverPublishesHandler() throws Exception {
        Files.createDirectories(directory.resolve("catalog"));
        Files.writeString(directory.resolve("catalog/snapshot.dat"), "invalid snapshot");
        assertThrows(IllegalStateException.class, this::open);
        assertFalse(handlers().containsKey("map:catalog"));
    }

    @Test @Timeout(20)
    void partialInstallDoesNotCheckpointAndCannotCloseCleanly() throws Exception {
        MapClusterService<String, Serializable> service = open();
        service.apply(UUID.randomUUID(), MapReplicationCommand.put("before", "safe"));
        service.onSnapshotInstalled();
        service.resetState();
        service.installSnapshot(MapReplicationCodec.encodeSnapshot(new HashMap<>(Map.of("partial", "chunk"))));
        assertThrows(IllegalStateException.class,
                () -> service.apply(UUID.randomUUID(), MapReplicationCommand.put("old-apply", "bad")));
        Thread.sleep(30); // crosses the configured periodic snapshot interval while install is suspended
        assertThrows(IOException.class, service::close);
        MapClusterService<String, Serializable> restarted = open();
        assertEquals(Set.of("before"), restarted.keySet());
        restarted.close();
    }

    @Test
    void checkpointFailureKeepsInstallationUnhealthyAndRejectsCleanClose() throws Exception {
        MapClusterService<String, Serializable> service = open();
        service.resetState();
        service.installSnapshot(MapReplicationCodec.encodeSnapshot(new HashMap<>(Map.of("leader", "data"))));
        Path temp = directory.resolve("catalog/snapshot.dat.tmp");
        Files.createDirectories(temp);
        Files.writeString(temp.resolve("obstruction"), "cannot delete");
        assertThrows(IOException.class, service::onSnapshotInstalled);
        assertFalse(service.isHealthy());
        assertThrows(IOException.class, service::close);
    }

    @Test @Timeout(20)
    void abandonedPartialInstallRestoresPreInstallAsyncMutationsAndResumesWrites() throws Exception {
        MapClusterService<String, Serializable> service = open();
        service.apply(UUID.randomUUID(), MapReplicationCommand.put("before", "trusted"));
        service.apply(UUID.randomUUID(), MapReplicationCommand.put("admitted", "queued-wal"));
        service.resetState();
        service.installSnapshot(MapReplicationCodec.encodeSnapshot(new HashMap<>(Map.of("partial", "bad"))));
        service.onSnapshotAborted();
        assertTrue(service.isHealthy());
        assertEquals(Set.of("before", "admitted"), service.keySet());
        service.apply(UUID.randomUUID(), MapReplicationCommand.put("after", "works"));
        service.close();
        try (var restarted = open()) {
            assertEquals(Set.of("before", "admitted", "after"), restarted.keySet());
        }
    }

    @Test @Timeout(20)
    void staleSuccessfulCheckpointRollbackRestoresDiskBeforeUnblocking() throws Exception {
        MapClusterService<String, Serializable> service = open();
        service.apply(UUID.randomUUID(), MapReplicationCommand.put("before", "trusted"));
        service.resetState();
        service.installSnapshot(MapReplicationCodec.encodeSnapshot(new HashMap<>(Map.of("stale", "installed"))));
        service.onSnapshotInstalled();
        service.onSnapshotAborted();
        assertTrue(service.isHealthy());
        assertEquals(Set.of("before"), service.keySet());
        service.close();
        try (var restarted = open()) {
            assertEquals(Set.of("before"), restarted.keySet());
        }
    }

    private static class BlockingRead implements Serializable {
        static volatile CountDownLatch entered = new CountDownLatch(0);
        static volatile CountDownLatch release = new CountDownLatch(0);
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            entered.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            in.defaultReadObject();
        }
    }
}
