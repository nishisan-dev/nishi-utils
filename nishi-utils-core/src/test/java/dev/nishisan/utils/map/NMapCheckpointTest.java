package dev.nishisan.utils.map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class NMapCheckpointTest {
    @TempDir Path directory;
    private NMapConfig config() {
        return NMapConfig.builder().mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalTime(Duration.ZERO).batchTimeout(Duration.ofMillis(5)).build();
    }

    @Test @Timeout(20)
    void forceCheckpointWaitsForDequeuedBatchAndDoesNotReplayItOverReplacement() throws Exception {
        Map<String, Serializable> state = new ConcurrentHashMap<>();
        NMapPersistence<String, Serializable> persistence = new NMapPersistence<>(config(), state, directory, "map");
        persistence.start();
        BlockingValue old = new BlockingValue();
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            persistence.appendAsync(NMapOperationType.PUT, "obsolete", old);
            assertTrue(old.entered.await(5, TimeUnit.SECONDS));
            state.put("correct", "replacement");
            Future<?> checkpoint = pool.submit(() -> { persistence.forceSnapshot(); return null; });
            assertThrows(TimeoutException.class, () -> checkpoint.get(100, TimeUnit.MILLISECONDS));
            old.release.countDown();
            checkpoint.get(5, TimeUnit.SECONDS);
            synchronized (state) {
                state.put("later", "tail");
                persistence.appendSync(NMapOperationType.PUT, "later", "tail");
            }
            persistence.close();
            Map<String, Serializable> recovered = new ConcurrentHashMap<>();
            NMapPersistence<String, Serializable> reopened = new NMapPersistence<>(config(), recovered, directory, "map");
            reopened.load();
            assertEquals(Map.of("correct", "replacement", "later", "tail"), recovered);
        } finally {
            old.release.countDown();
            pool.shutdownNow();
            persistence.close();
        }
    }

    @Test @Timeout(30)
    void concurrentSyncAndAsyncMutatorsSurviveRepeatedCheckpointsWithoutLostBatches() throws Exception {
        Map<String, String> state = new ConcurrentHashMap<>();
        java.util.concurrent.locks.ReentrantLock mutationLock = new java.util.concurrent.locks.ReentrantLock();
        NMapPersistence<String, String> persistence =
                new NMapPersistence<>(config(), state, directory, "concurrent", mutationLock);
        persistence.start();
        ExecutorService pool = Executors.newFixedThreadPool(3);
        try {
            List<Future<?>> writers = new ArrayList<>();
            for (int writer = 0; writer < 2; writer++) {
                final int id = writer;
                writers.add(pool.submit(() -> {
                    for (int i = 0; i < 200; i++) {
                        String key = "writer-" + id + "-" + i;
                        mutationLock.lock();
                        try {
                            state.put(key, "value");
                            if (id == 0) persistence.appendSync(NMapOperationType.PUT, key, "value");
                            else persistence.appendAsync(NMapOperationType.PUT, key, "value");
                            if (i % 3 == 0) {
                                state.remove(key);
                                persistence.appendAsync(NMapOperationType.REMOVE, key, null);
                            }
                        } finally { mutationLock.unlock(); }
                    }
                    return null;
                }));
            }
            Future<?> snapshots = pool.submit(() -> {
                for (int i = 0; i < 20; i++) persistence.forceSnapshot();
                return null;
            });
            for (Future<?> writer : writers) writer.get(20, TimeUnit.SECONDS);
            snapshots.get(20, TimeUnit.SECONDS);
            persistence.close();
            Map<String, String> recovered = new ConcurrentHashMap<>();
            NMapPersistence<String, String> reopened = new NMapPersistence<>(config(), recovered, directory, "concurrent");
            reopened.load();
            assertEquals(state, recovered);
            assertEquals(266, recovered.size());
        } finally {
            pool.shutdownNow();
            persistence.close();
        }
    }

    @Test
    void recoveryFinishesPreparedCheckpointAndSkipsCoveredOldWal() throws Exception {
        assertRecovery(false);
    }

    @Test
    void recoverySkipsCoveredOldWalAfterSnapshotReplacement() throws Exception {
        assertRecovery(true);
    }

    private void assertRecovery(boolean alreadyReplaced) throws Exception {
        Map<String, String> state = new ConcurrentHashMap<>();
        NMapPersistence<String, String> persistence = new NMapPersistence<>(config(), state, directory, "map");
        persistence.start();
        persistence.appendSync(NMapOperationType.PUT, "obsolete", "old-lineage");
        persistence.close();
        Path map = directory.resolve("map");
        Files.move(map.resolve("wal.log"), map.resolve("wal.log.old"));
        Path prepared = map.resolve(alreadyReplaced ? "snapshot.dat" : "snapshot.dat.tmp");
        try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(prepared))) {
            out.writeObject(new HashMap<>(Map.of("correct", "leader")));
        }
        Files.createFile(map.resolve("snapshot.pending"));
        Map<String, String> recovered = new ConcurrentHashMap<>();
        NMapPersistence<String, String> reopened = new NMapPersistence<>(config(), recovered, directory, "map");
        reopened.load();
        assertEquals(Map.of("correct", "leader"), recovered);
        assertFalse(Files.exists(map.resolve("wal.log.old")));
        assertFalse(Files.exists(map.resolve("snapshot.pending")));
    }

    @Test
    void checkpointFailurePropagatesAndCloseCannotClaimCleanPersistence() throws Exception {
        Map<String, String> state = new ConcurrentHashMap<>(Map.of("key", "value"));
        NMapPersistence<String, String> persistence = new NMapPersistence<>(config(), state, directory, "map");
        persistence.start();
        Path temp = directory.resolve("map/snapshot.dat.tmp");
        Files.createDirectories(temp);
        Files.writeString(temp.resolve("obstruction"), "cannot replace");
        assertThrows(IOException.class, persistence::forceSnapshot);
        assertTrue(persistence.failureCount() > 0);
        assertThrows(IOException.class, persistence::close);
    }

    @Test
    void fsyncFailureKeepsPreviousDurableStateAndCannotAuthorizeCleanClose() throws Exception {
        Map<String, String> state = new ConcurrentHashMap<>(Map.of("before", "durable"));
        NMapPersistence<String, String> persistence = new NMapPersistence<>(config(), state, directory, "fsync-failure");
        persistence.start();
        persistence.forceSnapshot();
        state.clear(); state.put("incoming", "unconfirmed");
        persistence.checkpointFaultInjector(path -> {
            if (path.getFileName().toString().equals("snapshot.dat.tmp")) throw new IOException("injected fsync failure");
        });
        IOException failure = assertThrows(IOException.class, persistence::forceSnapshot);
        assertEquals("injected fsync failure", failure.getMessage());
        assertThrows(IOException.class, persistence::close);
        Map<String, String> recovered = new ConcurrentHashMap<>();
        new NMapPersistence<>(config(), recovered, directory, "fsync-failure").load();
        assertEquals(Map.of("before", "durable"), recovered);
    }

    @Test
    void rotationFailureLeavesOriginalWalRecoverableAndCannotAuthorizeCleanClose() throws Exception {
        Map<String, String> state = new ConcurrentHashMap<>(Map.of("before", "durable"));
        NMapPersistence<String, String> persistence = new NMapPersistence<>(config(), state, directory, "rotation-failure");
        persistence.start();
        persistence.forceSnapshot();
        persistence.appendSync(NMapOperationType.PUT, "tail", "confirmed");
        state.clear(); state.put("incoming", "unconfirmed");
        Path obstacle = directory.resolve("rotation-failure/wal.log.old");
        Files.createDirectories(obstacle);
        Files.writeString(obstacle.resolve("obstruction"), "cannot rotate");
        assertThrows(IOException.class, persistence::forceSnapshot);
        assertThrows(IOException.class, persistence::close);
        assertTrue(Files.size(directory.resolve("rotation-failure/wal.log")) > 0, "original WAL was not removed");
        Files.delete(obstacle.resolve("obstruction")); Files.delete(obstacle);
        Map<String, String> recovered = new ConcurrentHashMap<>();
        new NMapPersistence<>(config(), recovered, directory, "rotation-failure").load();
        assertEquals(Map.of("before", "durable", "tail", "confirmed"), recovered);
    }

    @Test
    void startupForceFailureIsReportedBeforeWriterPublicationAndCleanClose() throws Exception {
        NMapPersistence<String, String> persistence = new NMapPersistence<>(config(),
                new ConcurrentHashMap<>(), directory.resolve("new/nested"), "startup");
        persistence.checkpointFaultInjector(path -> {
            if (path.equals(directory.resolve("new/nested/startup"))) throw new IOException("injected startup directory fsync failure");
        });
        persistence.start();
        assertEquals(1, persistence.failureCount(), "service constructor refuses publication on this failure");
        assertThrows(IOException.class, persistence::close);
        assertFalse(persistence.walOpen());
    }

    private static class BlockingValue implements Serializable {
        final transient CountDownLatch entered = new CountDownLatch(1);
        final transient CountDownLatch release = new CountDownLatch(1);
        private void writeObject(ObjectOutputStream out) throws IOException {
            entered.countDown();
            try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            out.defaultWriteObject();
        }
    }
}
