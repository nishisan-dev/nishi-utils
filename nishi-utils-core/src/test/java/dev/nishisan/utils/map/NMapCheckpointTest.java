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
        try (DataOutputStream marker = new DataOutputStream(Files.newOutputStream(map.resolve("snapshot.pending")))) {
            marker.writeInt(0x4E4D4350);
            marker.writeInt(1);
            marker.writeLong(Files.size(prepared));
            marker.write(java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(prepared)));
        }
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

    @Test
    void failedFinalFsyncStillClosesDescriptorWhenStartupDidNotPublishWriter() throws Exception {
        NMapPersistence<String, String> persistence = new NMapPersistence<>(config(),
                new ConcurrentHashMap<>(), directory, "startup-close");
        persistence.checkpointFaultInjector(path -> {
            if (path.getFileName().toString().equals("startup-close")) throw new IOException("startup failure");
        });
        persistence.start();
        java.lang.reflect.Field field = NMapPersistence.class.getDeclaredField("walChannel");
        field.setAccessible(true);
        java.nio.channels.FileChannel original = (java.nio.channels.FileChannel) field.get(persistence);
        assertTrue(original.isOpen());
        field.set(persistence, new ForceFailingChannel(original));
        assertThrows(IOException.class, persistence::close);
        assertFalse(original.isOpen(), "failure in final force must not leak the underlying descriptor");
        assertNull(field.get(persistence));
    }

    @Test @Timeout(20)
    void asyncAdmissionNeverWaitsForWriterFsync() throws Exception {
        Map<String, Serializable> state = new ConcurrentHashMap<>();
        java.util.concurrent.locks.ReentrantLock stateLock = new java.util.concurrent.locks.ReentrantLock();
        NMapPersistence<String, Serializable> persistence = new NMapPersistence<>(config(), state, directory, "admission", stateLock);
        persistence.start();
        java.lang.reflect.Field channelField = NMapPersistence.class.getDeclaredField("walChannel");
        channelField.setAccessible(true);
        BlockingForceChannel channel = new BlockingForceChannel((java.nio.channels.FileChannel) channelField.get(persistence));
        channelField.set(persistence, channel);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            stateLock.lock();
            try { state.put("first", "one"); persistence.appendAsync(NMapOperationType.PUT, "first", "one"); }
            finally { stateLock.unlock(); }
            assertTrue(channel.entered.await(5, TimeUnit.SECONDS));
            pool.submit(() -> {
                stateLock.lock();
                try { state.put("second", "two"); persistence.appendAsync(NMapOperationType.PUT, "second", "two"); }
                finally { stateLock.unlock(); }
            }).get(1, TimeUnit.SECONDS);
            channel.release.countDown();
            persistence.close();
            Map<String, Serializable> recovered = new ConcurrentHashMap<>();
            new NMapPersistence<>(config(), recovered, directory, "admission").load();
            assertEquals(Map.of("first", "one", "second", "two"), recovered);
        } finally { channel.release.countDown(); pool.shutdownNow(); persistence.close(); }
    }

    @Test @Timeout(20)
    void periodicSerializationDoesNotHoldMutationLockAndCloseCanFinish() throws Exception {
        assertPeriodicDoesNotBlock(true);
    }

    @Test @Timeout(20)
    void periodicTempFsyncDoesNotHoldMutationLockAndPreservesQueuedTail() throws Exception {
        assertPeriodicDoesNotBlock(false);
    }

    private void assertPeriodicDoesNotBlock(boolean serialization) throws Exception {
        Map<String, Serializable> state = new ConcurrentHashMap<>();
        BlockingValue blockingValue = new BlockingValue();
        state.put("base", serialization ? blockingValue : "original");
        java.util.concurrent.locks.ReentrantLock stateLock = new java.util.concurrent.locks.ReentrantLock();
        NMapConfig cfg = NMapConfig.builder().mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(0).snapshotIntervalTime(Duration.ofMillis(1))
                .batchTimeout(Duration.ofMillis(2)).build();
        NMapPersistence<String, Serializable> persistence = new NMapPersistence<>(cfg, state, directory, "periodic", stateLock);
        CountDownLatch forceEntered = new CountDownLatch(1), forceRelease = new CountDownLatch(1);
        if (!serialization) persistence.checkpointFaultInjector(path -> {
            if (path.getFileName().toString().equals("snapshot.dat.tmp")) {
                forceEntered.countDown();
                try { forceRelease.await(); } catch (InterruptedException e) { throw new IOException(e); }
            }
        });
        persistence.start();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            assertTrue((serialization ? blockingValue.entered : forceEntered).await(5, TimeUnit.SECONDS));
            pool.submit(() -> {
                stateLock.lock();
                try { state.put("tail", "new"); persistence.appendAsync(NMapOperationType.PUT, "tail", "new"); }
                finally { stateLock.unlock(); }
            }).get(1, TimeUnit.SECONDS);
            // A cluster service closes while owning the shared lifecycle state lock.
            Future<?> closing = pool.submit(() -> {
                stateLock.lock();
                try { persistence.close(); return null; }
                finally { stateLock.unlock(); }
            });
            blockingValue.release.countDown(); forceRelease.countDown();
            closing.get(5, TimeUnit.SECONDS);
            Map<String, Serializable> recovered = new ConcurrentHashMap<>();
            new NMapPersistence<>(cfg, recovered, directory, "periodic").load();
            assertEquals(Set.of("base", "tail"), recovered.keySet());
            assertEquals("new", recovered.get("tail"));
        } finally {
            blockingValue.release.countDown(); forceRelease.countDown(); pool.shutdownNow(); persistence.close();
        }
    }

    @Test @Timeout(20)
    void markerFsyncFailureIsFailStopAndPreservesConfirmedOldAndTailWrites() throws Exception {
        Map<String, String> state = new ConcurrentHashMap<>(Map.of("base", "original"));
        java.util.concurrent.locks.ReentrantLock stateLock = new java.util.concurrent.locks.ReentrantLock();
        NMapPersistence<String, String> persistence = new NMapPersistence<>(config(), state, directory, "marker-failure", stateLock);
        persistence.start(); persistence.forceSnapshot();
        stateLock.lock();
        try { state.put("confirmed-old", "before"); persistence.appendSync(NMapOperationType.PUT, "confirmed-old", "before"); }
        finally { stateLock.unlock(); }
        CountDownLatch markerEntered = new CountDownLatch(1), markerRelease = new CountDownLatch(1);
        persistence.checkpointFaultInjector(path -> {
            if (path.getFileName().toString().equals("snapshot.pending")) {
                markerEntered.countDown();
                try { markerRelease.await(); } catch (InterruptedException e) { throw new IOException(e); }
                throw new IOException("injected marker fsync failure");
            }
        });
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<?> checkpoint = pool.submit(() -> { persistence.forceSnapshot(); return null; });
            assertTrue(markerEntered.await(5, TimeUnit.SECONDS));
            stateLock.lock();
            try { state.put("confirmed-tail", "after"); persistence.appendSync(NMapOperationType.PUT, "confirmed-tail", "after"); }
            finally { stateLock.unlock(); }
            markerRelease.countDown();
            assertThrows(ExecutionException.class, () -> checkpoint.get(5, TimeUnit.SECONDS));
            Path temporary = directory.resolve("marker-failure/snapshot.dat.tmp");
            byte[] immutable = Files.readAllBytes(temporary);
            for (int i = 0; i < 100; i++) persistence.maybeSnapshot();
            assertThrows(IOException.class, persistence::forceSnapshot);
            assertThrows(IllegalStateException.class, () -> persistence.appendSync(NMapOperationType.PUT, "rejected", "value"));
            assertThrows(IllegalStateException.class, () -> persistence.appendAsync(NMapOperationType.PUT, "rejected", "value"));
            assertArrayEquals(immutable, Files.readAllBytes(temporary));
            assertEquals(1, persistence.failureCount(), "no retry storm and no repeated failure count");
            assertThrows(IOException.class, persistence::close);
            Map<String, String> recovered = new ConcurrentHashMap<>();
            NMapPersistence<String, String> reopened = new NMapPersistence<>(config(), recovered, directory, "marker-failure");
            reopened.load();
            assertEquals(state, recovered);
            assertEquals(0, reopened.failureCount());
        } finally { markerRelease.countDown(); pool.shutdownNow(); assertThrows(IOException.class, persistence::close); }
    }

    @Test
    void invalidBoundMarkerPreventsRawOpenAndPreservesRecoveryEvidence() throws Exception {
        Map<String, String> state = new ConcurrentHashMap<>(Map.of("base", "original"));
        NMapPersistence<String, String> persistence = new NMapPersistence<>(config(), state, directory, "bad-marker");
        persistence.start();
        java.util.concurrent.atomic.AtomicInteger directoryForces = new java.util.concurrent.atomic.AtomicInteger();
        persistence.checkpointFaultInjector(path -> {
            if (path.equals(directory.resolve("bad-marker")) && directoryForces.incrementAndGet() == 2)
                throw new IOException("crash after marker rename");
        });
        assertThrows(IOException.class, persistence::forceSnapshot);
        assertThrows(IOException.class, persistence::close);
        Path map = directory.resolve("bad-marker");
        assertTrue(Files.exists(map.resolve("snapshot.pending")));
        byte[] temporary = Files.readAllBytes(map.resolve("snapshot.dat.tmp"));
        temporary[temporary.length - 1] ^= 1;
        Files.write(map.resolve("snapshot.dat.tmp"), temporary);
        byte[] oldWal = Files.readAllBytes(map.resolve("wal.log.old"));
        assertThrows(IllegalStateException.class, () -> NMap.open(directory, "bad-marker", config()));
        assertArrayEquals(oldWal, Files.readAllBytes(map.resolve("wal.log.old")));
        assertArrayEquals(temporary, Files.readAllBytes(map.resolve("snapshot.dat.tmp")));
        assertTrue(Files.exists(map.resolve("snapshot.pending")));
    }

    @Test
    void ambiguousEmptyLegacyMarkerFailsClosedWithoutDeletingOldWal() throws Exception {
        Path map = directory.resolve("legacy-pending"); Files.createDirectories(map);
        try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(map.resolve("snapshot.dat.tmp")))) {
            out.writeObject(new HashMap<>(Map.of("incoming", "unidentified")));
        }
        Files.write(map.resolve("wal.log.old"), new byte[]{1, 2, 3});
        Files.createFile(map.resolve("snapshot.pending"));
        assertThrows(IllegalStateException.class, () -> NMap.open(directory, "legacy-pending", config()));
        assertArrayEquals(new byte[]{1, 2, 3}, Files.readAllBytes(map.resolve("wal.log.old")));
        assertTrue(Files.exists(map.resolve("snapshot.dat.tmp")));
    }

    @Test
    void legacyMarkerWithoutOldWalOrTempCanFinishProvablyCommittedSnapshot() throws Exception {
        Path map = directory.resolve("legacy-committed"); Files.createDirectories(map);
        try (ObjectOutputStream out = new ObjectOutputStream(Files.newOutputStream(map.resolve("snapshot.dat")))) {
            out.writeObject(new HashMap<>(Map.of("confirmed", "committed")));
        }
        Files.createFile(map.resolve("snapshot.pending"));
        try (NMap<String, String> recovered = NMap.open(directory, "legacy-committed", config())) {
            assertEquals(Optional.of("committed"), recovered.get("confirmed"));
        }
        assertFalse(Files.exists(map.resolve("snapshot.pending")));
    }

    @Test
    void historicalLoadFailureDoesNotPreventExplicitDestroyAfterTermination() throws Exception {
        Path map = directory.resolve("destroy-corrupt"); Files.createDirectories(map);
        Files.writeString(map.resolve("snapshot.dat"), "invalid snapshot");
        NMapPersistence<String, String> persistence = new NMapPersistence<>(config(), new ConcurrentHashMap<>(), directory, "destroy-corrupt");
        persistence.load();
        assertEquals(1, persistence.failureCount());
        persistence.destroy();
        assertFalse(Files.exists(map));
    }

    @Test @Timeout(20)
    void destroyNeverDeletesFilesWhileWriterStillOwnsThem() throws Exception {
        NMapPersistence<String, Serializable> persistence = new NMapPersistence<>(config(), new ConcurrentHashMap<>(), directory, "destroy-live");
        persistence.start();
        BlockingValue value = new BlockingValue();
        persistence.appendAsync(NMapOperationType.PUT, "blocked", value);
        assertTrue(value.entered.await(5, TimeUnit.SECONDS));
        persistence.closeJoinTimeoutMillis(100);
        try {
            assertThrows(IOException.class, persistence::destroy);
            assertTrue(Files.exists(directory.resolve("destroy-live/wal.log")));
        } finally { value.release.countDown(); }
        persistence.closeJoinTimeoutMillis(5_000);
        persistence.destroy();
        assertFalse(Files.exists(directory.resolve("destroy-live")));
    }

    @Test
    void startupOnlySyncsParentsOfActuallyCreatedDirectories() throws Exception {
        Path existing = directory.resolve("existing"); Files.createDirectories(existing);
        NMapPersistence<String, String> persistence = new NMapPersistence<>(config(), new ConcurrentHashMap<>(), existing.resolve("new/nested"), "map");
        Set<Path> forced = new HashSet<>();
        persistence.checkpointFaultInjector(path -> {
            forced.add(path);
            if (path.equals(directory) || path.equals(directory.getParent())) throw new IOException("unnecessary ancestor access");
        });
        persistence.load(); persistence.start();
        assertEquals(0, persistence.failureCount());
        assertTrue(forced.contains(existing));
        assertTrue(forced.contains(existing.resolve("new")));
        assertTrue(forced.contains(existing.resolve("new/nested")));
        assertFalse(forced.contains(directory));
        persistence.close();
    }

    @Test
    void recoveryTruncatesTornOldWalBeforeMergingConfirmedNewTail() throws Exception {
        NMapPersistence<String, String> old = new NMapPersistence<>(config(), new ConcurrentHashMap<>(), directory, "torn-old");
        old.start(); old.appendSync(NMapOperationType.PUT, "old", "confirmed"); old.close();
        Path map = directory.resolve("torn-old");
        Files.move(map.resolve("wal.log"), map.resolve("wal.log.old"));
        Files.write(map.resolve("wal.log.old"), new byte[]{7, 8, 9}, StandardOpenOption.APPEND);
        NMapPersistence<String, String> tail = new NMapPersistence<>(config(), new ConcurrentHashMap<>(), directory, "torn-old");
        tail.start(); tail.appendSync(NMapOperationType.PUT, "tail", "confirmed"); tail.close();
        Map<String, String> recovered = new ConcurrentHashMap<>();
        NMapPersistence<String, String> reopened = new NMapPersistence<>(config(), recovered, directory, "torn-old");
        reopened.load();
        assertEquals(0, reopened.failureCount());
        assertEquals(Map.of("old", "confirmed", "tail", "confirmed"), recovered);
        assertFalse(Files.exists(map.resolve("wal.log.old")));
        recovered.clear(); reopened.load();
        assertEquals(Map.of("old", "confirmed", "tail", "confirmed"), recovered);
    }

    @Test @Timeout(20)
    void destroyNeverDeletesFilesUnderExternalCheckpointSerialization() throws Exception {
        Map<String, Serializable> state = new ConcurrentHashMap<>();
        BlockingValue value = new BlockingValue(); state.put("base", value);
        NMapPersistence<String, Serializable> persistence = new NMapPersistence<>(config(), state, directory, "external-checkpoint");
        persistence.start(); persistence.closeJoinTimeoutMillis(100);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<?> checkpoint = pool.submit(() -> { persistence.forceSnapshot(); return null; });
            assertTrue(value.entered.await(5, TimeUnit.SECONDS));
            assertThrows(IOException.class, persistence::destroy);
            assertTrue(Files.exists(directory.resolve("external-checkpoint/wal.log.old")));
            assertTrue(Files.exists(directory.resolve("external-checkpoint/snapshot.dat.tmp")));
            value.release.countDown(); checkpoint.get(5, TimeUnit.SECONDS);
            persistence.closeJoinTimeoutMillis(5_000); persistence.destroy();
            assertFalse(Files.exists(directory.resolve("external-checkpoint")));
        } finally { value.release.countDown(); pool.shutdownNow(); }
    }

    @Test
    void interruptedCloseAfterFailedStartupReleasesIdleDescriptor() throws Exception {
        NMapPersistence<String, String> persistence = new NMapPersistence<>(config(), new ConcurrentHashMap<>(), directory, "interrupted-start");
        persistence.checkpointFaultInjector(path -> {
            if (path.getFileName().toString().equals("interrupted-start")) throw new IOException("startup failure");
        });
        persistence.start();
        assertTrue(persistence.walOpen());
        Thread.currentThread().interrupt();
        try {
            assertThrows(IOException.class, persistence::close);
            assertTrue(Thread.currentThread().isInterrupted());
            assertFalse(persistence.walOpen());
        } finally { Thread.interrupted(); }
    }

    private static class BlockingForceChannel extends ForceFailingChannel {
        final CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        final java.util.concurrent.atomic.AtomicBoolean first = new java.util.concurrent.atomic.AtomicBoolean(true);
        BlockingForceChannel(java.nio.channels.FileChannel delegate) { super(delegate); }
        @Override public void force(boolean metadata) throws IOException {
            if (first.compareAndSet(true, false)) {
                entered.countDown();
                try { release.await(); } catch (InterruptedException e) { throw new IOException(e); }
            }
            delegate.force(metadata);
        }
    }

    private static class ForceFailingChannel extends java.nio.channels.FileChannel {
        protected final java.nio.channels.FileChannel delegate;
        ForceFailingChannel(java.nio.channels.FileChannel delegate) { this.delegate = delegate; }
        public void force(boolean metadata) throws IOException { throw new IOException("injected final force failure"); }
        protected void implCloseChannel() throws IOException { delegate.close(); }
        public int read(java.nio.ByteBuffer dst) throws IOException { return delegate.read(dst); }
        public long read(java.nio.ByteBuffer[] dst, int offset, int length) throws IOException { return delegate.read(dst, offset, length); }
        public int write(java.nio.ByteBuffer src) throws IOException { return delegate.write(src); }
        public long write(java.nio.ByteBuffer[] src, int offset, int length) throws IOException { return delegate.write(src, offset, length); }
        public long position() throws IOException { return delegate.position(); }
        public java.nio.channels.FileChannel position(long value) throws IOException { delegate.position(value); return this; }
        public long size() throws IOException { return delegate.size(); }
        public java.nio.channels.FileChannel truncate(long size) throws IOException { delegate.truncate(size); return this; }
        public long transferTo(long position, long count, java.nio.channels.WritableByteChannel target) throws IOException { return delegate.transferTo(position, count, target); }
        public long transferFrom(java.nio.channels.ReadableByteChannel src, long position, long count) throws IOException { return delegate.transferFrom(src, position, count); }
        public int read(java.nio.ByteBuffer dst, long position) throws IOException { return delegate.read(dst, position); }
        public int write(java.nio.ByteBuffer src, long position) throws IOException { return delegate.write(src, position); }
        public java.nio.MappedByteBuffer map(MapMode mode, long position, long size) throws IOException { return delegate.map(mode, position, size); }
        public java.nio.channels.FileLock lock(long position, long size, boolean shared) throws IOException { return delegate.lock(position, size, shared); }
        public java.nio.channels.FileLock tryLock(long position, long size, boolean shared) throws IOException { return delegate.tryLock(position, size, shared); }
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
