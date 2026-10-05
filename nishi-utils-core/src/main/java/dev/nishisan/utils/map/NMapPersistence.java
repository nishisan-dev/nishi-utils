/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.map;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Local disk persistence engine for {@link NMap}. Uses an append-only WAL and
 * periodic full snapshots.
 * <p>
 * Ordinary WAL write failures are reported through logging and the health listener.
 * Initialization and checkpoint failures are fail-stop: the engine rejects later
 * mutations until restarted and preserves its recovery files. Explicit destruction
 * remains available after every writer and checkpoint has terminated.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class NMapPersistence<K, V> implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(NMapPersistence.class.getName());

    private static final String WAL_FILE = "wal.log";
    private static final String SNAPSHOT_FILE = "snapshot.dat";
    private static final String META_FILE = "map.meta";

    // Entry framing + compatibility
    private static final int ENTRY_MAGIC = 0x4E4D5741; // "NMWA"
    private static final int ENTRY_VERSION = 1;
    private static final int META_VERSION = 2;

    private final NMapConfig config;
    private final Map<K, V> data;
    private final NMapHealthListener healthListener;
    private final AtomicLong failureCount = new AtomicLong();
    private final Path mapDir;
    private final Path walPath;
    private final Path snapshotPath;
    private final Path metaPath;
    private final Path tempSnapshotPath;
    private final Path oldWalPath;
    private final Path pendingSnapshotPath;
    private volatile boolean snapshotsSuspended;
    private volatile IOException checkpointFailure;
    @FunctionalInterface
    interface CheckpointFaultInjector {
        void beforeForce(Path path) throws IOException;
    }
    private volatile CheckpointFaultInjector checkpointFaultInjector = path -> { };

    void checkpointFaultInjector(CheckpointFaultInjector injector) {
        checkpointFaultInjector = Objects.requireNonNull(injector, "injector");
    }

    private final LinkedBlockingQueue<NMapWALEntry> queue = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean();
    /** Bound of the writer join in {@link #close()}; package-visible setter for tests. */
    private volatile long closeJoinTimeoutMillis = TimeUnit.SECONDS.toMillis(10);
    private final AtomicLong lastMutationTimeMillis = new AtomicLong();
    private final ReentrantLock walLock = new ReentrantLock();
    private final ReentrantLock queueLock = new ReentrantLock();
    private final Condition queueAvailable = queueLock.newCondition();
    private final ReentrantLock checkpointLock = new ReentrantLock();
    private volatile boolean closeRequested;
    private final List<Path> createdDirectoryParents = new ArrayList<>();
    private final ReentrantLock stateLock;

    private volatile RandomAccessFile walRaf;
    private volatile FileChannel walChannel;
    private volatile Thread writerThread;

    private volatile long opsSinceSnapshot;
    private volatile long lastSnapshotTimeMillis;

    /**
     * Creates a new persistence instance.
     * Direct users must quiesce mutations during snapshots. Concurrent mutation integrations
     * should use the shared-lock overload for both periodic and explicit snapshots.
     *
     * @param config  the persistence configuration
     * @param data    the in-memory map to persist
     * @param baseDir the base directory for persistence files
     * @param mapName the map name (used as subdirectory)
     */
    public NMapPersistence(NMapConfig config, Map<K, V> data, Path baseDir, String mapName) {
        this(config, data, baseDir, mapName, new ReentrantLock());
    }

    /**
     * Creates a persistence engine with a shared mutation lock for infrastructure integrations.
     * Callers hold this lock through a state mutation and its WAL admission. Acquisition order
     * is mutation lock followed by the engine's WAL lock; neither pins virtual thread carriers.
     *
     * @param config the persistence configuration
     * @param data the in-memory map
     * @param baseDir the persistence base directory
     * @param mapName the logical map name
     * @param stateLock the lock shared with every mutator of the supplied map
     * @since 8.11.0
     */
    public NMapPersistence(NMapConfig config, Map<K, V> data, Path baseDir, String mapName,
            ReentrantLock stateLock) {
        this.stateLock = Objects.requireNonNull(stateLock, "stateLock");
        this.config = Objects.requireNonNull(config, "config");
        this.data = Objects.requireNonNull(data, "data");
        this.healthListener = config.healthListener() != null
                ? config.healthListener()
                : (name, type, cause) -> {
                };
        Objects.requireNonNull(baseDir, "baseDir");
        Objects.requireNonNull(mapName, "mapName");
        this.mapDir = baseDir.resolve(mapName);
        this.walPath = mapDir.resolve(WAL_FILE);
        this.snapshotPath = mapDir.resolve(SNAPSHOT_FILE);
        this.metaPath = mapDir.resolve(META_FILE);
        this.tempSnapshotPath = mapDir.resolve(SNAPSHOT_FILE + ".tmp");
        this.oldWalPath = mapDir.resolve(WAL_FILE + ".old");
        this.pendingSnapshotPath = mapDir.resolve("snapshot.pending");
    }

    /**
     * Returns the number of persistence failures since this instance was created.
     *
     * @return the failure count
     */
    public long failureCount() {
        return failureCount.get();
    }

    /**
     * Returns the timestamp of the last mutation known to this persistence
     * engine, or {@code 0} when no mutation has been observed.
     *
     * @return the last mutation timestamp in epoch millis
     */
    public long lastMutationTimestamp() {
        return lastMutationTimeMillis.get();
    }

    /**
     * Loads state from disk (snapshot + WAL replay). No-op when persistence is
     * disabled.
     */
    public void load() {
        if (config.mode() == NMapPersistenceMode.DISABLED) {
            return;
        }
        try {
            createMapDirectories();
            recoverPendingSnapshot();
            loadSnapshot();
            if (Files.exists(oldWalPath)) {
                LOGGER.info("Detected incomplete rotation. Replaying old WAL.");
                loadWal(oldWalPath);
            }
            loadWal(walPath);
            consolidateRecoveredWal();
            readMeta().ifPresent(meta -> {
                lastSnapshotTimeMillis = meta.lastSnapshotTimestamp();
                lastMutationTimeMillis.accumulateAndGet(meta.lastMutationTimestamp(), Math::max);
            });
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to load map persistence state", e);
            failureCount.incrementAndGet();
            checkpointFailure = e;
        }
    }

    /**
     * Starts the background writer thread. No-op when persistence is disabled.
     */
    public void start() {
        if (config.mode() == NMapPersistenceMode.DISABLED) {
            return;
        }
        if (checkpointFailure != null || closeRequested) return;
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            createMapDirectories();
            openWalForAppend();
            forceCheckpointChannel(requireWal(), walPath);
            forceDirectory();
            // load() may have created the hierarchy first. Remember actual creations there,
            // and sync only parents whose directory entries this engine changed.
            for (Path directory : createdDirectoryParents) forceDirectory(directory);
            createdDirectoryParents.clear();
            if (lastSnapshotTimeMillis <= 0) {
                lastSnapshotTimeMillis = System.currentTimeMillis();
            }
        } catch (IOException e) {
            running.set(false);
            checkpointFailure = e;
            LOGGER.log(Level.WARNING, "Failed to start map persistence (WAL open)", e);
            failureCount.incrementAndGet();
            healthListener.onPersistenceFailure(mapDir.getFileName().toString(),
                    NMapHealthListener.PersistenceFailureType.WAL_OPEN, e);
            return;
        }

        writerThread = new Thread(this::runWriterLoop, "nmap-persistence");
        writerThread.setDaemon(true);
        writerThread.start();
    }

    /**
     * Appends a WAL entry asynchronously.
     *
     * @param type  the operation type
     * @param key   the key
     * @param value the value (may be null for REMOVE)
     */
    public void appendAsync(NMapOperationType type, Object key, Object value) {
        appendAsync(System.currentTimeMillis(), type, key, value);
    }

    /**
     * Appends a WAL entry asynchronously using the provided mutation timestamp.
     *
     * @param timestamp the mutation timestamp in epoch millis
     * @param type      the operation type
     * @param key       the key
     * @param value     the value (may be null for REMOVE)
     */
    public void appendAsync(long timestamp, NMapOperationType type, Object key, Object value) {
        if (config.mode() == NMapPersistenceMode.DISABLED) {
            return;
        }
        Objects.requireNonNull(type, "type");
        if (type != NMapOperationType.CLEAR) {
            Objects.requireNonNull(key, "key");
        }
        lastMutationTimeMillis.accumulateAndGet(timestamp, Math::max);
        queueLock.lock();
        try {
            ensureWritable();
            queue.offer(new NMapWALEntry(timestamp, type, key, value));
            queueAvailable.signal();
        } finally {
            queueLock.unlock();
        }
    }

    /**
     * Appends a WAL entry synchronously. Used for critical maps that must survive
     * hard crashes.
     *
     * @param type  the operation type
     * @param key   the key
     * @param value the value (may be null for REMOVE)
     */
    public void appendSync(NMapOperationType type, Object key, Object value) {
        appendSync(System.currentTimeMillis(), type, key, value);
    }

    /**
     * Appends a WAL entry synchronously using the provided mutation timestamp.
     *
     * @param timestamp the mutation timestamp in epoch millis
     * @param type      the operation type
     * @param key       the key
     * @param value     the value (may be null for REMOVE)
     */
    public void appendSync(long timestamp, NMapOperationType type, Object key, Object value) {
        if (config.mode() == NMapPersistenceMode.DISABLED) {
            return;
        }
        Objects.requireNonNull(type, "type");
        if (type != NMapOperationType.CLEAR) {
            Objects.requireNonNull(key, "key");
        }
        lastMutationTimeMillis.accumulateAndGet(timestamp, Math::max);
        NMapWALEntry entry = new NMapWALEntry(timestamp, type, key, value);
        walLock.lock();
        try {
            try {
                ensureWritable();
                writeWalEntry(entry);
                if (config.mode() == NMapPersistenceMode.ASYNC_WITH_FSYNC) forceWal();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to append WAL entry synchronously", e);
                failureCount.incrementAndGet();
                healthListener.onPersistenceFailure(mapDir.getFileName().toString(),
                    NMapHealthListener.PersistenceFailureType.WAL_WRITE, e);
            }
        } finally {
            walLock.unlock();
        }
    }

    /**
     * Stops the writer thread after it drains the queued WAL entries, then closes the WAL channel.
     *
     * @throws IOException if a writer/checkpoint does not terminate within the close bound,
     *                     the final fsync fails, or this engine recorded a persistence failure.
     *                     A live writer/checkpoint retains ownership of its files.
     */
    @Override
    public void close() throws IOException {
        if (config.mode() == NMapPersistenceMode.DISABLED) {
            return;
        }
        closeRequested = true;
        running.set(false);
        signalWriter();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(closeJoinTimeoutMillis);
        Thread t = writerThread;
        if (t != null) {
            try { t.join(closeJoinTimeoutMillis); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (t != null && t.isAlive()) {
            throw new IOException("NMap persistence writer for " + mapDir.getFileName()
                    + " did not terminate within " + closeJoinTimeoutMillis + " ms");
        }
        boolean acquired = false;
        try {
            acquired = checkpointLock.tryLock(Math.max(0L, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            if (!acquired) throw new IOException("Map checkpoint did not terminate within " + closeJoinTimeoutMillis + " ms");
            walLock.lock();
            try {
                try { if (walChannel != null) forceWal(); }
                finally { closeWalQuietly(); }
            } finally { walLock.unlock(); }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // A constructor abandoning a failed start must not leak its idle descriptor just
            // because its caller is interrupted. Do not wait for or disturb an active owner.
            if (checkpointLock.tryLock()) {
                try {
                    if (walLock.tryLock()) {
                        try { closeWalQuietly(); }
                        finally { walLock.unlock(); }
                    }
                } finally { checkpointLock.unlock(); }
            }
            throw new IOException("Interrupted waiting for map checkpoint to terminate", e);
        } finally {
            if (acquired) checkpointLock.unlock();
        }
        if (checkpointFailure != null) throw new IOException("Map checkpoint failed", checkpointFailure);
        if (failureCount.get() != 0) throw new IOException("Map persistence recorded " + failureCount.get() + " failure(s)");
    }

    /** Whether the WAL channel is open (tests only). */
    boolean walOpen() {
        FileChannel ch = walChannel;
        return ch != null && ch.isOpen();
    }

    /** Overrides the writer join bound of {@link #close()} (tests only). */
    void closeJoinTimeoutMillis(long millis) {
        this.closeJoinTimeoutMillis = millis;
    }

    /**
     * Triggers a snapshot if the configured interval (by operations or time) has
     * been exceeded.
     */
    public void maybeSnapshot() {
        if (snapshotsSuspended || !running.get() || checkpointFailure != null) return;
        boolean byOps = config.snapshotIntervalOperations() > 0
                && opsSinceSnapshot >= config.snapshotIntervalOperations();
        boolean byTime = config.snapshotIntervalTime() != null && !config.snapshotIntervalTime().isZero()
                && System.currentTimeMillis() - lastSnapshotTimeMillis >= config.snapshotIntervalTime().toMillis();
        if ((!byOps && !byTime) || !checkpointLock.tryLock()) return;
        try {
            Checkpoint checkpoint = prepareCheckpoint(true);
            if (checkpoint != null) finishCheckpoint(checkpoint);
        } catch (IOException e) {
            failCheckpoint(e);
        } finally { checkpointLock.unlock(); }
    }

    // ── Writer Loop ──────────────────────────────────────────────────────

    private void runWriterLoop() {
        long batchTimeoutMs = Math.max(1L, config.batchTimeout().toMillis());
        while (checkpointFailure == null && (running.get() || !queue.isEmpty())) {
            try {
                queueLock.lock();
                try {
                    if (queue.isEmpty() && running.get()) queueAvailable.await(batchTimeoutMs, TimeUnit.MILLISECONDS);
                } finally { queueLock.unlock(); }
                walLock.lock();
                try {
                    if (checkpointFailure != null) break;
                    List<NMapWALEntry> batch = new ArrayList<>(config.batchSize());
                    // Admission never waits for this batch's serialization/fsync. A batch removed
                    // before a checkpoint still completes under walLock before rotation.
                    queueLock.lock();
                    try { queue.drainTo(batch, config.batchSize()); }
                    finally { queueLock.unlock(); }
                    writeBatch(batch);
                    opsSinceSnapshot += batch.size();
                } finally { walLock.unlock(); }
                maybeSnapshot();
            } catch (InterruptedException e) {
                running.set(false);
                Thread.interrupted();
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Unexpected error in persistence writer loop", e);
            }
        }
        walLock.lock();
        try {
            try { if (walChannel != null) forceWal(); }
            catch (IOException e) { if (checkpointFailure == null) checkpointFailure = e; }
            closeWalQuietly();
        } finally { walLock.unlock(); }
    }

    private void signalWriter() {
        queueLock.lock();
        try { queueAvailable.signalAll(); }
        finally { queueLock.unlock(); }
    }

    void ensureWritable() {
        if (checkpointFailure != null) throw new IllegalStateException("Map persistence is fail-stopped; restart required", checkpointFailure);
        if (closeRequested) throw new IllegalStateException("Map persistence is closed");
    }

    private void failCheckpoint(IOException failure) {
        boolean firstFailure;
        queueLock.lock();
        try {
            firstFailure = checkpointFailure == null;
            if (firstFailure) {
                checkpointFailure = failure;
                failureCount.incrementAndGet();
            }
            running.set(false);
            queueAvailable.signalAll();
        } finally { queueLock.unlock(); }
        if (firstFailure) {
            LOGGER.log(Level.WARNING, "Map checkpoint failed; persistence is fail-stopped until restart", failure);
            healthListener.onPersistenceFailure(mapDir.getFileName().toString(),
                    NMapHealthListener.PersistenceFailureType.SNAPSHOT_WRITE, failure);
        }
    }

    private void writeBatch(List<NMapWALEntry> batch) {
        if (batch.isEmpty()) {
            return;
        }
        walLock.lock();
        try {
            try {
                for (NMapWALEntry entry : batch) writeWalEntry(entry);
                if (config.mode() == NMapPersistenceMode.ASYNC_WITH_FSYNC) forceWal();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to append WAL batch", e);
                failureCount.incrementAndGet();
                healthListener.onPersistenceFailure(mapDir.getFileName().toString(),
                    NMapHealthListener.PersistenceFailureType.WAL_WRITE, e);
            }
        } finally {
            walLock.unlock();
        }
    }

    // ── Snapshot ─────────────────────────────────────────────────────────

    /**
     * Durably checkpoints the complete map and removes the WAL entries covered by it.
     * Mutators must hold the shared mutation lock through mutation and WAL admission;
     * direct users of the original constructor must quiesce writes for an explicit checkpoint.
     *
     * @throws IOException if writing, syncing or replacing the checkpoint fails
     * @since 8.11.0
     */
    public void forceSnapshot() throws IOException {
        if (config.mode() == NMapPersistenceMode.DISABLED) return;
        checkpointLock.lock();
        try {
            if (checkpointFailure != null) throw new IOException("Map persistence is fail-stopped; restart required", checkpointFailure);
            if (closeRequested) throw new IOException("Map persistence is closed");
            Checkpoint checkpoint = prepareCheckpoint(false);
            finishCheckpoint(checkpoint);
        } catch (IOException e) {
            failCheckpoint(e);
            throw e;
        } finally { checkpointLock.unlock(); }
    }

    /**
     * Internal lifecycle control: prevents periodic snapshots during a partial installation.
     * Explicit {@link #forceSnapshot()} remains available to finish the installation.
     *
     * @param suspended whether periodic snapshots must be suspended
     * @since 8.11.0
     */
    public void setSnapshotsSuspended(boolean suspended) {
        // No WAL wait: the caller owns the shared state lock. An already captured periodic
        // checkpoint contains the trusted state before reset and needs no state lock to finish.
        snapshotsSuspended = suspended;
    }

    private final class Checkpoint {
        final Map<K, V> snapshot;
        final long mutationTimestamp;
        Checkpoint(Map<K, V> snapshot, long mutationTimestamp) {
            this.snapshot = snapshot;
            this.mutationTimestamp = mutationTimestamp;
        }
    }

    private Checkpoint prepareCheckpoint(boolean periodic) throws IOException {
        if (periodic) {
            if (!stateLock.tryLock()) return null;
        } else stateLock.lock();
        try {
            if (periodic && (!running.get() || snapshotsSuspended || checkpointFailure != null)) return null;
            if (periodic) {
                if (!walLock.tryLock()) return null;
            } else walLock.lock();
            try {
                requireWal();
                if (Files.exists(pendingSnapshotPath) || Files.exists(oldWalPath)) {
                    throw new IOException("Unfinished checkpoint requires recovery before a new checkpoint");
                }
                Map<K, V> snapshot = new HashMap<>(data);
                long timestamp = lastMutationTimeMillis.get();
                // Admissions still queued are covered by the frozen map but not yet by this WAL.
                // Until the snapshot is durable, recovery replays old snapshot + wal.old + new
                // tail, so they must reach the rotated WAL in admission order: dropping them
                // would let later tail writes recover over a gap in the history. Mutators cannot
                // admit while stateLock is held, and walLock serializes this with the writer.
                drainQueueToWal();
                // WITH_FSYNC batches, synchronous writes and the drain above have forced this WAL.
                // NO_FSYNC mutations become durable when the complete snapshot is forced below.
                closeWalQuietly();
                Files.move(walPath, oldWalPath); // never overwrite recovery evidence
                openWalForAppend();
                forceCheckpointChannel(requireWal(), walPath);
                forceDirectory();
                opsSinceSnapshot = 0;
                return new Checkpoint(snapshot, timestamp);
            } finally { walLock.unlock(); }
        } finally { stateLock.unlock(); }
    }

    /**
     * Writes every queued admission to the current WAL, in queue order, before rotation.
     * Caller holds walLock. A failure propagates and fail-stops the checkpoint before the
     * WAL is rotated, so no later write can be recovered after the missing entries.
     */
    private void drainQueueToWal() throws IOException {
        List<NMapWALEntry> pending = new ArrayList<>();
        queueLock.lock();
        try { queue.drainTo(pending); }
        finally { queueLock.unlock(); }
        if (pending.isEmpty()) return;
        for (NMapWALEntry entry : pending) writeWalEntry(entry);
        if (config.mode() == NMapPersistenceMode.ASYNC_WITH_FSYNC) forceWal();
    }

    private void finishCheckpoint(Checkpoint checkpoint) throws IOException {
        // The potentially multi-second serialization and file fsync run with neither the
        // shared mutation lock nor WAL lock held. Post-capture writes keep flowing to the tail.
        writeSnapshotTemp(checkpoint.snapshot);
        long snapshotSize = Files.size(tempSnapshotPath);
        byte[] digest = snapshotDigest(tempSnapshotPath);
        writeCheckpointMarker(snapshotSize, digest);
        walLock.lock();
        try {
            installSnapshotTemp();
            forceDirectory();
            Files.deleteIfExists(oldWalPath);
            forceDirectory();
            Files.deleteIfExists(pendingSnapshotPath);
            forceDirectory();
        } finally { walLock.unlock(); }
        writeMeta(new NMapMetadata(0L, System.currentTimeMillis(), checkpoint.mutationTimestamp, META_VERSION));
        lastSnapshotTimeMillis = System.currentTimeMillis();
    }

    private void writeSnapshotTemp(Map<K, V> snapshot) throws IOException {
        if (Files.exists(pendingSnapshotPath)) throw new IOException("Pending checkpoint temporary snapshot is immutable");
        try (BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(tempSnapshotPath));
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(snapshot);
            oos.flush();
        }
        try (FileChannel channel = FileChannel.open(tempSnapshotPath, StandardOpenOption.WRITE)) {
            forceCheckpointChannel(channel, tempSnapshotPath);
        }
    }

    private static final int CHECKPOINT_MARKER_MAGIC = 0x4E4D4350; // NMCP
    private static final int CHECKPOINT_MARKER_VERSION = 1;

    private void writeCheckpointMarker(long size, byte[] digest) throws IOException {
        Path temporary = pendingSnapshotPath.resolveSibling("snapshot.pending.tmp");
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(temporary))) {
            out.writeInt(CHECKPOINT_MARKER_MAGIC);
            out.writeInt(CHECKPOINT_MARKER_VERSION);
            out.writeLong(size);
            out.write(digest);
        }
        try (FileChannel marker = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
            // Keep the existing named failure hook useful at the marker commit boundary.
            forceCheckpointChannel(marker, pendingSnapshotPath);
        }
        Files.move(temporary, pendingSnapshotPath, StandardCopyOption.ATOMIC_MOVE);
        forceDirectory();
    }

    private byte[] snapshotDigest(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (java.io.InputStream input = Files.newInputStream(path)) {
                byte[] buffer = new byte[64 * 1024];
                int read;
                while ((read = input.read(buffer)) != -1) digest.update(buffer, 0, read);
            }
            return digest.digest();
        } catch (NoSuchAlgorithmException e) { throw new AssertionError(e); }
    }

    private void installSnapshotTemp() throws IOException {
        Files.move(tempSnapshotPath, snapshotPath, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
    }

    private void recoverPendingSnapshot() throws IOException {
        if (!Files.exists(pendingSnapshotPath)) return;
        Path candidate = Files.exists(tempSnapshotPath) ? tempSnapshotPath : snapshotPath;
        if (Files.size(pendingSnapshotPath) == 0) {
            // The old empty marker cannot identify a temp rewritten by 8.11.0's retry bug.
            // Only an already committed snapshot with no old WAL is provably unambiguous.
            if (Files.exists(tempSnapshotPath) || Files.exists(oldWalPath)) {
                throw new IOException("Ambiguous legacy snapshot.pending; preserve files and recover with a trusted backup");
            }
            validateSnapshotObject(candidate);
        } else {
            long size;
            byte[] digest;
            try (DataInputStream in = new DataInputStream(Files.newInputStream(pendingSnapshotPath))) {
                if (in.readInt() != CHECKPOINT_MARKER_MAGIC || in.readInt() != CHECKPOINT_MARKER_VERSION) {
                    throw new IOException("Invalid checkpoint marker");
                }
                size = in.readLong();
                digest = in.readNBytes(32);
                if (digest.length != 32 || in.read() != -1) throw new IOException("Invalid checkpoint marker length");
            }
            if (!Files.exists(candidate) || Files.size(candidate) != size
                    || !MessageDigest.isEqual(snapshotDigest(candidate), digest)) {
                throw new IOException("Checkpoint snapshot does not match its durable marker");
            }
            validateSnapshotObject(candidate);
            if (candidate.equals(tempSnapshotPath)) installSnapshotTemp();
        }
        forceDirectory();
        Files.deleteIfExists(oldWalPath);
        forceDirectory();
        Files.deleteIfExists(pendingSnapshotPath);
        forceDirectory();
    }

    private void validateSnapshotObject(Path path) throws IOException {
        try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            if (!(in.readObject() instanceof Map<?, ?>) || in.read() != -1) throw new IOException("Invalid checkpoint snapshot");
        } catch (ClassNotFoundException | RuntimeException e) { throw new IOException("Invalid checkpoint snapshot", e); }
    }

    private void consolidateRecoveredWal() throws IOException {
        if (!Files.exists(oldWalPath)) return;
        Path merged = mapDir.resolve("wal.recovery.tmp");
        try (java.io.OutputStream out = Files.newOutputStream(merged)) {
            Files.copy(oldWalPath, out);
            if (Files.exists(walPath)) Files.copy(walPath, out);
        }
        try (FileChannel channel = FileChannel.open(merged, StandardOpenOption.WRITE)) {
            forceCheckpointChannel(channel, merged);
        }
        Files.move(merged, walPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        forceDirectory();
        Files.deleteIfExists(oldWalPath);
        forceDirectory();
    }

    private void createMapDirectories() throws IOException {
        List<Path> missing = new ArrayList<>();
        for (Path path = mapDir.toAbsolutePath(); path != null && !Files.exists(path); path = path.getParent()) {
            missing.add(path);
        }
        Files.createDirectories(mapDir);
        for (Path created : missing) {
            Path parent = created.getParent();
            if (parent != null && !createdDirectoryParents.contains(parent)) createdDirectoryParents.add(parent);
        }
    }

    private void forceDirectory() throws IOException { forceDirectory(mapDir); }

    private void forceDirectory(Path path) throws IOException {
        try (FileChannel directory = FileChannel.open(path, StandardOpenOption.READ)) {
            forceCheckpointChannel(directory, path);
        }
    }

    private void forceCheckpointChannel(FileChannel channel, Path path) throws IOException {
        checkpointFaultInjector.beforeForce(path);
        channel.force(true);
    }

    // Live WAL IO must finish even if a relay applier is interrupted during shutdown.
    // Interruptible FileChannel IO would close the shared descriptor and break the writer's
    // final drain. RAF/FD operations preserve interruption without canceling confirmed writes;
    // walLock still serializes every write, fsync and rotation.
    private void writeWalEntry(NMapWALEntry entry) throws IOException {
        requireWal();
        ByteBuffer buffer = encode(entry);
        walRaf.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
    }

    private void forceWal() throws IOException {
        requireWal();
        checkpointFaultInjector.beforeForce(walPath);
        walRaf.getFD().sync();
    }

    private FileChannel requireWal() throws IOException {
        if (walChannel == null || !walChannel.isOpen()) throw new IOException("Map WAL is unavailable");
        return walChannel;
    }

    private void loadSnapshot() throws IOException {
        if (!Files.exists(snapshotPath)) {
            return;
        }
        try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(snapshotPath));
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            @SuppressWarnings("unchecked")
            Map<K, V> snapshot = (Map<K, V>) ois.readObject();
            data.clear();
            data.putAll(snapshot);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize snapshot", e);
        }
    }

    // ── WAL Replay ──────────────────────────────────────────────────────

    private void loadWal(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
                FileChannel ch = raf.getChannel()) {
            long size = ch.size();
            long offset = 0L;
            while (offset < size) {
                ByteBuffer lenBuf = ByteBuffer.allocate(4);
                int r = readFully(ch, lenBuf, offset);
                if (r <= 0) {
                    break;
                }
                if (r < 4) {
                    ch.truncate(offset);
                    break;
                }
                lenBuf.flip();
                int entryLen = lenBuf.getInt();
                if (entryLen <= 0 || entryLen > (64 * 1024 * 1024)) {
                    ch.truncate(offset);
                    break;
                }
                long entryStart = offset + 4L;
                long entryEnd = entryStart + entryLen;
                if (entryEnd > size) {
                    ch.truncate(offset);
                    break;
                }
                byte[] payload = new byte[entryLen];
                ByteBuffer pb = ByteBuffer.wrap(payload);
                int read = readFully(ch, pb, entryStart);
                if (read < entryLen) {
                    ch.truncate(offset);
                    break;
                }
                try {
                    applyDecoded(payload);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Invalid WAL entry detected, truncating", e);
                    ch.truncate(offset);
                    break;
                }
                offset = entryEnd;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void applyDecoded(byte[] payload) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            int magic = in.readInt();
            int version = in.readInt();
            if (magic != ENTRY_MAGIC || version != ENTRY_VERSION) {
                throw new IOException("Unsupported WAL entry format");
            }
            int typeOrdinal = in.readUnsignedByte();
            NMapOperationType[] values = NMapOperationType.values();
            if (typeOrdinal < 0 || typeOrdinal >= values.length) {
                throw new IOException("Invalid WAL entry type");
            }
            NMapOperationType type = values[typeOrdinal];
            long timestamp = in.readLong();
            lastMutationTimeMillis.accumulateAndGet(timestamp, Math::max);
            if (type == NMapOperationType.CLEAR) {
                // CLEAR carries no key/value — empty the map and stop. The WAL
                // framing (entry length) lets the replay loop skip the trailing
                // zero-length key/value fields written by encode().
                data.clear();
                return;
            }
            int keyLen = in.readInt();
            if (keyLen <= 0 || keyLen > (16 * 1024 * 1024)) {
                throw new IOException("Invalid key length");
            }
            byte[] keyBytes = in.readNBytes(keyLen);
            Object key = deserialize(keyBytes);
            int valueLen = in.readInt();
            Object value = null;
            if (valueLen > 0) {
                if (valueLen > (64 * 1024 * 1024)) {
                    throw new IOException("Invalid value length");
                }
                byte[] valueBytes = in.readNBytes(valueLen);
                value = deserialize(valueBytes);
            }
            K k = (K) key;
            V v = (V) value;
            switch (type) {
                case PUT -> data.put(k, v);
                case REMOVE -> data.remove(k);
            }
        }
    }

    // ── Encoding / Decoding ─────────────────────────────────────────────

    private ByteBuffer encode(NMapWALEntry entry) throws IOException {
        byte[] keyBytes = entry.key() != null ? serialize(entry.key()) : new byte[0];
        byte[] valueBytes = entry.value() != null ? serialize(entry.value()) : new byte[0];

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(ENTRY_MAGIC);
            out.writeInt(ENTRY_VERSION);
            out.writeByte(entry.type().ordinal());
            out.writeLong(entry.timestamp());
            out.writeInt(keyBytes.length);
            out.write(keyBytes);
            out.writeInt(valueBytes.length);
            if (valueBytes.length > 0) {
                out.write(valueBytes);
            }
            out.flush();
        }

        byte[] body = bos.toByteArray();
        ByteBuffer framed = ByteBuffer.allocate(4 + body.length);
        framed.putInt(body.length);
        framed.put(body);
        framed.flip();
        return framed;
    }

    private static byte[] serialize(Object obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos))) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        }
    }

    private static Object deserialize(byte[] bytes) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(new ByteArrayInputStream(bytes)))) {
            return ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize WAL field", e);
        }
    }

    // ── WAL I/O ─────────────────────────────────────────────────────────

    private void openWalForAppend() throws IOException {
        createMapDirectories();
        walRaf = new RandomAccessFile(walPath.toFile(), "rw");
        walChannel = walRaf.getChannel();
        walChannel.position(walChannel.size());
    }

    private int readFully(FileChannel channel, ByteBuffer buffer, long offset) throws IOException {
        int total = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, offset + total);
            if (read <= 0) {
                break;
            }
            total += read;
        }
        return total;
    }

    private void closeWalQuietly() {
        FileChannel ch = walChannel;
        RandomAccessFile raf = walRaf;
        walChannel = null;
        walRaf = null;
        if (ch != null) {
            try {
                ch.close();
            } catch (IOException ignored) {
            }
        }
        if (raf != null) {
            try {
                raf.close();
            } catch (IOException ignored) {
            }
        }
    }

    // ── Destroy ─────────────────────────────────────────────────────────

    /**
     * Encerra a engine de persistência e remove todos os arquivos associados ao mapa
     * (WAL, snapshot, metadados e arquivos temporários), além do diretório do mapa
     * caso esteja vazio após a exclusão.
     *
     * Fluxo Negocial
     * 1. Invoca {@link #close()} para encerrar a writer thread e liberar o canal WAL.
     * 2. Remove os arquivos: {@code wal.log}, {@code wal.log.old}, {@code snapshot.dat},
     *    {@code snapshot.dat.tmp} e {@code map.meta}.
     * 3. Tenta remover o diretório do mapa ({@code mapDir}). A remoção só ocorre se o
     *    diretório estiver vazio; caso contrário, é silenciosamente ignorada.
     *
     * @throws IOException se ocorrer erro de I/O ao fechar ou remover arquivos
     */
    public void destroy() throws IOException {
        try { close(); }
        catch (IOException historical) {
            Thread writer = writerThread;
            if ((writer != null && writer.isAlive()) || checkpointLock.isLocked() || walOpen()) throw historical;
            // Historical durability errors block a clean marker, not explicit deletion after
            // every writer/checkpoint has terminated and all descriptors have been released.
        }
        Files.deleteIfExists(walPath);
        Files.deleteIfExists(oldWalPath);
        Files.deleteIfExists(snapshotPath);
        Files.deleteIfExists(tempSnapshotPath);
        Files.deleteIfExists(pendingSnapshotPath);
        Files.deleteIfExists(mapDir.resolve("snapshot.pending.tmp"));
        Files.deleteIfExists(mapDir.resolve("wal.recovery.tmp"));
        Files.deleteIfExists(metaPath);
        // Remove directory only if empty (offload files may still live there)
        try {
            Files.deleteIfExists(mapDir);
        } catch (java.nio.file.DirectoryNotEmptyException ignored) {
            // Expected when DiskOffload/HybridOffload subdirectories exist;
            // the strategy's own destroy() will handle those.
        }
    }

    // ── Metadata ────────────────────────────────────────────────────────

    private void writeMeta(NMapMetadata meta) throws IOException {
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(Files.newOutputStream(metaPath)))) {
            out.writeInt(meta.version());
            out.writeLong(meta.lastSnapshotOffset());
            out.writeLong(meta.lastSnapshotTimestamp());
            out.writeLong(meta.lastMutationTimestamp());
            out.flush();
        }
        try (FileChannel metadata = FileChannel.open(metaPath, StandardOpenOption.WRITE)) {
            forceCheckpointChannel(metadata, metaPath);
        }
        forceDirectory();
    }

    private java.util.Optional<NMapMetadata> readMeta() {
        if (!Files.exists(metaPath)) {
            return java.util.Optional.empty();
        }
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(metaPath)))) {
            int version = in.readInt();
            long offset = in.readLong();
            long ts = in.readLong();
            long lastMutationTs = version >= META_VERSION ? in.readLong() : 0L;
            return java.util.Optional.of(new NMapMetadata(offset, ts, lastMutationTs, version));
        } catch (IOException e) {
            return java.util.Optional.empty();
        }
    }
}
