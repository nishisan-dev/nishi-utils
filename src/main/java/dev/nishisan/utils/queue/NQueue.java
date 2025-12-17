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

package dev.nishisan.utils.queue;

import dev.nishisan.utils.stats.StatsUtils;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.CountDownLatch;

/**
 * NQueue is a class that implements a file-based persistent queue with
 * support for concurrent producers and consumers. This class is designed
 * to handle serialization and deserialization of objects while preserving
 * their order and supporting operations such as offer, poll, and peek.
 * <p>
 * This queue utilizes a backing data file for storage and a metadata file
 * for state persistence, ensuring data integrity and consistency across
 * application runs. Records are stored in a sequential manner, and the
 * internal state is periodically persisted to maintain the integrity of
 * the queue in case of application restarts or failures.
 * <p>
 * Note that objects stored in this queue must implement the Serializable interface.
 */
public class NQueue<T extends Serializable> implements Closeable {
    private static final String DATA_FILE = "data.log";
    private static final String META_FILE = "queue.meta";
    private final StatsUtils statsUtils = new StatsUtils();
    private final Path queueDir;
    private final Path dataPath;
    private final Path metaPath;
    private final Path tempDataPath;
    private volatile RandomAccessFile raf;
    private volatile FileChannel dataChannel;
    private final ReentrantLock lock;
    private final Condition notEmpty;
    private final Object metaWriteLock = new Object();
    private final double compactionWasteThreshold;
    private final long compactionIntervalNanos;
    private final int compactionBufferSize;
    private final Options options;
    private final ExecutorService compactionExecutor;
    private volatile Future<?> compactionFuture;
    private long consumerOffset;
    private long producerOffset;
    private long recordCount;
    private long lastIndex;
    private long lastCompactionTimeNanos;
    private volatile boolean closed;
    private volatile boolean shutdownRequested;
    private volatile CompactionState compactionState;
    
    // Memory buffer fields
    private final boolean enableMemoryBuffer;
    private final BlockingQueue<MemoryBufferEntry<T>> memoryBuffer;
    private final BlockingQueue<MemoryBufferEntry<T>> drainingQueue; // Queue for items collected by async drain
    private final AtomicLong memoryBufferModeUntil = new AtomicLong(0);
    private final AtomicBoolean drainingInProgress = new AtomicBoolean(false);
    private final AtomicLong drainingEntries = new AtomicLong(0);
    private final AtomicLong approximateSize = new AtomicLong(0);
    private volatile CountDownLatch drainCompletionLatch;
    private final ExecutorService drainExecutor;
    private final ScheduledExecutorService revalidationExecutor;
    private final long lockTryTimeoutNanos;
    private final long revalidationIntervalNanos;

    /**
     * Constructs a new instance of the NQueue class with the specified parameters.
     *
     * @param queueDir    The directory where the queue files are located.
     * @param raf         The {@link RandomAccessFile} used for accessing the queue's data file.
     * @param dataChannel The {@link FileChannel} associated with the queue's data file for efficient file operations.
     * @param state       The initial state of the queue, encapsulating offsets and record count.
     */
    private NQueue(Path queueDir,
                   RandomAccessFile raf,
                   FileChannel dataChannel,
                   QueueState state,
                   Options optsions,
                   double compactionWasteThreshold,
                   long compactionIntervalNanos,
                   int compactionBufferSize,
                   boolean enableMemoryBuffer,
                   int memoryBufferSize,
                   long lockTryTimeoutNanos,
                   long revalidationIntervalNanos) {
        this.metaPath = queueDir.resolve(META_FILE);
        this.queueDir = queueDir;
        this.dataPath = queueDir.resolve(DATA_FILE);
        this.raf = raf;
        this.dataChannel = dataChannel;
        this.lock = new ReentrantLock();
        this.notEmpty = this.lock.newCondition();
        this.compactionWasteThreshold = compactionWasteThreshold;
        this.compactionIntervalNanos = compactionIntervalNanos;
        this.compactionBufferSize = compactionBufferSize;
        this.consumerOffset = state.consumerOffset;
        this.producerOffset = state.producerOffset;
        this.recordCount = state.recordCount;
        this.lastIndex = state.lastIndex;
        this.approximateSize.set(state.recordCount);
        this.options = optsions;
        this.lastCompactionTimeNanos = System.nanoTime();
        this.compactionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "nqueue-compaction-worker");
            t.setDaemon(true);
            return t;
        });
        this.compactionState = CompactionState.IDLE;
        this.tempDataPath = queueDir.resolve(DATA_FILE + ".compacting");
        
        // Initialize memory buffer components
        this.enableMemoryBuffer = enableMemoryBuffer;
        this.lockTryTimeoutNanos = lockTryTimeoutNanos;
        this.revalidationIntervalNanos = revalidationIntervalNanos;

        if (enableMemoryBuffer) {
            this.memoryBuffer = new LinkedBlockingQueue<>(memoryBufferSize);
            // Draining queue can be same size or larger - it holds items collected by async drain
            this.drainingQueue = new LinkedBlockingQueue<>(memoryBufferSize);
            this.drainExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "nqueue-drain-worker");
                t.setDaemon(true);
                return t;
            });
            this.revalidationExecutor = new ScheduledThreadPoolExecutor(1, r -> {
                Thread t = new Thread(r, "nqueue-revalidation-worker");
                t.setDaemon(true);
                return t;
            });
        } else {
            this.memoryBuffer = null;
            this.drainingQueue = null;
            this.drainExecutor = null;
            this.revalidationExecutor = null;
        }
    }

    /**
     * Opens an existing NQueue or initializes a new one in the specified directory with the given queue name.
     * This method ensures that the queue's metadata and data files are properly initialized and consistent.
     *
     * @param baseDir   the base directory where the queue will be located; must not be null.
     * @param queueName the name of the queue to open or initialize; must not be null.
     * @param <T>       the type of objects to be stored in the queue, which must implement {@link Serializable}.
     * @return an instance of {@link NQueue} configured with the specified directory and queue name.
     * @throws IOException if an I/O error occurs during the initialization or access of the queue.
     */
    public static <T extends Serializable> NQueue<T> open(Path baseDir, String queueName) throws IOException {
        return open(baseDir, queueName, Options.defaults());
    }

    /**
     * Opens an existing NQueue or initializes a new one in the specified directory with the given queue name.
     * This overload allows callers to configure compaction behaviour via {@link Options}.
     *
     * @param baseDir   the base directory where the queue will be located; must not be null.
     * @param queueName the name of the queue to open or initialize; must not be null.
     * @param options   the options configuring queue behaviour; must not be null.
     * @param <T>       the type of objects to be stored in the queue, which must implement {@link Serializable}.
     * @return an instance of {@link NQueue} configured with the specified directory, queue name and options.
     * @throws IOException if an I/O error occurs during the initialization or access of the queue.
     */
    public static <T extends Serializable> NQueue<T> open(Path baseDir, String queueName, Options options) throws IOException {
        Objects.requireNonNull(baseDir, "baseDir cannot be null");
        Objects.requireNonNull(queueName, "queueName cannot be null");
        Objects.requireNonNull(options, "options cannot be null");

        Path queueDir = baseDir.resolve(queueName);
        Files.createDirectories(queueDir);

        Path dataPath = queueDir.resolve(DATA_FILE);
        Path metaPath = queueDir.resolve(META_FILE);

        RandomAccessFile raf = new RandomAccessFile(dataPath.toFile(), "rw");
        FileChannel ch = raf.getChannel();

        QueueState state;
        if (options.resetOnRestart) {
            // Ignorar arquivos existentes e inicializar fila nova
            state = rebuildState(ch);
            persistMeta(metaPath, state);
        } else if (Files.exists(metaPath)) {
            try {
                NQueueQueueMeta meta = NQueueQueueMeta.read(metaPath);
                state = new QueueState(meta.getConsumerOffset(), meta.getProducerOffset(), meta.getRecordCount(), meta.getLastIndex(), ch);
            } catch (IOException e) {
                // Metadata is corrupt/truncated; rebuild from the data log and overwrite meta.
                state = rebuildState(ch);
                persistMeta(metaPath, state);
                Options.Snapshot snapshot = options.snapshot();
                return new NQueue<>(queueDir, raf, ch, state,
                        options,
                        snapshot.compactionWasteThreshold,
                        snapshot.compactionIntervalNanos,
                        snapshot.compactionBufferSize,
                        snapshot.enableMemoryBuffer,
                        snapshot.memoryBufferSize,
                        snapshot.lockTryTimeoutNanos,
                        snapshot.revalidationIntervalNanos);
            }

            long fileSize = ch.size();
            boolean inconsistent = state.consumerOffset < 0
                    || state.producerOffset < 0
                    || state.consumerOffset > state.producerOffset
                    || fileSize < state.producerOffset;

            if (inconsistent) {
                state = rebuildState(ch);
                persistMeta(metaPath, state);
            } else if (fileSize > state.producerOffset) {
                ch.truncate(state.producerOffset);
                ch.force(options.withFsync);
            }
        } else {
            state = rebuildState(ch);
            persistMeta(metaPath, state);
        }

        Options.Snapshot snapshot = options.snapshot();
        return new NQueue<>(queueDir, raf, ch, state,
                options,
                snapshot.compactionWasteThreshold,
                snapshot.compactionIntervalNanos,
                snapshot.compactionBufferSize,
                snapshot.enableMemoryBuffer,
                snapshot.memoryBufferSize,
                snapshot.lockTryTimeoutNanos,
                snapshot.revalidationIntervalNanos);
    }


    /**
     * Adds the specified object to the queue and returns the file offset at which the object
     * has been stored. This method ensures thread-safety, persists the state of the queue
     * after writing the object, and signals any waiting consumers.
     * <p>
     * If memory buffer is enabled and the lock is unavailable (e.g., during compaction),
     * the item will be temporarily stored in memory and drained to disk when the lock becomes available.
     *
     * @param object the object to be added to the queue; must not be null.
     * @return the file offset at which the object has been stored, or -1 if stored in memory buffer.
     * @throws IOException if an I/O error occurs during the operation.
     */
    public long offer(T object) throws IOException {
        Objects.requireNonNull(object, "object");

        // If memory buffer is not enabled, use direct write path
        if (!enableMemoryBuffer) {
            return offerDirect(object);
        }

        // Check if we're in "memory mode" (semaphore active)
        long memoryModeUntil = memoryBufferModeUntil.get();
        boolean shouldUseMemory = System.nanoTime() < memoryModeUntil;

        // If not in memory mode, try normal lock
        if (!shouldUseMemory) {
            boolean lockAcquired = false;
            try {
                lockAcquired = lock.tryLock(lockTryTimeoutNanos, TimeUnit.NANOSECONDS);

                if (lockAcquired) {
                    // Lock acquired: check if compaction is running
                    try {
                        if (compactionState == CompactionState.RUNNING) {
                            // Compaction running: activate memory mode
                            activateMemoryMode();
                                              lock.unlock();
                            lockAcquired = false; // Mark as unlocked to prevent double unlock
                                              return offerToMemoryBuffer(object);
                        } else {
                                              if (shouldDrainMemoryBuffer()) {
                                drainMemoryBufferSync();
                            }
                            // Everything OK: write directly to disk
                            return offerDirectLocked(object);
                        }
                    } finally {
                        if (lockAcquired) {
                            // After writing to disk, try to drain memory if needed
                            drainMemoryBufferIfNeeded();
                            lock.unlock();
                        }
                    }
                } else {
                    // Lock not available: activate memory mode
                    activateMemoryMode();
                    return offerToMemoryBuffer(object);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                activateMemoryMode();
                return offerToMemoryBuffer(object);
            }
        } else {
            // Already in memory mode: check if revalidation is needed
            return offerWithRevalidation(object);
        }
    }

    /**
     * Writes an object directly to disk with lock acquisition.
     * Used when memory buffer is disabled or when lock is available.
     */
    private long offerDirect(T object) throws IOException {
        lock.lock();
        try {
            return offerDirectLocked(object);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Writes an object directly to disk. Must be called while holding the lock.
     * This is the original offer() logic extracted into a separate method.
     */
    private long offerDirectLocked(T object) throws IOException {
        if (!lock.isHeldByCurrentThread()) {
            throw new IllegalStateException("offerDirectLocked must be called while holding the lock");
        }

        // Serialize the object
        byte[] payload = toBytes(object);
        int payloadLen = payload.length;

        // Calculate next sequential index
        long nextIndex = lastIndex + 1;
        if (nextIndex < 0) {
            nextIndex = 0;
        }

        // Create record metadata
        NQueueRecordMetaData meta = new NQueueRecordMetaData(nextIndex, payloadLen, object.getClass().getCanonicalName());

        // Determine write position
        long writePos = producerOffset;
        long recordStart = writePos;

        // Write header
        ByteBuffer hb = meta.toByteBuffer();
        while (hb.hasRemaining()) {
            int written = dataChannel.write(hb, writePos);
            writePos += written;
        }

        // Write payload
        ByteBuffer pb = ByteBuffer.wrap(payload);
        while (pb.hasRemaining()) {
            int written = dataChannel.write(pb, writePos);
            writePos += written;
        }

        // Update internal state
        producerOffset = writePos;
        recordCount++;
        approximateSize.incrementAndGet();
        lastIndex = nextIndex;

        if (recordCount == 1) {
            consumerOffset = recordStart;
        }

        dataChannel.force(options.withFsync);
        persistCurrentStateLocked();
        maybeCompactLocked();
        notEmpty.signalAll();

        statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);
        return recordStart;
    }

    /**
     * Reads and deserializes a stored object located at the specified file offset
     * in the queue, if available, and returns it as an {@link Optional}.
     *
     * @param offset the file offset where the object is expected to be located; must be non-negative.
     * @return an {@link Optional} containing the deserialized object of type {@code T},
     * or {@link Optional#empty()} if no valid object exists at the specified offset.
     * @throws IOException if an I/O error occurs while reading or deserializing the object.
     */
    public Optional<T> readAt(long offset) throws IOException {
        NQueueRecord record;
        lock.lock();
        try {
            Optional<NQueueReadResult> result = readAtInternal(offset);
            if (result.isEmpty()) {
                return Optional.empty();
            }
            record = result.get().getRecord();
        } finally {
            lock.unlock();
        }
        return Optional.of(deserializeRecord(record));
    }

    /**
     * Reads a record located at the specified file offset in the queue and retrieves
     * it, along with the offset for the next record, encapsulated in an {@link NQueueReadResult}.
     * This method ensures thread safety by locking during the operation.
     *
     * @param offset the file offset where the record is expected to be located; must be non-negative.
     * @return an {@link Optional} containing the {@link NQueueReadResult} with the record and next offset,
     * or {@link Optional#empty()} if no valid record exists at the specified offset.
     * @throws IOException if an I/O error occurs while reading the record.
     */
    public Optional<NQueueReadResult> readRecordAt(long offset) throws IOException {
        lock.lock();
        try {
            return readAtInternal(offset);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves the next available record in the queue without removing it.
     * If the queue is empty, an empty {@code Optional} is returned.
     * <p>
     * This method ensures thread safety by employing a lock during its operation.
     *
     * @return an {@code Optional<NQueueRecord>} containing the next available record if the queue is not empty,
     * or {@code Optional.empty()} if the queue is empty.
     * @throws IOException if an I/O error occurs during the operation.
     */
    public Optional<NQueueRecord> peekRecord() throws IOException {
        lock.lock();
        try {
            if (recordCount == 0) {
                return Optional.empty();
            }
            return readAtInternal(consumerOffset).map(NQueueReadResult::getRecord);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves the next object from the queue without removing it.
     * If the queue is empty, an empty {@code Optional} is returned.
     * This method ensures thread safety by employing a lock during operation.
     *
     * @return an {@code Optional<T>} containing the next object if the queue is not empty,
     * or {@code Optional.empty()} if the queue is empty.
     * @throws IOException if an I/O error occurs during the operation.
     */
    public Optional<T> peek() throws IOException {
        NQueueRecord record;
        lock.lock();
        try {
            if (recordCount == 0) {
                return Optional.empty();
            }
            Optional<NQueueReadResult> result = readAtInternal(consumerOffset);
            if (result.isEmpty()) {
                return Optional.empty();
            }
            record = result.get().getRecord();
        } finally {
            statsUtils.notifyHitCounter(NQueueMetrics.PEEK_EVENT);
            lock.unlock();
        }
        return Optional.of(deserializeRecord(record));
    }

    /**
     * Retrieves and removes the next available object from the queue, if one exists.
     * This operation blocks until a record is available or the thread is interrupted.
     * <p>
     * The method ensures thread safety by locking during the operation and deserializes
     * the retrieved record into an object of type {@code T}.
     * <p>
     * If memory buffer is enabled, it will drain all items from memory to disk first
     * to ensure FIFO order.
     *
     * @return an {@code Optional<T>} containing the next object if the queue is not empty,
     * or {@code Optional.empty()} if the queue is empty.
     * @throws IOException if an I/O error occurs during the operation.
     */
    public Optional<T> poll() throws IOException {
        lock.lock();
        try {
            // If memory buffer is enabled, drain it first to ensure FIFO order
            if (shouldDrainMemoryBuffer()) {
                drainMemoryBufferSync();
            }

            awaitRecords();
            Optional<NQueueRecord> record = consumeNextRecordLocked();
            if (record.isEmpty()) {
                return Optional.empty();
            }
            // Keep deserialization under the same lock so that concurrent consumers observe FIFO
            // order in the completion order of poll() calls.
            return Optional.of(deserializeRecord(record.get()));
        } finally {
            statsUtils.notifyHitCounter(NQueueMetrics.POLL_EVENT);
            lock.unlock();
        }
    }

    /**
     * Retrieves and removes the next available object from the queue, if one exists, within the specified timeout.
     * This operation blocks until a record is available, the timeout expires, or the thread is interrupted.
     * <p>
     * Thread safety is ensured by employing a lock during the operation. If the queue remains empty
     * within the given timeout or the thread is interrupted, an empty {@code Optional} is returned.
     * <p>
     * If memory buffer is enabled, it will drain all items from memory to disk first
     * to ensure FIFO order.
     *
     * @param timeout the maximum time to wait for a record to become available; must be non-negative.
     * @param unit    the time unit of the {@code timeout} argument; must not be {@code null}.
     * @return an {@code Optional<T>} containing the next object if the queue is not empty within
     * the timeout, or {@code Optional.empty()} if no object is available or the thread is interrupted.
     * @throws IOException if an I/O error occurs during the operation.
     */
    public Optional<T> poll(long timeout, TimeUnit unit) throws IOException {
        Objects.requireNonNull(unit, "unit");
        long nanos = unit.toNanos(timeout);

        lock.lock();
        try {
            // Drain memory buffer first to ensure FIFO order
            if (shouldDrainMemoryBuffer()) {
                drainMemoryBufferSync();
            }

            while (recordCount == 0) {
                // Check and drain memory buffer if needed
                if (checkAndDrainMemoryBufferIfNeeded()) {
                    break; // Records available after draining
                }
                
                if (nanos <= 0L) {
                    // Final check before returning empty
                    checkAndDrainMemoryBufferIfNeeded();
                    return Optional.empty();
                }
                
                try {
                    nanos = notEmpty.awaitNanos(nanos);
                    // After waiting, check memory buffer again
                    if (checkAndDrainMemoryBufferIfNeeded()) {
                        break; // Records available after draining
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // Check memory buffer one more time before returning empty
                    checkAndDrainMemoryBufferIfNeeded();
                    return Optional.empty();
                }
            }
            
            Optional<NQueueRecord> record = consumeNextRecordLocked();
            if (record.isEmpty()) {
                return Optional.empty();
            }
            // Keep deserialization under the same lock so that concurrent consumers observe FIFO
            // order in the completion order of poll() calls.
            return Optional.of(deserializeRecord(record.get()));
        } finally {
            statsUtils.notifyHitCounter(NQueueMetrics.POLL_EVENT);
            lock.unlock();
        }
    }

    /**
     * Retrieves the current size of the queue, represented by the total number
     * of records present in the queue.
     * <p>
     * This method ensures thread safety by employing a lock during the operation.
     *
     * @return the total number of records currently in the queue.
     * @throws IOException if an I/O error occurs during the operation.
     */
    public long size() throws IOException {
        lock.lock();
        try {
            // With the new approach using draining queue, we can count directly:
            // - recordCount: items on disk
            // - memoryBuffer.size(): items in memory buffer (not yet collected)
            // - drainingQueue.size(): items in draining queue (collected by async drain)
            
            long size = recordCount;
            
            if (enableMemoryBuffer) {
                if (memoryBuffer != null) {
                    size += memoryBuffer.size();
                }
                if (drainingQueue != null) {
                    size += drainingQueue.size();
                }
            }
            
            return size;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves the current size of the queue, represented by the total number
     * of records present in the queue.
     * <p>
     * When {@code optimistic} is {@code true}, this method returns an approximate
     * size without acquiring locks, providing a fast but potentially slightly
     * inaccurate value. When {@code optimistic} is {@code false}, this method
     * behaves identically to {@link #size()}, returning an exact value with locks.
     *
     * @param optimistic if {@code true}, returns an approximate size without locks;
     *                   if {@code false}, returns an exact size using locks (same as {@link #size()})
     * @return the total number of records currently in the queue (approximate if optimistic, exact otherwise)
     * @throws IOException if an I/O error occurs during the operation (only when optimistic is false)
     */
    public long size(boolean optimistic) throws IOException {
        if (!optimistic) {
            return size();
        }
        return approximateSize.get();
    }

    /**
     * Waits for any ongoing memory buffer drain operations to complete.
     * 
     * With the new approach using draining queue, we simply wait for:
     * - drainingQueue to be empty
     * - drainingInProgress to be false
     * 
     * This is simpler and more reliable than the previous approach.
     */
    private void awaitDrainCompletion() {
        if (!enableMemoryBuffer) {
            return;
        }
        
        // Quick check first - avoid synchronization overhead if nothing is draining
        if (!drainingInProgress.get() && (drainingQueue == null || drainingQueue.isEmpty())) {
            return;
        }
        
        // Wait for drain completion using latch if available
        CountDownLatch latch = drainCompletionLatch;
        if (latch != null) {
            try {
                // Wait up to 200ms for drain completion
                boolean completed = latch.await(200, TimeUnit.MILLISECONDS);
                if (completed && (drainingQueue == null || drainingQueue.isEmpty())) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        // Fallback to polling if latch is not available or timeout occurred
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
        while ((drainingInProgress.get() || (drainingQueue != null && !drainingQueue.isEmpty())) 
               && System.nanoTime() < deadline) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Checks if the queue is empty.
     * This method is thread-safe, as it locks the internal state during the operation.
     * Considers disk records, memory buffer items, and draining queue items.
     *
     * @return {@code true} if the queue is empty, {@code false} otherwise.
     */
    public boolean isEmpty() {
        lock.lock();
        try {
            if (recordCount > 0) {
                return false;
            }
            // Check memory buffer if enabled
            if (enableMemoryBuffer && memoryBuffer != null && !memoryBuffer.isEmpty()) {
                return false;
            }
            // Check draining queue if enabled
            if (enableMemoryBuffer && drainingQueue != null && !drainingQueue.isEmpty()) {
                return false;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves the number of records currently in the queue.
     * This method ensures thread safety by locking the internal state during the operation.
     *
     * @return the total number of records present in the queue.
     */
    public long getRecordCount() {
        lock.lock();
        try {
            return recordCount;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        // Drain all items from memory before closing
        if (enableMemoryBuffer && memoryBuffer != null) {
            lock.lock();
            try {
                drainMemoryBufferSync(); // Drain everything that's in memory
            } finally {
                lock.unlock();
            }

            // Wait for async drain to finish
            if (drainExecutor != null) {
                drainExecutor.shutdown();
                try {
                    if (!drainExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        drainExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    drainExecutor.shutdownNow();
                }
            }

            if (revalidationExecutor != null) {
                revalidationExecutor.shutdown();
                try {
                    if (!revalidationExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
                        revalidationExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    revalidationExecutor.shutdownNow();
                }
            }
        }

        Future<?> future;
        QueueState inlineSnapshot = null;
        lock.lock();
        try {
            shutdownRequested = true;
            if (compactionFuture != null && compactionFuture.isDone()) {
                compactionFuture = null;
            }
            boolean canInlineCompaction = compactionFuture == null && consumerOffset > 0 && producerOffset >= consumerOffset;
            if (canInlineCompaction) {
                compactionState = CompactionState.RUNNING;
                inlineSnapshot = currentState();
                future = null;
            } else {
                maybeCompactLocked();
                future = compactionFuture;
            }
        } finally {
            lock.unlock();
        }

        if (inlineSnapshot != null) {
            runCompaction(inlineSnapshot);
            future = compactionFuture;
        }

        if (future != null) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException ignored) {
                // Exceptions during compaction are ignored on close.
            }
        }

        QueueState finalInline = null;
        lock.lock();
        try {
            if (consumerOffset > 0 && producerOffset >= consumerOffset) {
                compactionState = CompactionState.RUNNING;
                finalInline = currentState();
            }
        } finally {
            lock.unlock();
        }

        if (finalInline != null) {
            runCompaction(finalInline);
        }

        lock.lock();
        try {
            dataChannel.close();
            raf.close();
            closed = true;
        } finally {
            lock.unlock();
        }
        compactionExecutor.shutdownNow();
        try {
            compactionExecutor.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Reads data from the specified offset in the internal data channel and attempts to construct
     * an {@link Optional} containing an {@link NQueueReadResult} if the read operation is successful.
     * The method ensures that all required parts of the record (header and payload) are complete
     * based on the metadata and file size.
     *
     * @param offset the offset position in the data channel from which to start reading
     * @return an {@link Optional} containing {@link NQueueReadResult} if a valid record is read,
     * or an empty {@link Optional} if the offset does not contain a complete record
     * @throws IOException if there is an error reading from the data channel
     */
    private Optional<NQueueReadResult> readAtInternal(long offset) throws IOException {
        if (!lock.isHeldByCurrentThread()) {
            throw new IllegalStateException("NQueue internal read must be performed while holding the queue lock");
        }
        long size = dataChannel.size();
        if (offset + NQueueRecordMetaData.fixedPrefixSize() > size) {
            return Optional.empty();
        }

        NQueueRecordMetaData.HeaderPrefix pref = NQueueRecordMetaData.readPrefix(dataChannel, offset);
        long headerEnd = offset + NQueueRecordMetaData.fixedPrefixSize() + pref.headerLen;
        if (headerEnd > size) {
            return Optional.empty();
        }

        NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(dataChannel, offset, pref.headerLen);

        long payloadStart = headerEnd;
        long payloadEnd = payloadStart + meta.getPayloadLen();
        if (payloadEnd > size) {
            return Optional.empty();
        }

        byte[] payload = new byte[meta.getPayloadLen()];
        ByteBuffer pb = ByteBuffer.wrap(payload);
        int r = dataChannel.read(pb, payloadStart);
        if (r < payload.length) {
            throw new EOFException("Payload incompleto.");
        }

        long nextOffset = payloadEnd;
        return Optional.of(new NQueueReadResult(new NQueueRecord(meta, payload), nextOffset));
    }

    /**
     * Rebuilds the state of a queue by analyzing and repairing the content of a file channel.
     * Truncates the file channel if corrupted or incomplete data segments are detected.
     *
     * @param ch the FileChannel to read and sanitize the queue data from
     * @return a QueueState object that contains the reconstructed state of the queue,
     * including consumer offset, producer offset, element count, and last record index
     * @throws IOException if an I/O error occurs while reading or writing to the file channel
     */
    private static QueueState rebuildState(FileChannel ch) throws IOException {
        long size = ch.size();
        long offset = 0L;
        long count = 0L;
        long lastIndex = -1L;

        while (offset < size) {
            if (offset + NQueueRecordMetaData.fixedPrefixSize() > size) {
                ch.truncate(offset);
                size = offset;
                break;
            }

            NQueueRecordMetaData.HeaderPrefix prefix;
            try {
                prefix = NQueueRecordMetaData.readPrefix(ch, offset);
            } catch (EOFException e) {
                ch.truncate(offset);
                size = offset;
                break;
            }

            long headerEnd = offset + NQueueRecordMetaData.fixedPrefixSize() + prefix.headerLen;
            if (headerEnd > size) {
                ch.truncate(offset);
                size = offset;
                break;
            }

            NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(ch, offset, prefix.headerLen);
            long payloadEnd = headerEnd + meta.getPayloadLen();
            if (payloadEnd > size) {
                ch.truncate(offset);
                size = offset;
                break;
            }

            offset = payloadEnd;
            count++;
            lastIndex = meta.getIndex();
        }

        if (offset != size) {
            size = offset;
        }
        ch.force(true);

        long consumerOffset = count > 0 ? 0L : size;
        long producerOffset = size;
        if (lastIndex < 0 && count > 0) {
            lastIndex = count - 1;
        }
        if (count == 0) {
            lastIndex = -1L;
        }

        return new QueueState(consumerOffset, producerOffset, count, lastIndex, ch);
    }

    /**
     * Persists the metadata of the queue state to the specified file path.
     *
     * @param metaPath the file path where the metadata will be stored
     * @param state    the current state of the queue containing consumer offset,
     *                 producer offset, record count, and last index
     * @throws IOException if an I/O error occurs while writing the metadata
     */
    private static void persistMeta(Path metaPath, QueueState state) throws IOException {
        NQueueQueueMeta.write(metaPath, state.consumerOffset, state.producerOffset, state.recordCount, state.lastIndex);
    }

    /**
     * Retrieves the current state of the queue, encapsulating details such as the
     * consumer offset, producer offset, record count, and the last index.
     *
     * @return an instance of QueueState representing the current state of the queue.
     */
    private QueueState currentState() {
        return new QueueState(consumerOffset, producerOffset, recordCount, lastIndex, dataChannel);
    }

    /**
     * Persists the current state to a specified location in a thread-safe manner.
     * <p>
     * This method ensures that the current application's state is serialized
     * and stored persistently using the defined metaPath. It is expected to be
     * used in scenarios where the state information needs to be saved securely
     * to prevent data loss.
     * <p>
     * The method is designed to be invoked within a synchronized context to
     * enforce thread-safety when saving state data.
     *
     * @throws IOException if an I/O error occurs during the persisting process
     */
    private void persistCurrentStateLocked() throws IOException {
        synchronized (metaWriteLock) {
            persistMeta(metaPath, currentState());
        }
    }

    /**
     * Waits for records to become available in a thread-safe manner.
     * This method blocks the current thread until the condition that records are available
     * is met. The condition is evaluated based on the value of the recordCount field.
     * <p>
     * Utilizes the {@code notEmpty} condition to suspend the thread execution until signaled
     * that the records have been added (i.e., when {@code recordCount > 0}).
     * The awaiting is uninterruptible.
     * <p>
     * Note:
     * - Ensure the lock associated with the condition is held before invoking this method.
     * - Use caution when using uninterruptible waits, as they prevent the thread from responding to interruptions.
     */
    private void awaitRecords() {
        while (recordCount == 0) {
            notEmpty.awaitUninterruptibly();
        }
    }

    /**
     * Consumes the next record in the queue while the current state is locked,
     * updates the consumer offset, and decrements the record count. If the record
     * count reaches zero, the consumer offset is reset to the producer offset.
     * The state is persisted after processing.
     *
     * @return an {@code Optional} containing the next {@code NQueueRecord}
     * if available, or an empty {@code Optional} if no record is present.
     * @throws IOException if an I/O error occurs while reading the next record.
     */
    private Optional<NQueueRecord> consumeNextRecordLocked() throws IOException {
        Optional<NQueueReadResult> result = readAtInternal(consumerOffset);
        if (result.isEmpty()) {
            return Optional.empty();
        }

        NQueueReadResult rr = result.get();
        consumerOffset = rr.getNextOffset();
        recordCount--;
        approximateSize.decrementAndGet();
        if (recordCount == 0) {
            consumerOffset = producerOffset;
        }
        persistCurrentStateLocked();
        maybeCompactLocked();
        return Optional.of(rr.getRecord());
    }

    /**
     * Determines if compaction should be performed based on specific thresholds and conditions,
     * and executes the compaction if necessary. This method is invoked while holding a lock to ensure
     * thread safety during the evaluation and potential modification of shared state.
     * <p>
     * Compaction is triggered under the following conditions:
     * - The total bytes and wasted bytes are greater than zero.
     * - The record count is not zero, or the elapsed time since the last compaction exceeds a specified interval.
     * - The waste ratio (calculated as the ratio of wasted bytes to total bytes) exceeds a predefined threshold,
     * or the compaction interval has been exceeded and there are wasted bytes.
     * <p>
     * If conditions are satisfied, the compaction logic is executed, and the timestamp of the last
     * compaction is updated.
     *
     * @throws IOException if an I/O error occurs during the compaction process.
     */
    private void maybeCompactLocked() throws IOException {
        if (compactionState == CompactionState.RUNNING) {
            return;
        }
        long totalBytes = producerOffset;
        long wastedBytes = consumerOffset;
        if (totalBytes <= 0 || wastedBytes <= 0) {
            return;
        }

        long now = System.nanoTime();
        boolean intervalExceeded = compactionIntervalNanos > 0 && (now - lastCompactionTimeNanos) >= compactionIntervalNanos;
        // When the queue is empty but the data file still contains bytes (consumerOffset == producerOffset > 0),
        // we still want to compact to rewrite the file and reset offsets back to 0. On shutdown, this should
        // always be allowed; otherwise we keep the interval gate to avoid overly aggressive rewrites.
        if (!shutdownRequested && recordCount == 0 && !intervalExceeded) {
            return;
        }

        double wasteRatio = (double) wastedBytes / (double) totalBytes;
        boolean wasteExceeded = wasteRatio >= compactionWasteThreshold;
        if (!wasteExceeded && !(intervalExceeded && wastedBytes > 0)) {
            return;
        }

        startCompactionIfIdleLocked(now);
    }

    /**
     * Compacts the underlying data storage by removing all consumed data
     * and shifting the remaining data to the beginning of the storage.
     * This operation ensures that the storage utilizes space efficiently
     * by discarding unnecessary data and optimizing future access.
     * <p>
     * The heavy lifting is now delegated to a background compaction worker.
     * This method simply schedules the asynchronous compaction when invoked
     * while holding the main queue lock.
     *
     * @throws IOException if an I/O error occurs during the compaction process.
     *                     <p>
     *                     Key operations performed:
     *                     1. Calculates the length of data to be preserved.
     *                     2. Clears storage if no data is to be preserved.
     *                     3. Copies remaining data to the beginning of the storage.
     *                     4. Updates the producer and consumer offsets accordingly.
     *                     5. Truncates excess space and persists the updated state.
     */
    private void compactLocked() throws IOException {
        startCompactionIfIdleLocked(System.nanoTime());
    }

    private void startCompactionIfIdleLocked(long now) {
        // Only one compaction worker is allowed at a time. The lock protects the
        // state snapshot that is handed off to the worker and avoids scheduling
        // multiple background jobs concurrently.
        if (shutdownRequested || compactionState != CompactionState.IDLE) {
            return;
        }

        QueueState snapshot = currentState();
        compactionState = CompactionState.RUNNING;
        try {
            compactionFuture = compactionExecutor.submit(() -> runCompaction(snapshot));
            lastCompactionTimeNanos = now;
        } catch (RuntimeException e) {
            compactionState = CompactionState.IDLE;
            throw e;
        }
    }

    private void runCompaction(QueueState snapshot) {
        try {
            Files.deleteIfExists(tempDataPath);
            try (RandomAccessFile tmpRaf = new RandomAccessFile(tempDataPath.toFile(), "rw");
                 FileChannel tmpChannel = tmpRaf.getChannel()) {
                // Copies the active window [consumerOffset, producerOffset) into a temporary file
                // without holding the main queue lock, allowing offer/poll/peek to proceed. The
                // only synchronization points happen when switching the live file to the compacted one.
                copyActiveRegion(snapshot, tmpChannel);

                if (options.withFsync) {
                    tmpChannel.force(true);
                }

                finalizeCompaction(snapshot, tempDataPath, tmpChannel.size());
            }
        } catch (IOException ignored) {
            lock.lock();
            try {
                compactionState = CompactionState.IDLE;
            } finally {
                lock.unlock();
            }
        } finally {
            lock.lock();
            try {
                if (compactionState == CompactionState.IDLE) {
                    Files.deleteIfExists(tempDataPath);
                }
            } catch (IOException ignored) {
                // best-effort cleanup
            } finally {
                lock.unlock();
            }
        }
    }

    private void copyActiveRegion(QueueState snapshot, FileChannel target) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(compactionBufferSize);
        long readOffset = snapshot.consumerOffset;
        long limit = snapshot.producerOffset;
        FileChannel source = snapshot.dataChannel;

        while (readOffset < limit) {
            buffer.clear();
            int bytesRead = source.read(buffer, readOffset);
            if (bytesRead <= 0) {
                break;
            }
            buffer.flip();
            while (buffer.hasRemaining()) {
                target.write(buffer);
            }
            readOffset += bytesRead;
        }
    }

    private void finalizeCompaction(QueueState snapshot, Path tempPath, long newProducerOffset) throws IOException {
        lock.lock();
        try {
            boolean stateChanged = consumerOffset != snapshot.consumerOffset
                    || producerOffset != snapshot.producerOffset
                    || recordCount != snapshot.recordCount
                    || lastIndex != snapshot.lastIndex;

            if (stateChanged) {
                // State changed while we copied the active window. Abort this attempt.
                // Instead of immediately rescheduling compaction, update lastCompactionTimeNanos
                // to enforce a minimum interval before the next compaction attempt.
                compactionState = CompactionState.IDLE;
                compactionFuture = null;
                long now = System.nanoTime();
                // Force the next compaction evaluation to consider the interval exceeded so we
                // do not stall when the queue becomes empty right after a failed attempt.
                lastCompactionTimeNanos = compactionIntervalNanos > 0
                        ? now - compactionIntervalNanos
                        : now;
                maybeCompactLocked();
                return;
            }

            // Minimal critical section: close the old channel, atomically switch the file,
            // reopen the channel, and update in-memory offsets/state before releasing the lock.
            // Use local variables to avoid exposing closed channels to other threads via volatile fields.
            FileChannel oldDataChannel = dataChannel;
            RandomAccessFile oldRaf = raf;
            
            oldDataChannel.close();
            oldRaf.close();

            try {
                Files.move(tempPath, dataPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tempPath, dataPath, StandardCopyOption.REPLACE_EXISTING);
            }

            RandomAccessFile newRaf = new RandomAccessFile(dataPath.toFile(), "rw");
            FileChannel newDataChannel = newRaf.getChannel();
            
            this.raf = newRaf;
            this.dataChannel = newDataChannel;

            consumerOffset = 0L;
            producerOffset = newProducerOffset;

            persistCurrentStateLocked();
            if (options.withFsync) {
                dataChannel.force(true);
            }

            lastCompactionTimeNanos = System.nanoTime();
        } finally {
            compactionState = CompactionState.IDLE;
            compactionFuture = null;
            lock.unlock();
        }
    }

    /**
     * Adds an object to the memory buffer as a fallback when lock is unavailable.
     */
    private long offerToMemoryBuffer(T object) throws IOException {
        MemoryBufferEntry<T> entry = new MemoryBufferEntry<>(object);

        if (memoryBuffer.offer(entry)) {
            tryDrainMemoryBufferSync();
            triggerDrainIfNeeded();
            statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);
            return -1; // Offset will be assigned during drain
        } else {
            // Buffer full: block until space is available
            try {
                memoryBuffer.put(entry);
                tryDrainMemoryBufferSync();
                triggerDrainIfNeeded();
                statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);
                return -1;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for memory buffer", e);
            }
        }
    }

    /**
     * Handles offer when already in memory mode, with revalidation logic.
     */
    private long offerWithRevalidation(T object) throws IOException {
        // Check if buffer is full
        if (memoryBuffer.remainingCapacity() == 0) {
            // Buffer full: force immediate revalidation
            revalidateMemoryMode();

            // Try lock again
            boolean lockAcquired = false;
            try {
                lockAcquired = lock.tryLock(lockTryTimeoutNanos, TimeUnit.NANOSECONDS);

                if (lockAcquired) {
                    try {
                        if (compactionState != CompactionState.RUNNING) {
                            // Lock released: drain and write directly
                            if (shouldDrainMemoryBuffer()) {
                                drainMemoryBufferSync();
                            }
                            return offerDirectLocked(object);
                        } else {
                            // Still in compaction: unlock and block until space
                            lock.unlock();
                            lockAcquired = false;
                            try {
                                memoryBuffer.put(new MemoryBufferEntry<>(object));
                                triggerDrainIfNeeded();
                                return -1;
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IOException("Interrupted while waiting for memory buffer", e);
                            }
                        }
                    } finally {
                        if (lockAcquired) {
                            lock.unlock();
                        }
                    }
                } else {
                    // Lock still occupied: block until space in buffer
                    try {
                        memoryBuffer.put(new MemoryBufferEntry<>(object));
                        triggerDrainIfNeeded();
                        return -1;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while waiting for memory buffer", e);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while trying to acquire lock", e);
            }
        } else {
            // Buffer has space: add normally
            if (memoryBuffer.offer(new MemoryBufferEntry<>(object))) {
                tryDrainMemoryBufferSync();
                triggerDrainIfNeeded();
                statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);
                return -1;
            } else {
                // Race condition: buffer filled between check and offer
                return offerWithRevalidation(object); // Retry
            }
        }
    }

    /**
     * Activates memory mode for a period of time.
     */
    private void activateMemoryMode() {
        // Activate memory mode for a time period
        long until = System.nanoTime() + revalidationIntervalNanos;
        memoryBufferModeUntil.updateAndGet(current -> Math.max(current, until));

        // Schedule revalidation if not already scheduled
        scheduleRevalidationIfNeeded();
    }

    /**
     * Schedules revalidation if needed.
     */
    private void scheduleRevalidationIfNeeded() {
        // Avoid multiple simultaneous revalidations
        if (revalidationExecutor != null) {
            revalidationExecutor.schedule(this::revalidateMemoryMode,
                    revalidationIntervalNanos, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Re-validates whether to continue using memory mode or switch back to direct disk writes.
     */
    private void revalidateMemoryMode() {
        // Re-evaluate if we should continue in memory mode
        long memoryModeUntil = memoryBufferModeUntil.get();
        long now = System.nanoTime();

        if (now >= memoryModeUntil) {
            // Period expired: try to return to normal mode
            boolean lockAcquired = false;
            try {
                lockAcquired = lock.tryLock(lockTryTimeoutNanos, TimeUnit.NANOSECONDS);

                if (lockAcquired) {
                    try {
                        if (compactionState != CompactionState.RUNNING && !hasMemoryBufferItems()) {
                            // Everything OK: deactivate memory mode
                            memoryBufferModeUntil.set(0);
                        } else {
                            // Still needs memory: extend period
                            activateMemoryMode();
                        }
                    } finally {
                        lock.unlock();
                    }
                } else {
                    // Lock still occupied: extend memory mode
                    activateMemoryMode();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                activateMemoryMode(); // On error, keep memory mode
            }
        } else {
            // Still within period: schedule next revalidation
            scheduleRevalidationIfNeeded();
        }
    }

    /**
     * Triggers asynchronous drain if needed.
     */
    private void triggerDrainIfNeeded() {
        // Trigger drain only if not already in progress
        if (drainingInProgress.compareAndSet(false, true)) {
            // Create a new latch for this drain operation
            drainCompletionLatch = new CountDownLatch(1);
            drainExecutor.submit(this::drainMemoryBuffer);
        }
    }

    private void tryDrainMemoryBufferSync() {
        if (!hasMemoryBufferItems()) {
            return;
        }

        boolean acquired = false;
        try {
            acquired = lock.tryLock(lockTryTimeoutNanos, TimeUnit.NANOSECONDS);
            if (acquired) {
                drainMemoryBufferSync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
            // If drain fails, fallback to async drain
        } finally {
            if (acquired) {
                lock.unlock();
            }
        }
    }

    /**
     * Drains memory buffer asynchronously in background.
     * 
     * New approach using draining queue:
     * 1. Try to process items already in draining queue first
     * 2. If draining queue is empty, collect batch from memory buffer
     * 3. Move items to draining queue (atomic operation)
     * 4. Increment drainingEntries
     * 5. Try to acquire lock
     * 6. If lock acquired: process items from draining queue and decrement drainingEntries
     * 7. If lock not acquired: items remain in draining queue (will be processed by sync drain or next iteration)
     * 
     * This eliminates race conditions - items are never "lost" between memory buffer and draining queue.
     */
    private void drainMemoryBuffer() {
        try {
            while (true) {
                // First, try to process items already in draining queue
                if (drainingQueue != null && !drainingQueue.isEmpty()) {
                    boolean lockAcquired = false;
                    try {
                        lockAcquired = lock.tryLock(10, TimeUnit.MILLISECONDS);
                        
                        if (lockAcquired) {
                            // Process items from draining queue
                            java.util.List<MemoryBufferEntry<T>> toProcess = new java.util.ArrayList<>();
                            drainingQueue.drainTo(toProcess, 100); // Process up to 100 items
                            
                            // Update drainingEntries for items we're processing
                            long processed = toProcess.size();
                            for (long i = 0; i < processed; i++) {
                                drainingEntries.decrementAndGet();
                            }
                            
                            // Process items
                            for (MemoryBufferEntry<T> e : toProcess) {
                                try {
                                    offerDirectLocked(e.item);
                                } catch (IOException ex) {
                                    // Re-queue the item to memory buffer for a later attempt
                                    memoryBuffer.offer(e);
                                }
                            }
                            
                            // Continue loop to check for more items
                            lock.unlock();
                            lockAcquired = false;
                            continue;
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        break;
                    } finally {
                        if (lockAcquired) {
                            lock.unlock();
                        }
                    }
                }

                // Collect batch of items from memory buffer (max 100 items at a time)
                java.util.List<MemoryBufferEntry<T>> batch = new java.util.ArrayList<>();
                MemoryBufferEntry<T> batchEntry;
                while ((batchEntry = memoryBuffer.poll()) != null && batch.size() < 100) {
                    batch.add(batchEntry);
                }

                if (batch.isEmpty()) {
                    // No items in memory buffer and draining queue is empty (or we couldn't process it)
                    break; // Nothing to drain
                }

                // Move items to draining queue (atomic operation)
                // This ensures items are never "lost" - they're either in memory buffer or draining queue
                for (MemoryBufferEntry<T> e : batch) {
                    drainingQueue.offer(e);
                    drainingEntries.incrementAndGet();
                }

                // Try to acquire lock to process items from draining queue
                boolean lockAcquired = false;
                try {
                    lockAcquired = lock.tryLock(10, TimeUnit.MILLISECONDS);
                    
                    if (lockAcquired) {
                        // Lock acquired: process items from draining queue
                        // Drain items from queue to maintain FIFO order
                        java.util.List<MemoryBufferEntry<T>> toProcess = new java.util.ArrayList<>();
                        drainingQueue.drainTo(toProcess, 100);
                        
                        // Update drainingEntries for items we're processing
                        long processed = toProcess.size();
                        for (long i = 0; i < processed; i++) {
                            drainingEntries.decrementAndGet();
                        }
                        
                        // Process items
                        for (MemoryBufferEntry<T> e : toProcess) {
                            try {
                                offerDirectLocked(e.item);
                            } catch (IOException ex) {
                                // Re-queue the item to memory buffer for a later attempt
                                memoryBuffer.offer(e);
                            }
                        }
                    } else {
                        // Lock not available: items remain in draining queue
                        // They will be processed in next iteration or by sync drain
                        // Wait a bit before trying again to avoid busy-waiting
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        // Continue loop to try processing again
                        continue;
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    // On interruption, items remain in draining queue - they'll be processed later
                    break;
                } finally {
                    if (lockAcquired) {
                        lock.unlock();
                    }
                }
            }
        } finally {
            drainingInProgress.set(false);
            // Signal completion to any waiting threads
            CountDownLatch latch = drainCompletionLatch;
            if (latch != null) {
                latch.countDown();
                drainCompletionLatch = null;
            }
            // Handle items that might have been enqueued after the last drain batch
            if (hasMemoryBufferItems() || (drainingQueue != null && !drainingQueue.isEmpty())) {
                triggerDrainIfNeeded();
            }
        }
    }

    /**
     * Drains all items from memory buffer synchronously.
     * Used in poll() and close() to ensure FIFO order.
     * 
     * New approach using draining queue:
     * 1. Collect all items from memory buffer
     * 2. Atomically transfer all items from draining queue (steal from async drain)
     * 3. Process all items in FIFO order: memory buffer first, then draining queue
     * 4. Update drainingEntries to reflect stolen items
     * 
     * This method must be called while holding the lock.
     * 
     * This guarantees strict FIFO order and eliminates race conditions.
     */
    private void drainMemoryBufferSync() throws IOException {
        // Collect all items from memory buffer
        java.util.List<MemoryBufferEntry<T>> memoryItems = new java.util.ArrayList<>();
        if (hasMemoryBufferItems()) {
            memoryBuffer.drainTo(memoryItems);
        }

        // Atomically "steal" all items from draining queue
        // This is the key to eliminating race conditions
        java.util.List<MemoryBufferEntry<T>> drainingItems = new java.util.ArrayList<>();
        if (drainingQueue != null && !drainingQueue.isEmpty()) {
            drainingQueue.drainTo(drainingItems);
            // Update drainingEntries to reflect items we stole
            long stolen = drainingItems.size();
            for (long i = 0; i < stolen; i++) {
                drainingEntries.decrementAndGet();
            }
        }

        // Process all items in FIFO order: memory buffer first, then draining queue
        // This preserves strict FIFO ordering
        for (MemoryBufferEntry<T> entry : memoryItems) {
            offerDirectLocked(entry.item);
        }
        for (MemoryBufferEntry<T> entry : drainingItems) {
            offerDirectLocked(entry.item);
        }
    }

    /**
     * Checks if memory buffer needs draining and triggers it if needed.
     * Called after normal operations to ensure memory is drained.
     */
    private void drainMemoryBufferIfNeeded() {
        if (hasMemoryBufferItems() && !drainingInProgress.get()) {
            triggerDrainIfNeeded();
        }
    }

    /**
     * Helper method to check if memory buffer has items.
     * Must be called while holding the lock or when memory buffer operations are safe.
     *
     * @return true if memory buffer is enabled and has items
     */
    private boolean hasMemoryBufferItems() {
        return enableMemoryBuffer && memoryBuffer != null && !memoryBuffer.isEmpty();
    }

    /**
     * Helper method to check if memory buffer should be drained before operations.
     * Must be called while holding the lock.
     *
     * @return true if memory buffer should be drained
     */
    private boolean shouldDrainMemoryBuffer() {
        return hasMemoryBufferItems();
    }

    /**
     * Checks and drains memory buffer if needed.
     * Must be called while holding the lock.
     * 
     * @return true if records are now available after draining
     * @throws IOException if drain operation fails
     */
    private boolean checkAndDrainMemoryBufferIfNeeded() throws IOException {
        if (shouldDrainMemoryBuffer()) {
            drainMemoryBufferSync();
            return recordCount > 0;
        }
        return false;
    }

    /**
     * Converts the given object to its byte array representation.
     *
     * @param obj the object to be converted into a byte array. It should be serializable.
     * @return a byte array representing the serialized form of the given object.
     * @throws IOException if an I/O error occurs during the serialization process.
     */
    private byte[] toBytes(T obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos))) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        }
    }

    /**
     * Deserializes a record from its payload into an object of type T.
     *
     * @param record the NQueueRecord containing the serialized payload to be deserialized
     * @return an object of type T obtained by deserializing the provided record's payload
     * @throws IOException if an I/O error occurs during deserialization or the class of the object cannot be found
     */
    private T deserializeRecord(NQueueRecord record) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(record.payload());
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            @SuppressWarnings("unchecked")
            T obj = (T) ois.readObject();
            return obj;
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize record payload", e);
        }
    }

    private enum CompactionState {
        IDLE,
        RUNNING
    }

    /**
     * Internal class to track items in the memory buffer with timestamp.
     */
    private static final class MemoryBufferEntry<T> {
        final T item;
        final long timestamp;

        MemoryBufferEntry(T item) {
            this.item = item;
            this.timestamp = System.nanoTime();
        }
    }

    /**
     * Represents the current state of a queue including offsets, record count,
     * and the last index.
     */
    private static final class QueueState {
        final long consumerOffset;
        final long producerOffset;
        final long recordCount;
        final long lastIndex;
        final FileChannel dataChannel;

        QueueState(long consumerOffset, long producerOffset, long recordCount, long lastIndex, FileChannel dataChannel) {
            this.consumerOffset = consumerOffset;
            this.producerOffset = producerOffset;
            this.recordCount = recordCount;
            this.lastIndex = lastIndex;
            this.dataChannel = dataChannel;
        }
    }

    /**
     * Options used to configure queue behaviour.
     */
    public static final class Options {
        private double compactionWasteThreshold = 0.5d;
        private Duration compactionInterval = Duration.ofMinutes(5);
        private int compactionBufferSize = 128 * 1024;
        private boolean withFsync = true;
        private boolean enableMemoryBuffer = false;
        private int memoryBufferSize = 10000;
        private Duration lockTryTimeout = Duration.ofMillis(10);
        private Duration revalidationInterval = Duration.ofMillis(100);
        private boolean resetOnRestart = false;

        private Options() {
        }

        public static Options defaults() {
            return new Options();
        }

        /**
         * Sets the compaction waste threshold, which defines the proportion of wasted space
         * in data structures that can trigger a compaction process. The threshold must be
         * a value between 0.0 and 1.0 inclusive, representing percentages in decimal form.
         *
         * @param threshold the compaction waste threshold as a decimal value between 0.0 and 1.0
         * @return the current {@code Options} instance for method chaining
         * @throws IllegalArgumentException if {@code threshold} is less than 0.0 or greater than 1.0
         */
        public Options withCompactionWasteThreshold(double threshold) {
            if (threshold < 0.0d || threshold > 1.0d) {
                throw new IllegalArgumentException("threshold deve estar no intervalo [0.0, 1.0]");
            }
            this.compactionWasteThreshold = threshold;
            return this;
        }

        /**
         * Sets the compaction interval, which determines the frequency of compaction operations.
         * A valid, non-negative interval must be provided.
         *
         * @param interval the {@code Duration} specifying the compaction interval; must not be null or negative
         * @return the current {@code Options} instance for method chaining
         * @throws NullPointerException if {@code interval} is null
         * @throws IllegalArgumentException if {@code interval} is negative
         */
        public Options withCompactionInterval(Duration interval) {
            Objects.requireNonNull(interval, "interval");
            if (interval.isNegative()) {
                throw new IllegalArgumentException("interval não pode ser negativo");
            }
            this.compactionInterval = interval;
            return this;
        }

        /**
         * Sets the buffer size for compaction processes.
         * This size determines the allocation limit for buffer data during the compaction operation.
         *
         * @param bufferSize the size of the buffer in bytes; must be a positive value
         * @return the current {@code Options} instance for method chaining
         * @throws IllegalArgumentException if {@code bufferSize} is less than or equal to 0
         */
        public Options withCompactionBufferSize(int bufferSize) {
            if (bufferSize <= 0) {
                throw new IllegalArgumentException("bufferSize deve ser positivo");
            }
            this.compactionBufferSize = bufferSize;
            return this;
        }

        /**
         * Configures whether file system synchronization (fsync) should be performed.
         * When enabled, fsync ensures that data is physically written to disk, improving
         * data durability at the cost of performance. Disabling fsync may improve performance
         * but increases the risk of data loss in the event of a crash.
         *
         * @param fsync a boolean value indicating whether fsync should be enabled (true) or disabled (false)
         * @return the current {@code Options} instance for method chaining
         */
        public Options withFsync(boolean fsync) {
            this.withFsync = fsync;
            return this;
        }

        /**
         * Enables or disables the memory buffer fallback mechanism.
         * When enabled, if the queue lock is unavailable (e.g., during compaction),
         * items will be temporarily stored in memory and drained to disk when the lock becomes available.
         * 
         * @param enable true to enable memory buffer, false to disable (default)
         * @return the current {@code Options} instance for method chaining
         */
        public Options withMemoryBuffer(boolean enable) {
            this.enableMemoryBuffer = enable;
            return this;
        }

        /**
         * Sets the maximum size of the memory buffer queue.
         * When this limit is reached, the system will re-evaluate if the lock is available.
         * If the lock is still unavailable, operations will block until space becomes available.
         * 
         * @param size the maximum number of items in the memory buffer; must be positive
         * @return the current {@code Options} instance for method chaining
         * @throws IllegalArgumentException if {@code size} is less than or equal to 0
         */
        public Options withMemoryBufferSize(int size) {
            if (size <= 0) {
                throw new IllegalArgumentException("memoryBufferSize deve ser positivo");
            }
            this.memoryBufferSize = size;
            return this;
        }

        /**
         * Sets the timeout for attempting to acquire the queue lock.
         * If the lock cannot be acquired within this timeout, the memory buffer fallback will be used.
         * 
         * @param timeout the duration to wait for lock acquisition; must not be null or negative
         * @return the current {@code Options} instance for method chaining
         * @throws NullPointerException if {@code timeout} is null
         * @throws IllegalArgumentException if {@code timeout} is negative
         */
        public Options withLockTryTimeout(Duration timeout) {
            Objects.requireNonNull(timeout, "timeout");
            if (timeout.isNegative()) {
                throw new IllegalArgumentException("timeout não pode ser negativo");
            }
            this.lockTryTimeout = timeout;
            return this;
        }

        /**
         * Sets the interval for re-evaluating whether to continue using the memory buffer
         * or switch back to direct disk writes. After this interval, the system will check
         * if the lock is available and if compaction has finished.
         * 
         * @param interval the duration between revalidation checks; must not be null or negative
         * @return the current {@code Options} instance for method chaining
         * @throws NullPointerException if {@code interval} is null
         * @throws IllegalArgumentException if {@code interval} is negative
         */
        public Options withRevalidationInterval(Duration interval) {
            Objects.requireNonNull(interval, "interval");
            if (interval.isNegative()) {
                throw new IllegalArgumentException("interval não pode ser negativo");
            }
            this.revalidationInterval = interval;
            return this;
        }

        /**
         * Configures whether the queue should reset on restart, ignoring existing persistent data.
         * When enabled, even if persistence files exist, the queue will be initialized as empty,
         * effectively resetting the queue on each restart.
         *
         * @param resetOnRestart if {@code true}, ignores existing persistent data and initializes an empty queue;
         *                       if {@code false}, loads existing persistent data (default behavior)
         * @return the current {@code Options} instance for method chaining
         */
        public Options withResetOnRestart(boolean resetOnRestart) {
            this.resetOnRestart = resetOnRestart;
            return this;
        }

        /**
         * Creates a snapshot of the current configuration by encapsulating the values
         * of compaction waste threshold, compaction interval, and compaction buffer size.
         * A snapshot is immutable and stores these values for future reference or processing.
         *
         * @return a new {@code Snapshot} instance containing the current configuration values
         */
        private Snapshot snapshot() {
            Duration interval = compactionInterval;
            long intervalNanos = interval != null ? interval.toNanos() : 0L;
            long lockTryTimeoutNanos = lockTryTimeout != null ? lockTryTimeout.toNanos() : 0L;
            long revalidationIntervalNanos = revalidationInterval != null ? revalidationInterval.toNanos() : 0L;
            return new Snapshot(compactionWasteThreshold, intervalNanos, compactionBufferSize,
                    enableMemoryBuffer, memoryBufferSize, lockTryTimeoutNanos, revalidationIntervalNanos);
        }

        private static final class Snapshot {
            final double compactionWasteThreshold;
            final long compactionIntervalNanos;
            final int compactionBufferSize;
            final boolean enableMemoryBuffer;
            final int memoryBufferSize;
            final long lockTryTimeoutNanos;
            final long revalidationIntervalNanos;

            Snapshot(double compactionWasteThreshold, long compactionIntervalNanos, int compactionBufferSize,
                    boolean enableMemoryBuffer, int memoryBufferSize, long lockTryTimeoutNanos, long revalidationIntervalNanos) {
                this.compactionWasteThreshold = compactionWasteThreshold;
                this.compactionIntervalNanos = compactionIntervalNanos;
                this.compactionBufferSize = compactionBufferSize;
                this.enableMemoryBuffer = enableMemoryBuffer;
                this.memoryBufferSize = memoryBufferSize;
                this.lockTryTimeoutNanos = lockTryTimeoutNanos;
                this.revalidationIntervalNanos = revalidationIntervalNanos;
            }
        }
    }


    public StatsUtils getStatsUtils() {
        return statsUtils;
    }


}
