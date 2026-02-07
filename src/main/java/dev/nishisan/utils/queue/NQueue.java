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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Persistent, thread-safe FIFO queue.
 * <p>
 * Responsibilities
 * - Provide a durable, append-only, file-backed queue that preserves record
 * order across process restarts.
 * - Support multiple concurrent producers and consumers with predictable
 * blocking semantics.
 * - Persist consumption progress so that already-consumed records are not
 * re-delivered after restart.
 * - Reclaim disk space transparently via background compaction without
 * interrupting producers/consumers.
 * - Optionally stage new records in memory to maintain throughput during high
 * contention or compaction.
 * <p>
 * Execution flow (high-level)
 * - Enqueue: New records are accepted in FIFO order. Under normal conditions
 * they are durably appended to
 * the queue log and the logical size is updated. If temporary in-memory staging
 * is enabled and the queue
 * detects contention or a maintenance window, records may be first placed in a
 * bounded in-memory buffer
 * and later drained to disk in batches while preserving order.
 * - Dequeue: Consumers obtain the next available record in FIFO order. A
 * blocking variant waits until a
 * record becomes available; a timed variant returns empty on timeout. A
 * non-destructive peek is also
 * available. Once a record is returned by a dequeue operation, the queue
 * advances its internal cursor and
 * persists that advancement, meaning the record will not be re-delivered after
 * a restart.
 * - Startup and recovery: On open, the queue reconstructs its state from
 * persisted metadata. If metadata is
 * unavailable or inconsistent, it recovers by scanning the log up to the last
 * complete record and discarding
 * any torn tail, ensuring the queue starts in a consistent state.
 * - Maintenance and compaction: When a configurable threshold of consumed data
 * accumulates, or after a
 * configured interval, the queue compacts the log in the background by
 * retaining only the unconsumed
 * segment. This process is atomic from the perspective of users and never
 * discards unconsumed data. Normal
 * enqueue/dequeue operations remain available during compaction; if in-memory
 * staging is enabled, the queue
 * may temporarily route new records through the memory buffer to minimize
 * interference.
 * - Shutdown: On close, the queue attempts to flush any staged records, may
 * perform a final compaction when
 * appropriate, and then closes resources.
 * <p>
 * Business rules and guarantees
 * - Ordering: Records are delivered strictly in FIFO order, including across
 * restarts. Batched draining from
 * the in-memory buffer, when enabled, preserves overall enqueue order.
 * - Delivery semantics: Dequeue is at-most-once from the queue’s perspective.
 * After a record is returned to a
 * consumer, the queue advances and persists its position; the same record will
 * not be redelivered after
 * a crash or restart.
 * - Durability: With synchronous flushing enabled, records acknowledged by
 * enqueue are durable against process
 * crashes and OS-level failures subject to underlying storage guarantees. When
 * synchronous flushing is
 * disabled, a small window of most recent records may be lost on abrupt
 * termination or power failure.
 * - Capacity and backpressure: The on-disk queue grows with available disk
 * space. The optional in-memory
 * staging buffer is bounded; when it fills, producers may block until space
 * becomes available or until records
 * are drained to disk. Consumers may block when the queue is empty, depending
 * on the chosen API variant.
 * - Safety during compaction: Compaction never removes unconsumed data and
 * completes with an atomic file
 * replacement to avoid partial states. If compaction cannot complete, the
 * existing log remains intact.
 * - Concurrency: All public operations are thread-safe. Internal coordination
 * ensures that concurrent
 * producers and consumers observe consistent queue state and ordering.
 * - Compatibility: Queue elements must be serializable and readable with the
 * application’s classpath on
 * subsequent runs; schema evolution and cross-version compatibility are the
 * responsibility of the caller.
 * <p>
 * Configuration overview
 * - Compaction behavior can be tuned by a waste threshold and/or time interval.
 * - Durability can trade throughput vs. safety by toggling synchronous flushes.
 * - In-memory staging can be enabled and sized to absorb bursts and reduce
 * producer contention during
 * maintenance.
 * <p>
 * This documentation intentionally focuses on observable behavior and
 * operational characteristics rather than
 * implementation specifics.
 *
 * @param <T> Serializable element type stored and retrieved in FIFO order.
 */
public class NQueue<T extends Serializable> implements Closeable {
    private static final String DATA_FILE = "data.log";
    private static final String META_FILE = "queue.meta";
    private static final int MEMORY_DRAIN_BATCH_SIZE = 256;
    /**
     * Sentinel offset returned when an element is handed off directly to a waiting
     * consumer, bypassing
     * persistence and staging buffers.
     */
    public static final long OFFSET_HANDOFF = -2;

    private final StatsUtils statsUtils = new StatsUtils();
    private final Path queueDir, dataPath, metaPath, tempDataPath;
    private volatile RandomAccessFile raf;
    private volatile FileChannel dataChannel;
    private volatile RandomAccessFile metaRaf;
    private volatile FileChannel metaChannel;
    private final ReentrantLock lock;
    private final Condition notEmpty;
    private final Object metaWriteLock = new Object();
    private final Object memoryMutex = new Object();
    private final Options options;
    private final AtomicLong approximateSize = new AtomicLong(0);
    private final AtomicLong globalSequence;

    private long consumerOffset, producerOffset, recordCount, lastIndex, lastCompactionTimeNanos;
    private long lastDeliveredIndex = -1;
    private long outOfOrderCount = 0;
    private final boolean orderDetectionEnabled;
    private volatile boolean closed, shutdownRequested;
    private volatile CompactionState compactionState = CompactionState.IDLE;
    private PreIndexedItem<T> handoffItem;

    private final boolean enableMemoryBuffer;
    private final BlockingQueue<MemoryBufferEntry<T>> memoryBuffer;
    private final BlockingDeque<MemoryBufferEntry<T>> drainingQueue;
    private final AtomicLong memoryBufferModeUntil = new AtomicLong(0);
    private final AtomicBoolean drainingInProgress = new AtomicBoolean(false),
            switchBackRequested = new AtomicBoolean(false), revalidationScheduled = new AtomicBoolean(false);
    private volatile CountDownLatch drainCompletionLatch;

    private final ExecutorService drainExecutor, compactionExecutor;
    private final ScheduledExecutorService revalidationExecutor;
    private final ScheduledExecutorService maintenanceExecutor;

    private volatile long lastSizeReconciliationTimeNanos = System.nanoTime();

    /**
     * Constructs a queue instance bound to the supplied directory and I/O handles,
     * restoring the
     * persisted state captured in the provided snapshot and applying the chosen
     * options. The constructor
     * wires concurrency primitives and, when enabled, prepares in-memory staging
     * and maintenance
     * services used to balance throughput during compaction or contention. Intended
     * for internal use by
     * factory methods.
     *
     * @param queueDir    base directory for persistent data and metadata
     * @param raf         open random-access handle for the queue log
     * @param dataChannel file channel associated with the queue log
     * @param state       recovered or rebuilt queue state to initialize cursors and
     *                    counters
     * @param options     operational configuration snapshot
     */
    private NQueue(Path queueDir, RandomAccessFile raf, FileChannel dataChannel, QueueState state, Options options)
            throws IOException {
        this.queueDir = queueDir;
        this.dataPath = queueDir.resolve(DATA_FILE);
        this.metaPath = queueDir.resolve(META_FILE);
        this.tempDataPath = queueDir.resolve(DATA_FILE + ".compacting");
        this.raf = raf;
        this.dataChannel = dataChannel;

        // Initialize metadata channel
        this.metaRaf = new RandomAccessFile(this.metaPath.toFile(), "rw");
        this.metaChannel = this.metaRaf.getChannel();

        this.lock = new ReentrantLock();
        this.notEmpty = this.lock.newCondition();
        this.consumerOffset = state.consumerOffset;
        this.producerOffset = state.producerOffset;
        this.recordCount = state.recordCount;
        this.lastIndex = state.lastIndex;
        this.globalSequence = new AtomicLong(state.lastIndex);
        this.approximateSize.set(state.recordCount);
        this.options = options;
        this.enableMemoryBuffer = options.enableMemoryBuffer;
        this.orderDetectionEnabled = options.enableOrderDetection;
        this.lastCompactionTimeNanos = System.nanoTime();
        if (enableMemoryBuffer) {
            this.memoryBuffer = new LinkedBlockingQueue<>(options.memoryBufferSize);
            this.drainingQueue = new LinkedBlockingDeque<>(options.memoryBufferSize);
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
        this.compactionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "nqueue-compaction-worker");
            t.setDaemon(true);
            return t;
        });
        this.maintenanceExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "nqueue-maintenance-worker");
            t.setDaemon(true);
            return t;
        });

        // Schedule periodic size reconciliation
        this.maintenanceExecutor.scheduleWithFixedDelay(this::reconcileSize, options.maintenanceIntervalNanos,
                options.maintenanceIntervalNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Reconciles the optimistic approximate size with the exact size.
     * Tries to acquire lock opportunistically, but forces it if the last update was
     * too long ago.
     */
    private void reconcileSize() {
        if (closed || shutdownRequested)
            return;

        long maxDelay = options.maxSizeReconciliationIntervalNanos;
        boolean forced = maxDelay > 0 && (System.nanoTime() - lastSizeReconciliationTimeNanos) > maxDelay;
        boolean locked = false;

        try {
            if (forced) {
                lock.lock();
                locked = true;
            } else {
                locked = lock.tryLock();
            }

            if (locked) {
                try {
                    long exactSize = recordCount;
                    if (enableMemoryBuffer) {
                        exactSize += (long) memoryBuffer.size() + drainingQueue.size();
                    }
                    approximateSize.set(exactSize);
                    lastSizeReconciliationTimeNanos = System.nanoTime();
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception e) {
            // Log or ignore, just keep running
        }
    }

    /**
     * Opens or creates a named queue under the given base directory using default
     * options. On first use,
     * the directory structure is created. On subsequent runs, prior state is
     * recovered so that ordering and
     * consumption progress are preserved. Any incomplete tail data is safely pruned
     * before use.
     *
     * @param baseDir   base directory where the queue folder will reside
     * @param queueName logical queue name used as a directory under the base
     * @param <T>       serializable element type
     * @return an operational queue instance ready for concurrent producers and
     *         consumers
     * @throws IOException if the storage cannot be prepared or state cannot be
     *                     recovered
     */
    public static <T extends Serializable> NQueue<T> open(Path baseDir, String queueName) throws IOException {
        return open(baseDir, queueName, Options.defaults());
    }

    /**
     * Opens or creates a named queue under the given base directory with explicit
     * options. Startup
     * reconciles metadata with the durable log so the queue begins in a consistent
     * state. When requested,
     * a rebuild pass is performed to reconstruct state solely from the log.
     *
     * @param baseDir   base directory where the queue folder will reside
     * @param queueName logical queue name used as a directory under the base
     * @param options   operational configuration controlling durability,
     *                  compaction, and staging
     * @param <T>       serializable element type
     * @return an operational queue instance ready for concurrent producers and
     *         consumers
     * @throws IOException if the storage cannot be prepared or state cannot be
     *                     recovered
     */
    public static <T extends Serializable> NQueue<T> open(Path baseDir, String queueName, Options options)
            throws IOException {
        Objects.requireNonNull(baseDir);
        Objects.requireNonNull(queueName);
        Objects.requireNonNull(options);
        Path qDir = baseDir.resolve(queueName);
        Files.createDirectories(qDir);
        RandomAccessFile raf = new RandomAccessFile(qDir.resolve(DATA_FILE).toFile(), "rw");
        FileChannel ch = raf.getChannel();
        QueueState state = loadOrRebuildState(ch, qDir.resolve(META_FILE), options);
        if (ch.size() > state.producerOffset) {
            ch.truncate(state.producerOffset);
            ch.force(options.withFsync);
        }
        return new NQueue<>(qDir, raf, ch, state, options);
    }

    public StatsUtils getStats() {
        return statsUtils;
    }

    public Options.RetentionPolicy getRetentionPolicy() {
        return options.retentionPolicy;
    }

    /**
     * Enqueues a single record while preserving FIFO order. Depending on
     * configuration and current
     * conditions, the record may be staged in memory and durably appended later, or
     * appended directly to
     * the durable log. Producers observe backpressure only when the bounded staging
     * area is saturated
     * or when direct appends are momentarily contended.
     *
     * @param object element to append; must be non-null and serializable by the
     *               application classpath
     * @return a logical offset for the first byte of the record when appended
     *         directly, or a sentinel value
     *         indicating deferred durability when temporarily staged
     * @throws IOException if the enqueue cannot be acknowledged due to storage
     *                     errors
     */
    public long offer(T object) throws IOException {
        Objects.requireNonNull(object);

        if (enableMemoryBuffer) {
            if (System.nanoTime() < memoryBufferModeUntil.get())
                return offerToMemory(object, true);
            if (lock.tryLock()) {
                try {
                    // Short-circuit: if queue is empty (disk + memory) and a consumer is waiting,
                    // handoff directly.
                    if (options.allowShortCircuit && recordCount == 0 && handoffItem == null && drainingQueue.isEmpty()
                            && memoryBuffer.isEmpty() && lock.hasWaiters(notEmpty)) {
                        long seq = globalSequence.incrementAndGet();
                        PreIndexedItem<T> pItem = new PreIndexedItem<>(object, seq);
                        handoffItem = pItem;
                        notEmpty.signal();
                        statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);
                        return OFFSET_HANDOFF;
                    }

                    if (compactionState == CompactionState.RUNNING) {
                        activateMemoryMode();
                        return offerToMemory(object, false);
                    }
                    drainMemoryBufferSync();
                    long offset = offerDirectLocked(object);
                    triggerDrainIfNeeded();
                    return offset;
                } finally {
                    lock.unlock();
                }
            } else {
                activateMemoryMode();
                return offerToMemory(object, false);
            }
        } else {
            lock.lock();
            try {
                // Short-circuit: if queue is empty and a consumer is waiting, handoff directly.
                if (options.allowShortCircuit && recordCount == 0 && handoffItem == null && lock.hasWaiters(notEmpty)) {
                    long seq = globalSequence.incrementAndGet();
                    PreIndexedItem<T> pItem = new PreIndexedItem<>(object, seq);
                    handoffItem = pItem;
                    notEmpty.signal();
                    statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);
                    return OFFSET_HANDOFF;
                }
                return offerDirectLocked(object);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Retrieves and removes the head of the queue, blocking until an element
     * becomes available. The
     * consumption cursor is advanced and persisted before the element is delivered,
     * ensuring at-most-once
     * delivery from the queue’s perspective across restarts.
     *
     * @return the next available element
     * @throws IOException if the queue cannot read or persist consumption progress
     */
    public Optional<T> poll() throws IOException {
        lock.lock();
        try {
            // Optimization: Only force a sync drain if the disk queue is empty.
            // If we have records on disk, consume them first to avoid blocking readers on
            // write I/O.
            if (recordCount == 0 && handoffItem == null) {
                drainMemoryBufferSync();
            }

            if (handoffItem != null) {
                long idx = handoffItem.index();
                recordDeliveryIndex(idx);
                T item = handoffItem.item();
                handoffItem = null;
                return Optional.of(item);
            }

            while (recordCount == 0) {
                notEmpty.awaitUninterruptibly();
                if (handoffItem != null) {
                    long idx = handoffItem.index();
                    recordDeliveryIndex(idx);
                    T item = handoffItem.item();
                    handoffItem = null;
                    return Optional.of(item);
                }
            }
            return consumeNextRecordLocked().map(this::safeDeserialize);
        } finally {
            statsUtils.notifyHitCounter(NQueueMetrics.POLL_EVENT);
            lock.unlock();
        }
    }

    /**
     * Retrieves and removes the head of the queue, waiting up to the specified time
     * if necessary for an
     * element to become available. If the wait times out, an empty result is
     * returned. When an element is
     * returned, the consumption cursor is advanced and persisted.
     *
     * @param timeout maximum time to wait
     * @param unit    time unit for the timeout
     * @return the next available element or empty if the timeout elapses
     * @throws IOException if the queue cannot read or persist consumption progress
     */
    public Optional<T> poll(long timeout, TimeUnit unit) throws IOException {
        long nanos = unit.toNanos(timeout);
        lock.lock();
        try {
            // Optimization: Only force a sync drain if we absolutely need data and none is
            // on disk
            if (recordCount == 0 && handoffItem == null) {
                drainMemoryBufferSync();
            }

            if (handoffItem != null) {
                long idx = handoffItem.index();
                recordDeliveryIndex(idx);
                T item = handoffItem.item();
                handoffItem = null;
                return Optional.of(item);
            }

            while (recordCount == 0) {
                // If checking memory triggered a drain and resulted in records, break
                if (checkAndDrainMemorySync())
                    break;

                if (nanos <= 0L)
                    return Optional.empty();
                try {
                    nanos = notEmpty.awaitNanos(nanos);
                    if (handoffItem != null) {
                        long idx = handoffItem.index();
                        recordDeliveryIndex(idx);
                        T item = handoffItem.item();
                        handoffItem = null;
                        return Optional.of(item);
                    }
                    if (checkAndDrainMemorySync())
                        break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    checkAndDrainMemorySync();
                    return Optional.empty();
                }
            }
            return consumeNextRecordLocked().map(this::safeDeserialize);
        } finally {
            statsUtils.notifyHitCounter(NQueueMetrics.POLL_EVENT);
            lock.unlock();
        }
    }

    /**
     * Returns, without removing, the element at the head of the queue if present.
     * This operation does not
     * advance the consumption cursor and therefore does not alter delivery
     * semantics.
     *
     * @return the current head element or empty if the queue is logically empty
     * @throws IOException if the queue cannot access the stored data
     */
    public Optional<T> peek() throws IOException {
        lock.lock();
        try {
            // 0. If we have a handoff item, it is effectively the head
            if (handoffItem != null) {
                return Optional.of(handoffItem.item());
            }

            // 1. If we have records on disk, peek from there (FIFO head)
            if (recordCount > 0) {
                return readAtInternal(consumerOffset).map(res -> safeDeserialize(res.getRecord()));
            }

            // 2. If disk is empty but we have memory buffer enabled, check memory buffers.
            // NOTE: This does NOT drain/flush to disk, it just looks at what is waiting in
            // RAM.
            if (enableMemoryBuffer) {
                // Draining queue has older items than memoryBuffer, check it first.
                MemoryBufferEntry<T> fromDrain = drainingQueue.peekFirst();
                if (fromDrain != null) {
                    return Optional.of(fromDrain.item);
                }

                MemoryBufferEntry<T> fromMem = memoryBuffer.peek();
                if (fromMem != null) {
                    return Optional.of(fromMem.item);
                }
            }

            return Optional.empty();
        } finally {
            statsUtils.notifyHitCounter(NQueueMetrics.PEEK_EVENT);
            lock.unlock();
        }
    }

    /**
     * Returns the current logical size of the queue using strong accounting. When
     * in-memory staging is
     * enabled, the count includes both staged and durable records visible to
     * consumers.
     *
     * @return number of elements that would be observed across all sources
     */
    public long size() {
        lock.lock();
        try {
            long s = recordCount;
            if (handoffItem != null)
                s++;
            if (enableMemoryBuffer)
                s += (long) memoryBuffer.size() + drainingQueue.size();
            return s;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the queue size, optionally using a fast, approximate counter that may
     * lag under concurrency
     * but avoids coordination costs. This is useful for monitoring and heuristics
     * where exactness is not
     * required.
     *
     * @param optimistic when true, returns a fast approximation; when false,
     *                   computes a precise value
     * @return approximate or precise size depending on the chosen mode
     */
    public long size(boolean optimistic) {
        return optimistic ? approximateSize.get() : size();
    }

    /**
     * Indicates whether the queue is currently empty according to a precise size
     * check.
     *
     * @return true if no records are pending delivery; false otherwise
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns the count of durable records strictly within the persistent log
     * segment awaiting consumption.
     * This value does not include items that may be temporarily staged in memory.
     *
     * @return durable record count pending delivery
     */
    public long getRecordCount() {
        lock.lock();
        try {
            return recordCount;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads a range of items from the queue without consuming them.
     * This is useful for creating snapshots for replication.
     * 
     * @param startIndex zero-based logical index to start reading from
     * @param maxItems   maximum number of items to read
     * @return result containing the items and whether more items are available
     * @throws IOException if reading fails
     */
    public ReadRangeResult<T> readRange(int startIndex, int maxItems) throws IOException {
        lock.lock();
        try {
            List<T> items = new ArrayList<>();
            long offset = consumerOffset;
            int currentIndex = 0;

            // Skip to startIndex
            while (currentIndex < startIndex && offset < producerOffset) {
                Optional<NQueueReadResult> result = readAtInternal(offset);
                if (result.isEmpty())
                    break;
                offset = result.get().getNextOffset();
                currentIndex++;
            }

            // Read up to maxItems
            int readCount = 0;
            while (readCount < maxItems && offset < producerOffset) {
                Optional<NQueueReadResult> result = readAtInternal(offset);
                if (result.isEmpty())
                    break;
                items.add(safeDeserialize(result.get().getRecord()));
                offset = result.get().getNextOffset();
                readCount++;
            }

            boolean hasMore = offset < producerOffset;
            return new ReadRangeResult<>(items, hasMore, currentIndex + readCount);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Result of a range read operation.
     * 
     * @param items     the items read
     * @param hasMore   whether more items are available after this range
     * @param nextIndex the next index to continue reading from
     * @param <T>       the element type
     */
    public record ReadRangeResult<T>(List<T> items, boolean hasMore, int nextIndex) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    @Override
    /**
     * Shuts down the queue gracefully. Any staged records are flushed when
     * possible, a final compaction may
     * be scheduled if appropriate, and all background services and I/O resources
     * are closed. The method
     * attempts to complete maintenance promptly without compromising data safety.
     *
     * @throws IOException if underlying channels cannot be closed cleanly
     */
    public void close() throws IOException {
        // Stop maintenance first
        maintenanceExecutor.shutdownNow();
        statsUtils.shutdown();
        if (enableMemoryBuffer) {
            lock.lock();
            try {
                drainMemoryBufferSync();
            } catch (IOException ignored) {
            } finally {
                lock.unlock();
            }
        }
        lock.lock();
        try {
            shutdownRequested = true;
            if (compactionState == CompactionState.IDLE && consumerOffset > 0) {
                QueueState snap = currentState();
                compactionState = CompactionState.RUNNING;
                compactionExecutor.submit(() -> runCompactionTask(snap));
            }
        } finally {
            lock.unlock();
        }
        compactionExecutor.shutdown();
        try {
            compactionExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (enableMemoryBuffer) {
            drainExecutor.shutdownNow();
            revalidationExecutor.shutdownNow();
        }
        lock.lock();
        try {
            if (dataChannel.isOpen())
                dataChannel.close();
            if (raf != null)
                raf.close();
            if (metaChannel != null && metaChannel.isOpen())
                metaChannel.close();
            if (metaRaf != null)
                metaRaf.close();
            closed = true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits briefly for an in-progress memory-buffer drain to complete, if any.
     * Used to improve handoff
     * between maintenance phases and normal operation without blocking
     * indefinitely.
     */
    private void awaitDrainCompletion() {
        if (!enableMemoryBuffer)
            return;
        if (!drainingInProgress.get() && (drainingQueue == null || drainingQueue.isEmpty()))
            return;
        CountDownLatch latch = drainCompletionLatch;
        if (latch != null) {
            try {
                latch.await(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Callback invoked after a compaction attempt finishes. It reconciles the
     * operating mode with the
     * current workload, scheduling a revalidation step when necessary to decide
     * whether to continue using
     * the in-memory staging path or revert to direct durable appends.
     *
     * @param success whether the compaction reached a consistent end state
     * @param error   an optional cause when a failure was detected
     */
    private void onCompactionFinished(boolean success, Throwable error) {
        if (!enableMemoryBuffer)
            return;
        boolean acquired = lock.tryLock();
        if (acquired) {
            try {
                if (compactionState == CompactionState.RUNNING)
                    compactionState = CompactionState.IDLE;
                if (memoryBuffer.isEmpty() && drainingQueue.isEmpty()) {
                    memoryBufferModeUntil.set(0);
                    return;
                }
            } finally {
                lock.unlock();
            }
        }
        if (!switchBackRequested.compareAndSet(false, true))
            return;
        revalidationExecutor.execute(() -> {
            try {
                if (lock.tryLock()) {
                    try {
                        if (compactionState != CompactionState.RUNNING && memoryBuffer.isEmpty()
                                && drainingQueue.isEmpty()) {
                            memoryBufferModeUntil.set(0);
                            return;
                        }
                    } finally {
                        lock.unlock();
                    }
                }
                try {
                    if (lock.tryLock(options.lockTryTimeoutNanos, TimeUnit.NANOSECONDS)) {
                        try {
                            drainMemoryBufferSync();
                        } finally {
                            lock.unlock();
                        }
                    }
                } catch (Exception ignored) {
                }
                if (lock.tryLock()) {
                    try {
                        if (memoryBuffer.isEmpty() && drainingQueue.isEmpty()
                                && compactionState != CompactionState.RUNNING) {
                            memoryBufferModeUntil.set(0);
                        } else {
                            activateMemoryMode();
                            triggerDrainIfNeeded();
                        }
                    } finally {
                        lock.unlock();
                    }
                } else {
                    activateMemoryMode();
                }
            } finally {
                switchBackRequested.set(false);
            }
        });
    }

    /**
     * Reads, without advancing the consumption cursor, the record stored at the
     * provided durable offset and
     * returns the deserialized element if a complete record exists at that
     * position. This is intended for
     * diagnostic and auditing scenarios.
     *
     * @param offset durable position to inspect
     * @return the element at the offset or empty if no complete record is present
     * @throws IOException if reading from storage fails
     */
    public Optional<T> readAt(long offset) throws IOException {
        lock.lock();
        try {
            return readAtInternal(offset).map(res -> safeDeserialize(res.getRecord()));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads, without advancing the consumption cursor, the record stored at the
     * provided durable offset and
     * returns its structured representation together with the next record position.
     * This variant exposes
     * raw metadata for advanced tooling.
     *
     * @param offset durable position to inspect
     * @return a structured result with the record and the next offset, or empty if
     *         none is available
     * @throws IOException if reading from storage fails
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
     * Reads, without advancing the consumption cursor, the record with the specific
     * logical index.
     * This method scans the log to find the record. Since offsets change during
     * compaction, this is the
     * reliable way to retrieve records by identity in a distributed context.
     *
     * @param index logical index to find
     * @return a structured result with the record and the next offset, or empty if
     *         not found/expired
     * @throws IOException if reading from storage fails
     */
    public Optional<NQueueReadResult> readRecordAtIndex(long index) throws IOException {
        lock.lock();
        try {
            long offset = 0;
            long size = dataChannel.size();
            while (offset < size) {
                NQueueRecordMetaData.HeaderPrefix pref = NQueueRecordMetaData.readPrefix(dataChannel, offset);
                NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(dataChannel, offset, pref.headerLen);

                if (meta.getIndex() == index) {
                    long hEnd = offset + NQueueRecordMetaData.fixedPrefixSize() + pref.headerLen;
                    long pEnd = hEnd + meta.getPayloadLen();
                    byte[] payload = new byte[meta.getPayloadLen()];
                    ByteBuffer pb = ByteBuffer.wrap(payload);
                    while (pb.hasRemaining()) {
                        if (dataChannel.read(pb, hEnd + (long) pb.position()) < 0)
                            throw new EOFException();
                    }
                    return Optional.of(new NQueueReadResult(new NQueueRecord(meta, payload), pEnd));
                } else if (meta.getIndex() > index) {
                    // We passed the index, so it must have been compacted away (or doesn't exist
                    // yet if we are ahead)
                    return Optional.empty();
                }

                offset += NQueueRecordMetaData.fixedPrefixSize() + pref.headerLen + meta.getPayloadLen();
            }
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns, without advancing the consumption cursor, the raw representation of
     * the head record if one
     * exists. This provides visibility into metadata alongside the payload for
     * diagnostic use.
     *
     * @return the next record or empty if the queue is logically empty
     * @throws IOException if reading from storage fails
     */
    public Optional<NQueueRecord> peekRecord() throws IOException {
        lock.lock();
        try {
            if (recordCount == 0)
                return Optional.empty();
            return readAtInternal(consumerOffset).map(NQueueReadResult::getRecord);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Appends a single element directly to durable storage. Coordination must
     * already be in place to ensure
     * exclusive access to mutable state. Intended for internal paths that bypass
     * the in-memory staging layer.
     *
     * @param object element to append
     * @return logical durable offset of the appended record
     * @throws IOException if the append cannot be acknowledged
     */
    private long offerDirectLocked(T object) throws IOException {
        long seq = globalSequence.incrementAndGet();
        PreIndexedItem<T> pItem = new PreIndexedItem<>(object, seq);
        return offerBatchLocked(List.of(pItem), true);
    }

    /**
     * Appends a batch of elements directly to durable storage, advancing producer
     * cursors and updating
     * persistent metadata in one step. When requested by options, a synchronous
     * flush is performed before
     * returning. Consumers are signaled upon successful append.
     *
     * @param items elements to append in FIFO order
     * @param fsync whether to request a synchronous flush according to durability
     *              settings
     * @return logical durable offset of the first appended record, or a sentinel
     *         when no items are provided
     * @throws IOException if the append cannot be acknowledged
     */
    private long offerBatchLocked(List<PreIndexedItem<T>> items, boolean fsync) throws IOException {
        if (items.isEmpty())
            return -1;
        long writePos = producerOffset, firstStart = -1, initialCount = recordCount;
        for (PreIndexedItem<T> pItem : items) {
            T obj = pItem.item();
            byte[] payload = toBytes(obj);
            long idx = pItem.index();
            lastIndex = idx; // Update lastIndex to the sequence provided

            NQueueRecordMetaData meta = new NQueueRecordMetaData(idx, System.currentTimeMillis(), payload.length,
                    obj.getClass().getCanonicalName());
            ByteBuffer hb = meta.toByteBuffer();
            long rStart = writePos;
            while (hb.hasRemaining())
                writePos += dataChannel.write(hb, writePos);
            ByteBuffer pb = ByteBuffer.wrap(payload);
            while (pb.hasRemaining())
                writePos += dataChannel.write(pb, writePos);
            if (firstStart < 0)
                firstStart = rStart;
        }
        producerOffset = writePos;
        recordCount += items.size();
        approximateSize.addAndGet(items.size());
        if (initialCount == 0)
            consumerOffset = firstStart;
        persistCurrentStateLocked();
        if (fsync && options.withFsync)
            dataChannel.force(true);
        maybeCompactLocked();
        notEmpty.signalAll();
        statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);
        return firstStart;
    }

    /**
     * Internal read helper that validates the presence of a complete record at the
     * given durable offset and
     * returns its raw representation together with the next position. Incomplete or
     * inconsistent tails yield
     * an empty result.
     *
     * @param offset durable position to inspect
     * @return structured result with record and next offset, or empty if no
     *         complete record is present
     * @throws IOException if accessing storage fails
     */
    private Optional<NQueueReadResult> readAtInternal(long offset) throws IOException {
        long size = dataChannel.size();
        if (offset + NQueueRecordMetaData.fixedPrefixSize() > size)
            return Optional.empty();
        NQueueRecordMetaData.HeaderPrefix pref = NQueueRecordMetaData.readPrefix(dataChannel, offset);
        long hEnd = offset + NQueueRecordMetaData.fixedPrefixSize() + pref.headerLen;
        if (hEnd > size)
            return Optional.empty();
        NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(dataChannel, offset, pref.headerLen);
        long pEnd = hEnd + meta.getPayloadLen();
        if (pEnd > size)
            return Optional.empty();
        byte[] payload = new byte[meta.getPayloadLen()];
        ByteBuffer pb = ByteBuffer.wrap(payload);
        while (pb.hasRemaining()) {
            if (dataChannel.read(pb, hEnd + (long) pb.position()) < 0)
                throw new EOFException();
        }
        return Optional.of(new NQueueReadResult(new NQueueRecord(meta, payload), pEnd));
    }

    /**
     * Removes the head record from the durable log from the queue’s perspective by
     * advancing the consumption
     * cursor and persisting that advancement. The raw record is returned for
     * subsequent deserialization by
     * the caller.
     *
     * @return the consumed record if available
     * @throws IOException if the new state cannot be persisted
     */
    private Optional<NQueueRecord> consumeNextRecordLocked() throws IOException {
        return readAtInternal(consumerOffset).map(res -> {
            long currentIndex = res.getRecord().meta().getIndex();
            recordDeliveryIndex(currentIndex);

            consumerOffset = res.getNextOffset();
            recordCount--;
            approximateSize.decrementAndGet();
            if (recordCount == 0)
                consumerOffset = producerOffset;
            try {
                persistCurrentStateLocked();
                maybeCompactLocked();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return res.getRecord();
        });
    }

    /**
     * Persists the current queue cursors and counters so that progress is preserved
     * across restarts. This
     * method assumes exclusive access to the queue state.
     *
     * @throws IOException if the metadata cannot be written
     */
    private void persistCurrentStateLocked() throws IOException {
        synchronized (metaWriteLock) {
            NQueueQueueMeta.update(metaChannel, consumerOffset, producerOffset, recordCount, lastIndex,
                    options.withFsync);
        }
    }

    /**
     * Restores a consistent queue state using existing metadata when possible, or
     * by scanning the durable
     * log to rebuild cursors and counters. When the log contains incomplete data at
     * the tail, the torn tail
     * is discarded to ensure a consistent starting point.
     *
     * @param ch       channel to the durable log
     * @param metaPath path to the metadata file
     * @param options  startup options controlling reset behavior
     * @return recovered state snapshot
     * @throws IOException if recovery cannot complete
     */
    private static QueueState loadOrRebuildState(FileChannel ch, Path metaPath, Options options) throws IOException {
        if (options.resetOnRestart) {
            QueueState s = rebuildStateFromLog(ch);
            NQueueQueueMeta.write(metaPath, s.consumerOffset, s.producerOffset, s.recordCount, s.lastIndex);
            return s;
        }
        if (Files.exists(metaPath)) {
            try {
                NQueueQueueMeta meta = NQueueQueueMeta.read(metaPath);
                QueueState s = new QueueState(meta.getConsumerOffset(), meta.getProducerOffset(), meta.getRecordCount(),
                        meta.getLastIndex());
                if (s.consumerOffset >= 0 && s.producerOffset >= s.consumerOffset && ch.size() >= s.producerOffset)
                    return s;
            } catch (IOException ignored) {
            }
        }
        QueueState s = rebuildStateFromLog(ch);
        NQueueQueueMeta.write(metaPath, s.consumerOffset, s.producerOffset, s.recordCount, s.lastIndex);
        return s;
    }

    /**
     * Scans the durable log from the beginning to reconstruct a consistent state
     * snapshot, stopping at the
     * last complete record. Any partial tail is removed before the state is
     * returned. This operation is used
     * when metadata is absent or cannot be trusted.
     *
     * @param ch channel to the durable log
     * @return reconstructed state snapshot
     * @throws IOException if the log cannot be read or reconciled
     */
    private static QueueState rebuildStateFromLog(FileChannel ch) throws IOException {
        long offset = 0, count = 0, lastIdx = -1, size = ch.size();
        while (offset < size) {
            try {
                NQueueRecordMetaData.HeaderPrefix pref = NQueueRecordMetaData.readPrefix(ch, offset);
                NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(ch, offset, pref.headerLen);
                offset = offset + NQueueRecordMetaData.fixedPrefixSize() + pref.headerLen + meta.getPayloadLen();
                if (offset > size)
                    throw new EOFException();
                count++;
                lastIdx = meta.getIndex();
            } catch (Exception e) {
                ch.truncate(offset);
                size = offset;
                break;
            }
        }
        ch.force(true);
        return new QueueState(count > 0 ? 0 : size, size, count, lastIdx);
    }

    /**
     * Transfers a contiguous region from one channel to another. Used by background
     * maintenance to assemble
     * a compacted log while preserving the unconsumed segment and arrival order.
     *
     * @param src   source channel
     * @param start start position, inclusive
     * @param end   end position, exclusive
     * @param dst   destination channel
     * @throws IOException if the transfer fails
     */
    private void copyRegion(FileChannel src, long start, long end, FileChannel dst) throws IOException {
        long pos = start, count = end - start;
        while (count > 0) {
            long t = src.transferTo(pos, count, dst);
            if (t <= 0)
                break;
            pos += t;
            count -= t;
        }
    }

    /**
     * Serializes an element to a byte array using the platform’s object
     * serialization mechanism. Used to
     * produce a durable payload for storage.
     *
     * @param obj element to serialize
     * @return binary representation suitable for persistence
     * @throws IOException if the element cannot be serialized
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
     * Deserializes the payload of a raw record into the expected element type.
     * Errors during deserialization
     * surface as unchecked failures to signal data that cannot be interpreted on
     * the current classpath.
     *
     * @param record raw record containing a binary payload
     * @return deserialized element
     */
    private T safeDeserialize(NQueueRecord record) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(record.payload());
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (T) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Activates a temporary staging mode that routes new enqueues through the
     * in-memory buffer for a short
     * interval. This helps to smooth producer throughput during maintenance or
     * brief contention bursts. The
     * mode revalidates itself periodically to decide when to revert to direct
     * appends.
     */
    private void activateMemoryMode() {
        if (!enableMemoryBuffer)
            return;
        long until = System.nanoTime() + options.revalidationIntervalNanos;
        memoryBufferModeUntil.updateAndGet(c -> Math.max(c, until));
        scheduleRevalidation();
    }

    /**
     * Schedules a future revalidation of the temporary staging mode when not
     * already pending. The scheduled
     * task verifies whether conditions still warrant staying in memory-buffer mode.
     */
    private void scheduleRevalidation() {
        if (revalidationExecutor != null && revalidationScheduled.compareAndSet(false, true)) {
            revalidationExecutor.schedule(() -> {
                try {
                    revalidateMemoryMode();
                } finally {
                    revalidationScheduled.set(false);
                    if (memoryBufferModeUntil.get() > System.nanoTime())
                        scheduleRevalidation();
                }
            }, options.revalidationIntervalNanos, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Re-evaluates whether the in-memory staging mode should continue. When
     * conditions improve and no staged
     * work remains, the queue reverts to direct durable appends; otherwise, the
     * staging window is extended.
     */
    private void revalidateMemoryMode() {
        if (System.nanoTime() >= memoryBufferModeUntil.get()) {
            if (lock.tryLock()) {
                try {
                    if (compactionState != CompactionState.RUNNING
                            && (!enableMemoryBuffer || (memoryBuffer.isEmpty() && drainingQueue.isEmpty()))) {
                        memoryBufferModeUntil.set(0);
                        return;
                    }
                } finally {
                    lock.unlock();
                }
            }
            activateMemoryMode();
        }
    }

    /**
     * Attempts to place a new element into the bounded in-memory staging buffer.
     * When the buffer is full and
     * conditions allow, the queue may opportunistically revert to a direct durable
     * append for the current
     * element. Producers block only when the buffer is at capacity and no immediate
     * fallback is possible.
     *
     * @param object           element to stage
     * @param revalidateIfFull whether to reassess staging mode when the buffer is
     *                         saturated
     * @return a sentinel indicating that durability will be achieved by a later
     *         drain
     * @throws IOException if a fallback durable append is attempted and fails
     */
    private long offerToMemory(T object, boolean revalidateIfFull) throws IOException {
        try {
            if (revalidateIfFull && memoryBuffer.remainingCapacity() == 0) {
                revalidateMemoryMode();
                if (lock.tryLock()) {
                    try {
                        if (compactionState != CompactionState.RUNNING) {
                            drainMemoryBufferSync();
                            return offerDirectLocked(object);
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            }
            synchronized (memoryMutex) {
                long seq = globalSequence.incrementAndGet();
                MemoryBufferEntry<T> e = new MemoryBufferEntry<>(object, seq);
                if (!memoryBuffer.offer(e))
                    memoryBuffer.put(e);
            }
            triggerDrainIfNeeded();
            return -1;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException(ex);
        }
    }

    /**
     * Initiates an asynchronous drain from the staging buffer into durable storage
     * when necessary. A single
     * drain worker is kept active at a time to preserve ordering.
     */
    private void triggerDrainIfNeeded() {
        if (enableMemoryBuffer && !memoryBuffer.isEmpty() && drainingInProgress.compareAndSet(false, true)) {
            drainCompletionLatch = new CountDownLatch(1);
            drainExecutor.submit(this::drainMemoryBufferAsync);
        }
    }

    /**
     * Background worker that periodically acquires the necessary coordination to
     * flush staged elements into
     * durable storage in batches. The worker makes progress opportunistically to
     * minimize interference with
     * producers and consumers.
     */
    private void drainMemoryBufferAsync() {
        try {
            while (enableMemoryBuffer && (!memoryBuffer.isEmpty() || !drainingQueue.isEmpty())) {
                boolean ok = false;
                try {
                    if (lock.tryLock(options.lockTryTimeoutNanos, TimeUnit.NANOSECONDS)) {
                        try {
                            drainMemoryBufferSync();
                            ok = true;
                        } finally {
                            lock.unlock();
                        }
                    }
                } catch (Exception ignored) {
                }
                if (!ok) {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            drainingInProgress.set(false);
            if (drainCompletionLatch != null)
                drainCompletionLatch.countDown();
            if (enableMemoryBuffer && !memoryBuffer.isEmpty())
                triggerDrainIfNeeded();
        }
    }

    /**
     * Synchronously flushes staged elements into durable storage in FIFO order,
     * aggregating work into small
     * batches to reduce coordination overhead. Failures result in staged entries
     * being retained for a later
     * retry.
     *
     * @throws IOException if persistence fails and the drain cannot complete
     */
    private void drainMemoryBufferSync() throws IOException {
        if (!enableMemoryBuffer)
            return;
        while (true) {
            memoryBuffer.drainTo(drainingQueue, MEMORY_DRAIN_BATCH_SIZE);
            if (drainingQueue.isEmpty())
                break;
            List<PreIndexedItem<T>> batch = new ArrayList<>();
            List<MemoryBufferEntry<T>> entries = new ArrayList<>();
            for (int i = 0; i < MEMORY_DRAIN_BATCH_SIZE; i++) {
                MemoryBufferEntry<T> ent = drainingQueue.poll();
                if (ent == null)
                    break;
                batch.add(new PreIndexedItem<>(ent.item, ent.index));
                entries.add(ent);
            }
            if (batch.isEmpty())
                break;
            try {
                offerBatchLocked(batch, options.withFsync);
            } catch (IOException ex) {
                for (int i = entries.size() - 1; i >= 0; i--)
                    drainingQueue.addFirst(entries.get(i));
                throw ex;
            }
        }
    }

    /**
     * Checks for staged work and performs a synchronous drain when present,
     * indicating whether durable
     * records became available for consumption as a result.
     *
     * @return true if at least one durable record is now available; false otherwise
     * @throws IOException if draining encounters a storage error
     */
    private boolean checkAndDrainMemorySync() throws IOException {
        if (enableMemoryBuffer && (!memoryBuffer.isEmpty() || !drainingQueue.isEmpty())) {
            drainMemoryBufferSync();
            return recordCount > 0;
        }
        return false;
    }

    /**
     * Evaluates whether conditions warrant a background compaction and, when they
     * do, schedules one. The
     * decision considers the amount of consumed data and an optional time-based
     * trigger, and avoids running
     * when a compaction is already in progress or shutdown is underway.
     */
    private void maybeCompactLocked() {
        if (compactionState == CompactionState.RUNNING || shutdownRequested)
            return;
        if (producerOffset <= 0)
            return;

        long now = System.nanoTime();
        boolean shouldCompact = false;

        if (options.retentionPolicy == Options.RetentionPolicy.TIME_BASED) {
            // Check if the oldest record (at the beginning of the file) has expired.
            if (options.retentionTimeNanos > 0 && recordCount > 0) {
                try {
                    // Try reading the first header at offset 0
                    if (dataChannel.size() > NQueueRecordMetaData.fixedPrefixSize()) {
                        NQueueRecordMetaData.HeaderPrefix pref = NQueueRecordMetaData.readPrefix(dataChannel, 0);
                        NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(dataChannel, 0, pref.headerLen);
                        long recordAge = System.currentTimeMillis() - meta.getTimestamp();
                        // Convert nanos to millis for comparison
                        if (recordAge > TimeUnit.NANOSECONDS.toMillis(options.retentionTimeNanos)) {
                            shouldCompact = true;
                        }
                    }
                } catch (IOException e) {
                    // Ignore read errors
                }
            }
        } else {
            // Default DELETE_ON_CONSUME behavior
            if (consumerOffset <= 0)
                return;
            if (((double) consumerOffset / (double) producerOffset) >= options.compactionWasteThreshold) {
                shouldCompact = true;
            }
        }

        // Common time-based interval trigger
        if (!shouldCompact && options.compactionIntervalNanos > 0
                && (now - lastCompactionTimeNanos) >= options.compactionIntervalNanos && producerOffset > 0) {
            if (options.retentionPolicy == Options.RetentionPolicy.TIME_BASED || consumerOffset > 0) {
                shouldCompact = true;
            }
        }

        if (shouldCompact) {
            QueueState snap = currentState();
            compactionState = CompactionState.RUNNING;
            lastCompactionTimeNanos = now;
            compactionExecutor.submit(() -> runCompactionTask(snap));
        }
    }

    /**
     * Performs compaction by assembling a new log containing only the unconsumed
     * segment and then handing it
     * off for finalization. Progress is reported back to reconcile staging mode
     * with normal operation.
     *
     * @param snap a point-in-time snapshot used to drive compaction
     */
    private void runCompactionTask(QueueState snap) {
        try {
            Files.deleteIfExists(tempDataPath);
            try (RandomAccessFile tmpRaf = new RandomAccessFile(tempDataPath.toFile(), "rw");
                    FileChannel tmpCh = tmpRaf.getChannel()) {
                long copyStartOffset = 0;
                long copyEndOffset;

                lock.lock();
                try {
                    // We copy up to the current producer position to maximize work done outside the
                    // lock.
                    copyEndOffset = producerOffset;

                    if (options.retentionPolicy == Options.RetentionPolicy.DELETE_ON_CONSUME) {
                        // Start from consumer position
                        copyStartOffset = consumerOffset;
                    }
                } finally {
                    lock.unlock();
                }

                if (options.retentionPolicy == Options.RetentionPolicy.TIME_BASED) {
                    copyStartOffset = findTimeBasedCutoff(copyEndOffset);
                }

                if (copyEndOffset > copyStartOffset) {
                    copyRegion(dataChannel, copyStartOffset, copyEndOffset, tmpCh);
                }

                if (options.withFsync)
                    tmpCh.force(true);
                finalizeCompaction(copyStartOffset, copyEndOffset, tmpCh);
            }
        } catch (Throwable t) {
            lock.lock();
            try {
                compactionState = CompactionState.IDLE;
            } finally {
                lock.unlock();
            }
            try {
                Files.deleteIfExists(tempDataPath);
            } catch (IOException ignored) {
            }
        } finally {
            onCompactionFinished(compactionState == CompactionState.IDLE, null);
        }
    }

    private long findTimeBasedCutoff(long limitOffset) throws IOException {
        long offset = 0;
        long retentionMillis = TimeUnit.NANOSECONDS.toMillis(options.retentionTimeNanos);
        long now = System.currentTimeMillis();
        long cutoffTime = now - retentionMillis;

        while (offset < limitOffset) {
            try {
                NQueueRecordMetaData.HeaderPrefix pref = NQueueRecordMetaData.readPrefix(dataChannel, offset);
                NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(dataChannel, offset, pref.headerLen);

                // If records have timestamp 0 (legacy), they are treated as very old.
                // If we find a record strictly younger than cutoffTime (timestamp >=
                // cutoffTime),
                // this is the start of valid data.
                if (meta.getTimestamp() >= cutoffTime) {
                    return offset;
                }

                // Skip this record (it's expired)
                offset += NQueueRecordMetaData.fixedPrefixSize() + pref.headerLen + meta.getPayloadLen();
            } catch (EOFException e) {
                break;
            }
        }
        return offset; // All records expired or limit reached
    }

    /**
     * Finalizes a compaction by atomically replacing the active log with the
     * compacted version, recalculating
     * cursors to reflect additional data appended during the copy window, and
     * updating persisted metadata.
     * Upon completion, producers and consumers continue unaffected.
     *
     * @param copyStartOffset durable position where the background copy started
     * @param copyEndOffset   durable position where the background copy ended
     * @param tmpCh           channel containing the compacted content
     * @throws IOException if the replacement or state update cannot be completed
     */
    private void finalizeCompaction(long copyStartOffset, long copyEndOffset, FileChannel tmpCh) throws IOException {
        lock.lock();
        try {
            // 1. Copy any data that was appended while we were copying in background
            if (producerOffset > copyEndOffset) {
                copyRegion(dataChannel, copyEndOffset, producerOffset, tmpCh);
            }

            // 2. Calculate new offsets relative to the new file
            // The new file contains data starting from copyStartOffset.
            long newPO = tmpCh.position();
            long newCO = Math.max(0, consumerOffset - copyStartOffset);

            if (recordCount == 0) {
                newCO = 0;
                newPO = 0;
                tmpCh.truncate(0);
            }

            if (newCO > newPO)
                newCO = newPO;

            if (options.withFsync)
                tmpCh.force(true);
            tmpCh.close();

            // 3. Atomic swap
            try {
                Files.move(tempDataPath, dataPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tempDataPath, dataPath, StandardCopyOption.REPLACE_EXISTING);
            }

            RandomAccessFile nRaf = new RandomAccessFile(dataPath.toFile(), "rw");
            FileChannel nCh = nRaf.getChannel();
            try {
                dataChannel.close();
                raf.close();
            } catch (IOException ignored) {
            }

            raf = nRaf;
            dataChannel = nCh;
            consumerOffset = newCO;
            producerOffset = newPO;
            persistCurrentStateLocked();
        } finally {
            compactionState = CompactionState.IDLE;
            lock.unlock();
            triggerDrainIfNeeded();
        }
    }

    /**
     * Creates a lightweight snapshot of the queue’s current cursors and counters
     * for use in maintenance and
     * diagnostics.
     *
     * @return current state snapshot
     */
    private QueueState currentState() {
        return new QueueState(consumerOffset, producerOffset, recordCount, lastIndex);
    }

    /**
     * Indicates whether the queue is currently compacting or idle from a
     * maintenance perspective.
     */
    public enum CompactionState {
        IDLE, RUNNING
    }

    /**
     * Classe que representa um item pré-índiceado, fornecendo a combinação de um
     * elemento e seu índice.
     * Essa classe desempenha o papel crucial no processo de indexação prévia,
     * permitindo a identificação eficiente dos itens em uma coleção ordenada.
     * Ela se relaciona com outros componentes responsáveis pela manipulação da
     * coleção, como os métodos de inclusão e remoção de elementos.
     */
    private static record PreIndexedItem<T>(T item, long index) implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Centralized out-of-order delivery detector. Increments the outOfOrderCount
     * and notifies metrics if enabled.
     * Called whenever a record is delivered to a consumer (handoff or disk).
     */
    private void recordDeliveryIndex(long seq) {
        if (!orderDetectionEnabled)
            return;
        if (lastDeliveredIndex != -1 && seq <= lastDeliveredIndex) {
            if (seq != 0) {
                outOfOrderCount++;
                statsUtils.notifyHitCounter(NQueueMetrics.OUT_OF_ORDER);
            }
        }
        lastDeliveredIndex = seq;
    }

    /**
     * For debugging/diagnostics only. Returns the last delivered sequence index
     * observed by the internal
     * order detector, or -1 if no delivery was recorded.
     */
    public long getLastDeliveredIndex() {
        lock.lock();
        try {
            return lastDeliveredIndex;
        } finally {
            lock.unlock();
        }
    }

    /**
     * For debugging/diagnostics only. Returns how many out-of-order events the
     * internal detector recorded
     * during this queue instance lifetime.
     */
    public long getOutOfOrderCount() {
        lock.lock();
        try {
            return outOfOrderCount;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Wrapper for elements staged in the in-memory buffer, capturing arrival time
     * to aid operational
     * decisions and debugging.
     */
    private static class MemoryBufferEntry<T> {
        final T item;
        final long index;
        final long timestamp;

        /**
         * Captures an element destined for later durable append along with its arrival
         * timestamp.
         *
         * @param item element to stage
         */
        MemoryBufferEntry(T item, long index) {
            this.item = item;
            this.index = index;
            this.timestamp = System.nanoTime();
        }
    }

    /**
     * Immutable snapshot of queue cursors and counters used to coordinate
     * operations like compaction and
     * recovery.
     */
    private static class QueueState {
        final long consumerOffset, producerOffset, recordCount, lastIndex;

        /**
         * Builds a new snapshot capturing the given positions and counters.
         *
         * @param co consumer offset
         * @param po producer offset
         * @param rc record count
         * @param li last logical index
         */
        QueueState(long co, long po, long rc, long li) {
            this.consumerOffset = co;
            this.producerOffset = po;
            this.recordCount = rc;
            this.lastIndex = li;
        }
    }

    /**
     * Configuration for queue behavior including compaction policy, durability,
     * staging, and coordination
     * timing. Instances are mutable builders; use fluent setters to construct the
     * desired configuration.
     */
    public static final class Options {
        public enum RetentionPolicy {
            DELETE_ON_CONSUME,
            TIME_BASED
        }

        double compactionWasteThreshold = 0.5;
        long compactionIntervalNanos = TimeUnit.MINUTES.toNanos(5);
        int compactionBufferSize = 128 * 1024;
        boolean withFsync = true, enableMemoryBuffer = false, resetOnRestart = false, allowShortCircuit = true;
        boolean enableOrderDetection = false;
        int memoryBufferSize = 10000;
        long lockTryTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(10),
                revalidationIntervalNanos = TimeUnit.MILLISECONDS.toNanos(100);
        long maintenanceIntervalNanos = TimeUnit.SECONDS.toNanos(5);
        long maxSizeReconciliationIntervalNanos = TimeUnit.MINUTES.toNanos(1);
        RetentionPolicy retentionPolicy = RetentionPolicy.DELETE_ON_CONSUME;
        long retentionTimeNanos = 0;

        private Options() {
        }

        /**
         * Returns a fresh options instance with conservative defaults that favor safety
         * and predictable
         * maintenance behavior.
         *
         * @return new options instance with default values
         */
        public static Options defaults() {
            return new Options();
        }

        public Options withRetentionPolicy(RetentionPolicy policy) {
            Objects.requireNonNull(policy);
            this.retentionPolicy = policy;
            return this;
        }

        public Options withRetentionTime(Duration retention) {
            Objects.requireNonNull(retention);
            if (retention.isNegative())
                throw new IllegalArgumentException("negative");
            this.retentionTimeNanos = retention.toNanos();
            return this;
        }

        public Options copy() {
            Options copy = new Options();
            copy.compactionWasteThreshold = this.compactionWasteThreshold;
            copy.compactionIntervalNanos = this.compactionIntervalNanos;
            copy.compactionBufferSize = this.compactionBufferSize;
            copy.withFsync = this.withFsync;
            copy.enableMemoryBuffer = this.enableMemoryBuffer;
            copy.resetOnRestart = this.resetOnRestart;
            copy.allowShortCircuit = this.allowShortCircuit;
            copy.enableOrderDetection = this.enableOrderDetection;
            copy.memoryBufferSize = this.memoryBufferSize;
            copy.lockTryTimeoutNanos = this.lockTryTimeoutNanos;
            copy.revalidationIntervalNanos = this.revalidationIntervalNanos;
            copy.maintenanceIntervalNanos = this.maintenanceIntervalNanos;
            copy.maxSizeReconciliationIntervalNanos = this.maxSizeReconciliationIntervalNanos;
            copy.retentionPolicy = this.retentionPolicy;
            copy.retentionTimeNanos = this.retentionTimeNanos;
            return copy;
        }

        /**
         * Sets the fraction of consumed data that should trigger a compaction when
         * exceeded. Values close to
         * zero compact aggressively; values near one compact rarely.
         *
         * @param t threshold in the range [0.0, 1.0]
         * @return this builder for chaining
         */
        public Options withCompactionWasteThreshold(double t) {
            if (t < 0.0 || t > 1.0)
                throw new IllegalArgumentException("threshold [0.0, 1.0]");
            this.compactionWasteThreshold = t;
            return this;
        }

        /**
         * Sets a time-based trigger for compaction. When greater than zero, a
         * compaction may be scheduled
         * after at least this interval has elapsed since the last run and there is data
         * eligible to reclaim.
         *
         * @param i minimum interval between compactions
         * @return this builder for chaining
         */
        public Options withCompactionInterval(Duration i) {
            Objects.requireNonNull(i);
            if (i.isNegative())
                throw new IllegalArgumentException("negative");
            this.compactionIntervalNanos = i.toNanos();
            return this;
        }

        /**
         * Sets the internal buffer size used during maintenance activities. Larger
         * buffers may improve
         * throughput on some systems at the cost of memory.
         *
         * @param s positive buffer size in bytes
         * @return this builder for chaining
         */
        public Options withCompactionBufferSize(int s) {
            if (s <= 0)
                throw new IllegalArgumentException("positive");
            this.compactionBufferSize = s;
            return this;
        }

        /**
         * Controls whether to allow short-circuiting the persistence layer when
         * consumers are waiting.
         * When true (default), if the queue is empty and consumers are blocked waiting,
         * new elements
         * are handed off directly to consumers without hitting the disk.
         * When false, elements are always written to the queue log before delivery,
         * ensuring strict
         * persistence at the cost of latency.
         *
         * @param allow true to allow short-circuit (default), false to enforce
         *              persistence path
         * @return this builder for chaining
         */
        public Options withShortCircuit(boolean allow) {
            this.allowShortCircuit = allow;
            return this;
        }

        /**
         * Controls whether enqueues request synchronous flushing from the storage
         * layer. Enabling this
         * increases durability guarantees at the cost of throughput.
         *
         * @param f true to enable synchronous flush on critical operations
         * @return this builder for chaining
         */
        public Options withFsync(boolean f) {
            this.withFsync = f;
            return this;
        }

        /**
         * Enables or disables the in-memory staging buffer used to absorb bursts and
         * decouple producers from
         * maintenance phases.
         *
         * @param e true to enable staging
         * @return this builder for chaining
         */
        public Options withMemoryBuffer(boolean e) {
            this.enableMemoryBuffer = e;
            return this;
        }

        /**
         * Sets the maximum number of elements that may be staged in memory when the
         * staging buffer is enabled.
         *
         * @param s positive capacity in elements
         * @return this builder for chaining
         */
        public Options withMemoryBufferSize(int s) {
            if (s <= 0)
                throw new IllegalArgumentException("memoryBufferSize deve ser positivo");
            this.memoryBufferSize = s;
            return this;
        }

        /**
         * Enables or disables the internal out-of-order delivery detector.
         * When enabled, the queue will keep a lightweight monotonicity check of
         * delivered record indices and
         * increment the {@link NQueueMetrics#OUT_OF_ORDER} counter if a
         * regression/duplicate is observed.
         * Default is disabled to avoid any overhead in the hot path.
         *
         * @param e true to enable detection, false to disable (default)
         * @return this builder for chaining
         */
        public Options withOrderDetection(boolean e) {
            this.enableOrderDetection = e;
            return this;
        }

        /**
         * Sets the maximum time the queue will wait when attempting to acquire
         * coordination for draining or
         * maintenance before retrying. Useful to tune responsiveness under contention.
         *
         * @param t maximum wait duration when trying to acquire the lock
         * @return this builder for chaining
         */
        public Options withLockTryTimeout(Duration t) {
            Objects.requireNonNull(t);
            if (t.isNegative())
                throw new IllegalArgumentException("negative");
            this.lockTryTimeoutNanos = t.toNanos();
            return this;
        }

        /**
         * Sets the interval at which temporary staging mode should revalidate whether
         * to continue or revert
         * to direct durable appends.
         *
         * @param i time between revalidation checks
         * @return this builder for chaining
         */
        public Options withRevalidationInterval(Duration i) {
            Objects.requireNonNull(i);
            if (i.isNegative())
                throw new IllegalArgumentException("negative");
            this.revalidationIntervalNanos = i.toNanos();
            return this;
        }

        /**
         * Sets the interval at which the background maintenance task runs.
         * This task is responsible for tasks like opportunistic size reconciliation.
         * Default is 5 seconds.
         *
         * @param i interval between maintenance runs
         * @return this builder for chaining
         */
        public Options withMaintenanceInterval(Duration i) {
            Objects.requireNonNull(i);
            if (i.isNegative() || i.isZero())
                throw new IllegalArgumentException("positive");
            this.maintenanceIntervalNanos = i.toNanos();
            return this;
        }

        /**
         * Sets the maximum interval allowed without a precise size update. If this
         * interval elapses
         * without the maintenance task successfully acquiring a lock opportunistically,
         * the next
         * run will force a lock to ensure the size is reconciled.
         * <p>
         * If set to ZERO, forced locking is disabled, and size reconciliation will only
         * happen
         * when the lock is free (opportunistic only).
         * Default is 1 minute.
         *
         * @param d maximum interval between forced reconciliations, or ZERO to disable
         * @return this builder for chaining
         */
        public Options withMaxSizeReconciliationInterval(Duration d) {
            Objects.requireNonNull(d);
            if (d.isNegative())
                throw new IllegalArgumentException("negative");
            this.maxSizeReconciliationIntervalNanos = d.toNanos();
            return this;
        }

        /**
         * When enabled, disregards any previously persisted metadata at startup and
         * reconstructs state solely
         * from the durable log.
         *
         * @param r true to force a rebuild on restart
         * @return this builder for chaining
         */
        public Options withResetOnRestart(boolean r) {
            this.resetOnRestart = r;
            return this;
        }

        /**
         * Produces an immutable snapshot of the current options for distribution to
         * internal components.
         *
         * @return immutable view of the configured values
         */
        public Snapshot snapshot() {
            return new Snapshot(this);
        }

        /**
         * Immutable view of option values used by internal components to avoid
         * accidental mutation.
         */
        public static class Snapshot {
            final double compactionWasteThreshold;
            final long compactionIntervalNanos, lockTryTimeoutNanos, revalidationIntervalNanos,
                    maintenanceIntervalNanos, maxSizeReconciliationIntervalNanos;
            final int compactionBufferSize, memoryBufferSize;
            final boolean enableMemoryBuffer, allowShortCircuit;
            final boolean enableOrderDetection;
            final RetentionPolicy retentionPolicy;
            final long retentionTimeNanos;

            /**
             * Captures the current values from a mutable options builder for safe sharing.
             *
             * @param o source options builder
             */
            Snapshot(Options o) {
                this.compactionWasteThreshold = o.compactionWasteThreshold;
                this.compactionIntervalNanos = o.compactionIntervalNanos;
                this.compactionBufferSize = o.compactionBufferSize;
                this.enableMemoryBuffer = o.enableMemoryBuffer;
                this.memoryBufferSize = o.memoryBufferSize;
                this.lockTryTimeoutNanos = o.lockTryTimeoutNanos;
                this.revalidationIntervalNanos = o.revalidationIntervalNanos;
                this.maintenanceIntervalNanos = o.maintenanceIntervalNanos;
                this.maxSizeReconciliationIntervalNanos = o.maxSizeReconciliationIntervalNanos;
                this.allowShortCircuit = o.allowShortCircuit;
                this.enableOrderDetection = o.enableOrderDetection;
                this.retentionPolicy = o.retentionPolicy;
                this.retentionTimeNanos = o.retentionTimeNanos;
            }
        }
    }
}
