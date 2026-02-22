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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
 * - Delivery semantics: Dequeue is at-most-once from the queue's perspective.
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
 * application's classpath on
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
    /**
     * Sentinel offset returned when an element is handed off directly to a waiting
     * consumer, bypassing persistence and staging buffers.
     */
    public static final long OFFSET_HANDOFF = -2;

    private final StatsUtils statsUtils = new StatsUtils();
    private final Path dataPath, metaPath, tempDataPath;
    private volatile RandomAccessFile raf;
    private volatile FileChannel dataChannel;
    private volatile RandomAccessFile metaRaf;
    private volatile FileChannel metaChannel;
    private final ReentrantLock lock;
    private final Condition notEmpty;
    private final Object metaWriteLock = new Object();
    private final Options options;
    private final AtomicLong approximateSize = new AtomicLong(0);
    private final AtomicLong globalSequence;

    private long consumerOffset, producerOffset, recordCount, lastIndex;
    private long lastDeliveredIndex = -1;
    private long outOfOrderCount = 0;
    private final boolean orderDetectionEnabled;
    private volatile boolean closed, shutdownRequested;
    private PreIndexedItem<T> handoffItem;

    // ── Collaborators ────────────────────────────────────────────────────────
    private final boolean enableMemoryBuffer;
    private MemoryStager<T> stager;
    private CompactionEngine compactionEngine;

    private final ScheduledExecutorService maintenanceExecutor;
    private volatile long lastSizeReconciliationTimeNanos = System.nanoTime();

    /**
     * Constructs a queue instance bound to the supplied directory and I/O handles,
     * restoring the persisted state and applying the chosen options.
     */
    private NQueue(Path queueDir, RandomAccessFile raf, FileChannel dataChannel, QueueState state, Options options)
            throws IOException {

        this.dataPath = queueDir.resolve(DATA_FILE);
        this.metaPath = queueDir.resolve(META_FILE);
        this.tempDataPath = queueDir.resolve(DATA_FILE + ".compacting");
        this.raf = raf;
        this.dataChannel = dataChannel;

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

        // Wire MemoryStager primeiro (se habilitado)
        if (enableMemoryBuffer) {
            this.stager = new MemoryStager<>(options, this.lock,
                    batch -> offerBatchLocked(batch, options.withFsync),
                    () -> this.compactionEngine != null
                            && this.compactionEngine.getState() == CompactionState.RUNNING);
        } else {
            this.stager = null;
        }

        // Wire CompactionEngine — a ref ao stager já existe no heap
        this.compactionEngine = new CompactionEngine(
                options,
                this.dataPath,
                this.tempDataPath,
                this.lock,
                this::currentState,
                this::applyCompactionResult,
                compactionRunning -> {
                    if (this.stager != null) {
                        this.stager.onCompactionFinished(compactionRunning);
                    }
                });

        this.maintenanceExecutor = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "nqueue-maintenance-worker");
            t.setDaemon(true);
            return t;
        });
        this.maintenanceExecutor.scheduleWithFixedDelay(this::reconcileSize, options.maintenanceIntervalNanos,
                options.maintenanceIntervalNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Called inside the queue lock when compaction finalizes successfully.
     * Adopts the new file handles and updates cursor state.
     */
    private void applyCompactionResult(CompactionEngine.CompactionResult result) throws IOException {
        try {
            this.dataChannel.close();
            this.raf.close();
        } catch (IOException ignored) {
        }
        this.raf = result.newRaf();
        this.dataChannel = result.newChannel();
        this.consumerOffset = result.newConsumerOffset();
        this.producerOffset = result.newProducerOffset();
        persistCurrentStateLocked();
        if (stager != null) {
            stager.triggerDrainIfNeeded();
        }
    }

    /**
     * Reconciles the optimistic approximate size with the exact size.
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
                    if (enableMemoryBuffer && stager != null) {
                        exactSize += stager.size();
                    }
                    approximateSize.set(exactSize);
                    lastSizeReconciliationTimeNanos = System.nanoTime();
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception e) {
            // best-effort maintenance
        }
    }

    // ── Factory Methods ──────────────────────────────────────────────────────

    /**
     * Opens or creates a named queue under the given base directory using default
     * options.
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
     * options.
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

    // ── Public API ───────────────────────────────────────────────────────────

    /**
     * Enqueues a record without a key or headers.
     *
     * @param object element to append
     * @return logical offset of the appended record, or {@link #OFFSET_HANDOFF}
     * @throws IOException if persistence fails
     */
    public long offer(T object) throws IOException {
        return offer(null, NQueueHeaders.empty(), object);
    }

    /**
     * Enqueues a record with a routing key but no custom headers.
     *
     * @param key    optional routing/partitioning key ({@code null} = absent)
     * @param object element to append
     * @return logical offset of the appended record, or {@link #OFFSET_HANDOFF}
     * @throws IOException if persistence fails
     */
    public long offer(byte[] key, T object) throws IOException {
        return offer(key, NQueueHeaders.empty(), object);
    }

    /**
     * Enqueues a record with a routing key and custom headers.
     * <p>
     * Depending on configuration and current conditions, the record may be staged
     * in memory and durably appended later, or appended directly to the durable
     * log.
     *
     * @param key     optional routing/partitioning key ({@code null} = absent)
     * @param headers record headers; use {@link NQueueHeaders#empty()} when none
     * @param object  element to append; must be non-null and serializable
     * @return logical offset of the appended record, or {@link #OFFSET_HANDOFF}
     *         when handed off directly to a waiting consumer
     * @throws IOException if the enqueue cannot be acknowledged due to storage
     *                     errors
     */
    public long offer(byte[] key, NQueueHeaders headers, T object) throws IOException {
        Objects.requireNonNull(object);
        Objects.requireNonNull(headers);

        if (enableMemoryBuffer) {
            if (stager.isMemoryModeActive())
                return offerViaStager(key, headers, object, true);
            if (lock.tryLock()) {
                try {
                    if (key == null && options.allowShortCircuit && recordCount == 0 && handoffItem == null
                            && stager.isEmpty() && lock.hasWaiters(notEmpty)) {
                        long seq = globalSequence.incrementAndGet();
                        PreIndexedItem<T> pItem = new PreIndexedItem<>(object, seq, null, NQueueHeaders.empty());
                        handoffItem = pItem;
                        notEmpty.signal();
                        statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);
                        return OFFSET_HANDOFF;
                    }
                    if (compactionEngine.getState() == CompactionState.RUNNING) {
                        stager.activateMemoryMode();
                        return offerViaStager(key, headers, object, false);
                    }
                    stager.drainSync();
                    long offset = offerDirectLocked(key, headers, object);
                    stager.triggerDrainIfNeeded();
                    return offset;
                } finally {
                    lock.unlock();
                }
            } else {
                stager.activateMemoryMode();
                return offerViaStager(key, headers, object, false);
            }
        } else {
            lock.lock();
            try {
                if (key == null && options.allowShortCircuit && recordCount == 0 && handoffItem == null
                        && lock.hasWaiters(notEmpty)) {
                    long seq = globalSequence.incrementAndGet();
                    PreIndexedItem<T> pItem = new PreIndexedItem<>(object, seq, null, NQueueHeaders.empty());
                    handoffItem = pItem;
                    notEmpty.signal();
                    statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);
                    return OFFSET_HANDOFF;
                }
                return offerDirectLocked(key, headers, object);
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Routes an offer through the MemoryStager. When the stager returns a fallback
     * sentinel ({@code Long.MIN_VALUE}), we complete the direct offer inside this
     * method while still holding the stager's pre-check invariants.
     */
    private long offerViaStager(byte[] key, NQueueHeaders headers, T object, boolean revalidateIfFull)
            throws IOException {
        long seq = globalSequence.incrementAndGet();
        PreIndexedItem<T> pItem = new PreIndexedItem<>(object, seq, key, headers);
        long result = stager.offerToMemory(key, headers, pItem, revalidateIfFull);
        if (result == Long.MIN_VALUE) {
            // Stager signalled: drain complete, fall through to direct offer
            lock.lock();
            try {
                return offerDirectLocked(key, headers, object);
            } finally {
                lock.unlock();
            }
        }
        return result;
    }

    /**
     * Retrieves and removes the head of the queue, blocking until an element
     * becomes available.
     *
     * @return the next available element
     * @throws IOException if the queue cannot read or persist consumption progress
     */
    public Optional<T> poll() throws IOException {
        lock.lock();
        try {
            if (recordCount == 0 && handoffItem == null && enableMemoryBuffer) {
                stager.drainSync();
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
     * if necessary.
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
            if (recordCount == 0 && handoffItem == null && enableMemoryBuffer) {
                stager.drainSync();
            }

            if (handoffItem != null) {
                long idx = handoffItem.index();
                recordDeliveryIndex(idx);
                T item = handoffItem.item();
                handoffItem = null;
                return Optional.of(item);
            }

            while (recordCount == 0) {
                if (enableMemoryBuffer && stager.checkAndDrain(recordCount))
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
                    if (enableMemoryBuffer && stager.checkAndDrain(recordCount))
                        break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (enableMemoryBuffer)
                        stager.checkAndDrain(recordCount);
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
     *
     * @return the current head element or empty if the queue is logically empty
     * @throws IOException if the queue cannot access the stored data
     */
    public Optional<T> peek() throws IOException {
        lock.lock();
        try {
            if (handoffItem != null)
                return Optional.of(handoffItem.item());

            if (recordCount > 0)
                return readAtInternal(consumerOffset).map(res -> safeDeserialize(res.getRecord()));

            if (enableMemoryBuffer) {
                T first = stager.peekFirst();
                if (first != null)
                    return Optional.of(first);
            }

            return Optional.empty();
        } finally {
            statsUtils.notifyHitCounter(NQueueMetrics.PEEK_EVENT);
            lock.unlock();
        }
    }

    /**
     * Returns the current logical size of the queue using strong accounting.
     *
     * @return number of elements that would be observed across all sources
     */
    public long size() {
        lock.lock();
        try {
            long s = recordCount;
            if (handoffItem != null)
                s++;
            if (enableMemoryBuffer && stager != null)
                s += stager.size();
            return s;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the queue size, optionally using a fast, approximate counter.
     *
     * @param optimistic when true, returns a fast approximation
     * @return approximate or precise size depending on the chosen mode
     */
    public long size(boolean optimistic) {
        return optimistic ? approximateSize.get() : size();
    }

    /**
     * Indicates whether the queue is currently empty.
     *
     * @return true if no records are pending delivery; false otherwise
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns the count of durable records strictly within the persistent log
     * segment awaiting consumption.
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

            while (currentIndex < startIndex && offset < producerOffset) {
                Optional<NQueueReadResult> result = readAtInternal(offset);
                if (result.isEmpty())
                    break;
                offset = result.get().getNextOffset();
                currentIndex++;
            }

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
     * Shuts down the queue gracefully, flushing staged records and closing all
     * resources.
     *
     * @throws IOException if underlying channels cannot be closed cleanly
     */
    public void close() throws IOException {
        maintenanceExecutor.shutdownNow();
        statsUtils.shutdown();
        if (enableMemoryBuffer && stager != null) {
            lock.lock();
            try {
                stager.drainSync();
            } catch (IOException ignored) {
            } finally {
                lock.unlock();
            }
        }
        lock.lock();
        try {
            shutdownRequested = true;
            compactionEngine.scheduleShutdownCompaction(consumerOffset);
        } finally {
            lock.unlock();
        }
        compactionEngine.shutdown(10);
        if (enableMemoryBuffer && stager != null) {
            stager.shutdown();
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
     * Reads, without advancing the consumption cursor, the record stored at the
     * provided durable offset.
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
     * provided durable offset with structured metadata.
     *
     * @param offset durable position to inspect
     * @return a structured result with the record and the next offset, or empty
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
     *
     * @param index logical index to find
     * @return a structured result with the record and the next offset, or empty
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
     * the head record.
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

    // ── Internal Write Path ──────────────────────────────────────────────────

    private long offerDirectLocked(byte[] key, NQueueHeaders headers, T object) throws IOException {
        long seq = globalSequence.incrementAndGet();
        PreIndexedItem<T> pItem = new PreIndexedItem<>(object, seq, key, headers);
        return offerBatchLocked(List.of(pItem), true);
    }

    /**
     * Appends a batch of elements directly to durable storage.
     * Must be called while the queue lock is held.
     */
    long offerBatchLocked(List<PreIndexedItem<T>> items, boolean fsync) throws IOException {
        if (items.isEmpty())
            return -1;
        long writePos = producerOffset, firstStart = -1, initialCount = recordCount;
        for (PreIndexedItem<T> pItem : items) {
            T obj = pItem.item();
            byte[] payload = toBytes(obj);
            long idx = pItem.index();
            lastIndex = idx;

            NQueueRecordMetaData meta = new NQueueRecordMetaData(
                    idx,
                    System.currentTimeMillis(),
                    pItem.key(),
                    pItem.headers(),
                    payload.length,
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
        compactionEngine.maybeCompact(consumerOffset, producerOffset, recordCount, shutdownRequested);
        notEmpty.signalAll();
        statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);
        return firstStart;
    }

    // ── Internal Read Path ───────────────────────────────────────────────────

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
                compactionEngine.maybeCompact(consumerOffset, producerOffset, recordCount, shutdownRequested);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return res.getRecord();
        });
    }

    private void persistCurrentStateLocked() throws IOException {
        synchronized (metaWriteLock) {
            NQueueQueueMeta.update(metaChannel, consumerOffset, producerOffset, recordCount, lastIndex,
                    options.withFsync);
        }
    }

    // ── Recovery ─────────────────────────────────────────────────────────────

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

    // ── Serialization Helpers ─────────────────────────────────────────────────

    private byte[] toBytes(T obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos))) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        }
    }

    @SuppressWarnings("unchecked")
    private T safeDeserialize(NQueueRecord record) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(record.payload());
                ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (T) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ── Order Detection ───────────────────────────────────────────────────────

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
     * For debugging/diagnostics only. Returns the last delivered sequence index.
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
     * For debugging/diagnostics only. Returns how many out-of-order events were
     * recorded during this queue instance lifetime.
     */
    public long getOutOfOrderCount() {
        lock.lock();
        try {
            return outOfOrderCount;
        } finally {
            lock.unlock();
        }
    }

    // ── State Snapshot ────────────────────────────────────────────────────────

    private QueueState currentState() {
        return new QueueState(consumerOffset, producerOffset, recordCount, lastIndex);
    }

    // ── Testability Accessors (package-private) ───────────────────────────────

    /**
     * For testing only. Returns the in-memory staging buffer used by
     * {@link MemoryStager},
     * or null when the memory buffer is disabled.
     */
    java.util.concurrent.BlockingQueue<MemoryBufferEntry<T>> testGetMemoryBuffer() {
        return stager != null ? stager.memoryBuffer : null;
    }

    /**
     * For testing only. Returns the memory-buffer-mode expiry timestamp
     * from {@link MemoryStager}.
     */
    java.util.concurrent.atomic.AtomicLong testGetMemoryBufferModeUntil() {
        return stager != null ? stager.memoryBufferModeUntil : null;
    }

    /**
     * For testing only. Returns the current compaction state from the engine.
     */
    CompactionState testGetCompactionState() {
        return compactionEngine.state;
    }

    /**
     * For testing only. Directly sets compaction state on the engine.
     */
    void testSetCompactionState(CompactionState st) {
        compactionEngine.state = st;
    }

    /**
     * For testing only. Simulates a compaction finishing: sets IDLE state and
     * notifies the stager — reproducing the exact flow of the real finished
     * callback.
     */
    void testNotifyCompactionFinished() {
        compactionEngine.state = CompactionState.IDLE;
        if (stager != null) {
            stager.onCompactionFinished(true);
        }
    }

    /**
     * For testing only. Overrides memory buffer mode expiry on the stager.
     */
    void testActivateMemoryMode(long untilNanos) {
        if (stager != null)
            stager.memoryBufferModeUntil.set(untilNanos);
    }

    /**
     * For testing only. Waits for the stager's drain to complete.
     */
    void testAwaitDrainCompletion() {
        if (stager != null)
            stager.awaitDrainCompletion();
    }

    // ── Shared Inner Types ────────────────────────────────────────────────────

    /**
     * Indicates whether the queue is currently compacting or idle.
     */
    public enum CompactionState {
        IDLE, RUNNING
    }

    /**
     * Pre-indexed item combining an element with its sequence assignment.
     * Package-private to be shared with {@link MemoryStager} and
     * {@link CompactionEngine}.
     */
    static record PreIndexedItem<T>(T item, long index, byte[] key, NQueueHeaders headers)
            implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Wrapper for elements staged in the in-memory buffer.
     * Package-private to be shared with {@link MemoryStager}.
     */
    static class MemoryBufferEntry<T> {
        final T item;
        final long index;
        final long timestamp;
        final byte[] key;
        final NQueueHeaders headers;

        MemoryBufferEntry(T item, long index, byte[] key, NQueueHeaders headers) {
            this.item = item;
            this.index = index;
            this.timestamp = System.nanoTime();
            this.key = key;
            this.headers = headers != null ? headers : NQueueHeaders.empty();
        }
    }

    /**
     * Immutable snapshot of queue cursors used to coordinate compaction and
     * recovery.
     */
    static class QueueState {
        final long consumerOffset, producerOffset, recordCount, lastIndex;

        QueueState(long co, long po, long rc, long li) {
            this.consumerOffset = co;
            this.producerOffset = po;
            this.recordCount = rc;
            this.lastIndex = li;
        }
    }

    /**
     * Configuration for queue behavior including compaction policy, durability,
     * staging, and coordination timing.
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
         * Returns a fresh options instance with conservative defaults.
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

        public Options withCompactionWasteThreshold(double t) {
            if (t < 0.0 || t > 1.0)
                throw new IllegalArgumentException("threshold [0.0, 1.0]");
            this.compactionWasteThreshold = t;
            return this;
        }

        public Options withCompactionInterval(Duration i) {
            Objects.requireNonNull(i);
            if (i.isNegative())
                throw new IllegalArgumentException("negative");
            this.compactionIntervalNanos = i.toNanos();
            return this;
        }

        public Options withCompactionBufferSize(int s) {
            if (s <= 0)
                throw new IllegalArgumentException("positive");
            this.compactionBufferSize = s;
            return this;
        }

        public Options withShortCircuit(boolean allow) {
            this.allowShortCircuit = allow;
            return this;
        }

        public Options withFsync(boolean f) {
            this.withFsync = f;
            return this;
        }

        public Options withMemoryBuffer(boolean e) {
            this.enableMemoryBuffer = e;
            return this;
        }

        public Options withMemoryBufferSize(int s) {
            if (s <= 0)
                throw new IllegalArgumentException("memoryBufferSize deve ser positivo");
            this.memoryBufferSize = s;
            return this;
        }

        public Options withOrderDetection(boolean e) {
            this.enableOrderDetection = e;
            return this;
        }

        public Options withLockTryTimeout(Duration t) {
            Objects.requireNonNull(t);
            if (t.isNegative())
                throw new IllegalArgumentException("negative");
            this.lockTryTimeoutNanos = t.toNanos();
            return this;
        }

        public Options withRevalidationInterval(Duration i) {
            Objects.requireNonNull(i);
            if (i.isNegative())
                throw new IllegalArgumentException("negative");
            this.revalidationIntervalNanos = i.toNanos();
            return this;
        }

        public Options withMaintenanceInterval(Duration i) {
            Objects.requireNonNull(i);
            if (i.isNegative() || i.isZero())
                throw new IllegalArgumentException("positive");
            this.maintenanceIntervalNanos = i.toNanos();
            return this;
        }

        public Options withMaxSizeReconciliationInterval(Duration d) {
            Objects.requireNonNull(d);
            if (d.isNegative())
                throw new IllegalArgumentException("negative");
            this.maxSizeReconciliationIntervalNanos = d.toNanos();
            return this;
        }

        public Options withResetOnRestart(boolean r) {
            this.resetOnRestart = r;
            return this;
        }

        /**
         * Produces an immutable snapshot of the current options.
         *
         * @return immutable view of the configured values
         */
        public Snapshot snapshot() {
            return new Snapshot(this);
        }

        /**
         * Immutable view of option values used by internal components.
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
