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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages the optional in-memory staging buffer for {@link NQueue}.
 * <p>
 * When enabled, this collaborator absorbs producer bursts during compaction or
 * brief contention windows. Staged elements are drained to durable storage in
 * FIFO order without interfering with the queue's main lock whenever possible.
 * <p>
 * This class is package-private and intended solely for use by {@link NQueue}.
 *
 * @param <T> the element type handled by the owning queue
 */
class MemoryStager<T extends Serializable> {

    private static final int MEMORY_DRAIN_BATCH_SIZE = 256;

    /**
     * Callback invoked by the drain worker to persist a batch to durable storage.
     */
    @FunctionalInterface
    interface FlushCallback<T extends Serializable> {
        /**
         * Persists a pre-indexed batch. Called while the queue lock is held.
         *
         * @param batch ordered batch ready for durable append
         * @throws IOException if persistence fails
         */
        void flush(List<NQueue.PreIndexedItem<T>> batch) throws IOException;
    }

    /** Callback invoked when the compaction engine reports it has finished. */
    @FunctionalInterface
    interface CompactionStateSupplier {
        boolean isCompactionRunning();
    }

    private final NQueue.Options options;
    private final FlushCallback<T> flushCallback;
    private final CompactionStateSupplier compactionStateSupplier;
    private final ReentrantLock queueLock;

    final BlockingQueue<NQueue.MemoryBufferEntry<T>> memoryBuffer;
    final BlockingDeque<NQueue.MemoryBufferEntry<T>> drainingQueue;
    final AtomicLong memoryBufferModeUntil = new AtomicLong(0);
    private final AtomicBoolean drainingInProgress = new AtomicBoolean(false);
    private final AtomicBoolean switchBackRequested = new AtomicBoolean(false);
    private final AtomicBoolean revalidationScheduled = new AtomicBoolean(false);
    private volatile CountDownLatch drainCompletionLatch;

    private final ExecutorService drainExecutor;
    private final ScheduledExecutorService revalidationExecutor;

    /**
     * Builds a new stager wired to the owning queue's lock and persistence
     * callback.
     *
     * @param options                 queue options (buffer size, timing parameters)
     * @param queueLock               the main queue lock – acquired when draining
     * @param flushCallback           called inside the lock to persist a drained
     *                                batch
     * @param compactionStateSupplier reports whether compaction is currently
     *                                running
     */
    MemoryStager(NQueue.Options options, ReentrantLock queueLock, FlushCallback<T> flushCallback,
            CompactionStateSupplier compactionStateSupplier) {
        this.options = options;
        this.queueLock = queueLock;
        this.flushCallback = flushCallback;
        this.compactionStateSupplier = compactionStateSupplier;
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
    }

    // ── Offering ────────────────────────────────────────────────────────────

    /**
     * Attempts to place a new element into the bounded in-memory staging buffer.
     * When the buffer is full and conditions allow, the queue may opportunistically
     * revert to a direct durable append for the current element. Producers block
     * only when the buffer is at capacity and no immediate fallback is possible.
     *
     * @param key              optional routing key
     * @param headers          record headers
     * @param item             pre-indexed item to stage
     * @param revalidateIfFull whether to reassess staging mode when saturated
     * @return a sentinel indicating deferred durability, or the result of a
     *         fallback direct append (delegated back via callback)
     * @throws IOException if a fallback durable append is attempted and fails
     */
    long offerToMemory(byte[] key, NQueueHeaders headers, NQueue.PreIndexedItem<T> item, boolean revalidateIfFull)
            throws IOException {
        try {
            if (revalidateIfFull && memoryBuffer.remainingCapacity() == 0) {
                revalidateMemoryMode();
                if (queueLock.tryLock()) {
                    try {
                        if (!compactionStateSupplier.isCompactionRunning()) {
                            drainSync();
                            // Signal caller to do direct offer
                            return Long.MIN_VALUE;
                        }
                    } finally {
                        queueLock.unlock();
                    }
                }
            }
            synchronized (this) {
                NQueue.MemoryBufferEntry<T> e = new NQueue.MemoryBufferEntry<>(item.item(), item.index(), key, headers);
                if (!memoryBuffer.offer(e))
                    memoryBuffer.put(e);
            }
            triggerDrainIfNeeded();
            return -1L;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException(ex);
        }
    }

    // ── Drain ───────────────────────────────────────────────────────────────

    /**
     * Initiates an asynchronous drain from the staging buffer into durable storage
     * when necessary. A single drain worker is kept active at a time to preserve
     * ordering.
     */
    void triggerDrainIfNeeded() {
        if (!memoryBuffer.isEmpty() && drainingInProgress.compareAndSet(false, true)) {
            drainCompletionLatch = new CountDownLatch(1);
            drainExecutor.submit(this::drainAsync);
        }
    }

    /**
     * Background worker that periodically acquires the necessary coordination to
     * flush staged elements into durable storage in batches.
     */
    private void drainAsync() {
        try {
            while (!memoryBuffer.isEmpty() || !drainingQueue.isEmpty()) {
                boolean ok = false;
                try {
                    if (queueLock.tryLock(options.lockTryTimeoutNanos, TimeUnit.NANOSECONDS)) {
                        try {
                            drainSync();
                            ok = true;
                        } finally {
                            queueLock.unlock();
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
            if (!memoryBuffer.isEmpty())
                triggerDrainIfNeeded();
        }
    }

    /**
     * Synchronously flushes staged elements into durable storage in FIFO order,
     * aggregating work into small batches. Must be called while the queue lock is
     * held. Failures result in staged entries being retained for a later retry.
     *
     * @throws IOException if persistence fails
     */
    void drainSync() throws IOException {
        while (true) {
            memoryBuffer.drainTo(drainingQueue, MEMORY_DRAIN_BATCH_SIZE);
            if (drainingQueue.isEmpty())
                break;
            List<NQueue.PreIndexedItem<T>> batch = new ArrayList<>();
            List<NQueue.MemoryBufferEntry<T>> entries = new ArrayList<>();
            for (int i = 0; i < MEMORY_DRAIN_BATCH_SIZE; i++) {
                NQueue.MemoryBufferEntry<T> ent = drainingQueue.poll();
                if (ent == null)
                    break;
                batch.add(new NQueue.PreIndexedItem<>(ent.item, ent.index, ent.key, ent.headers));
                entries.add(ent);
            }
            if (batch.isEmpty())
                break;
            try {
                flushCallback.flush(batch);
            } catch (IOException ex) {
                for (int i = entries.size() - 1; i >= 0; i--)
                    drainingQueue.addFirst(entries.get(i));
                throw ex;
            }
        }
    }

    /**
     * Checks for staged work and performs a synchronous drain when present,
     * indicating whether durable records became available.
     * Must be called while the queue lock is held.
     *
     * @param recordCountSupplier supplies the current durable record count
     *                            (evaluated <b>after</b> drain completes)
     * @return true if at least one durable record is now available
     * @throws IOException if draining encounters a storage error
     */
    boolean checkAndDrain(java.util.function.LongSupplier recordCountSupplier) throws IOException {
        if (!memoryBuffer.isEmpty() || !drainingQueue.isEmpty()) {
            drainSync();
            return recordCountSupplier.getAsLong() > 0;
        }
        return false;
    }

    // ── Memory Mode ─────────────────────────────────────────────────────────

    /**
     * Activates a temporary staging mode that routes new enqueues through the
     * in-memory buffer for a short interval.
     */
    void activateMemoryMode() {
        long until = System.nanoTime() + options.revalidationIntervalNanos;
        memoryBufferModeUntil.updateAndGet(c -> Math.max(c, until));
        scheduleRevalidation();
    }

    /**
     * Returns whether the stager is currently in active memory-buffer mode.
     */
    boolean isMemoryModeActive() {
        return System.nanoTime() < memoryBufferModeUntil.get();
    }

    /**
     * Clears the memory-buffer mode flag unconditionally. Used when compaction
     * finishes and drain is complete.
     */
    void deactivateMemoryMode() {
        memoryBufferModeUntil.set(0);
    }

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

    private void revalidateMemoryMode() {
        if (System.nanoTime() >= memoryBufferModeUntil.get()) {
            if (queueLock.tryLock()) {
                try {
                    if (!compactionStateSupplier.isCompactionRunning()
                            && memoryBuffer.isEmpty() && drainingQueue.isEmpty()) {
                        memoryBufferModeUntil.set(0);
                        return;
                    }
                } finally {
                    queueLock.unlock();
                }
            }
            activateMemoryMode();
        }
    }

    /**
     * Callback invoked after a compaction attempt finishes. Reconciles operation
     * mode and schedules a revalidation step when necessary.
     *
     * @param compactionRunning whether compaction is still in progress
     */
    void onCompactionFinished(boolean compactionRunning) {
        boolean acquired = queueLock.tryLock();
        if (acquired) {
            try {
                if (!compactionRunning && memoryBuffer.isEmpty() && drainingQueue.isEmpty()) {
                    memoryBufferModeUntil.set(0);
                    return;
                }
            } finally {
                queueLock.unlock();
            }
        }
        if (!switchBackRequested.compareAndSet(false, true))
            return;
        revalidationExecutor.execute(() -> {
            try {
                if (queueLock.tryLock()) {
                    try {
                        if (!compactionStateSupplier.isCompactionRunning() && memoryBuffer.isEmpty()
                                && drainingQueue.isEmpty()) {
                            memoryBufferModeUntil.set(0);
                            return;
                        }
                    } finally {
                        queueLock.unlock();
                    }
                }
                try {
                    if (queueLock.tryLock(options.lockTryTimeoutNanos, TimeUnit.NANOSECONDS)) {
                        try {
                            drainSync();
                        } finally {
                            queueLock.unlock();
                        }
                    }
                } catch (Exception ignored) {
                }
                if (queueLock.tryLock()) {
                    try {
                        if (memoryBuffer.isEmpty() && drainingQueue.isEmpty()
                                && !compactionStateSupplier.isCompactionRunning()) {
                            memoryBufferModeUntil.set(0);
                        } else {
                            activateMemoryMode();
                            triggerDrainIfNeeded();
                        }
                    } finally {
                        queueLock.unlock();
                    }
                } else {
                    activateMemoryMode();
                }
            } finally {
                switchBackRequested.set(false);
            }
        });
    }

    // ── Inspection ──────────────────────────────────────────────────────────

    /**
     * Returns the combined size of the staging and draining buffers.
     */
    int size() {
        return memoryBuffer.size() + drainingQueue.size();
    }

    /**
     * Returns true if both staging and draining buffers are empty.
     */
    boolean isEmpty() {
        return memoryBuffer.isEmpty() && drainingQueue.isEmpty();
    }

    /**
     * Returns the item at the head of the draining queue, or null if absent.
     * The draining queue holds older items than the staging buffer.
     */
    T peekFirst() {
        NQueue.MemoryBufferEntry<T> from = drainingQueue.peekFirst();
        if (from != null)
            return from.item;
        NQueue.MemoryBufferEntry<T> fromMem = memoryBuffer.peek();
        return fromMem != null ? fromMem.item : null;
    }

    // ── Lifecycle ───────────────────────────────────────────────────────────

    /**
     * Waits briefly for an in-progress drain to complete, if any.
     */
    void awaitDrainCompletion() {
        if (!drainingInProgress.get() && drainingQueue.isEmpty())
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
     * Shuts down the drain and revalidation executors.
     */
    void shutdown() {
        drainExecutor.shutdownNow();
        revalidationExecutor.shutdownNow();
    }
}
