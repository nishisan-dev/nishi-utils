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
 * NQueue implements a file-based persistent queue with support for concurrent producers and consumers.
 */
public class NQueue<T extends Serializable> implements Closeable {
    private static final String DATA_FILE = "data.log";
    private static final String META_FILE = "queue.meta";
    private static final int MEMORY_DRAIN_BATCH_SIZE = 256;

    private final StatsUtils statsUtils = new StatsUtils();
    private final Path queueDir, dataPath, metaPath, tempDataPath;
    private volatile RandomAccessFile raf;
    private volatile FileChannel dataChannel;
    private final ReentrantLock lock;
    private final Condition notEmpty;
    private final Object metaWriteLock = new Object();
    private final Options options;
    private final AtomicLong approximateSize = new AtomicLong(0);

    private long consumerOffset, producerOffset, recordCount, lastIndex, lastCompactionTimeNanos;
    private volatile boolean closed, shutdownRequested;
    private volatile CompactionState compactionState = CompactionState.IDLE;

    private final boolean enableMemoryBuffer;
    private final BlockingQueue<MemoryBufferEntry<T>> memoryBuffer;
    private final BlockingDeque<MemoryBufferEntry<T>> drainingQueue;
    private final AtomicLong memoryBufferModeUntil = new AtomicLong(0);
    private final AtomicBoolean drainingInProgress = new AtomicBoolean(false), switchBackRequested = new AtomicBoolean(false), revalidationScheduled = new AtomicBoolean(false);
    private volatile CountDownLatch drainCompletionLatch;
    
    private final ExecutorService drainExecutor, compactionExecutor;
    private final ScheduledExecutorService revalidationExecutor;

    private NQueue(Path queueDir, RandomAccessFile raf, FileChannel dataChannel, QueueState state, Options options) {
        this.queueDir = queueDir; this.dataPath = queueDir.resolve(DATA_FILE); this.metaPath = queueDir.resolve(META_FILE); this.tempDataPath = queueDir.resolve(DATA_FILE + ".compacting");
        this.raf = raf; this.dataChannel = dataChannel; this.lock = new ReentrantLock(); this.notEmpty = this.lock.newCondition();
        this.consumerOffset = state.consumerOffset; this.producerOffset = state.producerOffset; this.recordCount = state.recordCount; this.lastIndex = state.lastIndex; this.approximateSize.set(state.recordCount);
        this.options = options; this.enableMemoryBuffer = options.enableMemoryBuffer; this.lastCompactionTimeNanos = System.nanoTime();
        if (enableMemoryBuffer) {
            this.memoryBuffer = new LinkedBlockingQueue<>(options.memoryBufferSize);
            this.drainingQueue = new LinkedBlockingDeque<>(options.memoryBufferSize);
            this.drainExecutor = Executors.newSingleThreadExecutor(r -> { Thread t = new Thread(r, "nqueue-drain-worker"); t.setDaemon(true); return t; });
            this.revalidationExecutor = new ScheduledThreadPoolExecutor(1, r -> { Thread t = new Thread(r, "nqueue-revalidation-worker"); t.setDaemon(true); return t; });
        } else { this.memoryBuffer = null; this.drainingQueue = null; this.drainExecutor = null; this.revalidationExecutor = null; }
        this.compactionExecutor = Executors.newSingleThreadExecutor(r -> { Thread t = new Thread(r, "nqueue-compaction-worker"); t.setDaemon(true); return t; });
    }

    public static <T extends Serializable> NQueue<T> open(Path baseDir, String queueName) throws IOException { return open(baseDir, queueName, Options.defaults()); }
    public static <T extends Serializable> NQueue<T> open(Path baseDir, String queueName, Options options) throws IOException {
        Objects.requireNonNull(baseDir); Objects.requireNonNull(queueName); Objects.requireNonNull(options);
        Path qDir = baseDir.resolve(queueName); Files.createDirectories(qDir);
        RandomAccessFile raf = new RandomAccessFile(qDir.resolve(DATA_FILE).toFile(), "rw"); FileChannel ch = raf.getChannel();
        QueueState state = loadOrRebuildState(ch, qDir.resolve(META_FILE), options);
        if (ch.size() > state.producerOffset) { ch.truncate(state.producerOffset); ch.force(options.withFsync); }
        return new NQueue<>(qDir, raf, ch, state, options);
    }

    public long offer(T object) throws IOException {
        Objects.requireNonNull(object);
        if (enableMemoryBuffer) {
            if (System.nanoTime() < memoryBufferModeUntil.get()) return offerToMemory(object, true);
            if (lock.tryLock()) {
                try {
                    if (compactionState == CompactionState.RUNNING) { activateMemoryMode(); return offerToMemory(object, false); }
                    drainMemoryBufferSync(); long offset = offerDirectLocked(object); triggerDrainIfNeeded(); return offset;
                } finally { lock.unlock(); }
            } else { activateMemoryMode(); return offerToMemory(object, false); }
        } else { lock.lock(); try { return offerDirectLocked(object); } finally { lock.unlock(); } }
    }

    public Optional<T> poll() throws IOException {
        lock.lock(); try {
            drainMemoryBufferSync(); while (recordCount == 0) notEmpty.awaitUninterruptibly();
            return consumeNextRecordLocked().map(this::safeDeserialize);
        } finally { statsUtils.notifyHitCounter(NQueueMetrics.POLL_EVENT); lock.unlock(); }
    }

    public Optional<T> poll(long timeout, TimeUnit unit) throws IOException {
        long nanos = unit.toNanos(timeout);
        lock.lock(); try {
            drainMemoryBufferSync();
            while (recordCount == 0) {
                if (checkAndDrainMemorySync()) break;
                if (nanos <= 0L) return Optional.empty();
                try { nanos = notEmpty.awaitNanos(nanos); if (checkAndDrainMemorySync()) break; }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); checkAndDrainMemorySync(); return Optional.empty(); }
            }
            return consumeNextRecordLocked().map(this::safeDeserialize);
        } finally { statsUtils.notifyHitCounter(NQueueMetrics.POLL_EVENT); lock.unlock(); }
    }

    public Optional<T> peek() throws IOException {
        lock.lock(); try { if (recordCount == 0) return Optional.empty(); return readAtInternal(consumerOffset).map(res -> safeDeserialize(res.getRecord())); }
        finally { statsUtils.notifyHitCounter(NQueueMetrics.PEEK_EVENT); lock.unlock(); }
    }

    public long size() { lock.lock(); try { long s = recordCount; if (enableMemoryBuffer) s += (long) memoryBuffer.size() + drainingQueue.size(); return s; } finally { lock.unlock(); } }
    public long size(boolean optimistic) { return optimistic ? approximateSize.get() : size(); }
    public boolean isEmpty() { return size() == 0; }
    public long getRecordCount() { lock.lock(); try { return recordCount; } finally { lock.unlock(); } }

    @Override
    public void close() throws IOException {
        if (enableMemoryBuffer) { lock.lock(); try { drainMemoryBufferSync(); } catch (IOException ignored) {} finally { lock.unlock(); } }
        lock.lock(); try {
            shutdownRequested = true;
            if (compactionState == CompactionState.IDLE && consumerOffset > 0) {
                QueueState snap = currentState(); compactionState = CompactionState.RUNNING;
                compactionExecutor.submit(() -> runCompactionTask(snap));
            }
        } finally { lock.unlock(); }
        compactionExecutor.shutdown(); try { compactionExecutor.awaitTermination(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        if (enableMemoryBuffer) { drainExecutor.shutdownNow(); revalidationExecutor.shutdownNow(); }
        lock.lock(); try { if (dataChannel.isOpen()) dataChannel.close(); if (raf != null) raf.close(); closed = true; } finally { lock.unlock(); }
    }

    private void awaitDrainCompletion() {
        if (!enableMemoryBuffer) return;
        if (!drainingInProgress.get() && (drainingQueue == null || drainingQueue.isEmpty())) return;
        CountDownLatch latch = drainCompletionLatch;
        if (latch != null) { try { latch.await(200, TimeUnit.MILLISECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); } }
    }

    private void onCompactionFinished(boolean success, Throwable error) {
        if (!enableMemoryBuffer) return;
        boolean acquired = lock.tryLock();
        if (acquired) {
            try {
                if (compactionState == CompactionState.RUNNING) compactionState = CompactionState.IDLE;
                if (memoryBuffer.isEmpty() && drainingQueue.isEmpty()) { memoryBufferModeUntil.set(0); return; }
            } finally { lock.unlock(); }
        }
        if (!switchBackRequested.compareAndSet(false, true)) return;
        revalidationExecutor.execute(() -> {
            try {
                if (lock.tryLock()) { try { if (compactionState != CompactionState.RUNNING && memoryBuffer.isEmpty() && drainingQueue.isEmpty()) { memoryBufferModeUntil.set(0); return; } } finally { lock.unlock(); } }
                try { if (lock.tryLock(options.lockTryTimeoutNanos, TimeUnit.NANOSECONDS)) { try { drainMemoryBufferSync(); } finally { lock.unlock(); } } } catch (Exception ignored) {}
                if (lock.tryLock()) {
                    try { if (memoryBuffer.isEmpty() && drainingQueue.isEmpty() && compactionState != CompactionState.RUNNING) { memoryBufferModeUntil.set(0); } else { activateMemoryMode(); triggerDrainIfNeeded(); } }
                    finally { lock.unlock(); }
                } else { activateMemoryMode(); }
            } finally { switchBackRequested.set(false); }
        });
    }

    public Optional<T> readAt(long offset) throws IOException { lock.lock(); try { return readAtInternal(offset).map(res -> safeDeserialize(res.getRecord())); } finally { lock.unlock(); } }
    public Optional<NQueueReadResult> readRecordAt(long offset) throws IOException { lock.lock(); try { return readAtInternal(offset); } finally { lock.unlock(); } }
    public Optional<NQueueRecord> peekRecord() throws IOException { lock.lock(); try { if (recordCount == 0) return Optional.empty(); return readAtInternal(consumerOffset).map(NQueueReadResult::getRecord); } finally { lock.unlock(); } }

    private long offerDirectLocked(T object) throws IOException { return offerBatchLocked(List.of(object), true); }
    private long offerBatchLocked(List<T> items, boolean fsync) throws IOException {
        if (items.isEmpty()) return -1;
        long writePos = producerOffset, firstStart = -1, initialCount = recordCount;
        for (T obj : items) {
            byte[] payload = toBytes(obj); lastIndex++; if (lastIndex < 0) lastIndex = 0;
            NQueueRecordMetaData meta = new NQueueRecordMetaData(lastIndex, payload.length, obj.getClass().getCanonicalName());
            ByteBuffer hb = meta.toByteBuffer(); long rStart = writePos;
            while (hb.hasRemaining()) writePos += dataChannel.write(hb, writePos);
            ByteBuffer pb = ByteBuffer.wrap(payload); while (pb.hasRemaining()) writePos += dataChannel.write(pb, writePos);
            if (firstStart < 0) firstStart = rStart;
        }
        producerOffset = writePos; recordCount += items.size(); approximateSize.addAndGet(items.size());
        if (initialCount == 0) consumerOffset = firstStart;
        persistCurrentStateLocked(); if (fsync && options.withFsync) dataChannel.force(true);
        maybeCompactLocked(); notEmpty.signalAll(); statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT); return firstStart;
    }

    private Optional<NQueueReadResult> readAtInternal(long offset) throws IOException {
        long size = dataChannel.size(); if (offset + NQueueRecordMetaData.fixedPrefixSize() > size) return Optional.empty();
        NQueueRecordMetaData.HeaderPrefix pref = NQueueRecordMetaData.readPrefix(dataChannel, offset);
        long hEnd = offset + NQueueRecordMetaData.fixedPrefixSize() + pref.headerLen; if (hEnd > size) return Optional.empty();
        NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(dataChannel, offset, pref.headerLen);
        long pEnd = hEnd + meta.getPayloadLen(); if (pEnd > size) return Optional.empty();
        byte[] payload = new byte[meta.getPayloadLen()]; ByteBuffer pb = ByteBuffer.wrap(payload);
        while (pb.hasRemaining()) { if (dataChannel.read(pb, hEnd + (long) pb.position()) < 0) throw new EOFException(); }
        return Optional.of(new NQueueReadResult(new NQueueRecord(meta, payload), pEnd));
    }

    private Optional<NQueueRecord> consumeNextRecordLocked() throws IOException {
        return readAtInternal(consumerOffset).map(res -> {
            consumerOffset = res.getNextOffset(); recordCount--; approximateSize.decrementAndGet();
            if (recordCount == 0) consumerOffset = producerOffset;
            try { persistCurrentStateLocked(); maybeCompactLocked(); } catch (IOException e) { throw new RuntimeException(e); }
            return res.getRecord();
        });
    }

    private void persistCurrentStateLocked() throws IOException { synchronized (metaWriteLock) { NQueueQueueMeta.write(metaPath, consumerOffset, producerOffset, recordCount, lastIndex); } }
    private static QueueState loadOrRebuildState(FileChannel ch, Path metaPath, Options options) throws IOException {
        if (options.resetOnRestart) { QueueState s = rebuildStateFromLog(ch); NQueueQueueMeta.write(metaPath, s.consumerOffset, s.producerOffset, s.recordCount, s.lastIndex); return s; }
        if (Files.exists(metaPath)) {
            try {
                NQueueQueueMeta meta = NQueueQueueMeta.read(metaPath); QueueState s = new QueueState(meta.getConsumerOffset(), meta.getProducerOffset(), meta.getRecordCount(), meta.getLastIndex());
                if (s.consumerOffset >= 0 && s.producerOffset >= s.consumerOffset && ch.size() >= s.producerOffset) return s;
            } catch (IOException ignored) {}
        }
        QueueState s = rebuildStateFromLog(ch); NQueueQueueMeta.write(metaPath, s.consumerOffset, s.producerOffset, s.recordCount, s.lastIndex); return s;
    }

    private static QueueState rebuildStateFromLog(FileChannel ch) throws IOException {
        long offset = 0, count = 0, lastIdx = -1, size = ch.size();
        while (offset < size) {
            try {
                NQueueRecordMetaData.HeaderPrefix pref = NQueueRecordMetaData.readPrefix(ch, offset); NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(ch, offset, pref.headerLen);
                offset = offset + NQueueRecordMetaData.fixedPrefixSize() + pref.headerLen + meta.getPayloadLen(); if (offset > size) throw new EOFException();
                count++; lastIdx = meta.getIndex();
            } catch (Exception e) { ch.truncate(offset); size = offset; break; }
        }
        ch.force(true); return new QueueState(count > 0 ? 0 : size, size, count, lastIdx);
    }

    private void copyRegion(FileChannel src, long start, long end, FileChannel dst) throws IOException {
        long pos = start, count = end - start; while (count > 0) { long t = src.transferTo(pos, count, dst); if (t <= 0) break; pos += t; count -= t; }
    }

    private byte[] toBytes(T obj) throws IOException { try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos))) { oos.writeObject(obj); oos.flush(); return bos.toByteArray(); } }
    private T safeDeserialize(NQueueRecord record) { try (ByteArrayInputStream bis = new ByteArrayInputStream(record.payload()); ObjectInputStream ois = new ObjectInputStream(bis)) { return (T) ois.readObject(); } catch (Exception e) { throw new RuntimeException(e); } }

    private void activateMemoryMode() { if (!enableMemoryBuffer) return; long until = System.nanoTime() + options.revalidationIntervalNanos; memoryBufferModeUntil.updateAndGet(c -> Math.max(c, until)); scheduleRevalidation(); }
    private void scheduleRevalidation() { if (revalidationExecutor != null && revalidationScheduled.compareAndSet(false, true)) { revalidationExecutor.schedule(() -> { try { revalidateMemoryMode(); } finally { revalidationScheduled.set(false); if (memoryBufferModeUntil.get() > System.nanoTime()) scheduleRevalidation(); } }, options.revalidationIntervalNanos, TimeUnit.NANOSECONDS); } }
    private void revalidateMemoryMode() { if (System.nanoTime() >= memoryBufferModeUntil.get()) { if (lock.tryLock()) { try { if (compactionState != CompactionState.RUNNING && (!enableMemoryBuffer || (memoryBuffer.isEmpty() && drainingQueue.isEmpty()))) { memoryBufferModeUntil.set(0); return; } } finally { lock.unlock(); } } activateMemoryMode(); } }
    private long offerToMemory(T object, boolean revalidateIfFull) throws IOException {
        try {
            if (revalidateIfFull && memoryBuffer.remainingCapacity() == 0) { revalidateMemoryMode(); if (lock.tryLock()) { try { if (compactionState != CompactionState.RUNNING) { drainMemoryBufferSync(); return offerDirectLocked(object); } } finally { lock.unlock(); } } }
            MemoryBufferEntry<T> e = new MemoryBufferEntry<>(object); if (!memoryBuffer.offer(e)) memoryBuffer.put(e); triggerDrainIfNeeded(); return -1;
        } catch (InterruptedException ex) { Thread.currentThread().interrupt(); throw new IOException(ex); }
    }
    private void triggerDrainIfNeeded() { if (enableMemoryBuffer && !memoryBuffer.isEmpty() && drainingInProgress.compareAndSet(false, true)) { drainCompletionLatch = new CountDownLatch(1); drainExecutor.submit(this::drainMemoryBufferAsync); } }
    private void drainMemoryBufferAsync() {
        try { while (enableMemoryBuffer && (!memoryBuffer.isEmpty() || !drainingQueue.isEmpty())) { boolean ok = false; try { if (lock.tryLock(options.lockTryTimeoutNanos, TimeUnit.NANOSECONDS)) { try { drainMemoryBufferSync(); ok = true; } finally { lock.unlock(); } } } catch (Exception ignored) {} if (!ok) { try { Thread.sleep(5); } catch (InterruptedException ex) { Thread.currentThread().interrupt(); break; } } } }
        finally { drainingInProgress.set(false); if (drainCompletionLatch != null) drainCompletionLatch.countDown(); if (enableMemoryBuffer && !memoryBuffer.isEmpty()) triggerDrainIfNeeded(); }
    }
    private void drainMemoryBufferSync() throws IOException {
        if (!enableMemoryBuffer) return;
        while (true) {
            memoryBuffer.drainTo(drainingQueue, MEMORY_DRAIN_BATCH_SIZE); if (drainingQueue.isEmpty()) break;
            List<T> batch = new ArrayList<>(); List<MemoryBufferEntry<T>> entries = new ArrayList<>();
            for (int i=0; i<MEMORY_DRAIN_BATCH_SIZE; i++) { MemoryBufferEntry<T> ent = drainingQueue.poll(); if (ent == null) break; batch.add(ent.item); entries.add(ent); }
            if (batch.isEmpty()) break;
            try { offerBatchLocked(batch, options.withFsync); } catch (IOException ex) { for (int i = entries.size() - 1; i >= 0; i--) drainingQueue.addFirst(entries.get(i)); throw ex; }
        }
    }
    private boolean checkAndDrainMemorySync() throws IOException { if (enableMemoryBuffer && (!memoryBuffer.isEmpty() || !drainingQueue.isEmpty())) { drainMemoryBufferSync(); return recordCount > 0; } return false; }

    private void maybeCompactLocked() {
        if (compactionState == CompactionState.RUNNING || shutdownRequested) return;
        if (producerOffset <= 0 || consumerOffset <= 0) return;
        long now = System.nanoTime();
        if (((double) consumerOffset / (double) producerOffset) >= options.compactionWasteThreshold || (options.compactionIntervalNanos > 0 && (now - lastCompactionTimeNanos) >= options.compactionIntervalNanos && consumerOffset > 0)) {
            QueueState snap = currentState(); compactionState = CompactionState.RUNNING; lastCompactionTimeNanos = now;
            compactionExecutor.submit(() -> runCompactionTask(snap));
        }
    }
    private void runCompactionTask(QueueState snap) {
        try {
            Files.deleteIfExists(tempDataPath);
            try (RandomAccessFile tmpRaf = new RandomAccessFile(tempDataPath.toFile(), "rw"); FileChannel tmpCh = tmpRaf.getChannel()) {
                long actualCO; lock.lock(); try { actualCO = consumerOffset; } finally { lock.unlock(); }
                copyRegion(dataChannel, actualCO, snap.producerOffset, tmpCh);
                if (options.withFsync) tmpCh.force(true); finalizeCompaction(snap, tmpCh, actualCO);
            }
        } catch (Throwable t) { lock.lock(); try { compactionState = CompactionState.IDLE; } finally { lock.unlock(); } try { Files.deleteIfExists(tempDataPath); } catch (IOException ignored) {} }
        finally { onCompactionFinished(compactionState == CompactionState.IDLE, null); }
    }
    private void finalizeCompaction(QueueState snap, FileChannel tmpCh, long copiedFromCO) throws IOException {
        lock.lock(); try {
            if (producerOffset < snap.producerOffset) return;
            long delta = producerOffset - snap.producerOffset; if (delta > 0) copyRegion(dataChannel, snap.producerOffset, producerOffset, tmpCh);
            long bytesConsumedSinceCopyStarted = consumerOffset - copiedFromCO;
            long newPO = tmpCh.position(); long newCO = Math.max(0, bytesConsumedSinceCopyStarted);
            if (recordCount == 0) { newCO = 0; newPO = 0; tmpCh.truncate(0); }
            if (newCO > newPO) newCO = newPO;
            if (options.withFsync) tmpCh.force(true); tmpCh.close();
            try { Files.move(tempDataPath, dataPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING); } catch (AtomicMoveNotSupportedException e) { Files.move(tempDataPath, dataPath, StandardCopyOption.REPLACE_EXISTING); }
            RandomAccessFile nRaf = new RandomAccessFile(dataPath.toFile(), "rw"); FileChannel nCh = nRaf.getChannel();
            try { dataChannel.close(); raf.close(); } catch (IOException ignored) {}
            raf = nRaf; dataChannel = nCh; consumerOffset = newCO; producerOffset = newPO; persistCurrentStateLocked();
        } finally { compactionState = CompactionState.IDLE; lock.unlock(); triggerDrainIfNeeded(); }
    }

    private QueueState currentState() { return new QueueState(consumerOffset, producerOffset, recordCount, lastIndex); }

    public enum CompactionState { IDLE, RUNNING }
    private static class MemoryBufferEntry<T> { final T item; final long timestamp; MemoryBufferEntry(T item) { this.item = item; this.timestamp = System.nanoTime(); } }
    private static class QueueState { final long consumerOffset, producerOffset, recordCount, lastIndex; QueueState(long co, long po, long rc, long li) { this.consumerOffset = co; this.producerOffset = po; this.recordCount = rc; this.lastIndex = li; } }

    public static final class Options {
        double compactionWasteThreshold = 0.5; long compactionIntervalNanos = TimeUnit.MINUTES.toNanos(5); int compactionBufferSize = 128 * 1024;
        boolean withFsync = true, enableMemoryBuffer = false, resetOnRestart = false; int memoryBufferSize = 10000; long lockTryTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(10), revalidationIntervalNanos = TimeUnit.MILLISECONDS.toNanos(100);
        private Options() {}
        public static Options defaults() { return new Options(); }
        public Options withCompactionWasteThreshold(double t) { if (t < 0.0 || t > 1.0) throw new IllegalArgumentException("threshold [0.0, 1.0]"); this.compactionWasteThreshold = t; return this; }
        public Options withCompactionInterval(Duration i) { Objects.requireNonNull(i); if (i.isNegative()) throw new IllegalArgumentException("negative"); this.compactionIntervalNanos = i.toNanos(); return this; }
        public Options withCompactionBufferSize(int s) { if (s <= 0) throw new IllegalArgumentException("positive"); this.compactionBufferSize = s; return this; }
        public Options withFsync(boolean f) { this.withFsync = f; return this; }
        public Options withMemoryBuffer(boolean e) { this.enableMemoryBuffer = e; return this; }
        public Options withMemoryBufferSize(int s) { if (s <= 0) throw new IllegalArgumentException("memoryBufferSize deve ser positivo"); this.memoryBufferSize = s; return this; }
        public Options withLockTryTimeout(Duration t) { Objects.requireNonNull(t); if (t.isNegative()) throw new IllegalArgumentException("negative"); this.lockTryTimeoutNanos = t.toNanos(); return this; }
        public Options withRevalidationInterval(Duration i) { Objects.requireNonNull(i); if (i.isNegative()) throw new IllegalArgumentException("negative"); this.revalidationIntervalNanos = i.toNanos(); return this; }
        public Options withResetOnRestart(boolean r) { this.resetOnRestart = r; return this; }
        public Snapshot snapshot() { return new Snapshot(this); }
        public static class Snapshot {
            final double compactionWasteThreshold; final long compactionIntervalNanos, lockTryTimeoutNanos, revalidationIntervalNanos; final int compactionBufferSize, memoryBufferSize; final boolean enableMemoryBuffer;
            Snapshot(Options o) { this.compactionWasteThreshold = o.compactionWasteThreshold; this.compactionIntervalNanos = o.compactionIntervalNanos; this.compactionBufferSize = o.compactionBufferSize; this.enableMemoryBuffer = o.enableMemoryBuffer; this.memoryBufferSize = o.memoryBufferSize; this.lockTryTimeoutNanos = o.lockTryTimeoutNanos; this.revalidationIntervalNanos = o.revalidationIntervalNanos; }
        }
    }
}