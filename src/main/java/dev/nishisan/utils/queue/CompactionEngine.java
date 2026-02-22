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

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages background compaction for {@link NQueue}.
 * <p>
 * Compaction reclaims disk space by copying only the unconsumed segment of the
 * durable log into a fresh file, then atomically swapping it with the original.
 * The process never discards unconsumed data and is transparent to ongoing
 * producers and consumers.
 * <p>
 * This class is package-private and intended solely for use by {@link NQueue}.
 */
class CompactionEngine {

    /** Result of a successful compaction: new file handles ready to be adopted. */
    record CompactionResult(RandomAccessFile newRaf, FileChannel newChannel,
            long newConsumerOffset, long newProducerOffset) {
    }

    /** Called when compaction finishes to let the stager reconcile its mode. */
    @FunctionalInterface
    interface CompactionFinishedCallback {
        void accept(boolean compactionStillRunning);
    }

    @FunctionalInterface
    interface StateSnapshotProvider {
        NQueue.QueueState get();
    }

    @FunctionalInterface
    interface CompactionApplyCallback {
        void apply(CompactionResult result) throws IOException;
    }

    private final NQueue.Options options;
    private final Path dataPath;
    private final Path tempDataPath;
    private final ReentrantLock queueLock;
    private final StateSnapshotProvider snapshotProvider;
    private final CompactionApplyCallback applyCallback;
    private final CompactionFinishedCallback finishedCallback;

    /** Package-private so NQueue can expose it for testing. */
    volatile NQueue.CompactionState state = NQueue.CompactionState.IDLE;
    private long lastCompactionTimeNanos = System.nanoTime();

    private final ExecutorService compactionExecutor;

    CompactionEngine(NQueue.Options options, Path dataPath, Path tempDataPath, ReentrantLock queueLock,
            StateSnapshotProvider snapshotProvider,
            CompactionApplyCallback applyCallback,
            CompactionFinishedCallback finishedCallback) {
        this.options = options;
        this.dataPath = dataPath;
        this.tempDataPath = tempDataPath;
        this.queueLock = queueLock;
        this.snapshotProvider = snapshotProvider;
        this.applyCallback = applyCallback;
        this.finishedCallback = finishedCallback;
        this.compactionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "nqueue-compaction-worker");
            t.setDaemon(true);
            return t;
        });
    }

    // ── Compaction Trigger ───────────────────────────────────────────────────

    void maybeCompact(long consumerOffset, long producerOffset, long recordCount,
            boolean shutdownRequested) {
        if (state == NQueue.CompactionState.RUNNING || shutdownRequested)
            return;
        if (producerOffset <= 0)
            return;

        long now = System.nanoTime();
        boolean shouldCompact = false;

        if (options.retentionPolicy == NQueue.Options.RetentionPolicy.TIME_BASED) {
            if (options.retentionTimeNanos > 0 && recordCount > 0) {
                try (RandomAccessFile peekRaf = new RandomAccessFile(dataPath.toFile(), "r");
                        FileChannel peekCh = peekRaf.getChannel()) {
                    if (peekCh.size() > NQueueRecordMetaData.fixedPrefixSize()) {
                        NQueueRecordMetaData.HeaderPrefix pref = NQueueRecordMetaData.readPrefix(peekCh, 0);
                        NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(peekCh, 0, pref.headerLen);
                        long recordAge = System.currentTimeMillis() - meta.getTimestamp();
                        if (recordAge > TimeUnit.NANOSECONDS.toMillis(options.retentionTimeNanos)) {
                            shouldCompact = true;
                        }
                    }
                } catch (IOException ignored) {
                }
            }
        } else {
            if (consumerOffset <= 0)
                return;
            if (((double) consumerOffset / (double) producerOffset) >= options.compactionWasteThreshold)
                shouldCompact = true;
        }

        if (!shouldCompact && options.compactionIntervalNanos > 0
                && (now - lastCompactionTimeNanos) >= options.compactionIntervalNanos && producerOffset > 0) {
            if (options.retentionPolicy == NQueue.Options.RetentionPolicy.TIME_BASED || consumerOffset > 0)
                shouldCompact = true;
        }

        if (shouldCompact) {
            NQueue.QueueState snap = snapshotProvider.get();
            state = NQueue.CompactionState.RUNNING;
            lastCompactionTimeNanos = now;
            compactionExecutor.submit(() -> runCompactionTask(snap));
        }
    }

    // ── Compaction Execution ─────────────────────────────────────────────────

    /**
     * Runs compaction in two phases:
     * <ol>
     * <li><b>Background copy</b> – copies the unconsumed region without holding the
     * queue lock (only a brief lock to read the snapshot offsets).</li>
     * <li><b>Finalize under lock</b> – appends any tail data written during the
     * background copy, closes the temp channel, atomically swaps the files,
     * opens the new handle, and wires it into the queue. All of this happens
     * under a single lock hold so no data is lost.</li>
     * </ol>
     * <p>
     * The temporary file channel ({@code tmpCh}) is managed <em>manually</em>
     * rather than via try-with-resources to avoid double-close issues when the
     * channel is closed inside {@code finalizeCompaction} and then again by the
     * ARM block.
     */
    private void runCompactionTask(NQueue.QueueState snap) {
        RandomAccessFile tmpRaf = null;
        FileChannel tmpCh = null;
        try {
            Files.deleteIfExists(tempDataPath);

            // ── Phase 1: background copy (no lock held for the bulk transfer) ──

            long copyStartOffset;
            long copyEndOffset;

            tmpRaf = new RandomAccessFile(tempDataPath.toFile(), "rw");
            tmpCh = tmpRaf.getChannel();

            // Read snapshot offsets under lock.
            queueLock.lock();
            try {
                copyEndOffset = snap.producerOffset;
                copyStartOffset = (options.retentionPolicy == NQueue.Options.RetentionPolicy.DELETE_ON_CONSUME)
                        ? snap.consumerOffset
                        : 0L;
            } finally {
                queueLock.unlock();
            }

            // Open a separate read-only handle for the background copy so we don't
            // interfere with NQueue's own dataChannel.
            try (RandomAccessFile srcRaf = new RandomAccessFile(dataPath.toFile(), "r");
                    FileChannel srcCh = srcRaf.getChannel()) {

                if (options.retentionPolicy == NQueue.Options.RetentionPolicy.TIME_BASED) {
                    copyStartOffset = findTimeBasedCutoff(srcCh, copyEndOffset);
                }

                if (copyEndOffset > copyStartOffset) {
                    copyRegion(srcCh, copyStartOffset, copyEndOffset, tmpCh);
                }
            }

            // ── Phase 2: finalize under a single lock hold ─────────────────────
            // Everything from here (tail copy, close, move, wire) is atomic w.r.t.
            // the queue so no concurrent offer/poll can see an inconsistent state.

            queueLock.lock();
            try {
                NQueue.QueueState current = snapshotProvider.get();
                long currentProducer = current.producerOffset;
                long currentConsumer = current.consumerOffset;
                long recordCount = current.recordCount;

                // Append any records written since the snapshot.
                if (currentProducer > copyEndOffset) {
                    try (RandomAccessFile tailRaf = new RandomAccessFile(dataPath.toFile(), "r");
                            FileChannel tailCh = tailRaf.getChannel()) {
                        copyRegion(tailCh, copyEndOffset, currentProducer, tmpCh);
                    }
                }

                long newPO = tmpCh.position();
                long newCO = Math.max(0L, currentConsumer - copyStartOffset);

                if (recordCount == 0) {
                    newCO = 0L;
                    newPO = 0L;
                    tmpCh.truncate(0);
                }
                if (newCO > newPO)
                    newCO = newPO;

                if (options.withFsync)
                    tmpCh.force(true);

                // Close the temporary channel BEFORE the move – required on some
                // filesystems and prevents any further accidental writes.
                tmpCh.close();
                tmpRaf.close();
                tmpCh = null;
                tmpRaf = null;

                try {
                    Files.move(tempDataPath, dataPath, StandardCopyOption.ATOMIC_MOVE,
                            StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tempDataPath, dataPath, StandardCopyOption.REPLACE_EXISTING);
                }

                RandomAccessFile nRaf = new RandomAccessFile(dataPath.toFile(), "rw");
                FileChannel nCh = nRaf.getChannel();

                state = NQueue.CompactionState.IDLE;
                applyCallback.apply(new CompactionResult(nRaf, nCh, newCO, newPO));
            } finally {
                queueLock.unlock();
            }

        } catch (Throwable t) {
            // Close temp resources if they're still open.
            closeQuietly(tmpCh);
            closeQuietly(tmpRaf);

            queueLock.lock();
            try {
                state = NQueue.CompactionState.IDLE;
            } finally {
                queueLock.unlock();
            }
            try {
                Files.deleteIfExists(tempDataPath);
            } catch (IOException ignored) {
            }
        } finally {
            finishedCallback.accept(state == NQueue.CompactionState.IDLE);
        }
    }

    private long findTimeBasedCutoff(FileChannel dataChannel, long limitOffset) throws IOException {
        long offset = 0;
        long retentionMillis = TimeUnit.NANOSECONDS.toMillis(options.retentionTimeNanos);
        long cutoffTime = System.currentTimeMillis() - retentionMillis;

        while (offset < limitOffset) {
            try {
                NQueueRecordMetaData.HeaderPrefix pref = NQueueRecordMetaData.readPrefix(dataChannel, offset);
                NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(dataChannel, offset, pref.headerLen);
                if (meta.getTimestamp() >= cutoffTime)
                    return offset;
                offset += NQueueRecordMetaData.fixedPrefixSize() + pref.headerLen + meta.getPayloadLen();
            } catch (EOFException e) {
                break;
            }
        }
        return offset;
    }

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

    private static void closeQuietly(AutoCloseable c) {
        if (c != null) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
    }

    // ── State & Lifecycle ────────────────────────────────────────────────────

    NQueue.CompactionState getState() {
        return state;
    }

    boolean scheduleShutdownCompaction(long consumerOffset) {
        if (state == NQueue.CompactionState.IDLE && consumerOffset > 0) {
            NQueue.QueueState snap = snapshotProvider.get();
            state = NQueue.CompactionState.RUNNING;
            compactionExecutor.submit(() -> runCompactionTask(snap));
            return true;
        }
        return false;
    }

    void shutdown(int awaitSeconds) {
        compactionExecutor.shutdown();
        try {
            compactionExecutor.awaitTermination(awaitSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
