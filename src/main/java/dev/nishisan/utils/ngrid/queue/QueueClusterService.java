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

package dev.nishisan.utils.ngrid.queue;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.replication.QuorumUnreachableException;
import dev.nishisan.utils.ngrid.replication.ReplicationHandler;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import dev.nishisan.utils.ngrid.replication.ReplicationResult;
import dev.nishisan.utils.queue.NQueue;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Coordinates queue operations with the replication manager ensuring that the
 * persistent {@link NQueue} backend stays consistent across cluster members.
 *
 * @param <T> the queue element type
 */
public final class QueueClusterService<T extends Serializable> implements Closeable, ReplicationHandler {
    private static final Logger LOGGER = Logger.getLogger(QueueClusterService.class.getName());

    private final NQueue<T> queue;
    private final String queueName;
    private final String topic; // Per-queue topic
    private final ReplicationManager replicationManager;
    private final ReentrantLock clientLock = new ReentrantLock(true);
    private final ReentrantLock operationLock = new ReentrantLock(true);
    private final Integer replicationFactor;
    private final java.util.concurrent.atomic.AtomicBoolean pendingOffsetSync = new java.util.concurrent.atomic.AtomicBoolean(
            false);

    /**
     * Creates a queue cluster service with default options.
     *
     * @param baseDir            the base directory for queue storage
     * @param queueName          the queue name
     * @param replicationManager the replication manager
     */
    public QueueClusterService(Path baseDir, String queueName, ReplicationManager replicationManager) {
        this(baseDir, queueName, replicationManager, NQueue.Options.defaults(), null, null);
    }

    /**
     * Creates a queue cluster service with custom options.
     *
     * @param baseDir            the base directory for queue storage
     * @param queueName          the queue name
     * @param replicationManager the replication manager
     * @param options            the queue options
     */
    public QueueClusterService(Path baseDir, String queueName, ReplicationManager replicationManager,
            NQueue.Options options) {
        this(baseDir, queueName, replicationManager, options, null, null);
    }

    private final OffsetStore offsetStore;
    private final LocalOffsetStore localOffsetStore;
    private final DistributedOffsetStore distributedOffsetStore;

    /**
     * Creates a queue cluster service with full configuration.
     *
     * @param baseDir            the base directory for queue storage
     * @param queueName          the queue name
     * @param replicationManager the replication manager
     * @param options            the queue options
     * @param replicationFactor  the desired replication factor, or {@code null} for
     *                           default
     * @param offsetStore        the offset store, or {@code null} for local-only
     */
    public QueueClusterService(Path baseDir, String queueName, ReplicationManager replicationManager,
            NQueue.Options options, Integer replicationFactor, OffsetStore offsetStore) {
        try {
            this.queue = NQueue.open(baseDir, queueName, enforceGridOptions(options));
            this.queueName = queueName;
            this.topic = "queue:" + queueName; // Per-queue topic
            this.localOffsetStore = new LocalOffsetStore(baseDir.resolve(queueName).resolve("offsets.dat"));
            this.distributedOffsetStore = offsetStore instanceof DistributedOffsetStore distributed
                    ? distributed
                    : null;
            this.offsetStore = offsetStore != null ? offsetStore : localOffsetStore;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open NQueue", e);
        }
        this.replicationManager = Objects.requireNonNull(replicationManager, "replicationManager");
        this.replicationFactor = replicationFactor;
        LOGGER.info(() -> "QueueClusterService created: queueName=" + queueName + ", topic=" + topic);
        this.replicationManager.registerHandler(topic, this);
    }

    /**
     * Offers a value to the queue, replicating the operation across the cluster.
     *
     * @param value the value to offer
     */
    public void offer(T value) {
        Objects.requireNonNull(value, "value");
        ensureLeaderReady();
        CompletableFuture<ReplicationResult> future;
        clientLock.lock();
        try {
            QueueReplicationCommand command = QueueReplicationCommand.offer(value);
            future = replicationManager.replicate(topic, command, replicationFactor);
        } finally {
            clientLock.unlock(); // Release lock BEFORE waiting
        }
        // Wait for replication OUTSIDE of lock to avoid deadlock
        waitForReplication(future);
    }

    public Optional<T> poll() {
        return poll(null);
    }

    public Optional<T> poll(NodeId consumerId) {
        return poll(consumerId, null);
    }

    public Optional<T> poll(NodeId consumerId, Long hintOffset) {
        ensureLeaderReady();
        clientLock.lock();
        operationLock.lock();
        try {
            // If consumerId is provided, we use the offset-based consumption (Log mode)
            // BUT only if the queue is configured for TIME_BASED retention.
            // If it is DELETE_ON_CONSUME, we ignore the consumerId and perform a
            // destructive poll
            // to maintain backward compatibility and expected Queue semantics.
            LOGGER.info(() -> String.format("poll: consumerId=%s, retentionPolicy=%s, queueName=%s",
                    consumerId, queue.getRetentionPolicy(), queueName));
            if (consumerId != null && queue.getRetentionPolicy() == NQueue.Options.RetentionPolicy.TIME_BASED) {
                long storedOffset = offsetStore.getOffset(consumerId);
                long nextIndex = storedOffset;
                if (hintOffset != null && hintOffset > storedOffset) {
                    LOGGER.info(() -> "Forwarding offset for " + consumerId + " from " + storedOffset + " to hint "
                            + hintOffset);
                    nextIndex = hintOffset;
                }

                // If nextIndex is 0 (new consumer), we might want to start from the beginning.
                // But if the queue starts at 1 (which it does), asking for 0 will fail.
                // We should check "earliest available" if we miss.

                Optional<dev.nishisan.utils.queue.NQueueReadResult> result = queue.readRecordAtIndex(nextIndex);

                if (result.isEmpty()) {
                    // Check if we are behind (expired) or if nextIndex is simply 0 and queue starts
                    // at 1
                    Optional<dev.nishisan.utils.queue.NQueueRecord> oldest = queue.peekRecord();
                    if (oldest.isPresent()) {
                        long oldestIndex = oldest.get().meta().getIndex();
                        if (oldestIndex > nextIndex) {
                            // We are behind (or asking for 0 when start is 1). Fast-forward.
                            long finalNextIndex = nextIndex;
                            LOGGER.info(() -> "Consumer " + consumerId + " offset " + finalNextIndex
                                    + " is behind/invalid. Fast-forwarding to " + oldestIndex);
                            nextIndex = oldestIndex;
                            safeUpdateOffset(consumerId, nextIndex); // Persist correction
                            result = queue.readRecordAtIndex(nextIndex);
                        }
                    }
                }

                if (result.isPresent()) {
                    dev.nishisan.utils.queue.NQueueRecord record = result.get().getRecord();
                    // Deserialize payload
                    try (java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(record.payload());
                            java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bis)) {
                        T item = (T) ois.readObject();

                        // Advance offset to next record
                        long newOffset = record.meta().getIndex() + 1;
                        safeUpdateOffset(consumerId, newOffset);

                        return Optional.of(item);
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Failed to deserialize record", e);
                        // Skip poison pill
                        safeUpdateOffset(consumerId, nextIndex + 1);
                        return Optional.empty();
                    }
                } else {
                    return Optional.empty();
                }
            }

            // Legacy/Queue Mode (Destructive)
            Optional<T> next = queue.peek();
            if (next.isEmpty()) {
                return Optional.empty();
            }
            QueueReplicationCommand command = QueueReplicationCommand.poll((Serializable) next.get());
            CompletableFuture<ReplicationResult> future = replicationManager.replicate(topic, command,
                    replicationFactor);

            // CRITICAL: Release BOTH locks before waiting to avoid deadlock
            // The NQueue has its own internal lock - if we hold operationLock while
            // waiting,
            // and replication tries to apply() which needs operationLock to call
            // queue.offer(),
            // we get a classic deadlock
            operationLock.unlock();
            clientLock.unlock();

            // Wait for replication OUTSIDE of locks
            waitForReplication(future);
            return next;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read from queue", e);
        } finally {
            if (operationLock.isHeldByCurrentThread()) {
                operationLock.unlock();
            }
            if (clientLock.isHeldByCurrentThread()) {
                clientLock.unlock();
            }
        }
    }

    public Optional<T> peek() {
        ensureLeaderReady();
        clientLock.lock();
        operationLock.lock();
        try {
            return queue.peek();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to peek queue", e);
        } finally {
            operationLock.unlock();
            clientLock.unlock();
        }
    }

    // ... existing waitForReplication ...

    private static class LocalOffsetStore implements OffsetStore {
        private final Path storagePath;
        private final java.util.Map<NodeId, Long> offsets = new java.util.concurrent.ConcurrentHashMap<>();

        LocalOffsetStore(Path storagePath) {
            this.storagePath = storagePath;
            load();
        }

        @Override
        public long getOffset(NodeId nodeId) {
            return offsets.getOrDefault(nodeId, 0L);
        }

        @Override
        public void updateOffset(NodeId nodeId, long offset) {
            offsets.compute(nodeId, (key, current) -> {
                if (current == null || offset > current) {
                    return offset;
                }
                LOGGER.fine(() -> "Ignored offset regression for " + nodeId + ": " + offset + " <= " + current);
                return current;
            });
            save(); // Naive persistence on every update
        }

        java.util.Map<NodeId, Long> snapshot() {
            return new java.util.HashMap<>(offsets);
        }

        /**
         * Clears all consumer offsets. Called during snapshot install so that
         * the new NQueue indices align with a fresh offset baseline.
         */
        void reset() {
            offsets.clear();
            save();
        }

        private void load() {
            if (!Files.exists(storagePath))
                return;
            try (java.io.ObjectInputStream ois = new java.io.ObjectInputStream(Files.newInputStream(storagePath))) {
                java.util.Map<String, Long> raw = (java.util.Map<String, Long>) ois.readObject();
                raw.forEach((k, v) -> offsets.put(NodeId.of(k), v));
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to load offsets", e);
            }
        }

        private void save() {
            try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(Files.newOutputStream(storagePath))) {
                java.util.Map<String, Long> raw = new java.util.HashMap<>();
                offsets.forEach((k, v) -> raw.put(k.value(), v));
                oos.writeObject(raw);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to save offsets", e);
            }
        }
    }

    private void safeUpdateOffset(NodeId consumerId, long offset) {
        if (distributedOffsetStore != null) {
            try {
                // Critical: Update distributed store FIRST.
                // If this fails (replication timeout/partition), we must NOT update local store
                // and we must NOT return the message to the client (throw exception).
                // This ensures that the state remains consistent: if client gets data, offset
                // is committed.
                distributedOffsetStore.updateOffset(consumerId, offset);
                pendingOffsetSync.set(false);
            } catch (RuntimeException e) {
                LOGGER.log(Level.WARNING, "Failed to update distributed offset for " + consumerId, e);
                // Propagate exception to abort the poll
                throw e;
            }
        }
        // Only update local store if distributed update succeeded (or if running in
        // local-only mode)
        localOffsetStore.updateOffset(consumerId, offset);
    }

    public void syncOffsetsIfNeeded() {
        if (distributedOffsetStore == null || !pendingOffsetSync.get()) {
            return;
        }
        boolean failed = false;
        for (java.util.Map.Entry<NodeId, Long> entry : localOffsetStore.snapshot().entrySet()) {
            try {
                distributedOffsetStore.updateOffset(entry.getKey(), entry.getValue());
            } catch (RuntimeException e) {
                failed = true;
                LOGGER.log(Level.WARNING, "Failed to sync offset for " + entry.getKey(), e);
            }
        }
        pendingOffsetSync.set(failed);
    }

    private void waitForReplication(CompletableFuture<ReplicationResult> future) {
        long totalTimeoutMs = Math.max(1L, replicationManager.operationTimeout().toMillis());
        long windowMs = Math.min(1000L, totalTimeoutMs / 3); // Poll every 1s or 1/3 of timeout
        long elapsed = 0L;

        while (elapsed < totalTimeoutMs) {
            try {
                long remaining = Math.min(windowMs, totalTimeoutMs - elapsed);
                future.get(remaining, TimeUnit.MILLISECONDS);
                return; // Success
            } catch (TimeoutException e) {
                elapsed += windowMs;
                if (elapsed >= totalTimeoutMs) {
                    throw new IllegalStateException("Replication operation timed out", e);
                }
                // Give quorum adjustment time to take effect
                final long elapsedCopy = elapsed;
                LOGGER.fine(() -> "Waiting for replication... " + elapsedCopy + "ms elapsed");
            } catch (CompletionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof TimeoutException) {
                    throw new IllegalStateException("Replication operation timed out", cause);
                } else if (cause instanceof QuorumUnreachableException) {
                    throw new IllegalStateException("Quorum unreachable for replication operation", cause);
                } else {
                    throw new IllegalStateException("Replication operation failed", cause != null ? cause : e);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Replication operation interrupted", e);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof TimeoutException) {
                    throw new IllegalStateException("Replication operation timed out", cause);
                } else if (cause instanceof QuorumUnreachableException) {
                    throw new IllegalStateException("Quorum unreachable for replication operation", cause);
                } else {
                    throw new IllegalStateException("Replication operation failed", cause != null ? cause : e);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public void apply(UUID operationId, Serializable payload) {
        QueueReplicationCommand command = (QueueReplicationCommand) payload;
        long start = System.nanoTime();
        operationLock.lock();
        try {
            switch (command.type()) {
                case OFFER -> queue.offer((T) command.value());
                case POLL -> {
                    // With sequencing guarantees from ReplicationManager, we can trust
                    // that operations arrive in the correct order. No need for optimistic
                    // validation.
                    queue.poll();
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to apply replicated queue operation", e);
        } finally {
            operationLock.unlock();
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
            LOGGER.info(
                    () -> "Applied replication command " + command.type() + " in " + elapsedMs + "ms on " + queueName);
        }
    }

    private static final int SNAPSHOT_CHUNK_SIZE = 1000;

    /** {@inheritDoc} */
    @Override
    public SnapshotChunk getSnapshotChunk(int chunkIndex) {
        try {
            int startIndex = chunkIndex * SNAPSHOT_CHUNK_SIZE;
            NQueue.ReadRangeResult<T> result = queue.readRange(startIndex, SNAPSHOT_CHUNK_SIZE);

            if (result.items().isEmpty() && !result.hasMore()) {
                // No more data
                return new SnapshotChunk(new java.util.ArrayList<>(), false);
            }

            @SuppressWarnings("unchecked")
            java.util.ArrayList<Serializable> chunk = new java.util.ArrayList<>(
                    (java.util.Collection<Serializable>) result.items());
            return new SnapshotChunk(chunk, result.hasMore());
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to get snapshot chunk " + chunkIndex, e);
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void resetState() throws Exception {
        // For queues, we clear by polling all items
        // This is called before installing a snapshot
        while (queue.size() > 0) {
            queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
        // Reset consumer offsets so they align with the new snapshot indices.
        // Without this, stale offsets cause duplicate message delivery after
        // snapshot install because the NQueue assigns new indices starting from 1.
        localOffsetStore.reset();
        LOGGER.info(() -> "Queue " + queueName + " state reset for snapshot install (offsets cleared)");
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public void installSnapshot(Serializable snapshot) throws Exception {
        if (snapshot instanceof java.util.List<?> items) {
            for (Object item : items) {
                queue.offer((T) item);
            }
            LOGGER.info(() -> "Installed " + items.size() + " items to queue " + queueName);
        }
    }

    private static NQueue.Options enforceGridOptions(NQueue.Options options) {
        Objects.requireNonNull(options, "options");
        options.withShortCircuit(false);
        return options;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        queue.close();
    }

    private void ensureLeaderReady() {
        if (replicationManager.isLeaderSyncing()) {
            throw new IllegalStateException("Leader sync in progress");
        }
    }

    /**
     * Returns the current consumer offset for the given consumer.
     *
     * @param consumerId the consumer node ID
     * @return the current offset
     */
    public long getCurrentOffset(NodeId consumerId) {
        long local = localOffsetStore.getOffset(consumerId);
        long distributed = distributedOffsetStore != null ? distributedOffsetStore.getOffset(consumerId) : 0;
        return Math.max(local, distributed);
    }

    /**
     * Updates the local offset for a consumer.
     *
     * @param consumerId the consumer node ID
     * @param offset     the new offset
     */
    public void updateLocalOffset(NodeId consumerId, long offset) {
        localOffsetStore.updateOffset(consumerId, offset);
    }

    /**
     * A queue record pairing an offset with its value.
     *
     * @param <T> the value type
     */
    public static class QueueRecord<T> implements Serializable {
        private final long offset;
        private final T value;

        /**
         * Creates a new queue record.
         *
         * @param offset the record offset
         * @param value  the record value
         */
        public QueueRecord(long offset, T value) {
            this.offset = offset;
            this.value = value;
        }

        /**
         * Returns the record offset.
         *
         * @return the offset
         */
        public long offset() {
            return offset;
        }

        /**
         * Returns the record value.
         *
         * @return the value
         */
        public T value() {
            return value;
        }
    }

    /**
     * Polls the next record for a consumer, returning offset and value.
     *
     * @param consumerId the consumer node ID
     * @param hintOffset the offset hint, or {@code null}
     * @return the next record, or empty
     */
    public Optional<QueueRecord<T>> pollRecord(NodeId consumerId, Long hintOffset) {
        Optional<T> val = poll(consumerId, hintOffset);
        if (val.isEmpty())
            return Optional.empty();
        long current = getCurrentOffset(consumerId);
        return Optional.of(new QueueRecord<>(current - 1, val.get()));
    }
}
