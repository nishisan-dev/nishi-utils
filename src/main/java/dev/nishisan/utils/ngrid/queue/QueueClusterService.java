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
 * persistent
 * {@link NQueue} backend stays consistent across cluster members.
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

    public QueueClusterService(Path baseDir, String queueName, ReplicationManager replicationManager) {
        this(baseDir, queueName, replicationManager, NQueue.Options.defaults(), null);
    }

    public QueueClusterService(Path baseDir, String queueName, ReplicationManager replicationManager,
            NQueue.Options options) {
        this(baseDir, queueName, replicationManager, options, null);
    }

    private final OffsetManager offsetManager;

    public QueueClusterService(Path baseDir, String queueName, ReplicationManager replicationManager,
            NQueue.Options options, Integer replicationFactor) {
        try {
            this.queue = NQueue.open(baseDir, queueName, enforceGridOptions(options));
            this.queueName = queueName;
            this.topic = "queue:" + queueName; // Per-queue topic
            this.offsetManager = new OffsetManager(baseDir.resolve(queueName).resolve("offsets.dat"));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open NQueue", e);
        }
        this.replicationManager = Objects.requireNonNull(replicationManager, "replicationManager");
        this.replicationFactor = replicationFactor;
        LOGGER.info(() -> "QueueClusterService created: queueName=" + queueName + ", topic=" + topic);
        this.replicationManager.registerHandler(topic, this);
    }

    public void offer(T value) {
        Objects.requireNonNull(value, "value");
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
        clientLock.lock();
        operationLock.lock();
        try {
            // If consumerId is provided, we use the offset-based consumption (Log mode)
            // BUT only if the queue is configured for TIME_BASED retention.
            // If it is DELETE_ON_CONSUME, we ignore the consumerId and perform a
            // destructive poll
            // to maintain backward compatibility and expected Queue semantics.
            if (consumerId != null && queue.getRetentionPolicy() == NQueue.Options.RetentionPolicy.TIME_BASED) {
                long nextIndex = offsetManager.getOffset(consumerId);

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
                            offsetManager.updateOffset(consumerId, nextIndex); // Persist correction
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
                        offsetManager.updateOffset(consumerId, newOffset);

                        return Optional.of(item);
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Failed to deserialize record", e);
                        // Skip poison pill
                        offsetManager.updateOffset(consumerId, nextIndex + 1);
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

    private static class OffsetManager {
        private final Path storagePath;
        private final java.util.Map<NodeId, Long> offsets = new java.util.concurrent.ConcurrentHashMap<>();

        OffsetManager(Path storagePath) {
            this.storagePath = storagePath;
            load();
        }

        long getOffset(NodeId nodeId) {
            return offsets.getOrDefault(nodeId, 0L);
        }

        void updateOffset(NodeId nodeId, long offset) {
            offsets.put(nodeId, offset);
            save(); // Naive persistence on every update
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

    private void waitForReplication(CompletableFuture<ReplicationResult> future) {
        try {
            long timeoutMs = Math.max(1L, replicationManager.operationTimeout().toMillis());
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TimeoutException) {
                throw new IllegalStateException("Replication operation timed out", cause);
            } else if (cause instanceof QuorumUnreachableException) {
                throw new IllegalStateException("Quorum unreachable for replication operation", cause);
            } else {
                throw new IllegalStateException("Replication operation failed", cause != null ? cause : e);
            }
        } catch (TimeoutException e) {
            throw new IllegalStateException("Replication operation timed out", e);
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

    @Override
    public SnapshotChunk getSnapshotChunk(int chunkIndex) {
        // Queue catch-up not implemented yet
        return null;
    }

    private static NQueue.Options enforceGridOptions(NQueue.Options options) {
        Objects.requireNonNull(options, "options");
        options.withShortCircuit(false);
        return options;
    }

    @Override
    public void close() throws IOException {
        queue.close();
    }
}
