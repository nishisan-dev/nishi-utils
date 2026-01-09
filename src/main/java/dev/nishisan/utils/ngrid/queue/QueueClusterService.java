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

import dev.nishisan.utils.ngrid.replication.QuorumUnreachableException;
import dev.nishisan.utils.ngrid.replication.ReplicationHandler;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import dev.nishisan.utils.ngrid.replication.ReplicationResult;
import dev.nishisan.utils.queue.NQueue;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
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
 * Coordinates queue operations with the replication manager ensuring that the persistent
 * {@link NQueue} backend stays consistent across cluster members.
 */
public final class QueueClusterService<T extends Serializable> implements Closeable, ReplicationHandler {
    private static final Logger LOGGER = Logger.getLogger(QueueClusterService.class.getName());
    public static final String TOPIC = "queue";

    private final NQueue<T> queue;
    private final ReplicationManager replicationManager;
    private final ReentrantLock operationLock = new ReentrantLock(true);
    private final Integer replicationFactor;

    public QueueClusterService(Path baseDir, String queueName, ReplicationManager replicationManager) {
        this(baseDir, queueName, replicationManager, NQueue.Options.defaults(), null);
    }

    public QueueClusterService(Path baseDir, String queueName, ReplicationManager replicationManager, NQueue.Options options) {
        this(baseDir, queueName, replicationManager, options, null);
    }

    public QueueClusterService(Path baseDir, String queueName, ReplicationManager replicationManager, NQueue.Options options, Integer replicationFactor) {
        try {
            this.queue = NQueue.open(baseDir, queueName, enforceGridOptions(options));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open NQueue", e);
        }
        this.replicationManager = Objects.requireNonNull(replicationManager, "replicationManager");
        this.replicationFactor = replicationFactor;
        this.replicationManager.registerHandler(TOPIC, this);
    }

    public void offer(T value) {
        Objects.requireNonNull(value, "value");
        operationLock.lock();
        try {
            QueueReplicationCommand command = QueueReplicationCommand.offer(value);
            waitForReplication(replicationManager.replicate(TOPIC, command, replicationFactor));
        } finally {
            operationLock.unlock();
        }
    }

    public Optional<T> poll() {
        operationLock.lock();
        try {
            Optional<T> next = queue.peek();
            if (next.isEmpty()) {
                return Optional.empty();
            }
            QueueReplicationCommand command = QueueReplicationCommand.poll((Serializable) next.get());
            waitForReplication(replicationManager.replicate(TOPIC, command, replicationFactor));
            return next;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read from queue", e);
        } finally {
            operationLock.unlock();
        }
    }

    public Optional<T> peek() {
        operationLock.lock();
        try {
            return queue.peek();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to peek queue", e);
        } finally {
            operationLock.unlock();
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
        operationLock.lock();
        try {
            switch (command.type()) {
                case OFFER -> queue.offer((T) command.value());
                case POLL -> {
                    Optional<T> current = queue.peek();
                    Serializable expected = command.value();
                    if (expected != null) {
                        if (current.isEmpty()) {
                            String msg = "POLL replication mismatch. Expected " + expected + " but queue is empty";
                            LOGGER.severe(msg);
                            throw new IllegalStateException(msg);
                        }
                        if (!current.get().equals(expected)) {
                            String msg = "POLL replication mismatch. Expected " + expected + " but found " + current.get();
                            LOGGER.severe(msg);
                            throw new IllegalStateException(msg);
                        }
                    }
                    queue.poll();
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to apply replicated queue operation", e);
        } finally {
            operationLock.unlock();
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
