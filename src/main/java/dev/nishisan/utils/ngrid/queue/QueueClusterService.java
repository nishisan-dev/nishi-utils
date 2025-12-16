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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Coordinates queue operations with the replication manager ensuring that the persistent
 * {@link NQueue} backend stays consistent across cluster members.
 */
public final class QueueClusterService<T extends Serializable> implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(QueueClusterService.class.getName());
    public static final String TOPIC = "queue";

    private final NQueue<T> queue;
    private final ReplicationManager replicationManager;

    public QueueClusterService(Path baseDir, String queueName, ReplicationManager replicationManager) {
        try {
            this.queue = NQueue.open(baseDir, queueName);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open NQueue", e);
        }
        this.replicationManager = Objects.requireNonNull(replicationManager, "replicationManager");
        this.replicationManager.registerHandler(TOPIC, this::applyReplication);
    }

    public void offer(T value) {
        Objects.requireNonNull(value, "value");
        QueueReplicationCommand command = QueueReplicationCommand.offer(value);
        waitForReplication(replicationManager.replicate(TOPIC, command));
    }

    public Optional<T> poll() {
        try {
            Optional<T> next = queue.peek();
            if (next.isEmpty()) {
                return Optional.empty();
            }
            QueueReplicationCommand command = QueueReplicationCommand.poll((Serializable) next.get());
            waitForReplication(replicationManager.replicate(TOPIC, command));
            return next;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read from queue", e);
        }
    }

    public Optional<T> peek() {
        try {
            return queue.peek();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to peek queue", e);
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

    @SuppressWarnings("unchecked")
    private void applyReplication(UUID operationId, Serializable payload) {
        QueueReplicationCommand command = (QueueReplicationCommand) payload;
        try {
            switch (command.type()) {
                case OFFER -> queue.offer((T) command.value());
                case POLL -> {
                    Optional<T> current = queue.peek();
                    if (command.value() != null && current.isPresent() && !current.get().equals(command.value())) {
                        LOGGER.log(Level.WARNING, "POLL replication mismatch. Expected {0} but found {1}", new Object[]{command.value(), current});
                    }
                    queue.poll();
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to apply replicated queue operation", e);
        }
    }

    @Override
    public void close() throws IOException {
        queue.close();
    }
}
