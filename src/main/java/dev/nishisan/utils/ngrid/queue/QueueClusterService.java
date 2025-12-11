package dev.nishisan.utils.ngrid.queue;

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
        future.join();
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
