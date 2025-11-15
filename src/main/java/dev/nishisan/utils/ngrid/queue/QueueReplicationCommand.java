package dev.nishisan.utils.ngrid.queue;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Serializable command replicated across the cluster for queue operations.
 */
public final class QueueReplicationCommand implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final QueueReplicationCommandType type;
    private final Serializable value;

    private QueueReplicationCommand(QueueReplicationCommandType type, Serializable value) {
        this.type = Objects.requireNonNull(type, "type");
        this.value = value;
    }

    public static QueueReplicationCommand offer(Serializable value) {
        return new QueueReplicationCommand(QueueReplicationCommandType.OFFER, value);
    }

    public static QueueReplicationCommand poll(Serializable value) {
        return new QueueReplicationCommand(QueueReplicationCommandType.POLL, value);
    }

    public QueueReplicationCommandType type() {
        return type;
    }

    public Serializable value() {
        return value;
    }
}
