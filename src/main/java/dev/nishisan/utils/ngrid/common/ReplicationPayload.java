package dev.nishisan.utils.ngrid.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Encapsulates an operation that must be replicated across cluster members.
 */
public final class ReplicationPayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID operationId;
    private final String topic;
    private final Serializable data;

    public ReplicationPayload(UUID operationId, String topic, Serializable data) {
        this.operationId = Objects.requireNonNull(operationId, "operationId");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.data = Objects.requireNonNull(data, "data");
    }

    public UUID operationId() {
        return operationId;
    }

    public String topic() {
        return topic;
    }

    public Serializable data() {
        return data;
    }
}
