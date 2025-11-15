package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.common.OperationStatus;

import java.io.Serializable;
import java.util.UUID;

/**
 * Maintains the replicated history locally so that a node can recover operations after
 * a restart or a temporary disconnection.
 */
public final class ReplicatedRecord implements Serializable {
    private final UUID operationId;
    private final String topic;
    private final Serializable payload;
    private volatile OperationStatus status;

    public ReplicatedRecord(UUID operationId, String topic, Serializable payload, OperationStatus status) {
        this.operationId = operationId;
        this.topic = topic;
        this.payload = payload;
        this.status = status;
    }

    public UUID operationId() {
        return operationId;
    }

    public String topic() {
        return topic;
    }

    public Serializable payload() {
        return payload;
    }

    public OperationStatus status() {
        return status;
    }

    public void status(OperationStatus status) {
        this.status = status;
    }
}
