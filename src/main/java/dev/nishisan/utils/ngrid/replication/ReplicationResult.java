package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.common.OperationStatus;

import java.io.Serializable;
import java.util.UUID;

/**
 * Result of a replication request initiated by the leader.
 */
public final class ReplicationResult implements Serializable {
    private final UUID operationId;
    private final OperationStatus status;

    public ReplicationResult(UUID operationId, OperationStatus status) {
        this.operationId = operationId;
        this.status = status;
    }

    public UUID operationId() {
        return operationId;
    }

    public OperationStatus status() {
        return status;
    }
}
