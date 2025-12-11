package dev.nishisan.utils.ngrid.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Acknowledgement for a replication operation.
 */
public final class ReplicationAckPayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID operationId;
    private final boolean accepted;

    public ReplicationAckPayload(UUID operationId, boolean accepted) {
        this.operationId = Objects.requireNonNull(operationId, "operationId");
        this.accepted = accepted;
    }

    public UUID operationId() {
        return operationId;
    }

    public boolean accepted() {
        return accepted;
    }
}
