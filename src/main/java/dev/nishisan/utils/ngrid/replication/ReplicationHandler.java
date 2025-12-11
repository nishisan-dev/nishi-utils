package dev.nishisan.utils.ngrid.replication;

import java.io.Serializable;
import java.util.UUID;

/**
 * Callback invoked to apply a replicated operation locally.
 */
@FunctionalInterface
public interface ReplicationHandler {
    void apply(UUID operationId, Serializable payload) throws Exception;
}
