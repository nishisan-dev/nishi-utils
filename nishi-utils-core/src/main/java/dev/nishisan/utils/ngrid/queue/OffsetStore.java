package dev.nishisan.utils.ngrid.queue;

import dev.nishisan.utils.ngrid.common.NodeId;

/**
 * Abstraction for per-node offset tracking in distributed queues.
 */
public interface OffsetStore {

    /**
     * Returns the committed offset for the given node.
     *
     * @param nodeId the node identifier
     * @return the committed offset, or {@code 0} if none recorded
     */
    long getOffset(NodeId nodeId);

    /**
     * Updates the committed offset for the given node.
     *
     * @param nodeId the node identifier
     * @param offset the new offset value
     */
    void updateOffset(NodeId nodeId, long offset);
}
