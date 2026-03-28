package dev.nishisan.utils.ngrid.queue;

import dev.nishisan.utils.ngrid.common.NodeId;

/**
 * Abstraction for offset tracking in distributed queues.
 */
public interface OffsetStore {

    /**
     * Returns the committed offset for the given consumer key.
     *
     * @param consumerKey the consumer identifier
     * @return the committed offset, or {@code 0} if none recorded
     */
    long getOffset(String consumerKey);

    /**
     * Updates the committed offset for the given consumer key.
     *
     * @param consumerKey the consumer identifier
     * @param offset the new offset value
     */
    void updateOffset(String consumerKey, long offset);

    /**
     * Backward-compatible wrapper for node-based consumers.
     *
     * @param nodeId the node identifier
     * @return the committed offset
     */
    default long getOffset(NodeId nodeId) {
        return getOffset(nodeId.value());
    }

    /**
     * Backward-compatible wrapper for node-based consumers.
     *
     * @param nodeId the node identifier
     * @param offset the new offset value
     */
    default void updateOffset(NodeId nodeId, long offset) {
        updateOffset(nodeId.value(), offset);
    }
}
