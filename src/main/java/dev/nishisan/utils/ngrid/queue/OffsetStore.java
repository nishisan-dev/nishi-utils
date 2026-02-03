package dev.nishisan.utils.ngrid.queue;

import dev.nishisan.utils.ngrid.common.NodeId;

public interface OffsetStore {
    long getOffset(NodeId nodeId);

    void updateOffset(NodeId nodeId, long offset);
}
