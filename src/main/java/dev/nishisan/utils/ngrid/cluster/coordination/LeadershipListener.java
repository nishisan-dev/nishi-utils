package dev.nishisan.utils.ngrid.cluster.coordination;

import dev.nishisan.utils.ngrid.common.NodeId;

/**
 * Callback invoked whenever the local node observes a leader change.
 */
@FunctionalInterface
public interface LeadershipListener {
    void onLeaderChanged(NodeId newLeader);
}
