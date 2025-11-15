package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

/**
 * Listener for transport events including message delivery and peer connectivity changes.
 */
public interface TransportListener {
    void onPeerConnected(NodeInfo peer);

    void onPeerDisconnected(NodeId peerId);

    void onMessage(ClusterMessage message);
}
