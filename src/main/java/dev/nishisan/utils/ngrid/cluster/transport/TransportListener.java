/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

/**
 * Listener for transport events including message delivery and peer
 * connectivity changes.
 */
public interface TransportListener {
    /**
     * Invoked when a new peer connects to the transport layer.
     *
     * @param peer information about the connected peer
     */
    void onPeerConnected(NodeInfo peer);

    /**
     * Invoked when a peer disconnects from the transport layer.
     *
     * @param peerId identifier of the disconnected peer
     */
    void onPeerDisconnected(NodeId peerId);

    /**
     * Invoked when a cluster message is received from a peer.
     *
     * @param message the received cluster message
     */
    void onMessage(ClusterMessage message);
}
