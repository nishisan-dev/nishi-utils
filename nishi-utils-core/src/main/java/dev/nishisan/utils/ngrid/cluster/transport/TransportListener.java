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
     * Invoked when the transport <b>forgets</b> a departed peer: the id left the known-peer set for
     * good (a graceful LEAVE of an ephemeral member, or an ephemeral member disconnected for too long)
     * and is tombstoned against second-hand re-admission. Unlike {@link #onPeerDisconnected(NodeId)},
     * no reconnection is expected; per-peer state may be dropped. A later direct handshake from the
     * same id is a new incarnation and is reported through {@link #onPeerConnected(NodeInfo)}.
     * <p>
     * Defaults to {@link #onPeerDisconnected(NodeId)}; the transport does not report the same
     * departure again as a plain disconnect.
     *
     * @param peerId identifier of the forgotten peer
     * @since 8.7.0
     */
    default void onPeerLeft(NodeId peerId) {
        onPeerDisconnected(peerId);
    }

    /**
     * Invoked when a <b>leader-eligible</b> peer announced, first-hand, that it is closing (LEAVE). The
     * peer is not forgotten — it stays a known voter, so the leadership majority is never shrunk
     * without consensus — but its connection is already closed and it is not coming back soon, so the
     * disconnect grace may be skipped. The regular {@link #onPeerDisconnected(NodeId)} follows.
     * No-op by default.
     *
     * @param peerId identifier of the leaving peer
     * @since 8.7.0
     */
    default void onPeerLeaving(NodeId peerId) {
    }

    /**
     * Invoked when a cluster message is received from a peer.
     *
     * @param message the received cluster message
     */
    void onMessage(ClusterMessage message);
}
