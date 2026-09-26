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

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Transport abstraction providing basic messaging primitives between nodes.
 */
public interface Transport extends Closeable {
    void start();

    NodeInfo local();

    Collection<NodeInfo> peers();

    void addListener(TransportListener listener);

    void removeListener(TransportListener listener);

    void broadcast(ClusterMessage message);

    void send(ClusterMessage message);

    CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message);

    boolean isConnected(NodeId nodeId);

    boolean isReachable(NodeId nodeId);

    /**
     * Whether there is an <b>established proxy route</b> to the node (i.e. a direct link
     * failed and a relay was selected), as opposed to the optimistic default-direct route
     * that {@link #isReachable(NodeId)} reports for any known peer. Used to distinguish
     * genuine reachability (open connection or active proxy) from a merely-known peer.
     *
     * @return {@code true} only if a proxy route is currently in effect for the node
     */
    default boolean isProxied(NodeId nodeId) {
        return false;
    }

    void addPeer(NodeInfo peer);

    /**
     * Whether {@code nodeId} is a peer this transport forgot as departed and whose id is still
     * tombstoned: second-hand sources cannot bring it back until a direct handshake from it (a new
     * incarnation), an explicit {@link #addPeer} or the tombstone's expiry. Messages from such an id
     * that were already in flight when it was forgotten must not re-create state for it.
     *
     * @param nodeId the peer id
     * @return {@code true} while the id is tombstoned; {@code false} by default
     * @since 8.7.0
     */
    default boolean isDeparted(NodeId nodeId) {
        return false;
    }

    /**
     * Forgets a LEADER-ELIGIBLE peer for good (operator decommission, revisão #178, B9): the peer
     * leaves the known-peers set (so it no longer counts in the voter majority), its connections are
     * closed and its id is tombstoned for a long window so gossip from nodes that still list it does
     * not bring it back. A voter is never forgotten on its own (a graceful LEAVE only marks it
     * inactive), so replacing a storage under a new id used to raise the required majority forever.
     * Run it on EVERY node of the cluster (the ngrrd admin CLI fans it out); listeners receive
     * {@link TransportListener#onPeerLeft(NodeId)}.
     *
     * @param nodeId the voter to decommission
     * @return {@code true} if the peer was known and is now forgotten; {@code false} by default
     * @since 8.8.0
     */
    default boolean decommissionPeer(NodeId nodeId) {
        return false;
    }

    /**
     * Current outbound replication queue depth per node (RF3, issue #113).
     * Implementations without per-connection buffering return an empty map.
     *
     * @return a snapshot of pending replication messages by node
     * @since 2.2.0
     */
    default Map<NodeId, Integer> outboundQueueDepths() {
        return Collections.emptyMap();
    }

    /**
     * Cumulative replication messages dropped by outbound backpressure per node
     * (issue #113). Implementations without backpressure return an empty map.
     *
     * @return a snapshot of dropped replication counts by node
     * @since 2.2.0
     */
    default Map<NodeId, Long> outboundDropped() {
        return Collections.emptyMap();
    }
}
