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
