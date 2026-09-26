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

package dev.nishisan.utils.ngrid.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Payload for broadcasting updated peer lists across the cluster.
 */
public final class PeerUpdatePayload {

    private final Set<NodeInfo> peers;
    private final Map<NodeId, Double> latencies;
    /**
     * Peers the sender forgot as departed (8.7.0), with the remaining time-to-live of each tombstone in
     * milliseconds. Admission-only for the receiver: it blocks second-hand re-admission of those ids and
     * lets a node that never reached the leaver forget it too, but never evicts a peer the receiver
     * holds a handshaked connection to, nor a leader-eligible one. Absent in updates of older nodes
     * (empty), and ignored by them ({@code FAIL_ON_UNKNOWN_PROPERTIES} is off).
     */
    private final Map<NodeId, Long> departed;
    /**
     * Ids of the peers the sender currently holds an open connection to (8.8.0): the peers it can relay
     * to right now. {@code null} in updates of older nodes, whose peer list then still counts as
     * reachability evidence (see {@link HandshakePayload#connectedPeers()}).
     */
    private final Set<NodeId> connectedPeers;

    public PeerUpdatePayload(Set<NodeInfo> peers, Map<NodeId, Double> latencies) {
        this(peers, latencies, null, null);
    }

    /**
     * Creates an update without the connected-peer report ({@code connectedPeers} absent, as in the wire
     * form of 8.7.0).
     *
     * @param peers     the peers known to the sender
     * @param latencies latencies measured by the sender
     * @param departed  departed peers with the remaining tombstone TTL, or {@code null}
     */
    public PeerUpdatePayload(Set<NodeInfo> peers, Map<NodeId, Double> latencies, Map<NodeId, Long> departed) {
        this(peers, latencies, departed, null);
    }

    @JsonCreator
    public PeerUpdatePayload(
            @JsonProperty("peers") Set<NodeInfo> peers,
            @JsonProperty("latencies") Map<NodeId, Double> latencies,
            @JsonProperty("departed") Map<NodeId, Long> departed,
            @JsonProperty("connectedPeers") Set<NodeId> connectedPeers) {
        this.peers = Collections.unmodifiableSet(new HashSet<>(Objects.requireNonNull(peers, "peers")));
        this.latencies = Collections.unmodifiableMap(new HashMap<>(Objects.requireNonNull(latencies, "latencies")));
        this.departed = departed == null ? Map.of() : Collections.unmodifiableMap(new HashMap<>(departed));
        this.connectedPeers = connectedPeers == null ? null : Collections.unmodifiableSet(new HashSet<>(connectedPeers));
    }

    /**
     * Ids of the peers the sender holds an open connection to, or {@code null} when the update predates
     * the field.
     *
     * @return the sender's connected peer ids, or {@code null} for updates without the field
     * @since 8.8.0
     */
    public Set<NodeId> connectedPeers() {
        return connectedPeers;
    }

    public Set<NodeInfo> peers() {
        return peers;
    }

    public Map<NodeId, Double> latencies() {
        return latencies;
    }

    /**
     * Departed peers known to the sender, with the remaining tombstone time-to-live in milliseconds;
     * empty for updates of nodes that predate the field.
     *
     * @return departed peer ids and their remaining tombstone TTL (ms)
     * @since 8.7.0
     */
    public Map<NodeId, Long> departed() {
        return departed;
    }
}
