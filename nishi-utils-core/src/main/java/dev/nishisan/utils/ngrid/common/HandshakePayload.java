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
 * Payload exchanged during the initial handshake containing local node metadata
 * and the peer list currently known by the sender.
 */
public final class HandshakePayload {

    private final NodeInfo local;
    private final Set<NodeInfo> peers;
    private final Map<NodeId, Double> latencies;
    private final boolean supportsCompression;

    /**
     * Creates a handshake payload without latency information.
     *
     * @param local the local node information
     * @param peers the set of known peers
     */
    public HandshakePayload(NodeInfo local, Set<NodeInfo> peers) {
        this(local, peers, Collections.emptyMap(), true);
    }

    /**
     * Creates a handshake payload with latency information, advertising compression support.
     *
     * @param local     the local node information
     * @param peers     the set of known peers
     * @param latencies measured latencies to known peers
     */
    public HandshakePayload(NodeInfo local, Set<NodeInfo> peers, Map<NodeId, Double> latencies) {
        this(local, peers, latencies, true);
    }

    /**
     * Creates a handshake payload with latency information and an explicit compression
     * capability flag.
     *
     * <p>{@code supportsCompression} advertises whether this node can handle LZ4-compressed
     * transport frames. It is the {@link JsonCreator} target, so a payload from an older node
     * that never serialized this field deserializes it as {@code false} (the primitive default) —
     * meaning peers must not compress towards it. Newer nodes default this to {@code true} via the
     * convenience constructors above.
     *
     * @param local               the local node information
     * @param peers               the set of known peers
     * @param latencies           measured latencies to known peers
     * @param supportsCompression whether this node accepts LZ4-compressed transport frames
     */
    @JsonCreator
    public HandshakePayload(
            @JsonProperty("local") NodeInfo local,
            @JsonProperty("peers") Set<NodeInfo> peers,
            @JsonProperty("latencies") Map<NodeId, Double> latencies,
            @JsonProperty("supportsCompression") boolean supportsCompression) {
        this.local = Objects.requireNonNull(local, "local");
        this.peers = Collections.unmodifiableSet(new HashSet<>(Objects.requireNonNull(peers, "peers")));
        this.latencies = Collections.unmodifiableMap(new HashMap<>(Objects.requireNonNull(latencies, "latencies")));
        this.supportsCompression = supportsCompression;
    }

    public NodeInfo local() {
        return local;
    }

    public Set<NodeInfo> peers() {
        return peers;
    }

    public Map<NodeId, Double> latencies() {
        return latencies;
    }

    /**
     * Whether the sending node accepts LZ4-compressed transport frames.
     *
     * @return {@code true} if the peer can decode compressed frames; {@code false} for legacy
     *         nodes that did not advertise the capability
     */
    public boolean supportsCompression() {
        return supportsCompression;
    }
}
