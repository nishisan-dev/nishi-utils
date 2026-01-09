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

import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Payload exchanged during the initial handshake containing local node metadata and the
 * peer list currently known by the sender.
 */
public final class HandshakePayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 2L;

    private final NodeInfo local;
    private final Set<NodeInfo> peers;
    private final Map<NodeId, Double> latencies;

    public HandshakePayload(NodeInfo local, Set<NodeInfo> peers) {
        this(local, peers, Collections.emptyMap());
    }

    public HandshakePayload(NodeInfo local, Set<NodeInfo> peers, Map<NodeId, Double> latencies) {
        this.local = Objects.requireNonNull(local, "local");
        this.peers = Collections.unmodifiableSet(new HashSet<>(Objects.requireNonNull(peers, "peers")));
        this.latencies = Collections.unmodifiableMap(new HashMap<>(Objects.requireNonNull(latencies, "latencies")));
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
}
