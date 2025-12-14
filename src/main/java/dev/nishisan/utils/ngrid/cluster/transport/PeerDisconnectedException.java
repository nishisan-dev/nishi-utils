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

import dev.nishisan.utils.ngrid.common.NodeId;

import java.util.Objects;
import java.util.UUID;

/**
 * Raised when a peer disconnects while a request/response operation is in-flight.
 */
public final class PeerDisconnectedException extends RuntimeException {
    private final NodeId peerId;
    private final UUID requestId;

    public PeerDisconnectedException(NodeId peerId, UUID requestId) {
        super("Peer disconnected peerId=" + peerId + " requestId=" + requestId);
        this.peerId = Objects.requireNonNull(peerId, "peerId");
        this.requestId = Objects.requireNonNull(requestId, "requestId");
    }

    public NodeId peerId() {
        return peerId;
    }

    public UUID requestId() {
        return requestId;
    }
}


