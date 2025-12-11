package dev.nishisan.utils.ngrid.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Payload exchanged during the initial handshake containing local node metadata and the
 * peer list currently known by the sender.
 */
public final class HandshakePayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final NodeInfo local;
    private final Set<NodeInfo> peers;

    public HandshakePayload(NodeInfo local, Set<NodeInfo> peers) {
        this.local = Objects.requireNonNull(local, "local");
        this.peers = Collections.unmodifiableSet(new HashSet<>(Objects.requireNonNull(peers, "peers")));
    }

    public NodeInfo local() {
        return local;
    }

    public Set<NodeInfo> peers() {
        return peers;
    }
}
