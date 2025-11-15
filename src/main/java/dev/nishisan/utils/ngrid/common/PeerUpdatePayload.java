package dev.nishisan.utils.ngrid.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Broadcasts the known peers of a node so that a full mesh can be approximated over time.
 */
public final class PeerUpdatePayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final Set<NodeInfo> peers;

    public PeerUpdatePayload(Set<NodeInfo> peers) {
        this.peers = Collections.unmodifiableSet(new HashSet<>(Objects.requireNonNull(peers, "peers")));
    }

    public Set<NodeInfo> peers() {
        return peers;
    }
}
