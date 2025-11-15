package dev.nishisan.utils.ngrid.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Describes a node in the cluster including host/port information necessary to establish
 * a TCP connection.
 */
public final class NodeInfo implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final NodeId nodeId;
    private final String host;
    private final int port;

    public NodeInfo(NodeId nodeId, String host, int port) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
    }

    public NodeId nodeId() {
        return nodeId;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeInfo nodeInfo)) return false;
        return port == nodeInfo.port && nodeId.equals(nodeInfo.nodeId) && host.equals(nodeInfo.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, host, port);
    }

    @Override
    public String toString() {
        return nodeId + "@" + host + ':' + port;
    }
}
