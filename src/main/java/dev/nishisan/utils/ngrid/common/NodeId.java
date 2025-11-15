package dev.nishisan.utils.ngrid.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable identifier for a cluster node. The identifier is globally unique and comparable
 * to allow deterministic leader election across the cluster.
 */
public final class NodeId implements Comparable<NodeId>, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final String value;

    private NodeId(String value) {
        this.value = value;
    }

    public static NodeId randomId() {
        return new NodeId(UUID.randomUUID().toString());
    }

    public static NodeId of(String value) {
        return new NodeId(Objects.requireNonNull(value, "value"));
    }

    public String value() {
        return value;
    }

    @Override
    public int compareTo(NodeId other) {
        return this.value.compareTo(other.value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeId nodeId)) return false;
        return value.equals(nodeId.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return value;
    }
}
