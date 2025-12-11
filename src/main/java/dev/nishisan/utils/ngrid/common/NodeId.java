/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura@gmail.com>
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
