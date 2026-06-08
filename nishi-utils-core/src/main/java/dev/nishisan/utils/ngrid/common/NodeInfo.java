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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Describes a node in the cluster including host/port information necessary to establish
 * a TCP connection.
 */
public final class NodeInfo {

    private final NodeId nodeId;
    private final String host;
    private final int port;
    private final Set<String> roles;
    private final int priority;

    public NodeInfo(NodeId nodeId, String host, int port) {
        this(nodeId, host, port, Collections.emptySet(), 0);
    }

    public NodeInfo(NodeId nodeId, String host, int port, Set<String> roles) {
        this(nodeId, host, port, roles, 0);
    }

    @JsonCreator
    public NodeInfo(
            @JsonProperty("nodeId") NodeId nodeId,
            @JsonProperty("host") String host,
            @JsonProperty("port") int port,
            @JsonProperty("roles") Set<String> roles,
            @JsonProperty("priority") int priority) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.roles = Collections.unmodifiableSet(new HashSet<>(Objects.requireNonNull(roles, "roles")));
        this.priority = priority;
    }

    /**
     * Returns a copy of this node info carrying the given leadership priority.
     *
     * @param priority the leadership affinity (higher = preferred leader)
     * @return a new {@code NodeInfo} with {@code priority} applied
     */
    public NodeInfo withPriority(int priority) {
        return new NodeInfo(nodeId, host, port, roles, priority);
    }

    public NodeId nodeId() {
        return nodeId;
    }

    /**
     * Returns this node's leadership priority (affinity). Higher values are preferred when electing
     * a leader; ties are broken deterministically by {@link NodeId}. Defaults to {@code 0}.
     *
     * @return the leadership priority
     */
    public int priority() {
        return priority;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public Set<String> roles() {
        return roles;
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
        return nodeId + "@" + host + ':' + port + roles + (priority != 0 ? " prio=" + priority : "");
    }
}
