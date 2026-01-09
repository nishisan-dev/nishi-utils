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

package dev.nishisan.utils.ngrid.cluster.coordination;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Internal representation of a cluster member with heartbeat tracking.
 */
final class ClusterMember {
    private final NodeInfo nodeInfo;
    private final AtomicLong lastHeartbeat = new AtomicLong();
    private volatile boolean active;

    ClusterMember(NodeInfo nodeInfo) {
        this.nodeInfo = Objects.requireNonNull(nodeInfo, "nodeInfo");
        touch();
        this.active = true;
    }

    NodeId id() {
        return nodeInfo.nodeId();
    }

    NodeInfo info() {
        return nodeInfo;
    }

    void touch() {
        lastHeartbeat.set(Instant.now().toEpochMilli());
        active = true;
    }

    long lastHeartbeat() {
        return lastHeartbeat.get();
    }

    void markInactive() {
        active = false;
    }

    boolean isActive() {
        return active;
    }
}
