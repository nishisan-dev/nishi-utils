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

    synchronized void touch() {
        lastHeartbeat.set(Instant.now().toEpochMilli());
        active = true;
    }

    /**
     * Marks the member inactive only if its last heartbeat is still older than {@code thresholdMs}
     * (revisão #178, B10): the eviction sweep's check-then-mark is otherwise not atomic with a
     * concurrent {@link #touch()}, and a heartbeat landing between the two was lost for a cycle.
     *
     * @return {@code true} when the member was active and is now inactive
     */
    synchronized boolean markInactiveIfStale(long thresholdMs) {
        if (!active || lastHeartbeat.get() > thresholdMs) {
            return false;
        }
        active = false;
        return true;
    }

    /** True when the member was inactive before this {@link #touch()} (reactivation). */
    synchronized boolean touchAndReportReactivation() {
        boolean wasInactive = !active;
        touch();
        return wasInactive;
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
