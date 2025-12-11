package dev.nishisan.utils.ngrid.cluster.coordination;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

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
