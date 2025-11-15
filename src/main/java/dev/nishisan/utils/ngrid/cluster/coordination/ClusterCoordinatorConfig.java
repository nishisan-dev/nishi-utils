package dev.nishisan.utils.ngrid.cluster.coordination;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the cluster coordinator.
 */
public final class ClusterCoordinatorConfig {
    private final Duration heartbeatInterval;
    private final Duration heartbeatTimeout;

    private ClusterCoordinatorConfig(Duration heartbeatInterval, Duration heartbeatTimeout) {
        this.heartbeatInterval = heartbeatInterval;
        this.heartbeatTimeout = heartbeatTimeout;
    }

    public static ClusterCoordinatorConfig defaults() {
        return new ClusterCoordinatorConfig(Duration.ofSeconds(1), Duration.ofSeconds(5));
    }

    public static ClusterCoordinatorConfig of(Duration interval, Duration timeout) {
        Objects.requireNonNull(interval, "interval");
        Objects.requireNonNull(timeout, "timeout");
        return new ClusterCoordinatorConfig(interval, timeout);
    }

    public Duration heartbeatInterval() {
        return heartbeatInterval;
    }

    public Duration heartbeatTimeout() {
        return heartbeatTimeout;
    }
}
