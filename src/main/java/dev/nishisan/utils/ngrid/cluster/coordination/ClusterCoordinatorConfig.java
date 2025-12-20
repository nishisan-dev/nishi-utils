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

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the cluster coordinator.
 */
public final class ClusterCoordinatorConfig {
    private final Duration heartbeatInterval;
    private final Duration heartbeatTimeout;
    private final int minClusterSize;

    private ClusterCoordinatorConfig(Duration heartbeatInterval, Duration heartbeatTimeout, int minClusterSize) {
        this.heartbeatInterval = heartbeatInterval;
        this.heartbeatTimeout = heartbeatTimeout;
        this.minClusterSize = minClusterSize;
    }

    public static ClusterCoordinatorConfig defaults() {
        return new ClusterCoordinatorConfig(Duration.ofSeconds(1), Duration.ofSeconds(5), 1);
    }

    public static ClusterCoordinatorConfig of(Duration interval, Duration timeout) {
        return of(interval, timeout, 1);
    }

    public static ClusterCoordinatorConfig of(Duration interval, Duration timeout, int minClusterSize) {
        Objects.requireNonNull(interval, "interval");
        Objects.requireNonNull(timeout, "timeout");
        if (minClusterSize < 1) {
            throw new IllegalArgumentException("minClusterSize must be >= 1");
        }
        return new ClusterCoordinatorConfig(interval, timeout, minClusterSize);
    }

    public Duration heartbeatInterval() {
        return heartbeatInterval;
    }

    public Duration heartbeatTimeout() {
        return heartbeatTimeout;
    }

    public int minClusterSize() {
        return minClusterSize;
    }
}
