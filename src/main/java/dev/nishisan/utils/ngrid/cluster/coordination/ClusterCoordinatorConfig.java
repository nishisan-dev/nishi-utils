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
