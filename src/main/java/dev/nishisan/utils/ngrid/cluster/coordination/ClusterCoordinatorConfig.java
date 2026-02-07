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

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the cluster coordinator.
 */
public final class ClusterCoordinatorConfig {
    private final Duration heartbeatInterval;
    private final Duration heartbeatTimeout;
    private final Duration leaseTimeout;
    private final int minClusterSize;
    private final Path dataDirectory;

    private ClusterCoordinatorConfig(Duration heartbeatInterval, Duration heartbeatTimeout,
            Duration leaseTimeout, int minClusterSize, Path dataDirectory) {
        this.heartbeatInterval = heartbeatInterval;
        this.heartbeatTimeout = heartbeatTimeout;
        this.leaseTimeout = leaseTimeout;
        this.minClusterSize = minClusterSize;
        this.dataDirectory = dataDirectory;
    }

    /**
     * Returns a default configuration with 1 second heartbeat interval,
     * 5 second timeout, and minimum cluster size of 1.
     *
     * @return default configuration
     */
    public static ClusterCoordinatorConfig defaults() {
        Duration defaultTimeout = Duration.ofSeconds(5);
        return new ClusterCoordinatorConfig(Duration.ofSeconds(1), defaultTimeout,
                defaultTimeout.multipliedBy(3), 1, null);
    }

    /**
     * Creates a configuration with the given interval and timeout.
     *
     * @param interval heartbeat interval
     * @param timeout  heartbeat timeout
     * @return a new configuration
     */
    public static ClusterCoordinatorConfig of(Duration interval, Duration timeout) {
        return of(interval, timeout, null, 1, null);
    }

    /**
     * Creates a configuration with the given interval, timeout, and minimum cluster
     * size.
     *
     * @param interval       heartbeat interval
     * @param timeout        heartbeat timeout
     * @param minClusterSize minimum active members for leader election
     * @return a new configuration
     */
    public static ClusterCoordinatorConfig of(Duration interval, Duration timeout, int minClusterSize) {
        return of(interval, timeout, null, minClusterSize, null);
    }

    /**
     * Creates a configuration with the given interval, timeout, minimum cluster
     * size,
     * and data directory.
     *
     * @param interval       heartbeat interval
     * @param timeout        heartbeat timeout
     * @param minClusterSize minimum active members for leader election
     * @param dataDirectory  directory for persistent state
     * @return a new configuration
     */
    public static ClusterCoordinatorConfig of(Duration interval, Duration timeout, int minClusterSize,
            Path dataDirectory) {
        return of(interval, timeout, null, minClusterSize, dataDirectory);
    }

    /**
     * Creates a configuration with an explicit lease timeout.
     *
     * @param interval       heartbeat interval
     * @param timeout        heartbeat timeout (used to detect dead members)
     * @param leaseTimeout   leader lease timeout; if {@code null}, defaults to
     *                       {@code 3 × timeout}
     * @param minClusterSize minimum active members for leader election
     * @param dataDirectory  directory for persistent state (epoch file)
     * @return a new configuration
     */
    public static ClusterCoordinatorConfig of(Duration interval, Duration timeout, Duration leaseTimeout,
            int minClusterSize, Path dataDirectory) {
        Objects.requireNonNull(interval, "interval");
        Objects.requireNonNull(timeout, "timeout");
        if (minClusterSize < 1) {
            throw new IllegalArgumentException("minClusterSize must be >= 1");
        }
        Duration effectiveLease = leaseTimeout != null ? leaseTimeout : timeout.multipliedBy(3);
        return new ClusterCoordinatorConfig(interval, timeout, effectiveLease, minClusterSize, dataDirectory);
    }

    /**
     * Returns the heartbeat broadcast interval.
     *
     * @return heartbeat interval
     */
    public Duration heartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * Returns the heartbeat timeout used to detect dead members.
     *
     * @return heartbeat timeout
     */
    public Duration heartbeatTimeout() {
        return heartbeatTimeout;
    }

    /**
     * Returns the leader lease timeout. A leader that has not received
     * acknowledgment from a majority of followers within this duration will
     * step down automatically. Defaults to {@code 3 × heartbeatTimeout}.
     *
     * @return leader lease timeout
     */
    public Duration leaseTimeout() {
        return leaseTimeout;
    }

    /**
     * Returns the minimum number of active members required for leader election.
     *
     * @return minimum cluster size
     */
    public int minClusterSize() {
        return minClusterSize;
    }

    /**
     * Returns the directory used for persistent state (e.g. epoch file),
     * or {@code null} if no persistence is configured.
     *
     * @return data directory path, or {@code null}
     */
    public Path dataDirectory() {
        return dataDirectory;
    }
}
