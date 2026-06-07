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

package dev.nishisan.utils.ngrid.replication;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for the replication manager.
 */
public final class ReplicationConfig {
    private final int quorum;
    private final Duration operationTimeout;
    private final Duration retryInterval;
    private final boolean strictConsistency;
    private final Path dataDirectory;
    private final int resendGapThreshold;
    private final Duration resendTimeout;
    private final int replicationLogRetention;
    private final int appliedSetMaxSize;
    private final int operationLogMaxSize;
    private final boolean leaderLocalApply;

    private ReplicationConfig(int quorum, Duration operationTimeout, Duration retryInterval, boolean strictConsistency,
            Path dataDirectory, int resendGapThreshold, Duration resendTimeout, int replicationLogRetention,
            int appliedSetMaxSize, int operationLogMaxSize, boolean leaderLocalApply) {
        this.quorum = quorum;
        this.operationTimeout = Objects.requireNonNull(operationTimeout, "operationTimeout");
        this.retryInterval = Objects.requireNonNull(retryInterval, "retryInterval");
        this.strictConsistency = strictConsistency;
        this.dataDirectory = Objects.requireNonNull(dataDirectory, "dataDirectory");
        this.resendGapThreshold = resendGapThreshold;
        this.resendTimeout = Objects.requireNonNull(resendTimeout, "resendTimeout");
        this.replicationLogRetention = replicationLogRetention;
        this.appliedSetMaxSize = appliedSetMaxSize;
        this.operationLogMaxSize = operationLogMaxSize;
        this.leaderLocalApply = leaderLocalApply;
    }

    public static ReplicationConfig of(int quorum) {
        return builder(quorum).build();
    }

    public static ReplicationConfig of(int quorum, Duration operationTimeout) {
        return builder(quorum)
                .operationTimeout(operationTimeout)
                .build();
    }

    public static Builder builder(int quorum) {
        return new Builder(quorum);
    }

    public int quorum() {
        return quorum;
    }

    public Duration operationTimeout() {
        return operationTimeout;
    }

    public Duration retryInterval() {
        return retryInterval;
    }

    public boolean strictConsistency() {
        return strictConsistency;
    }

    public Path dataDirectory() {
        return dataDirectory;
    }

    public int resendGapThreshold() {
        return resendGapThreshold;
    }

    public Duration resendTimeout() {
        return resendTimeout;
    }

    public int replicationLogRetention() {
        return replicationLogRetention;
    }

    /**
     * Maximum number of entries to keep in the dedup guard ({@code applied} set).
     * Oldest entries are evicted FIFO when the limit is reached.
     *
     * @return the cap for the applied-operations set
     */
    public int appliedSetMaxSize() {
        return appliedSetMaxSize;
    }

    /**
     * Maximum number of entries to keep in the operation audit log.
     * Committed entries are periodically trimmed when the limit is reached.
     *
     * @return the cap for the operation log map
     */
    public int operationLogMaxSize() {
        return operationLogMaxSize;
    }

    /**
     * Whether the leader applies committed operations to its OWN local state via the registered
     * {@link ReplicationHandler}. Defaults to {@code true} (correct for backends where the leader is
     * the source of truth, e.g. DistributedMap).
     *
     * <p>Set to {@code false} when an external engine already owns the authoritative state and only
     * uses the op-log to ship deltas to followers (e.g. Cardinal's correlation engine). In that mode
     * the leader-local apply would be redundant work that builds an unbounded backlog at scale; the
     * manager instead commits and indexes each operation synchronously when quorum is met, so every
     * sent operation is immediately resendable to a catching-up follower.</p>
     *
     * @return {@code true} to apply on the leader, {@code false} to skip the redundant apply
     */
    public boolean leaderLocalApply() {
        return leaderLocalApply;
    }

    public static final class Builder {
        private final int quorum;
        private Duration operationTimeout = Duration.ofSeconds(30);
        private Duration retryInterval = Duration.ofSeconds(1);
        private boolean strictConsistency = true;
        private Path dataDirectory;
        private int resendGapThreshold = 50;
        private Duration resendTimeout = Duration.ofSeconds(2);
        private int replicationLogRetention = 1000;
        private int appliedSetMaxSize = 5000;
        private int operationLogMaxSize = 2000;
        private boolean leaderLocalApply = true;

        private Builder(int quorum) {
            if (quorum < 1) {
                throw new IllegalArgumentException("Quorum must be >= 1");
            }
            this.quorum = quorum;
        }

        public Builder operationTimeout(Duration operationTimeout) {
            this.operationTimeout = Objects.requireNonNull(operationTimeout, "operationTimeout");
            return this;
        }

        public Builder retryInterval(Duration retryInterval) {
            Objects.requireNonNull(retryInterval, "retryInterval");
            if (retryInterval.isNegative() || retryInterval.isZero()) {
                throw new IllegalArgumentException("retryInterval must be positive");
            }
            this.retryInterval = retryInterval;
            return this;
        }

        public Builder strictConsistency(boolean strict) {
            this.strictConsistency = strict;
            return this;
        }

        public Builder dataDirectory(Path dataDirectory) {
            this.dataDirectory = Objects.requireNonNull(dataDirectory, "dataDirectory");
            return this;
        }

        public Builder resendGapThreshold(int threshold) {
            if (threshold < 1) {
                throw new IllegalArgumentException("resendGapThreshold must be >= 1");
            }
            this.resendGapThreshold = threshold;
            return this;
        }

        public Builder resendTimeout(Duration timeout) {
            Objects.requireNonNull(timeout, "resendTimeout");
            if (timeout.isNegative() || timeout.isZero()) {
                throw new IllegalArgumentException("resendTimeout must be positive");
            }
            this.resendTimeout = timeout;
            return this;
        }

        public Builder replicationLogRetention(int retention) {
            if (retention < 1) {
                throw new IllegalArgumentException("replicationLogRetention must be >= 1");
            }
            this.replicationLogRetention = retention;
            return this;
        }

        /**
         * Sets the maximum number of entries in the applied-operations dedup set.
         * Oldest UUIDs are evicted FIFO when the limit is exceeded.
         *
         * @param maxSize maximum entries (must be >= 1)
         * @return this builder
         */
        public Builder appliedSetMaxSize(int maxSize) {
            if (maxSize < 1) {
                throw new IllegalArgumentException("appliedSetMaxSize must be >= 1");
            }
            this.appliedSetMaxSize = maxSize;
            return this;
        }

        /**
         * Sets the maximum number of entries in the operation audit log.
         * Committed entries are periodically trimmed when the limit is exceeded.
         *
         * @param maxSize maximum entries (must be >= 1)
         * @return this builder
         */
        public Builder operationLogMaxSize(int maxSize) {
            if (maxSize < 1) {
                throw new IllegalArgumentException("operationLogMaxSize must be >= 1");
            }
            this.operationLogMaxSize = maxSize;
            return this;
        }

        /**
         * Controls whether the leader applies committed operations to its own local state through
         * the registered handler. Leave {@code true} for source-of-truth backends; set {@code false}
         * when an external engine owns the state and the op-log is delta-shipping only.
         *
         * @param leaderLocalApply {@code true} to apply on the leader, {@code false} to skip it
         * @return this builder
         */
        public Builder leaderLocalApply(boolean leaderLocalApply) {
            this.leaderLocalApply = leaderLocalApply;
            return this;
        }

        public ReplicationConfig build() {
            if (dataDirectory == null) {
                throw new IllegalStateException("dataDirectory must be set");
            }
            return new ReplicationConfig(quorum, operationTimeout, retryInterval, strictConsistency, dataDirectory,
                    resendGapThreshold, resendTimeout, replicationLogRetention,
                    appliedSetMaxSize, operationLogMaxSize, leaderLocalApply);
        }
    }
}
