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

    private ReplicationConfig(int quorum, Duration operationTimeout, Duration retryInterval, boolean strictConsistency,
            Path dataDirectory, int resendGapThreshold, Duration resendTimeout, int replicationLogRetention) {
        this.quorum = quorum;
        this.operationTimeout = Objects.requireNonNull(operationTimeout, "operationTimeout");
        this.retryInterval = Objects.requireNonNull(retryInterval, "retryInterval");
        this.strictConsistency = strictConsistency;
        this.dataDirectory = Objects.requireNonNull(dataDirectory, "dataDirectory");
        this.resendGapThreshold = resendGapThreshold;
        this.resendTimeout = Objects.requireNonNull(resendTimeout, "resendTimeout");
        this.replicationLogRetention = replicationLogRetention;
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

    public static final class Builder {
        private final int quorum;
        private Duration operationTimeout = Duration.ofSeconds(30);
        private Duration retryInterval = Duration.ofSeconds(1);
        private boolean strictConsistency = true;
        private Path dataDirectory;
        private int resendGapThreshold = 50;
        private Duration resendTimeout = Duration.ofSeconds(2);
        private int replicationLogRetention = 1000;

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

        public ReplicationConfig build() {
            if (dataDirectory == null) {
                throw new IllegalStateException("dataDirectory must be set");
            }
            return new ReplicationConfig(quorum, operationTimeout, retryInterval, strictConsistency, dataDirectory,
                    resendGapThreshold, resendTimeout, replicationLogRetention);
        }
    }
}
