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

    private ReplicationConfig(int quorum, Duration operationTimeout, Duration retryInterval, boolean strictConsistency,
            Path dataDirectory) {
        this.quorum = quorum;
        this.operationTimeout = Objects.requireNonNull(operationTimeout, "operationTimeout");
        this.retryInterval = Objects.requireNonNull(retryInterval, "retryInterval");
        this.strictConsistency = strictConsistency;
        this.dataDirectory = Objects.requireNonNull(dataDirectory, "dataDirectory");
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

    public static final class Builder {
        private final int quorum;
        private Duration operationTimeout = Duration.ofSeconds(30);
        private Duration retryInterval = Duration.ofSeconds(1);
        private boolean strictConsistency = false;
        private Path dataDirectory;

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

        public ReplicationConfig build() {
            if (dataDirectory == null) {
                throw new IllegalStateException("dataDirectory must be set");
            }
            return new ReplicationConfig(quorum, operationTimeout, retryInterval, strictConsistency, dataDirectory);
        }
    }
}
