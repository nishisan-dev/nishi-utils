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

package dev.nishisan.utils.ngrid.map;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for local map persistence (WAL + snapshots).
 */
public final class MapPersistenceConfig {
    private final MapPersistenceMode mode;
    private final Path mapDirectory;
    private final String mapName;

    private final int snapshotIntervalOperations;
    private final Duration snapshotIntervalTime;

    private final int batchSize;
    private final Duration batchTimeout;
    private final PersistenceHealthListener healthListener;

    private MapPersistenceConfig(Builder builder) {
        this.mode = Objects.requireNonNull(builder.mode, "mode");
        this.mapDirectory = Objects.requireNonNull(builder.mapDirectory, "mapDirectory");
        this.mapName = Objects.requireNonNull(builder.mapName, "mapName");
        this.snapshotIntervalOperations = builder.snapshotIntervalOperations;
        this.snapshotIntervalTime = Objects.requireNonNull(builder.snapshotIntervalTime, "snapshotIntervalTime");
        this.batchSize = builder.batchSize;
        this.batchTimeout = Objects.requireNonNull(builder.batchTimeout, "batchTimeout");
        this.healthListener = builder.healthListener;
        validate();
    }

    public static MapPersistenceConfig defaults(Path mapDirectory, String mapName, MapPersistenceMode mode) {
        return builder(mapDirectory, mapName)
                .mode(mode)
                .snapshotIntervalOperations(10_000)
                .snapshotIntervalTime(Duration.ofMinutes(5))
                .batchSize(100)
                .batchTimeout(Duration.ofMillis(10))
                .build();
    }

    public static Builder builder(Path mapDirectory, String mapName) {
        return new Builder(mapDirectory, mapName);
    }

    public MapPersistenceMode mode() {
        return mode;
    }

    public Path mapDirectory() {
        return mapDirectory;
    }

    public String mapName() {
        return mapName;
    }

    public int snapshotIntervalOperations() {
        return snapshotIntervalOperations;
    }

    public Duration snapshotIntervalTime() {
        return snapshotIntervalTime;
    }

    public int batchSize() {
        return batchSize;
    }

    public Duration batchTimeout() {
        return batchTimeout;
    }

    public PersistenceHealthListener healthListener() {
        return healthListener;
    }

    private void validate() {
        if (mapName.isBlank()) {
            throw new IllegalArgumentException("mapName cannot be blank");
        }
        if (snapshotIntervalOperations < 0) {
            throw new IllegalArgumentException("snapshotIntervalOperations must be >= 0");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0");
        }
        if (snapshotIntervalTime.isNegative()) {
            throw new IllegalArgumentException("snapshotIntervalTime cannot be negative");
        }
        if (batchTimeout.isNegative() || batchTimeout.isZero()) {
            throw new IllegalArgumentException("batchTimeout must be > 0");
        }
    }

    public static final class Builder {
        private final Path mapDirectory;
        private final String mapName;
        private MapPersistenceMode mode = MapPersistenceMode.DISABLED;
        private int snapshotIntervalOperations = 10_000;
        private Duration snapshotIntervalTime = Duration.ofMinutes(5);
        private int batchSize = 100;
        private Duration batchTimeout = Duration.ofMillis(10);
        private PersistenceHealthListener healthListener = (name, type, cause) -> {
        };

        private Builder(Path mapDirectory, String mapName) {
            this.mapDirectory = Objects.requireNonNull(mapDirectory, "mapDirectory");
            this.mapName = Objects.requireNonNull(mapName, "mapName");
        }

        public Builder mode(MapPersistenceMode mode) {
            this.mode = Objects.requireNonNull(mode, "mode");
            return this;
        }

        public Builder snapshotIntervalOperations(int operations) {
            this.snapshotIntervalOperations = operations;
            return this;
        }

        public Builder snapshotIntervalTime(Duration time) {
            this.snapshotIntervalTime = Objects.requireNonNull(time, "time");
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder batchTimeout(Duration timeout) {
            this.batchTimeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        public Builder healthListener(PersistenceHealthListener listener) {
            this.healthListener = Objects.requireNonNull(listener, "healthListener");
            return this;
        }

        public MapPersistenceConfig build() {
            return new MapPersistenceConfig(this);
        }
    }
}
