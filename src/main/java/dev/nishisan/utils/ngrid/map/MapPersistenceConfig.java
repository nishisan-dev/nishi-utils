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

    /**
     * Creates a default configuration.
     *
     * @param mapDirectory the persistence directory
     * @param mapName      the map name
     * @param mode         the persistence mode
     * @return the config
     */
    public static MapPersistenceConfig defaults(Path mapDirectory, String mapName, MapPersistenceMode mode) {
        return builder(mapDirectory, mapName)
                .mode(mode)
                .snapshotIntervalOperations(10_000)
                .snapshotIntervalTime(Duration.ofMinutes(5))
                .batchSize(100)
                .batchTimeout(Duration.ofMillis(10))
                .build();
    }

    /**
     * Creates a builder.
     *
     * @param mapDirectory the persistence directory
     * @param mapName      the map name
     * @return the builder
     */
    public static Builder builder(Path mapDirectory, String mapName) {
        return new Builder(mapDirectory, mapName);
    }

    /**
     * Returns the persistence mode.
     * 
     * @return the mode
     */
    public MapPersistenceMode mode() {
        return mode;
    }

    /**
     * Returns the persistence directory.
     * 
     * @return the directory
     */
    public Path mapDirectory() {
        return mapDirectory;
    }

    /**
     * Returns the map name.
     * 
     * @return the map name
     */
    public String mapName() {
        return mapName;
    }

    /**
     * Returns the snapshot interval in operations.
     * 
     * @return the interval
     */
    public int snapshotIntervalOperations() {
        return snapshotIntervalOperations;
    }

    /**
     * Returns the snapshot interval in time.
     * 
     * @return the interval
     */
    public Duration snapshotIntervalTime() {
        return snapshotIntervalTime;
    }

    /**
     * Returns the WAL batch size.
     * 
     * @return the batch size
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * Returns the WAL batch timeout.
     * 
     * @return the timeout
     */
    public Duration batchTimeout() {
        return batchTimeout;
    }

    /**
     * Returns the health listener.
     * 
     * @return the listener
     */
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

    /**
     * Builder for {@link MapPersistenceConfig}.
     */
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

        /**
         * Sets the persistence mode.
         * 
         * @param mode the mode
         * @return this builder
         */
        public Builder mode(MapPersistenceMode mode) {
            this.mode = Objects.requireNonNull(mode, "mode");
            return this;
        }

        /**
         * Sets the snapshot interval in operations.
         * 
         * @param operations the count
         * @return this builder
         */
        public Builder snapshotIntervalOperations(int operations) {
            this.snapshotIntervalOperations = operations;
            return this;
        }

        /**
         * Sets the snapshot interval in time.
         * 
         * @param time the duration
         * @return this builder
         */
        public Builder snapshotIntervalTime(Duration time) {
            this.snapshotIntervalTime = Objects.requireNonNull(time, "time");
            return this;
        }

        /**
         * Sets the WAL batch size.
         * 
         * @param batchSize the batch size
         * @return this builder
         */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /**
         * Sets the WAL batch timeout.
         * 
         * @param timeout the timeout
         * @return this builder
         */
        public Builder batchTimeout(Duration timeout) {
            this.batchTimeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        /**
         * Sets the health listener.
         * 
         * @param listener the listener
         * @return this builder
         */
        public Builder healthListener(PersistenceHealthListener listener) {
            this.healthListener = Objects.requireNonNull(listener, "healthListener");
            return this;
        }

        /**
         * Builds the configuration.
         * 
         * @return the config
         */
        public MapPersistenceConfig build() {
            return new MapPersistenceConfig(this);
        }
    }
}
