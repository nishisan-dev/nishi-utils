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

package dev.nishisan.utils.map;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for {@link NMap} local persistence (WAL + snapshots).
 */
public final class NMapConfig {
    private final NMapPersistenceMode mode;
    private final int snapshotIntervalOperations;
    private final Duration snapshotIntervalTime;
    private final int batchSize;
    private final Duration batchTimeout;
    private final NMapHealthListener healthListener;
    private final NMapOffloadStrategyFactory offloadStrategyFactory;
    private final OffloadMode offloadMode;
    private final int hotCacheMaxEntries;
    private final EvictionPolicy evictionPolicy;
    private final int shardDepth;
    private final int shardWidth;

    private NMapConfig(Builder builder) {
        this.mode = Objects.requireNonNull(builder.mode, "mode");
        this.snapshotIntervalOperations = builder.snapshotIntervalOperations;
        this.snapshotIntervalTime = Objects.requireNonNull(builder.snapshotIntervalTime, "snapshotIntervalTime");
        this.batchSize = builder.batchSize;
        this.batchTimeout = Objects.requireNonNull(builder.batchTimeout, "batchTimeout");
        this.healthListener = builder.healthListener;
        this.offloadStrategyFactory = builder.offloadStrategyFactory;
        this.offloadMode = Objects.requireNonNull(builder.offloadMode, "offloadMode");
        this.hotCacheMaxEntries = builder.hotCacheMaxEntries;
        this.evictionPolicy = Objects.requireNonNull(builder.evictionPolicy, "evictionPolicy");
        this.shardDepth = builder.shardDepth;
        this.shardWidth = builder.shardWidth;
        validate();
    }

    /**
     * Creates a default configuration with sensible production defaults.
     *
     * @param mode the persistence mode
     * @return the config
     */
    public static NMapConfig defaults(NMapPersistenceMode mode) {
        return builder()
                .mode(mode)
                .snapshotIntervalOperations(10_000)
                .snapshotIntervalTime(Duration.ofMinutes(5))
                .batchSize(100)
                .batchTimeout(Duration.ofMillis(10))
                .build();
    }

    /**
     * Creates a default in-memory-only configuration (no persistence).
     *
     * @return the config
     */
    public static NMapConfig inMemory() {
        return defaults(NMapPersistenceMode.DISABLED);
    }

    /**
     * Creates a new builder.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the persistence mode. */
    public NMapPersistenceMode mode() {
        return mode;
    }

    /** Returns the snapshot interval in operations. */
    public int snapshotIntervalOperations() {
        return snapshotIntervalOperations;
    }

    /** Returns the snapshot interval in time. */
    public Duration snapshotIntervalTime() {
        return snapshotIntervalTime;
    }

    /** Returns the WAL batch size. */
    public int batchSize() {
        return batchSize;
    }

    /** Returns the WAL batch timeout. */
    public Duration batchTimeout() {
        return batchTimeout;
    }

    /** Returns the health listener, or {@code null} if none configured. */
    public NMapHealthListener healthListener() {
        return healthListener;
    }

    /**
     * Returns the offload strategy factory, or {@code null} if none configured.
     * When set, it takes precedence over {@link #offloadMode()}.
     */
    public NMapOffloadStrategyFactory offloadStrategyFactory() {
        return offloadStrategyFactory;
    }

    /**
     * Returns the declarative offload mode (default {@link OffloadMode#IN_MEMORY}).
     * Ignored when an {@link #offloadStrategyFactory()} is set.
     */
    public OffloadMode offloadMode() {
        return offloadMode;
    }

    /**
     * Returns the upper bound on the in-memory hot cache for disk-backed
     * strategies ({@code 0} disables caching).
     */
    public int hotCacheMaxEntries() {
        return hotCacheMaxEntries;
    }

    /** Returns the eviction policy used by the {@link OffloadMode#HYBRID} mode. */
    public EvictionPolicy evictionPolicy() {
        return evictionPolicy;
    }

    /** Returns the shard depth (directory levels) for {@link OffloadMode#DISK}. */
    public int shardDepth() {
        return shardDepth;
    }

    /** Returns the shard width (hex chars per level) for {@link OffloadMode#DISK}. */
    public int shardWidth() {
        return shardWidth;
    }

    private void validate() {
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
        if (hotCacheMaxEntries < 0) {
            throw new IllegalArgumentException("hotCacheMaxEntries must be >= 0");
        }
        if (shardWidth < 1) {
            throw new IllegalArgumentException("shardWidth must be >= 1");
        }
        if (shardDepth < 0) {
            throw new IllegalArgumentException("shardDepth must be >= 0");
        }
        if (shardDepth * shardWidth > 40) {
            throw new IllegalArgumentException("shardDepth * shardWidth must be <= 40 (SHA-1 hex length)");
        }
    }

    /**
     * Builder for {@link NMapConfig}.
     */
    public static final class Builder {
        private NMapPersistenceMode mode = NMapPersistenceMode.DISABLED;
        private int snapshotIntervalOperations = 10_000;
        private Duration snapshotIntervalTime = Duration.ofMinutes(5);
        private int batchSize = 100;
        private Duration batchTimeout = Duration.ofMillis(10);
        private NMapHealthListener healthListener = (name, type, cause) -> {
        };
        private NMapOffloadStrategyFactory offloadStrategyFactory;
        private OffloadMode offloadMode = OffloadMode.IN_MEMORY;
        private int hotCacheMaxEntries = 10_000;
        private EvictionPolicy evictionPolicy = EvictionPolicy.LRU;
        private int shardDepth = OffloadLayout.DEFAULT_SHARD_DEPTH;
        private int shardWidth = OffloadLayout.DEFAULT_SHARD_WIDTH;

        private Builder() {
        }

        /** Sets the persistence mode. */
        public Builder mode(NMapPersistenceMode mode) {
            this.mode = Objects.requireNonNull(mode, "mode");
            return this;
        }

        /** Sets the snapshot interval in operations. */
        public Builder snapshotIntervalOperations(int operations) {
            this.snapshotIntervalOperations = operations;
            return this;
        }

        /** Sets the snapshot interval in time. */
        public Builder snapshotIntervalTime(Duration time) {
            this.snapshotIntervalTime = Objects.requireNonNull(time, "time");
            return this;
        }

        /** Sets the WAL batch size. */
        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /** Sets the WAL batch timeout. */
        public Builder batchTimeout(Duration timeout) {
            this.batchTimeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        /** Sets the health listener. */
        public Builder healthListener(NMapHealthListener listener) {
            this.healthListener = Objects.requireNonNull(listener, "healthListener");
            return this;
        }

        /**
         * Sets the offload strategy factory. When set, the strategy will be used
         * instead of the default in-memory storage. Pass {@code null} to use the
         * default in-memory strategy.
         */
        public Builder offloadStrategyFactory(NMapOffloadStrategyFactory factory) {
            this.offloadStrategyFactory = factory;
            return this;
        }

        /**
         * Selects the declarative offload backend. When set (and no
         * {@link #offloadStrategyFactory(NMapOffloadStrategyFactory)} is
         * provided), {@link NMap} assembles the matching strategy from the
         * remaining knobs. Default: {@link OffloadMode#IN_MEMORY}.
         */
        public Builder offloadMode(OffloadMode mode) {
            this.offloadMode = Objects.requireNonNull(mode, "offloadMode");
            return this;
        }

        /**
         * Sets the upper bound on the in-memory hot cache for disk-backed
         * strategies (also the {@code maxInMemoryEntries} of
         * {@link OffloadMode#HYBRID}). Use {@code 0} to disable caching.
         * Default: 10,000.
         */
        public Builder hotCacheMaxEntries(int max) {
            this.hotCacheMaxEntries = max;
            return this;
        }

        /** Sets the eviction policy for {@link OffloadMode#HYBRID}. Default: LRU. */
        public Builder evictionPolicy(EvictionPolicy policy) {
            this.evictionPolicy = Objects.requireNonNull(policy, "evictionPolicy");
            return this;
        }

        /**
         * Sets the shard fan-out for {@link OffloadMode#DISK}: {@code shardDepth}
         * directory levels of {@code shardWidth} hex characters each. Must
         * satisfy {@code shardDepth * shardWidth <= 40}. Default: 2 / 2.
         */
        public Builder shardFanOut(int shardDepth, int shardWidth) {
            this.shardDepth = shardDepth;
            this.shardWidth = shardWidth;
            return this;
        }

        /** Builds the configuration. */
        public NMapConfig build() {
            return new NMapConfig(this);
        }
    }
}
