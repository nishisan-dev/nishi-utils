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

package dev.nishisan.utils.ngrid.structures;

import dev.nishisan.utils.queue.NQueue;

import java.time.Duration;
import java.util.Objects;

/**
 * Configuration for a single distributed queue.
 * <p>
 * Encapsulates all settings needed to create and manage a distributed queue,
 * including retention policy, size limits, and NQueue-specific options.
 * 
 * @since 2.1.0
 */
public final class QueueConfig {
    private final String name;
    private final RetentionPolicy retention;
    private final Long maxSizeBytes;
    private final Integer maxItems;
    private final boolean compressionEnabled;
    private final NQueue.Options nqueueOptions;

    private QueueConfig(Builder builder) {
        this.name = builder.name;
        this.retention = builder.retention;
        this.maxSizeBytes = builder.maxSizeBytes;
        this.maxItems = builder.maxItems;
        this.compressionEnabled = builder.compressionEnabled;
        this.nqueueOptions = builder.nqueueOptions;
    }

    public String name() {
        return name;
    }

    public RetentionPolicy retention() {
        return retention;
    }

    public Long maxSizeBytes() {
        return maxSizeBytes;
    }

    public Integer maxItems() {
        return maxItems;
    }

    public boolean compressionEnabled() {
        return compressionEnabled;
    }

    public NQueue.Options nqueueOptions() {
        return nqueueOptions;
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    @Override
    public String toString() {
        return "QueueConfig{name='" + name + "', retention=" + retention + "}";
    }

    public static final class Builder {
        private final String name;
        private RetentionPolicy retention = RetentionPolicy.timeBased(Duration.ofDays(7));
        private Long maxSizeBytes;
        private Integer maxItems;
        private boolean compressionEnabled = false;
        private NQueue.Options nqueueOptions;

        private Builder(String name) {
            this.name = Objects.requireNonNull(name, "queue name cannot be null");
            if (name.trim().isEmpty()) {
                throw new IllegalArgumentException("queue name cannot be empty");
            }
        }

        /**
         * Sets the retention policy for this queue.
         */
        public Builder retention(RetentionPolicy policy) {
            this.retention = Objects.requireNonNull(policy, "retention policy cannot be null");
            return this;
        }

        /**
         * Sets the maximum size in bytes for this queue.
         */
        public Builder maxSizeBytes(long bytes) {
            if (bytes <= 0) {
                throw new IllegalArgumentException("maxSizeBytes must be positive");
            }
            this.maxSizeBytes = bytes;
            return this;
        }

        /**
         * Sets the maximum number of items for this queue.
         */
        public Builder maxItems(int items) {
            if (items <= 0) {
                throw new IllegalArgumentException("maxItems must be positive");
            }
            this.maxItems = items;
            return this;
        }

        /**
         * Enables or disables compression for queue items.
         */
        public Builder compressionEnabled(boolean enabled) {
            this.compressionEnabled = enabled;
            return this;
        }

        /**
         * Sets NQueue-specific options.
         * <p>
         * Note: The grid layer will override short-circuiting settings to preserve
         * ordering and disk-first semantics.
         */
        public Builder nqueueOptions(NQueue.Options options) {
            this.nqueueOptions = Objects.requireNonNull(options, "nqueueOptions cannot be null");
            return this;
        }

        public QueueConfig build() {
            return new QueueConfig(this);
        }
    }

    /**
     * Retention policy for distributed queues.
     */
    public static final class RetentionPolicy {
        public enum Type {
            TIME_BASED,
            SIZE_BASED,
            COUNT_BASED
        }

        private final Type type;
        private final Duration duration;
        private final Long maxSize;
        private final Integer maxCount;

        private RetentionPolicy(Type type, Duration duration, Long maxSize, Integer maxCount) {
            this.type = type;
            this.duration = duration;
            this.maxSize = maxSize;
            this.maxCount = maxCount;
        }

        /**
         * Creates a time-based retention policy.
         * Items older than the specified duration will be eligible for removal.
         */
        public static RetentionPolicy timeBased(Duration duration) {
            Objects.requireNonNull(duration, "duration cannot be null");
            if (duration.isNegative() || duration.isZero()) {
                throw new IllegalArgumentException("duration must be positive");
            }
            return new RetentionPolicy(Type.TIME_BASED, duration, null, null);
        }

        /**
         * Creates a size-based retention policy.
         * Queue will be trimmed when total size exceeds the specified limit.
         */
        public static RetentionPolicy sizeBased(long maxSizeBytes) {
            if (maxSizeBytes <= 0) {
                throw new IllegalArgumentException("maxSizeBytes must be positive");
            }
            return new RetentionPolicy(Type.SIZE_BASED, null, maxSizeBytes, null);
        }

        /**
         * Creates a count-based retention policy.
         * Queue will be trimmed when item count exceeds the specified limit.
         */
        public static RetentionPolicy countBased(int maxItems) {
            if (maxItems <= 0) {
                throw new IllegalArgumentException("maxItems must be positive");
            }
            return new RetentionPolicy(Type.COUNT_BASED, null, null, maxItems);
        }

        public Type type() {
            return type;
        }

        public Duration duration() {
            return duration;
        }

        public Long maxSize() {
            return maxSize;
        }

        public Integer maxCount() {
            return maxCount;
        }

        @Override
        public String toString() {
            switch (type) {
                case TIME_BASED:
                    return "TimeBased{" + duration + "}";
                case SIZE_BASED:
                    return "SizeBased{" + maxSize + " bytes}";
                case COUNT_BASED:
                    return "CountBased{" + maxCount + " items}";
                default:
                    return "Unknown";
            }
        }
    }
}
