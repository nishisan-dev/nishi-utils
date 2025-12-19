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

import java.util.Objects;

/**
 * Queue-level configuration for distributed queues created on a grid node.
 */
public final class DistributedQueueConfig {
    private final String name;
    private final Integer replicationFactor;
    private final NQueue.Options queueOptions;

    private DistributedQueueConfig(Builder builder) {
        this.name = builder.name;
        this.replicationFactor = builder.replicationFactor;
        this.queueOptions = builder.queueOptions;
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public String name() {
        return name;
    }

    public Integer replicationFactor() {
        return replicationFactor;
    }

    public NQueue.Options queueOptions() {
        return queueOptions;
    }

    public static final class Builder {
        private final String name;
        private Integer replicationFactor;
        private NQueue.Options queueOptions;

        private Builder(String name) {
            this.name = Objects.requireNonNull(name, "name");
        }

        public Builder replicationFactor(int factor) {
            if (factor < 1) {
                throw new IllegalArgumentException("Replication factor must be >= 1");
            }
            this.replicationFactor = factor;
            return this;
        }

        public Builder queueOptions(NQueue.Options options) {
            this.queueOptions = Objects.requireNonNull(options, "options");
            return this;
        }

        public DistributedQueueConfig build() {
            return new DistributedQueueConfig(this);
        }
    }
}
