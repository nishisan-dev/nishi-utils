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

import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.map.MapPersistenceMode;
import dev.nishisan.utils.queue.NQueue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Configuration container used to bootstrap an {@link NGridNode} instance.
 */
public final class NGridConfig {
    private final String clusterName;
    private final NodeInfo local;
    private final Set<NodeInfo> peers;
    private final int replicationQuorum;
    private final int replicationFactor;
    private final Duration replicationOperationTimeout;
    private final Duration rttProbeInterval;
    private final Duration heartbeatInterval;
    private final Duration leaseTimeout;
    private final boolean leaderReelectionEnabled;
    private final Duration leaderReelectionInterval;
    private final Duration leaderReelectionCooldown;
    private final Duration leaderReelectionSuggestionTtl;
    private final double leaderReelectionMinDelta;

    // New fields for multiple queues
    private final Path dataDirectory;
    private final List<QueueConfig> queues;

    // Legacy fields (deprecated but maintained for compatibility)
    @Deprecated
    private final Path queueDirectory;
    @Deprecated
    private final String queueName;
    private final NQueue.Options queueOptions;

    private final Path mapDirectory;
    private final String mapName;
    private final MapPersistenceMode mapPersistenceMode;
    private final boolean strictConsistency;
    private final Duration connectTimeout;
    private final Duration reconnectInterval;
    private final Duration requestTimeout;
    private final int transportWorkerThreads;

    private NGridConfig(Builder builder) {
        this.clusterName = builder.clusterName;
        this.local = builder.local;
        this.peers = Collections.unmodifiableSet(new HashSet<>(builder.peers));
        int effectiveReplication = builder.replicationFactor != null ? builder.replicationFactor
                : builder.replicationQuorum;
        this.replicationQuorum = effectiveReplication;
        this.replicationFactor = effectiveReplication;
        this.replicationOperationTimeout = builder.replicationOperationTimeout;
        this.rttProbeInterval = builder.rttProbeInterval;
        this.heartbeatInterval = builder.heartbeatInterval;
        this.leaseTimeout = builder.leaseTimeout;
        this.leaderReelectionEnabled = builder.leaderReelectionEnabled;
        this.leaderReelectionInterval = builder.leaderReelectionInterval;
        this.leaderReelectionCooldown = builder.leaderReelectionCooldown;
        this.leaderReelectionSuggestionTtl = builder.leaderReelectionSuggestionTtl;
        this.leaderReelectionMinDelta = builder.leaderReelectionMinDelta;

        // New fields
        this.dataDirectory = builder.dataDirectory != null ? builder.dataDirectory : builder.queueDirectory;
        this.queues = Collections.unmodifiableList(new ArrayList<>(builder.queues));

        // Legacy fields (deprecated)
        this.queueDirectory = builder.queueDirectory;
        this.queueName = builder.queueName;
        this.queueOptions = builder.queueOptions;

        this.mapDirectory = builder.mapDirectory != null ? builder.mapDirectory
                : (dataDirectory != null ? dataDirectory.resolve("maps") : builder.queueDirectory.resolve("maps"));
        this.mapName = builder.mapName;
        this.mapPersistenceMode = builder.mapPersistenceMode;
        this.strictConsistency = builder.strictConsistency;
        this.connectTimeout = builder.connectTimeout;
        this.reconnectInterval = builder.reconnectInterval;
        this.requestTimeout = builder.requestTimeout;
        this.transportWorkerThreads = builder.transportWorkerThreads;
    }

    public String clusterName() {
        return clusterName;
    }

    public NodeInfo local() {
        return local;
    }

    public Set<NodeInfo> peers() {
        return peers;
    }

    public int replicationQuorum() {
        return replicationQuorum;
    }

    public int replicationFactor() {
        return replicationFactor;
    }

    /**
     * Optional replication operation timeout. When null, the replication layer
     * default is used.
     */
    public Duration replicationOperationTimeout() {
        return replicationOperationTimeout;
    }

    public Duration rttProbeInterval() {
        return rttProbeInterval;
    }

    public Duration heartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * Optional leader lease timeout. When {@code null}, the coordinator will use
     * its default ({@code 3 × heartbeatTimeout}).
     */
    public Duration leaseTimeout() {
        return leaseTimeout;
    }

    public boolean strictConsistency() {
        return strictConsistency;
    }

    public Duration connectTimeout() {
        return connectTimeout;
    }

    public Duration reconnectInterval() {
        return reconnectInterval;
    }

    public Duration requestTimeout() {
        return requestTimeout;
    }

    public int transportWorkerThreads() {
        return transportWorkerThreads;
    }

    public boolean leaderReelectionEnabled() {
        return leaderReelectionEnabled;
    }

    public Duration leaderReelectionInterval() {
        return leaderReelectionInterval;
    }

    public Duration leaderReelectionCooldown() {
        return leaderReelectionCooldown;
    }

    public Duration leaderReelectionSuggestionTtl() {
        return leaderReelectionSuggestionTtl;
    }

    public double leaderReelectionMinDelta() {
        return leaderReelectionMinDelta;
    }

    /**
     * @return the data directory for all distributed structures
     * @since 2.1.0
     */
    public Path dataDirectory() {
        return dataDirectory;
    }

    /**
     * @return the list of configured queues
     * @since 2.1.0
     */
    public List<QueueConfig> queues() {
        return queues;
    }

    /**
     * @return the legacy queue directory (deprecated)
     * @deprecated Use {@link #dataDirectory()} instead
     */
    @Deprecated
    public Path queueDirectory() {
        return queueDirectory;
    }

    /**
     * @return the legacy queue name (deprecated)
     * @deprecated Use {@link #queues()} instead
     */
    @Deprecated
    public String queueName() {
        return queueName;
    }

    public NQueue.Options queueOptions() {
        return queueOptions;
    }

    public Path mapDirectory() {
        return mapDirectory;
    }

    public String mapName() {
        return mapName;
    }

    public MapPersistenceMode mapPersistenceMode() {
        return mapPersistenceMode;
    }

    public static Builder builder(NodeInfo local) {
        return new Builder(local);
    }

    public static final class Builder {
        private String clusterName = "default-cluster";
        private final NodeInfo local;
        private final Set<NodeInfo> peers = new HashSet<>();
        private int replicationQuorum = 2;
        private Integer replicationFactor;
        private Duration replicationOperationTimeout;
        private Duration rttProbeInterval = Duration.ofSeconds(2);
        private Duration heartbeatInterval = Duration.ofSeconds(1);
        private Duration leaseTimeout;
        private boolean leaderReelectionEnabled = false;
        private Duration leaderReelectionInterval = Duration.ofSeconds(5);
        private Duration leaderReelectionCooldown = Duration.ofSeconds(60);
        private Duration leaderReelectionSuggestionTtl = Duration.ofSeconds(30);
        private double leaderReelectionMinDelta = 20.0;

        // New fields
        private Path dataDirectory;
        private final List<QueueConfig> queues = new ArrayList<>();

        // Legacy fields (deprecated)
        @Deprecated
        private Path queueDirectory;
        @Deprecated
        private String queueName = "ngrid";
        private NQueue.Options queueOptions;

        private Path mapDirectory;
        private String mapName = "default-map";
        private MapPersistenceMode mapPersistenceMode = MapPersistenceMode.DISABLED;
        private boolean strictConsistency = true;
        private Duration connectTimeout = Duration.ofSeconds(5);
        private Duration reconnectInterval = Duration.ofMillis(500);
        private Duration requestTimeout = Duration.ofSeconds(20);
        private int transportWorkerThreads = 2;

        private Builder(NodeInfo local) {
            this.local = Objects.requireNonNull(local, "local");
        }

        public Builder clusterName(String name) {
            this.clusterName = Objects.requireNonNull(name, "clusterName");
            return this;
        }

        public Builder connectTimeout(Duration timeout) {
            this.connectTimeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        public Builder reconnectInterval(Duration interval) {
            this.reconnectInterval = Objects.requireNonNull(interval, "interval");
            return this;
        }

        public Builder requestTimeout(Duration timeout) {
            this.requestTimeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        public Builder transportWorkerThreads(int threads) {
            if (threads < 1) {
                throw new IllegalArgumentException("transportWorkerThreads must be >= 1");
            }
            this.transportWorkerThreads = threads;
            return this;
        }

        public Builder strictConsistency(boolean strict) {
            this.strictConsistency = strict;
            return this;
        }

        /**
         * Sets the data directory for all distributed structures.
         * This is the recommended way to configure storage location.
         * 
         * @since 2.1.0
         */
        public Builder dataDirectory(Path directory) {
            this.dataDirectory = Objects.requireNonNull(directory, "dataDirectory");
            return this;
        }

        /**
         * Adds a queue configuration. Multiple queues can be added.
         * 
         * @since 2.1.0
         */
        public Builder addQueue(QueueConfig config) {
            this.queues.add(Objects.requireNonNull(config, "queue config"));
            return this;
        }

        public Builder addPeer(NodeInfo peer) {
            if (!peer.nodeId().equals(local.nodeId())) {
                peers.add(peer);
            }
            return this;
        }

        public Builder replicationQuorum(int quorum) {
            if (quorum < 1) {
                throw new IllegalArgumentException("Quorum must be >= 1");
            }
            this.replicationQuorum = quorum;
            return this;
        }

        public Builder replicationFactor(int factor) {
            if (factor < 1) {
                throw new IllegalArgumentException("Replication factor must be >= 1");
            }
            this.replicationFactor = factor;
            return this;
        }

        public Builder replicationOperationTimeout(Duration timeout) {
            this.replicationOperationTimeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        public Builder rttProbeInterval(Duration interval) {
            Objects.requireNonNull(interval, "interval");
            if (interval.isNegative()) {
                throw new IllegalArgumentException("interval must be >= 0");
            }
            this.rttProbeInterval = interval;
            return this;
        }

        public Builder heartbeatInterval(Duration interval) {
            Objects.requireNonNull(interval, "interval");
            if (interval.isNegative() || interval.isZero()) {
                throw new IllegalArgumentException("interval must be positive");
            }
            this.heartbeatInterval = interval;
            return this;
        }

        /**
         * Sets the leader lease timeout. A leader that has not received
         * acknowledgment from followers within this duration will step down.
         * If not set, defaults to {@code 3 × heartbeatTimeout}.
         */
        public Builder leaseTimeout(Duration timeout) {
            Objects.requireNonNull(timeout, "timeout");
            if (timeout.isNegative() || timeout.isZero()) {
                throw new IllegalArgumentException("timeout must be positive");
            }
            this.leaseTimeout = timeout;
            return this;
        }

        public Builder leaderReelectionEnabled(boolean enabled) {
            this.leaderReelectionEnabled = enabled;
            return this;
        }

        public Builder leaderReelectionInterval(Duration interval) {
            Objects.requireNonNull(interval, "interval");
            if (interval.isNegative() || interval.isZero()) {
                throw new IllegalArgumentException("interval must be positive");
            }
            this.leaderReelectionInterval = interval;
            return this;
        }

        public Builder leaderReelectionCooldown(Duration cooldown) {
            Objects.requireNonNull(cooldown, "cooldown");
            if (cooldown.isNegative()) {
                throw new IllegalArgumentException("cooldown must be >= 0");
            }
            this.leaderReelectionCooldown = cooldown;
            return this;
        }

        public Builder leaderReelectionSuggestionTtl(Duration ttl) {
            Objects.requireNonNull(ttl, "ttl");
            if (ttl.isNegative() || ttl.isZero()) {
                throw new IllegalArgumentException("ttl must be positive");
            }
            this.leaderReelectionSuggestionTtl = ttl;
            return this;
        }

        public Builder leaderReelectionMinDelta(double minDelta) {
            if (minDelta < 0.0) {
                throw new IllegalArgumentException("minDelta must be >= 0");
            }
            this.leaderReelectionMinDelta = minDelta;
            return this;
        }

        /**
         * Sets the queu directory (legacy API).
         * 
         * @deprecated Use {@link #dataDirectory(Path)} instead
         */
        @Deprecated
        public Builder queueDirectory(Path directory) {
            this.queueDirectory = Objects.requireNonNull(directory, "directory");
            return this;
        }

        /**
         * Sets the queue name (legacy API).
         * 
         * @deprecated Use {@link #addQueue(QueueConfig)} instead
         */
        @Deprecated
        public Builder queueName(String name) {
            this.queueName = Objects.requireNonNull(name, "name");
            return this;
        }

        /**
         * Options applied to the underlying {@link NQueue} instance. The grid layer
         * will
         * always disable short-circuiting to preserve ordering and disk-first
         * semantics.
         */
        public Builder queueOptions(NQueue.Options options) {
            this.queueOptions = Objects.requireNonNull(options, "options");
            return this;
        }

        public Builder mapDirectory(Path directory) {
            this.mapDirectory = Objects.requireNonNull(directory, "directory");
            return this;
        }

        public Builder mapName(String name) {
            this.mapName = Objects.requireNonNull(name, "name");
            return this;
        }

        public Builder mapPersistenceMode(MapPersistenceMode mode) {
            this.mapPersistenceMode = Objects.requireNonNull(mode, "mode");
            return this;
        }

        public NGridConfig build() {
            // Backward compatibility: convert legacy config to new format
            if (queueDirectory != null && dataDirectory == null) {
                dataDirectory = queueDirectory;
            }

            if (queues.isEmpty() && queueDirectory != null) {
                // Legacy mode: create a single queue from queueName
                QueueConfig.Builder queueBuilder = QueueConfig.builder(queueName);
                if (queueOptions != null) {
                    queueBuilder.nqueueOptions(queueOptions);
                }
                queues.add(queueBuilder.build());
            }

            if (dataDirectory == null) {
                throw new IllegalStateException(
                        "Data directory must be specified (use dataDirectory() or legacy queueDirectory())");
            }

            return new NGridConfig(this);
        }
    }
}
