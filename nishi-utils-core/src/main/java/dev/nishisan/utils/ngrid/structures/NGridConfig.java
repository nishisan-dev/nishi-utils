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
import dev.nishisan.utils.ngrid.replication.FollowerIngestMode;
import dev.nishisan.utils.ngrid.replication.RelayDurability;
import dev.nishisan.utils.map.NMapPersistenceMode;
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
    private final DeploymentProfile deploymentProfile;
    private final Set<NodeInfo> peers;
    private final int replicationQuorum;
    private final int replicationFactor;
    private final Duration replicationOperationTimeout;
    private final Integer replicationLogRetention;
    private final Duration replicationLogRetentionTime;
    private final Long resendLogMaxEntries;
    private final Long resendLogSegmentMaxBytes;
    private final Integer resendLogMaxSegments;
    private final FollowerIngestMode followerIngestMode;
    private final RelayDurability relayDurability;
    private final Duration relayGroupCommitInterval;
    private final boolean persistentResendLog;
    private final int relayApplyBatchSize;
    private final boolean leaderPauseOnJoin;
    private final Duration joinQuiesceMaxDuration;
    private final Duration rttProbeInterval;
    private final Duration heartbeatInterval;
    private final Duration leaseTimeout;
    private final boolean leaderReelectionEnabled;
    private final Duration leaderReelectionInterval;
    private final Duration leaderReelectionCooldown;
    private final Duration leaderReelectionSuggestionTtl;
    private final double leaderReelectionMinDelta;
    private final boolean dashboardEnabled;

    // New fields for multiple queues and maps
    private final Path dataDirectory;
    private final List<QueueConfig> queues;
    private final List<MapConfig> configuredMaps;

    // Legacy fields (deprecated but maintained for compatibility)
    @Deprecated
    private final Path queueDirectory;
    @Deprecated
    private final String queueName;
    private final NQueue.Options queueOptions;

    private final Path mapDirectory;
    private final String mapName;
    private final NMapPersistenceMode mapPersistenceMode;
    private final boolean mapLeaderLocalByReference;
    private final boolean strictConsistency;
    private final Duration connectTimeout;
    private final Duration reconnectInterval;
    private final Duration requestTimeout;
    private final int transportWorkerThreads;
    private final int outboundQueueCapacity;
    private final boolean transportCompressionEnabled;
    private final int transportCompressionMinSize;

    private NGridConfig(Builder builder) {
        this.clusterName = builder.clusterName;
        this.local = builder.local;
        this.deploymentProfile = builder.deploymentProfile;
        this.peers = Collections.unmodifiableSet(new HashSet<>(builder.peers));
        int effectiveReplication = builder.replicationFactor != null ? builder.replicationFactor
                : builder.replicationQuorum;
        this.replicationQuorum = effectiveReplication;
        this.replicationFactor = effectiveReplication;
        this.replicationOperationTimeout = builder.replicationOperationTimeout;
        this.replicationLogRetention = builder.replicationLogRetention;
        this.replicationLogRetentionTime = builder.replicationLogRetentionTime;
        this.resendLogMaxEntries = builder.resendLogMaxEntries;
        this.resendLogSegmentMaxBytes = builder.resendLogSegmentMaxBytes;
        this.resendLogMaxSegments = builder.resendLogMaxSegments;
        this.followerIngestMode = builder.followerIngestMode;
        this.relayDurability = builder.relayDurability;
        this.relayGroupCommitInterval = builder.relayGroupCommitInterval;
        this.persistentResendLog = builder.persistentResendLog;
        this.relayApplyBatchSize = builder.relayApplyBatchSize;
        this.leaderPauseOnJoin = builder.leaderPauseOnJoin;
        this.joinQuiesceMaxDuration = builder.joinQuiesceMaxDuration;
        this.rttProbeInterval = builder.rttProbeInterval;
        this.heartbeatInterval = builder.heartbeatInterval;
        this.leaseTimeout = builder.leaseTimeout;
        this.leaderReelectionEnabled = builder.leaderReelectionEnabled;
        this.leaderReelectionInterval = builder.leaderReelectionInterval;
        this.leaderReelectionCooldown = builder.leaderReelectionCooldown;
        this.leaderReelectionSuggestionTtl = builder.leaderReelectionSuggestionTtl;
        this.leaderReelectionMinDelta = builder.leaderReelectionMinDelta;
        this.dashboardEnabled = builder.dashboardEnabled != null
                ? builder.dashboardEnabled
                : builder.deploymentProfile == DeploymentProfile.PRODUCTION;

        // New fields
        this.dataDirectory = builder.dataDirectory != null ? builder.dataDirectory : builder.queueDirectory;
        this.queues = Collections.unmodifiableList(new ArrayList<>(builder.queues));
        this.configuredMaps = Collections.unmodifiableList(new ArrayList<>(builder.configuredMaps));

        // Legacy fields (deprecated)
        this.queueDirectory = builder.queueDirectory;
        this.queueName = builder.queueName;
        this.queueOptions = builder.queueOptions;

        this.mapDirectory = builder.mapDirectory != null ? builder.mapDirectory
                : (dataDirectory != null ? dataDirectory.resolve("maps") : builder.queueDirectory.resolve("maps"));
        this.mapName = builder.mapName;
        this.mapPersistenceMode = builder.mapPersistenceMode;
        this.mapLeaderLocalByReference = builder.mapLeaderLocalByReference;
        this.strictConsistency = builder.strictConsistency;
        this.connectTimeout = builder.connectTimeout;
        this.reconnectInterval = builder.reconnectInterval;
        this.requestTimeout = builder.requestTimeout;
        this.transportWorkerThreads = builder.transportWorkerThreads;
        this.outboundQueueCapacity = builder.outboundQueueCapacity;
        this.transportCompressionEnabled = builder.transportCompressionEnabled;
        this.transportCompressionMinSize = builder.transportCompressionMinSize;
    }

    public String clusterName() {
        return clusterName;
    }

    /**
     * Returns the deployment profile for this configuration.
     *
     * @return the deployment profile (never null)
     * @since 3.2.0
     */
    public DeploymentProfile deploymentProfile() {
        return deploymentProfile;
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

    /**
     * Returns the replication factor.
     * 
     * @return the factor
     */
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

    /**
     * Optional count-based retention cap for the leader-side resend log (op-log). When null, the
     * replication layer default is used.
     *
     * @return the resend-log count cap, or {@code null} when unset
     */
    public Integer replicationLogRetention() {
        return replicationLogRetention;
    }

    /**
     * Optional count cap for the leader-side durable op-log (the binlog: ResendLog), in entries per
     * topic. This is the deep on-disk window that bounds how far a lagging follower can fall behind
     * and still stream (below it the follower bootstraps from a snapshot) — distinct from the small
     * heap hot-cache {@link #replicationLogRetention()}. When {@code null}, the replication-layer
     * default (10,000,000) is used. Tune it to size the binlog retention window (analogous to a
     * MySQL binlog size/count bound).
     *
     * @return the op-log count cap, or {@code null} when unset (replication default applies)
     */
    public Long resendLogMaxEntries() {
        return resendLogMaxEntries;
    }

    /**
     * Optional per-segment byte cap for the leader-side binlog (ResendLog) — the MySQL
     * {@code max_binlog_size} analog. A segment rolls on whichever per-segment bound is hit first
     * (entries, age, or bytes). When {@code null}, byte-based rolling is disabled (entry/age rolling
     * still applies). Combine with {@link #resendLogMaxSegments()} to express "N files of X bytes".
     *
     * @return the per-segment byte cap, or {@code null} when unset
     */
    public Long resendLogSegmentMaxBytes() {
        return resendLogSegmentMaxBytes;
    }

    /**
     * Optional cap on the number of binlog segment files retained per topic — the MySQL "keep N
     * binlog files" model. When the count would exceed this, the oldest sealed segment is dropped
     * whole (the active tail segment is never dropped). When {@code null}, the cap is disabled
     * (count/time retention still applies). Combine with {@link #resendLogSegmentMaxBytes()} for
     * "N files of X bytes" or with the replication-layer per-segment entry cap for "N files of X ops".
     *
     * @return the retained segment-count cap, or {@code null} when unset
     */
    public Integer resendLogMaxSegments() {
        return resendLogMaxSegments;
    }

    /**
     * Optional temporal retention window for the leader-side resend log (op-log). When null, the
     * replication layer default is used (temporal eviction disabled). Complements
     * {@link #replicationLogRetention()} — whichever limit is reached first evicts.
     *
     * @return the resend-log temporal retention window, or {@code null} when unset
     */
    public Duration replicationLogRetentionTime() {
        return replicationLogRetentionTime;
    }

    /**
     * How a follower ingests replicated operations. Defaults to
     * {@link FollowerIngestMode#INLINE}; {@link FollowerIngestMode#RELAY_LOG} enables
     * the on-disk relay-log ingestion path (#124).
     *
     * @return the follower ingest mode (never {@code null})
     */
    public FollowerIngestMode followerIngestMode() {
        return followerIngestMode;
    }

    /**
     * Relay-log tail durability policy (#124), analogous to MySQL's {@code sync_relay_log}.
     * Defaults to {@link RelayDurability#OS_MANAGED}.
     *
     * @return the relay durability policy (never {@code null})
     */
    public RelayDurability relayDurability() {
        return relayDurability;
    }

    /**
     * Whether the leader-side resend op-log is backed by a durable, segmented on-disk store (#127),
     * keeping the deep backlog window off-heap. Defaults to {@code false}. Pair with
     * {@link #replicationLogRetentionTime()} to size the temporal window.
     *
     * @return {@code true} when the disk-backed resend op-log is enabled
     */
    public boolean persistentResendLog() {
        return persistentResendLog;
    }

    /**
     * Number of relay-log entries a follower's apply consumer drains per batch (#128). Higher values
     * raise apply throughput under burst by amortizing per-operation overhead; the consumer stays
     * single-threaded and in strict sequence order. Defaults to 256.
     *
     * @return the relay apply batch size
     */
    public int relayApplyBatchSize() {
        return relayApplyBatchSize;
    }

    /**
     * Whether the leader pauses production while a not-caught-up follower joins (#129). Defaults to
     * {@code false}. Relevant in RELAY_LOG deployments needing deterministic bootstrap convergence.
     *
     * @return {@code true} when leader-pause-on-join is enabled
     */
    public boolean leaderPauseOnJoin() {
        return leaderPauseOnJoin;
    }

    /**
     * Hard cap on a leader-pause-on-join pause (#129), so a follower that dies mid-join cannot freeze
     * the leader. Defaults to 10s.
     *
     * @return the maximum join-quiesce duration
     */
    public Duration joinQuiesceMaxDuration() {
        return joinQuiesceMaxDuration;
    }

    /**
     * Interval between forced relay syncs when {@link RelayDurability#GROUP_COMMIT} is active.
     *
     * @return the group-commit interval (never {@code null})
     */
    public Duration relayGroupCommitInterval() {
        return relayGroupCommitInterval;
    }

    public Duration rttProbeInterval() {
        return rttProbeInterval;
    }

    /**
     * Returns whether the dashboard YAML reporter is enabled.
     * Defaults to {@code true} for {@link DeploymentProfile#PRODUCTION},
     * {@code false} otherwise.
     *
     * @return true if dashboard reporting is enabled
     * @since 3.3.0
     */
    public boolean dashboardEnabled() {
        return dashboardEnabled;
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

    /**
     * Per-connection outbound replication capacity. When the number of pending
     * replication messages on a connection reaches this value, excess replication
     * is dropped and the lagging follower recovers via the gap/snapshot catch-up.
     * Control traffic is never bounded. {@code 0} means unbounded (default).
     *
     * @return the outbound replication capacity
     * @since 2.2.0
     */
    public int outboundQueueCapacity() {
        return outboundQueueCapacity;
    }

    /**
     * Whether outbound transport frames are eligible for LZ4 compression. Compression is
     * additionally gated per-peer by the handshake capability negotiation, and decoding of
     * compressed frames is always supported regardless of this flag. Defaults to {@code true}.
     *
     * @return {@code true} if outbound transport compression is enabled
     * @since 4.6.0
     */
    public boolean transportCompressionEnabled() {
        return transportCompressionEnabled;
    }

    /**
     * Minimum serialized JSON size, in bytes, below which a transport frame is never
     * compressed. Defaults to {@code 512}.
     *
     * @return the minimum payload size eligible for compression
     * @since 4.6.0
     */
    public int transportCompressionMinSize() {
        return transportCompressionMinSize;
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
     * Returns the list of configured maps.
     *
     * @return an unmodifiable list of map configurations
     * @since 2.2.0
     */
    public List<MapConfig> configuredMaps() {
        return configuredMaps;
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

    public NMapPersistenceMode mapPersistenceMode() {
        return mapPersistenceMode;
    }

    /**
     * Global default for leader-local by-reference mode applied to maps that do not
     * override it via {@link MapConfig#leaderLocalByReference()}. Defaults to
     * {@code false}. Internal maps (e.g. {@code _ngrid-queue-offsets}) are never
     * affected.
     *
     * @return the global default for leader-local by-reference
     */
    public boolean mapLeaderLocalByReference() {
        return mapLeaderLocalByReference;
    }

    /**
     * Creates a builder for the given local node.
     * 
     * @param local the local node info
     * @return the builder
     */
    public static Builder builder(NodeInfo local) {
        return new Builder(local);
    }

    public static final class Builder {
        private String clusterName = "default-cluster";
        private final NodeInfo local;
        private DeploymentProfile deploymentProfile = DeploymentProfile.DEV;
        private final Set<NodeInfo> peers = new HashSet<>();
        private int replicationQuorum = 2;
        private Integer replicationFactor;
        private Duration replicationOperationTimeout;
        private Integer replicationLogRetention;
        private Long resendLogMaxEntries;
        private Long resendLogSegmentMaxBytes;
        private Integer resendLogMaxSegments;
        private Duration replicationLogRetentionTime;
        private FollowerIngestMode followerIngestMode = FollowerIngestMode.INLINE;
        private RelayDurability relayDurability = RelayDurability.OS_MANAGED;
        private Duration relayGroupCommitInterval = Duration.ofSeconds(1);
        private boolean persistentResendLog = false;
        private int relayApplyBatchSize = 256;
        private boolean leaderPauseOnJoin = false;
        private Duration joinQuiesceMaxDuration = Duration.ofSeconds(10);
        private Duration rttProbeInterval = Duration.ofSeconds(10);
        private Duration heartbeatInterval = Duration.ofSeconds(3);
        private Duration leaseTimeout;
        private boolean leaderReelectionEnabled = false;
        private Duration leaderReelectionInterval = Duration.ofSeconds(5);
        private Duration leaderReelectionCooldown = Duration.ofSeconds(60);
        private Duration leaderReelectionSuggestionTtl = Duration.ofSeconds(30);
        private double leaderReelectionMinDelta = 20.0;
        private Boolean dashboardEnabled;

        // New fields
        private Path dataDirectory;
        private final List<QueueConfig> queues = new ArrayList<>();
        private final List<MapConfig> configuredMaps = new ArrayList<>();

        // Legacy fields (deprecated)
        @Deprecated
        private Path queueDirectory;
        @Deprecated
        private String queueName = "ngrid";
        private NQueue.Options queueOptions;

        private Path mapDirectory;
        private String mapName = "default-map";
        private NMapPersistenceMode mapPersistenceMode = NMapPersistenceMode.DISABLED;
        private boolean mapLeaderLocalByReference = false;
        private boolean strictConsistency = true;
        private Duration connectTimeout = Duration.ofSeconds(5);
        private Duration reconnectInterval = Duration.ofMillis(500);
        private Duration requestTimeout = Duration.ofSeconds(20);
        private int transportWorkerThreads = 2;
        private int outboundQueueCapacity = 0;
        private boolean transportCompressionEnabled = true;
        private int transportCompressionMinSize = 512;

        private Builder(NodeInfo local) {
            this.local = Objects.requireNonNull(local, "local");
        }

        /**
         * Sets the deployment profile. When set to {@link DeploymentProfile#PRODUCTION},
         * the {@link #build()} method enforces strict safety invariants:
         * <ul>
         *   <li>{@code strictConsistency} must be {@code true}</li>
         *   <li>{@code replicationFactor} must be {@code >= 2}</li>
         *   <li>Maps with persistence {@code DISABLED} are rejected</li>
         * </ul>
         *
         * @param profile the deployment profile (default: {@link DeploymentProfile#DEV})
         * @return this builder
         * @since 3.2.0
         */
        public Builder deploymentProfile(DeploymentProfile profile) {
            this.deploymentProfile = Objects.requireNonNull(profile, "profile");
            return this;
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

        /**
         * Sets the per-connection outbound replication capacity. When the number
         * of pending replication messages on a connection reaches this value,
         * excess replication is dropped and the lagging follower recovers via the
         * gap/snapshot catch-up. Control traffic is never bounded.
         *
         * @param capacity the capacity, {@code 0} = unbounded (default), must be {@code >= 0}
         * @return this builder
         * @since 2.2.0
         */
        public Builder outboundQueueCapacity(int capacity) {
            if (capacity < 0) {
                throw new IllegalArgumentException("outboundQueueCapacity must be >= 0");
            }
            this.outboundQueueCapacity = capacity;
            return this;
        }

        /**
         * Enables or disables LZ4 compression of outbound transport frames (default
         * {@code true}). Compression is still negotiated per-peer in the handshake; decoding
         * of compressed frames is always supported regardless of this flag.
         *
         * @param enabled whether to compress eligible outbound frames
         * @return this builder
         * @since 4.6.0
         */
        public Builder transportCompressionEnabled(boolean enabled) {
            this.transportCompressionEnabled = enabled;
            return this;
        }

        /**
         * Sets the minimum serialized JSON size (bytes) eligible for compression (default
         * {@code 512}). Smaller frames are sent uncompressed.
         *
         * @param minSize the minimum payload size, must be {@code >= 0}
         * @return this builder
         * @since 4.6.0
         */
        public Builder transportCompressionMinSize(int minSize) {
            if (minSize < 0) {
                throw new IllegalArgumentException("transportCompressionMinSize must be >= 0");
            }
            this.transportCompressionMinSize = minSize;
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

        /**
         * Adds a map configuration. Multiple maps can be added.
         *
         * @since 2.2.0
         */
        public Builder addMap(MapConfig config) {
            this.configuredMaps.add(Objects.requireNonNull(config, "map config"));
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

        /**
         * Sets the count-based retention cap for the leader-side resend log (op-log). When unset, the
         * replication layer default is used.
         *
         * @param retention maximum resend-log entries per topic (must be >= 1)
         * @return this builder
         */
        public Builder replicationLogRetention(int retention) {
            if (retention < 1) {
                throw new IllegalArgumentException("replicationLogRetention must be >= 1");
            }
            this.replicationLogRetention = retention;
            return this;
        }

        /**
         * Sets the count cap for the leader-side durable op-log (binlog), in entries per topic. This
         * sizes the deep on-disk retention window that bounds how far a follower can lag and still
         * stream (below it the follower bootstraps from a snapshot). Defaults to 10,000,000 when
         * unset. See {@link NGridConfig#resendLogMaxEntries()}.
         *
         * @param maxEntries the op-log count cap (must be {@code >= 1})
         * @return this builder
         */
        public Builder resendLogMaxEntries(long maxEntries) {
            if (maxEntries < 1) {
                throw new IllegalArgumentException("resendLogMaxEntries must be >= 1");
            }
            this.resendLogMaxEntries = maxEntries;
            return this;
        }

        /**
         * Sets the per-segment byte cap for the leader-side binlog (ResendLog) — the MySQL
         * {@code max_binlog_size} analog. A segment rolls on whichever per-segment bound is hit first
         * (entries, age, or bytes). Combine with {@link #resendLogMaxSegments(int)} to express
         * "N files of X bytes". When unset (or {@code 0}), byte-based rolling is disabled.
         *
         * @param maxBytes bytes per binlog segment (must be {@code >= 0}; 0 disables)
         * @return this builder
         */
        public Builder resendLogSegmentMaxBytes(long maxBytes) {
            if (maxBytes < 0) {
                throw new IllegalArgumentException("resendLogSegmentMaxBytes must be >= 0");
            }
            this.resendLogSegmentMaxBytes = maxBytes;
            return this;
        }

        /**
         * Sets the cap on the number of binlog segment files retained per topic — the MySQL "keep N
         * binlog files" model. When exceeded, the oldest sealed segment is dropped whole (the active
         * tail segment is never dropped). Combine with {@link #resendLogSegmentMaxBytes(long)} for
         * "N files of X bytes". When unset (or {@code 0}), the cap is disabled.
         *
         * @param maxSegments the retained segment-count cap (must be {@code >= 0}; 0 disables)
         * @return this builder
         */
        public Builder resendLogMaxSegments(int maxSegments) {
            if (maxSegments < 0) {
                throw new IllegalArgumentException("resendLogMaxSegments must be >= 0");
            }
            this.resendLogMaxSegments = maxSegments;
            return this;
        }

        /**
         * Sets the temporal retention window for the leader-side resend log (op-log). Complements
         * {@link #replicationLogRetention(int)} — whichever limit is reached first evicts. When unset
         * (or {@link Duration#ZERO}), temporal eviction is disabled (count-only behavior).
         *
         * @param retentionTime the retention window ({@link Duration#ZERO} disables; must not be negative)
         * @return this builder
         */
        public Builder replicationLogRetentionTime(Duration retentionTime) {
            Objects.requireNonNull(retentionTime, "retentionTime");
            if (retentionTime.isNegative()) {
                throw new IllegalArgumentException("replicationLogRetentionTime must not be negative");
            }
            this.replicationLogRetentionTime = retentionTime;
            return this;
        }

        /**
         * Sets how a follower ingests replicated operations. Defaults to
         * {@link FollowerIngestMode#INLINE} (legacy behavior); {@link FollowerIngestMode#RELAY_LOG}
         * enables the on-disk relay-log ingestion path (#124).
         *
         * @param followerIngestMode the follower ingest mode (must not be {@code null})
         * @return this builder
         */
        public Builder followerIngestMode(FollowerIngestMode followerIngestMode) {
            this.followerIngestMode = Objects.requireNonNull(followerIngestMode, "followerIngestMode");
            return this;
        }

        /**
         * Sets the relay-log tail durability policy (#124), analogous to MySQL's
         * {@code sync_relay_log}. Defaults to {@link RelayDurability#OS_MANAGED}.
         *
         * @param relayDurability the durability policy (must not be {@code null})
         * @return this builder
         */
        public Builder relayDurability(RelayDurability relayDurability) {
            this.relayDurability = Objects.requireNonNull(relayDurability, "relayDurability");
            return this;
        }

        /**
         * Enables the disk-backed resend op-log (#127), keeping the deep backlog window off-heap.
         * Pair with {@link #replicationLogRetentionTime(Duration)} to size the temporal window.
         * Defaults to {@code false}.
         *
         * @param persistentResendLog {@code true} to enable the on-disk resend op-log
         * @return this builder
         */
        public Builder persistentResendLog(boolean persistentResendLog) {
            this.persistentResendLog = persistentResendLog;
            return this;
        }

        /**
         * Sets how many relay-log entries a follower's apply consumer drains per batch (#128).
         * Defaults to 256.
         *
         * @param batchSize the relay apply batch size (must be >= 1)
         * @return this builder
         */
        public Builder relayApplyBatchSize(int batchSize) {
            if (batchSize < 1) {
                throw new IllegalArgumentException("relayApplyBatchSize must be >= 1");
            }
            this.relayApplyBatchSize = batchSize;
            return this;
        }

        /**
         * Enables leader-pause-on-join (#129). Defaults to {@code false}.
         *
         * @param leaderPauseOnJoin {@code true} to pause production while a behind follower joins
         * @return this builder
         */
        public Builder leaderPauseOnJoin(boolean leaderPauseOnJoin) {
            this.leaderPauseOnJoin = leaderPauseOnJoin;
            return this;
        }

        /**
         * Sets the hard cap on a leader-pause-on-join pause (#129).
         *
         * @param duration the maximum join-quiesce duration (must be positive)
         * @return this builder
         */
        public Builder joinQuiesceMaxDuration(Duration duration) {
            Objects.requireNonNull(duration, "joinQuiesceMaxDuration");
            if (duration.isNegative() || duration.isZero()) {
                throw new IllegalArgumentException("joinQuiesceMaxDuration must be positive");
            }
            this.joinQuiesceMaxDuration = duration;
            return this;
        }

        /**
         * Sets the forced-sync interval used when {@link RelayDurability#GROUP_COMMIT} is active.
         *
         * @param interval the group-commit interval (must be positive)
         * @return this builder
         */
        public Builder relayGroupCommitInterval(Duration interval) {
            Objects.requireNonNull(interval, "relayGroupCommitInterval");
            if (interval.isNegative() || interval.isZero()) {
                throw new IllegalArgumentException("relayGroupCommitInterval must be positive");
            }
            this.relayGroupCommitInterval = interval;
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

        /**
         * Enables or disables the dashboard YAML reporter.
         * Defaults to {@code true} for {@link DeploymentProfile#PRODUCTION},
         * {@code false} for other profiles.
         *
         * @param enabled whether dashboard reporting is enabled
         * @return this builder
         * @since 3.3.0
         */
        public Builder dashboardEnabled(boolean enabled) {
            this.dashboardEnabled = enabled;
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

        public Builder mapPersistenceMode(NMapPersistenceMode mode) {
            this.mapPersistenceMode = Objects.requireNonNull(mode, "mode");
            return this;
        }

        /**
         * Sets the global default for leader-local by-reference mode. Maps that do
         * not override it via {@link MapConfig.Builder#leaderLocalByReference(boolean)}
         * inherit this value. Internal maps are never affected.
         *
         * @param leaderLocalByReference the global default
         * @return this builder
         */
        public Builder mapLeaderLocalByReference(boolean leaderLocalByReference) {
            this.mapLeaderLocalByReference = leaderLocalByReference;
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

            // ── Production guardrails ──
            if (deploymentProfile == DeploymentProfile.PRODUCTION) {
                validateProductionConfig();
            }

            return new NGridConfig(this);
        }

        private void validateProductionConfig() {
            int effectiveFactor = replicationFactor != null ? replicationFactor : replicationQuorum;

            if (!strictConsistency) {
                throw new IllegalArgumentException(
                        "[PRODUCTION] strictConsistency must be true. "
                                + "Relaxed consistency is not allowed in production profile. "
                                + "Set strictConsistency(true) or use DeploymentProfile.STAGING.");
            }

            if (effectiveFactor < 2) {
                throw new IllegalArgumentException(
                        "[PRODUCTION] replicationFactor must be >= 2 (current: " + effectiveFactor + "). "
                                + "Single-replica deployment is not safe for production.");
            }

            for (MapConfig map : configuredMaps) {
                if (map.persistenceMode() == NMapPersistenceMode.DISABLED) {
                    throw new IllegalArgumentException(
                            "[PRODUCTION] Map '" + map.name() + "' has persistence DISABLED. "
                                    + "All maps must be persisted in production profile. "
                                    + "Set persistence to ASYNC_WITH_FSYNC or SYNC.");
                }
            }

            // Binlog (ResendLog) retention guardrails: only sanity floors when the operator opts in,
            // never mandatory (the resendLogMaxEntries count backstop already bounds disk growth).
            if (resendLogSegmentMaxBytes != null && resendLogSegmentMaxBytes > 0
                    && resendLogSegmentMaxBytes < 1_048_576L) {
                throw new IllegalArgumentException(
                        "[PRODUCTION] resendLogSegmentMaxBytes must be >= 1MB (current: "
                                + resendLogSegmentMaxBytes + "). A tiny per-segment byte cap would roll "
                                + "pathologically many micro-segments. Raise it or use DeploymentProfile.STAGING.");
            }

            if (resendLogMaxSegments != null && resendLogMaxSegments > 0 && resendLogMaxSegments < 2) {
                throw new IllegalArgumentException(
                        "[PRODUCTION] resendLogMaxSegments must be >= 2 (current: " + resendLogMaxSegments
                                + "). The binlog needs at least the active segment plus one history "
                                + "segment to serve resend/stream. Raise it or use DeploymentProfile.STAGING.");
            }
        }
    }
}
