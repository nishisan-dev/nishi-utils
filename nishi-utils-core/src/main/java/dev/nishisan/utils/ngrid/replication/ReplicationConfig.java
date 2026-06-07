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
    private final Duration replicationLogRetentionTime;
    private final int appliedSetMaxSize;
    private final int operationLogMaxSize;
    private final boolean leaderLocalApply;
    private final FollowerIngestMode followerIngestMode;
    private final RelayDurability relayDurability;
    private final Duration relayGroupCommitInterval;
    private final boolean persistentResendLog;
    private final int resendLogSegmentMaxEntries;
    private final Duration resendLogSegmentMaxAge;
    private final long resendLogMaxEntries;
    private final int resendLogReadBatchMax;

    private ReplicationConfig(int quorum, Duration operationTimeout, Duration retryInterval, boolean strictConsistency,
            Path dataDirectory, int resendGapThreshold, Duration resendTimeout, int replicationLogRetention,
            Duration replicationLogRetentionTime, int appliedSetMaxSize, int operationLogMaxSize,
            boolean leaderLocalApply, FollowerIngestMode followerIngestMode, RelayDurability relayDurability,
            Duration relayGroupCommitInterval, boolean persistentResendLog, int resendLogSegmentMaxEntries,
            Duration resendLogSegmentMaxAge, long resendLogMaxEntries, int resendLogReadBatchMax) {
        this.quorum = quorum;
        this.operationTimeout = Objects.requireNonNull(operationTimeout, "operationTimeout");
        this.retryInterval = Objects.requireNonNull(retryInterval, "retryInterval");
        this.strictConsistency = strictConsistency;
        this.dataDirectory = Objects.requireNonNull(dataDirectory, "dataDirectory");
        this.resendGapThreshold = resendGapThreshold;
        this.resendTimeout = Objects.requireNonNull(resendTimeout, "resendTimeout");
        this.replicationLogRetention = replicationLogRetention;
        this.replicationLogRetentionTime = Objects.requireNonNull(replicationLogRetentionTime,
                "replicationLogRetentionTime");
        this.appliedSetMaxSize = appliedSetMaxSize;
        this.operationLogMaxSize = operationLogMaxSize;
        this.leaderLocalApply = leaderLocalApply;
        this.followerIngestMode = Objects.requireNonNull(followerIngestMode, "followerIngestMode");
        this.relayDurability = Objects.requireNonNull(relayDurability, "relayDurability");
        this.relayGroupCommitInterval = Objects.requireNonNull(relayGroupCommitInterval, "relayGroupCommitInterval");
        this.persistentResendLog = persistentResendLog;
        this.resendLogSegmentMaxEntries = resendLogSegmentMaxEntries;
        this.resendLogSegmentMaxAge = Objects.requireNonNull(resendLogSegmentMaxAge, "resendLogSegmentMaxAge");
        this.resendLogMaxEntries = resendLogMaxEntries;
        this.resendLogReadBatchMax = resendLogReadBatchMax;
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
     * Temporal retention window for the leader-side resend log (op-log). Entries older than this
     * duration are evicted from {@code replicationLogBySequence}, complementing the count-based
     * {@link #replicationLogRetention()} cap — whichever limit is reached first evicts (count bounds
     * memory; time bounds the backlog window). A follower that requests deltas beyond the expired
     * window falls into the existing gap-detection → snapshot fallback path, never silent divergence.
     *
     * <p>{@link Duration#ZERO} (the default) disables temporal eviction, preserving the previous
     * count-only behavior.</p>
     *
     * @return the temporal retention window, or {@link Duration#ZERO} when disabled
     */
    public Duration replicationLogRetentionTime() {
        return replicationLogRetentionTime;
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

    /**
     * How this node, when acting as a follower, ingests replicated operations.
     * {@link FollowerIngestMode#INLINE} (the default) preserves the legacy in-memory
     * buffer + apply path; {@link FollowerIngestMode#RELAY_LOG} persists each request
     * to an on-disk relay and applies it from a separate consumer (#124).
     *
     * @return the follower ingest mode (never {@code null})
     */
    public FollowerIngestMode followerIngestMode() {
        return followerIngestMode;
    }

    /**
     * Durability policy for the follower relay-log tail (#124), analogous to MySQL's
     * {@code sync_relay_log}. Defaults to {@link RelayDurability#OS_MANAGED}.
     *
     * @return the relay durability policy (never {@code null})
     */
    public RelayDurability relayDurability() {
        return relayDurability;
    }

    /**
     * Interval between forced syncs of the relay when {@link RelayDurability#GROUP_COMMIT}
     * is active. Ignored for the other policies.
     *
     * @return the group-commit interval (never {@code null})
     */
    public Duration relayGroupCommitInterval() {
        return relayGroupCommitInterval;
    }

    /**
     * Whether the leader-side resend op-log is backed by a durable, segmented on-disk store (#127)
     * in addition to the in-heap hot cache. Defaults to {@code false} (heap-only, the previous
     * behavior). When {@code true}, the heap cache holds only the freshest window (bounded by
     * {@link #replicationLogRetention()}) and the deep, time-governed backlog window
     * ({@link #replicationLogRetentionTime()}) lives on disk — so a large window costs disk, not
     * heap, eliminating the count-vs-time eviction that collapsed the window under load.
     *
     * @return {@code true} to enable the disk-backed resend op-log
     */
    public boolean persistentResendLog() {
        return persistentResendLog;
    }

    /**
     * Maximum number of entries per on-disk resend-log segment before a new segment is rolled.
     * Retention drops whole sealed segments, so this also sets the granularity of eviction.
     *
     * @return the per-segment entry cap
     */
    public int resendLogSegmentMaxEntries() {
        return resendLogSegmentMaxEntries;
    }

    /**
     * Maximum age of an on-disk resend-log segment before a new one is rolled, bounding how long an
     * entry waits before its segment can age out of the temporal window. {@link Duration#ZERO}
     * disables age-based rolling (count-based rolling still applies).
     *
     * @return the per-segment max age
     */
    public Duration resendLogSegmentMaxAge() {
        return resendLogSegmentMaxAge;
    }

    /**
     * Hard count backstop for the on-disk resend op-log across all segments of a topic, guarding
     * against unbounded disk growth. The temporal window ({@link #replicationLogRetentionTime()}) is
     * the intended governor; this is a safety cap.
     *
     * @return the disk-side entry cap per topic
     */
    public long resendLogMaxEntries() {
        return resendLogMaxEntries;
    }

    /**
     * Maximum number of sequences served from the disk resend op-log in a single resend response,
     * bounding one read. Larger gaps fall into the snapshot-fallback path.
     *
     * @return the per-response read cap
     */
    public int resendLogReadBatchMax() {
        return resendLogReadBatchMax;
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
        private Duration replicationLogRetentionTime = Duration.ZERO;
        private int appliedSetMaxSize = 5000;
        private int operationLogMaxSize = 2000;
        private boolean leaderLocalApply = true;
        private FollowerIngestMode followerIngestMode = FollowerIngestMode.INLINE;
        private RelayDurability relayDurability = RelayDurability.OS_MANAGED;
        private Duration relayGroupCommitInterval = Duration.ofSeconds(1);
        private boolean persistentResendLog = false;
        private int resendLogSegmentMaxEntries = 65_536;
        private Duration resendLogSegmentMaxAge = Duration.ofMinutes(5);
        private long resendLogMaxEntries = 10_000_000L;
        private int resendLogReadBatchMax = 5_000;

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
         * Sets the temporal retention window for the leader-side resend log (op-log). Entries older
         * than {@code retentionTime} are evicted, complementing the count-based
         * {@link #replicationLogRetention(int)} cap (whichever is reached first evicts). Pass
         * {@link Duration#ZERO} to disable temporal eviction (count-only behavior, the default).
         *
         * @param retentionTime the retention window ({@link Duration#ZERO} disables; must not be negative)
         * @return this builder
         */
        public Builder replicationLogRetentionTime(Duration retentionTime) {
            Objects.requireNonNull(retentionTime, "replicationLogRetentionTime");
            if (retentionTime.isNegative()) {
                throw new IllegalArgumentException("replicationLogRetentionTime must not be negative");
            }
            this.replicationLogRetentionTime = retentionTime;
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

        /**
         * Sets how this node ingests replication when acting as a follower. Defaults to
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

        /**
         * Enables the disk-backed resend op-log (#127). See {@link #persistentResendLog()}. Pair with
         * {@link #replicationLogRetentionTime(Duration)} to set the temporal backlog window.
         *
         * @param persistentResendLog {@code true} to back the resend op-log with the on-disk store
         * @return this builder
         */
        public Builder persistentResendLog(boolean persistentResendLog) {
            this.persistentResendLog = persistentResendLog;
            return this;
        }

        /**
         * Sets the per-segment entry cap for the on-disk resend op-log.
         *
         * @param maxEntries entries per segment (must be >= 1)
         * @return this builder
         */
        public Builder resendLogSegmentMaxEntries(int maxEntries) {
            if (maxEntries < 1) {
                throw new IllegalArgumentException("resendLogSegmentMaxEntries must be >= 1");
            }
            this.resendLogSegmentMaxEntries = maxEntries;
            return this;
        }

        /**
         * Sets the per-segment max age for the on-disk resend op-log. {@link Duration#ZERO} disables
         * age-based segment rolling.
         *
         * @param maxAge the per-segment max age (must not be negative)
         * @return this builder
         */
        public Builder resendLogSegmentMaxAge(Duration maxAge) {
            Objects.requireNonNull(maxAge, "resendLogSegmentMaxAge");
            if (maxAge.isNegative()) {
                throw new IllegalArgumentException("resendLogSegmentMaxAge must not be negative");
            }
            this.resendLogSegmentMaxAge = maxAge;
            return this;
        }

        /**
         * Sets the hard disk-side count backstop per topic for the on-disk resend op-log.
         *
         * @param maxEntries the disk entry cap (must be >= 1)
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
         * Sets the maximum number of sequences served from the disk resend op-log in one response.
         *
         * @param maxBatch the per-response read cap (must be >= 1)
         * @return this builder
         */
        public Builder resendLogReadBatchMax(int maxBatch) {
            if (maxBatch < 1) {
                throw new IllegalArgumentException("resendLogReadBatchMax must be >= 1");
            }
            this.resendLogReadBatchMax = maxBatch;
            return this;
        }

        public ReplicationConfig build() {
            if (dataDirectory == null) {
                throw new IllegalStateException("dataDirectory must be set");
            }
            return new ReplicationConfig(quorum, operationTimeout, retryInterval, strictConsistency, dataDirectory,
                    resendGapThreshold, resendTimeout, replicationLogRetention, replicationLogRetentionTime,
                    appliedSetMaxSize, operationLogMaxSize, leaderLocalApply, followerIngestMode, relayDurability,
                    relayGroupCommitInterval, persistentResendLog, resendLogSegmentMaxEntries, resendLogSegmentMaxAge,
                    resendLogMaxEntries, resendLogReadBatchMax);
        }
    }
}
