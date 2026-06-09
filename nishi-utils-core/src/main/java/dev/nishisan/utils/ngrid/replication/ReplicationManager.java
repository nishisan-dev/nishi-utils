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

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.LeaseExpiredException;
import dev.nishisan.utils.ngrid.cluster.coordination.LeadershipListener;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.BroadcastListener;
import dev.nishisan.utils.ngrid.common.BroadcastMessagePayload;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.FollowerProgressPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.OperationStatus;
import dev.nishisan.utils.ngrid.common.RelayStreamBatchPayload;
import dev.nishisan.utils.ngrid.common.RelayStreamFetchPayload;
import dev.nishisan.utils.ngrid.common.ReplicationPayload;
import dev.nishisan.utils.ngrid.common.SyncRequestPayload;
import dev.nishisan.utils.ngrid.common.SyncResponsePayload;
import dev.nishisan.utils.queue.NQueue;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Coordinates quorum based replication leveraging the transport. The manager
 * handles both
 * leader initiated operations and replication requests coming from other nodes.
 */
public class ReplicationManager
        implements TransportListener, LeadershipListener, ClusterCoordinator.MembershipListener, Closeable {
    private static final Logger LOGGER = Logger.getLogger(ReplicationManager.class.getName());

    private final Transport transport;
    private final ClusterCoordinator coordinator;
    private final ReplicationConfig config;
    private final Map<String, ReplicationHandler> handlers = new ConcurrentHashMap<>();
    private final Map<UUID, PendingOperation> pending = new ConcurrentHashMap<>();
    private final Map<UUID, ReplicatedRecord> log = new ConcurrentHashMap<>();
    private final LinkedHashSet<UUID> applied = new LinkedHashSet<>();
    private final java.util.concurrent.atomic.AtomicLong globalSequence = new java.util.concurrent.atomic.AtomicLong(0);
    private final Map<String, java.util.concurrent.atomic.AtomicLong> sequenceByTopic = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> leaderEmissionLocksByTopic = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong appliedSequence = new java.util.concurrent.atomic.AtomicLong(
            0);
    private volatile long lastAppliedSequence = 0;
    private final Set<String> syncingTopics = ConcurrentHashMap.newKeySet();
    // Janitor state: timestamp (millis) of the LAST sync activity per topic — updated on every
    // SYNC_RESPONSE chunk received. The janitor releases a sync guard only when NO chunk has arrived
    // for SYNC_STUCK_TIMEOUT_MS (a genuinely lost/hung sync), NOT merely because nextExpected has
    // not advanced yet. A large multi-chunk (byte-sliced) snapshot legitimately takes many seconds
    // and only advances nextExpected on the FINAL chunk; keying the janitor on chunk arrival keeps
    // it from killing a healthy in-flight transfer mid-way (which would resetState the follower and
    // never converge).
    private final Map<String, Long> lastSyncActivityByTopic = new ConcurrentHashMap<>();
    private static final long SYNC_STUCK_TIMEOUT_MS = 15_000L;
    // Watermark captured at chunk 0 of an in-flight snapshot, reused for all chunks of that snapshot
    // so a multi-chunk (byte-sliced) snapshot lands a consistent nextExpected on the follower.
    // Keyed by "<followerNodeId>::<topic>".
    private final Map<String, Long> activeSyncWatermark = new ConcurrentHashMap<>();
    private final Set<String> leaderSyncTopics = ConcurrentHashMap.newKeySet();
    private final java.util.concurrent.atomic.AtomicBoolean leaderSyncing = new java.util.concurrent.atomic.AtomicBoolean(
            false);
    private final java.util.concurrent.atomic.AtomicReference<NodeId> lastLeader = new java.util.concurrent.atomic.AtomicReference<>();

    // Multi-thread executor to prevent starvation when async apply callbacks recursively
    // submit tasks (leader local-apply path).
    private final ExecutorService executor = Executors.newFixedThreadPool(4, r -> {
        Thread t = new Thread(r, "ngrid-replication");
        t.setDaemon(true);
        return t;
    });
    private final ScheduledExecutorService timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ngrid-replication-timeout");
        t.setDaemon(true);
        return t;
    });

    private volatile boolean running;

    // Durable per-topic apply frontier (the next sequence the follower expects to apply) and its
    // coalesced disk persistence. This is the RELAY_STREAM apply cursor; the dedicated lock below
    // serializes frontier advances against snapshot cutover.
    private final Map<String, Long> nextExpectedSequenceByTopic = new ConcurrentHashMap<>();
    private final Path sequenceStatePath;
    // Sequence-state persistence is a recovery HINT (lost state just triggers a re-sync), so it is
    // coalesced: the hot path marks it dirty (no I/O, no lock cost) and a scheduled flush writes it
    // at most once per interval, OFF the lock. Writing the whole file on every applied op (2k+/s)
    // under sequenceBufferLock made the lock hold time bounded by disk latency — a throughput
    // bottleneck and a freeze hazard on any I/O stall.
    private volatile boolean sequenceStateDirty = false;
    // Serializes apply-frontier advances (commitRelayBatch) against snapshot cutover and frontier
    // reads. Named historically; it now guards only the durable apply frontier of the relay stream.
    private final ReentrantLock sequenceBufferLock = new ReentrantLock();
    // Max time to wait when acquiring sequenceBufferLock on the replication path. A bounded tryLock
    // (instead of an unbounded lock()) means that if the lock is ever orphaned — e.g. a worker dies
    // leaving the ReentrantLock held with no live owner — the replication path degrades to a
    // recoverable timeout (the operation aborts and is retried / re-synced) instead of parking the
    // whole replication pool forever and freezing the node (which then drops the cluster's leader).
    private static final long LOCK_ACQUIRE_TIMEOUT_MS = 15_000L;

    // Leader-side replication log indexed by sequence (per topic) for resend
    // support. Values carry a leader-local index timestamp (TimedPayload) so the log can be
    // evicted both by count (memory cap) and by time (backlog window) — see indexReplicationPayload.
    private final Map<String, java.util.NavigableMap<Long, TimedPayload>> replicationLogBySequence = new ConcurrentHashMap<>();

    // Disk tier of the hybrid resend op-log (#127): when persistentResendLog is enabled, the heap map
    // above keeps only the freshest window (count-capped) and this durable, segmented, time-governed
    // store holds the deep backlog window off-heap — so a large window costs disk, not heap (the cause
    // of the re-snapshot death spiral under high production). Null when persistence is disabled.
    private volatile ResendLogStore resendLogStore;

    // ── Join convergence (#129) ──────────────────────────────────────────────────
    // Wall-clock of construction, used by the boot-window guard that stops a transient lone
    // self-election from advertising empty state as a ready, caught-up leadership (3c).
    private final long startedAtMillis = System.currentTimeMillis();
    // Leader-side join-quiesce gate (3b, opt-in via config.leaderPauseOnJoin): the leader pauses
    // production while a not-caught-up follower joins, until it catches up / disconnects / times out.
    private final java.util.concurrent.atomic.AtomicBoolean joinQuiescing = new java.util.concurrent.atomic.AtomicBoolean(
            false);
    private final Map<NodeId, Long> followerAppliedByNode = new ConcurrentHashMap<>();
    private final Set<NodeId> quiescingFor = ConcurrentHashMap.newKeySet();
    private volatile long joinQuiesceStartedMs;
    // Active members observed on the previous membership change, to detect newly-joined nodes.
    private final Set<NodeId> knownActiveMembers = ConcurrentHashMap.newKeySet();

    // User-level broadcast messaging (broadcastMessage API): best-effort fire-and-forget messages
    // between nodes for coordination. Listeners are invoked on a worker thread (keep them non-blocking).
    private final Set<BroadcastListener> broadcastListeners = new java.util.concurrent.CopyOnWriteArraySet<>();

    // ── Relay-stream ingestion (RELAY_STREAM) ────────────────────────────────────
    // The follower PULLS the leader's durable op-log as a strictly contiguous stream, persisting
    // each run to a durable relay (one NQueue per topic); a per-topic consumer applies it at its own
    // pace, decoupling reception from application. nextExpectedSequenceByTopic is the durable apply
    // frontier. Because the pull is contiguous by construction there is no gap/reorder buffer.
    private volatile RelayStore relayStore;
    private final Map<String, Thread> relayApplyThreads = new ConcurrentHashMap<>();
    private final Map<String, Object> relaySignals = new ConcurrentHashMap<>();
    private static final int RELAY_STALE_DISCARD_BATCH = 4096;
    // Topics that must bootstrap (fresh snapshot) on this start because the prior shutdown was
    // unclean — the coalesced apply frontier may be behind the durable handler state, which would
    // re-apply (duplicating the non-idempotent queue OFFER). A clean shutdown writes a marker that
    // makes a clean restart resume without bootstrap.
    // Set on an unclean restart (crash): the coalesced apply frontier may be behind — or entirely lost
    // — vs. the durable handler state, so every relay topic with prior data must bootstrap (a fresh
    // snapshot replaces local state) instead of risking a stale/lost-frontier re-apply that duplicates
    // the non-idempotent queue OFFER. relayPendingBootstrap holds those topics until the apply loop
    // requests the snapshot (deferred so a follower syncs from the leader, never from itself).
    private volatile boolean relayUncleanRestart = false;
    private final Set<String> relayPendingBootstrap = ConcurrentHashMap.newKeySet();

    // RELAY_STREAM follower IO: a per-topic fetch loop pulls the leader op-log from a durable cursor
    // (the relay tail) and persists contiguous runs in order; the existing apply loop drains the relay.
    private final Map<String, Thread> relayFetchThreads = new ConcurrentHashMap<>();
    private final Map<String, Object> relayFetchSignals = new ConcurrentHashMap<>();
    private final Map<String, java.util.concurrent.atomic.AtomicLong> relayStreamCursorByTopic =
            new ConcurrentHashMap<>();
    // Epoch-millis before which the fetch loop must not issue a new fetch for a topic: set when a fetch
    // is in flight (cleared on batch arrival), and reused as the caught-up long-poll gap.
    private final Map<String, Long> relayFetchPendingUntilByTopic = new ConcurrentHashMap<>();
    // Last leader high-watermark per topic, learned from RELAY_STREAM_BATCH (lag observability).
    private final Map<String, Long> leaderHwmByTopic = new ConcurrentHashMap<>();
    // Leader's retained-window floor per topic, learned from RELAY_STREAM_BATCH (snapshot-risk view).
    private final Map<String, Long> leaderOldestByTopic = new ConcurrentHashMap<>();
    // Cumulative stream bytes pulled per topic (RELAY_STREAM throughput observability).
    private final Map<String, java.util.concurrent.atomic.AtomicLong> streamBytesInByTopic =
            new ConcurrentHashMap<>();

    // Metrics
    // RELAY_STREAM has no gap detection (the pull is contiguous by construction), so this counter
    // never increments — it is retained at 0 to keep the public getGapsDetected() API/observability
    // contract stable (RELAY_STREAM tests assert gapsDetected == 0).
    private final java.util.concurrent.atomic.AtomicLong gapsDetected = new java.util.concurrent.atomic.AtomicLong(0);
    // Evicted-gap skips and resend convergence are legacy concerns absent in RELAY_STREAM; these
    // counters never increment and stay at 0 (kept for the stable public metrics API).
    private final java.util.concurrent.atomic.AtomicLong evictedSkipCount = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong resendSuccessCount = new java.util.concurrent.atomic.AtomicLong(
            0);
    private final java.util.concurrent.atomic.AtomicLong snapshotFallbackCount = new java.util.concurrent.atomic.AtomicLong(
            0);
    // Total snapshot/sync requests this node has initiated (chunk 0). In RELAY_STREAM this stays flat
    // in regime — lag is absorbed by the stream/relay; a snapshot is only the cold-bootstrap path.
    private final java.util.concurrent.atomic.AtomicLong syncRequestCount = new java.util.concurrent.atomic.AtomicLong(
            0);
    private final java.util.concurrent.atomic.AtomicLong totalConvergenceTimeMs = new java.util.concurrent.atomic.AtomicLong(
            0);
    private final java.util.concurrent.atomic.AtomicLong convergenceCount = new java.util.concurrent.atomic.AtomicLong(
            0);
    private final java.util.concurrent.atomic.AtomicLong replicationLogTimeEvictedCount = new java.util.concurrent.atomic.AtomicLong(
            0);

    private static record Failure(PendingOperation operation, Throwable error) {
    }

    /**
     * Resend-log entry: the wire {@link ReplicationPayload} plus the leader-local epoch-millis
     * timestamp captured when the operation was committed and indexed. The timestamp drives the
     * temporal retention window ({@link ReplicationConfig#replicationLogRetentionTime()}); it never
     * travels on the wire (the payload is unwrapped before being sent to followers).
     */
    private static record TimedPayload(ReplicationPayload payload, long indexedAtMillis) {
    }

    /**
     * Read-only per-topic follower state used by diagnostics dashboards and regression tests.
     *
     * @param topic                 replication topic
     * @param nextExpectedSequence  next sequence the follower expects to apply
     * @param relayHeadSequence     oldest unapplied relay sequence, or 0 when there is no relay head
     * @param relayBacklog          number of unapplied relay entries
     * @param syncing               true while a snapshot sync is active for the topic
     * @param relayPendingBootstrap true while relay replay is waiting for bootstrap sync
     * @param resendPending         always {@code false} since 5.0.0 (RELAY_STREAM has no NAK/resend)
     * @param streamCursor          RELAY_STREAM: highest sequence persisted to the relay (pull cursor)
     * @param leaderHighWatermark   leader's highest sequence for the topic (own, or learned via stream)
     * @param leaderOldestSequence  leader's retained-window floor (oldest streamable), or -1 if unknown
     * @param lag                   leader high-watermark minus the applied frontier (follower lag)
     * @param streamBytesIn         cumulative stream bytes pulled for the topic (RELAY_STREAM)
     * @param streaming             true while actively streaming the topic from the leader
     */
    public record TopicReplicationStatus(
            String topic,
            long nextExpectedSequence,
            long relayHeadSequence,
            long relayBacklog,
            boolean syncing,
            boolean relayPendingBootstrap,
            boolean resendPending,
            long streamCursor,
            long leaderHighWatermark,
            long leaderOldestSequence,
            long lag,
            long streamBytesIn,
            boolean streaming) {
    }

    public ReplicationManager(Transport transport, ClusterCoordinator coordinator, ReplicationConfig config) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.config = Objects.requireNonNull(config, "config");
        this.sequenceStatePath = config.dataDirectory().resolve("sequence-state.dat");
        loadSequenceState();
    }

    /**
     * Protected constructor for testing purposes only.
     */
    protected ReplicationManager() {
        this.transport = null;
        this.coordinator = null;
        this.config = null;
        this.sequenceStatePath = null;
    }

    public void start() {
        if (running) {
            return;
        }
        running = true;
        // Wire the leader high-watermark supplier so heartbeats carry a real watermark even when the
        // ReplicationManager is assembled manually (without NGridNode). The follower reads this
        // watermark to drive the proactive cold-join sync (#129); the coordinator default (-1) starved
        // it, so a fresh follower never converged against a quiescent leader in manual assemblies (#131).
        coordinator.setLeaderHighWatermarkSupplier(
                () -> coordinator.isLeader() ? getGlobalSequence() : getLastAppliedSequence());
        if (isStreamMode()) {
            // Restore the applied progress metric from the durable per-topic frontiers so a clean
            // restart does not report 0 applied (the counter is otherwise per-session). A bootstrap,
            // if one happens, re-anchors it at the snapshot watermark.
            seedAppliedSequenceFromFrontier();
            detectUncleanRestartAndMarkBootstrap();
            // Relay retention mirrors the leader op-log window (#122): an over-retention
            // backlog is surfaced to the consumer and resolved by bootstrap, never silently
            // dropped (the clamp guarantees that).
            this.relayStore = new RelayStore(config.dataDirectory().resolve("relay"),
                    config.replicationLogRetentionTime(), config.relayDurability(),
                    config.relayGroupCommitInterval(), config.relayExpireAfterWrite());
            // Start consumers for any handler registered before start() (normal flow registers after).
            for (String topic : handlers.keySet()) {
                ensureRelayApplyLoop(topic);
                ensureRelayFetchLoop(topic);
            }
            // Disk-backed op-log (the leader's binlog and stream source of truth). Initialized
            // regardless of role — a follower may be promoted to leader later and must already be
            // serving the stream from a durable window.
            this.resendLogStore = new ResendLogStore(config.dataDirectory().resolve("resend-log"),
                    config.resendLogSegmentMaxEntries(), config.resendLogSegmentMaxAge(),
                    config.resendLogSegmentMaxBytes(), config.replicationLogRetentionTime(),
                    config.resendLogMaxEntries(), config.resendLogMaxSegments(),
                    config.relayDurability() == RelayDurability.ALWAYS);
        }
        transport.addListener(this);
        coordinator.addLeadershipListener(this);
        if (config.leaderPauseOnJoin()) {
            // Leader-pause-on-join (#129): detect joins (membership), have followers report progress, and
            // periodically re-evaluate the quiesce gate (release on catch-up / disconnect / timeout).
            coordinator.addMembershipListener(this);
            long progressMs = Math.max(50L, config.followerProgressInterval().toMillis());
            timeoutScheduler.scheduleAtFixedRate(this::sendFollowerProgress, progressMs, progressMs,
                    TimeUnit.MILLISECONDS);
            timeoutScheduler.scheduleAtFixedRate(this::checkJoinQuiesce, 200, 200, TimeUnit.MILLISECONDS);
        }
        Duration timeout = config.operationTimeout();
        long periodMs = Math.max(100L, timeout.toMillis() / 2);
        timeoutScheduler.scheduleAtFixedRate(this::checkTimeouts, periodMs, periodMs, TimeUnit.MILLISECONDS);
        Duration retryInterval = config.retryInterval();
        long retryMs = Math.max(100L, retryInterval.toMillis());
        timeoutScheduler.scheduleAtFixedRate(this::retryPending, retryMs, retryMs, TimeUnit.MILLISECONDS);
        timeoutScheduler.scheduleAtFixedRate(this::checkStuckSyncs, 1000, 1000, TimeUnit.MILLISECONDS);
        // Coalesced off-lock persistence of the sequence state (dirty-flag flush)
        timeoutScheduler.scheduleAtFixedRate(this::flushSequenceStateIfDirty, 1000, 1000, TimeUnit.MILLISECONDS);
        // Periodic memory eviction for the operation audit log
        long cleanupPeriodMs = Math.max(5000L, timeout.toMillis() * 5);
        timeoutScheduler.scheduleAtFixedRate(this::trimLog, cleanupPeriodMs, cleanupPeriodMs, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        // Stop the RELAY_STREAM fetch loops first: they only pull and persist to the relay (no apply
        // state), so quiescing them before the apply loops keeps shutdown ordering simple and avoids a
        // late append racing the clean-marker.
        stopRelayFetchLoops();
        // Interrupt + wake the per-topic relay apply consumers, then JOIN them: no apply may be in
        // flight when we flush the final frontier and mark the shutdown clean — otherwise a thread still
        // inside handler.apply() could advance/poll the relay AFTER the flush, leaving durable state
        // ahead of the saved frontier under a clean marker (gaps/duplicates on the next start).
        List<Thread> relayThreads = new ArrayList<>(relayApplyThreads.values());
        for (Map.Entry<String, Thread> e : relayApplyThreads.entrySet()) {
            e.getValue().interrupt();
            Object sig = relaySignals.get(e.getKey());
            if (sig != null) {
                synchronized (sig) {
                    sig.notifyAll();
                }
            }
        }
        boolean allAppliersTerminated = true;
        for (Thread t : relayThreads) {
            try {
                t.join(5000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            if (t.isAlive()) {
                allAppliersTerminated = false;
            }
        }
        relayApplyThreads.clear();
        // Frontier is now consistent (no in-flight apply). Persist it, then mark the shutdown clean ONLY
        // if every applier actually terminated; otherwise leave no marker so the next start bootstraps
        // (the safe side).
        flushSequenceStateIfDirty();
        if (allAppliersTerminated) {
            writeCleanShutdownMarker();
        } else {
            LOGGER.warning("Relay apply consumers did not all terminate on stop; skipping clean-shutdown "
                    + "marker so the next start bootstraps as a safety measure.");
        }
        transport.removeListener(this);
        coordinator.removeLeadershipListener(this);
        failAllPending("ReplicationManager stopped");
    }

    /**
     * Detects whether the previous shutdown was unclean (no clean-shutdown marker). On a crash the
     * coalesced apply frontier may be behind — or lost entirely — so every relay topic with prior data
     * must bootstrap (the snapshot replaces local state, so a stale/lost frontier cannot duplicate the
     * non-idempotent queue OFFER by re-applying). The per-topic decision is made in {@link
     * #registerHandler} (which knows the topic) and the snapshot request is deferred to the apply loop
     * (so a follower syncs from the leader, never from itself).
     */
    private void detectUncleanRestartAndMarkBootstrap() {
        Path relayDir = config.dataDirectory().resolve("relay");
        relayUncleanRestart = RelayStore.isUncleanRestart(relayDir);
        RelayStore.consumeCleanMarker(relayDir);
        if (relayUncleanRestart) {
            LOGGER.warning("Unclean relay restart detected; topics with prior data will bootstrap from a "
                    + "fresh snapshot before applying (frontier may be stale or lost).");
        }
    }

    private void writeCleanShutdownMarker() {
        if (!isStreamMode()) {
            return;
        }
        RelayStore.writeCleanMarker(config.dataDirectory().resolve("relay"));
    }

    public void registerHandler(String topic, ReplicationHandler handler) {
        handlers.put(topic, handler);
        if (isStreamMode()) {
            if (relayUncleanRestart
                    && RelayStore.hasTopicData(config.dataDirectory().resolve("relay"), topic)) {
                // Unclean restart and this topic had prior relay data: it must bootstrap before applying
                // (regardless of any saved frontier, which may be lost). The snapshot is actually
                // requested by the apply loop once a leader is known and this node is a follower
                // (drainRelayOnce), avoiding a leader-syncs-from-itself loop.
                relayPendingBootstrap.add(topic);
            }
            // Tie the apply consumer to handler registration, not to start(): an op may reach
            // the relay before the handler exists (it is durably parked on disk until then),
            // but apply must not begin until the handler is available.
            ensureRelayApplyLoop(topic);
            ensureRelayFetchLoop(topic);
        }
    }

    public long getGlobalSequence() {
        return globalSequence.get();
    }

    public long getLastAppliedSequence() {
        return lastAppliedSequence;
    }

    /**
     * Seeds the applied-progress metric from the durable per-topic apply frontiers, so a restart
     * resumes the metric instead of reporting 0 (the counter is per-session). The total applied op
     * count equals the sum of {@code (nextExpected - 1)} across topics.
     */
    private void seedAppliedSequenceFromFrontier() {
        long applied = 0L;
        for (Map.Entry<String, Long> e : nextExpectedSequenceByTopic.entrySet()) {
            applied += Math.max(0L, e.getValue() - 1L);
        }
        if (applied > 0L) {
            appliedSequence.set(applied);
            lastAppliedSequence = applied;
        }
    }

    /**
     * Returns the current number of entries in the applied-operations dedup set.
     * Intended for testing and observability.
     */
    public int getAppliedSetSize() {
        acquireSequenceLock();
        try {
            return applied.size();
        } finally {
            sequenceBufferLock.unlock();
        }
    }

    /**
     * Returns the current number of entries in the operation audit log.
     * Intended for testing and observability.
     */
    public int getOperationLogSize() {
        return log.size();
    }

    /**
     * Resets the persisted sequence state. Used when stale data from a previous
     * epoch is truncated — the old sequence numbers become invalid and the
     * ReplicationManager must start fresh so followers can sync correctly.
     */
    public void resetSequenceState() {
        globalSequence.set(0);
        lastAppliedSequence = 0;
        nextExpectedSequenceByTopic.clear();
        sequenceByTopic.clear();
        if (sequenceStatePath != null) {
            try {
                Files.deleteIfExists(sequenceStatePath);
                LOGGER.info("Sequence state reset due to epoch truncation");
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to delete sequence state file", e);
            }
        }
    }

    public boolean isLeaderSyncing() {
        return leaderSyncing.get();
    }

    /**
     * Epoch-millis timestamp of the oldest unapplied relay entry for {@code topic} (the relay head),
     * or {@link Long#MAX_VALUE} when there is no relay/entry. Backs the public
     * {@link #getRelayHeadAgeMillis(String)} apply-lag metric.
     */
    private long relayHeadTimestamp(String topic) {
        RelayStore store = relayStore;
        if (store == null) {
            return Long.MAX_VALUE;
        }
        try {
            return store.relayFor(topic).peekRecord()
                    .map(record -> record.meta().getTimestamp())
                    .orElse(Long.MAX_VALUE);
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Relay head-age read failed for topic " + topic, e);
            return Long.MAX_VALUE;
        }
    }

    /**
     * Exposes the configured replication operation timeout for callers that need to
     * bound their waiting time (e.g. higher-level services calling
     * {@code future.get(...)}).
     */
    public Duration operationTimeout() {
        return config.operationTimeout();
    }

    public CompletableFuture<ReplicationResult> replicate(String topic, Object payload) {
        return replicate(topic, payload, null);
    }

    public CompletableFuture<ReplicationResult> replicate(String topic, Object payload, Integer quorumOverride) {
        return replicate(topic, payload, payload, quorumOverride);
    }

    /**
     * Replicates an operation, allowing the leader's local apply to use a payload
     * distinct from the one shipped to followers.
     *
     * <p>The {@code wirePayload} is what travels to followers and is recorded in the
     * resend log (it must be serialization-stable across the transport, e.g. the
     * {@code byte[]} produced by {@code MapReplicationCodec}). The
     * {@code localApplyPayload} is handed to the local {@link ReplicationHandler#apply}
     * on this (leader) node only. Passing the live command object as
     * {@code localApplyPayload} lets the leader keep the original value instance in
     * its local state (leader-local by-reference), while followers still receive the
     * serialized, type-faithful copy.
     *
     * <p>When {@code wirePayload == localApplyPayload} this behaves exactly like the
     * single-payload overload.
     */
    public CompletableFuture<ReplicationResult> replicate(String topic, Object wirePayload,
            Object localApplyPayload, Integer quorumOverride) {
        if (!coordinator.isLeader()) {
            throw new IllegalStateException("Replication can only be initiated by the leader");
        }
        // Gate writes while this node, although leader, is still catching up from a peer. Accepting
        // a write now would advance from stale state and overwrite the previous leader's progress —
        // defense in depth that closes the divergence window for ALL backends (queue and map).
        if (isLeaderSyncing()) {
            throw new LeaderSyncingException(
                    "Leader is syncing (catch-up in progress), write rejected to prevent stale-state divergence");
        }
        if (config.leaderPauseOnJoin() && isJoinQuiescing()) {
            // Leader-pause-on-join (#129): a not-caught-up follower is joining; pause production until it
            // drains so convergence is deterministic (no firehose during bootstrap). Bounded + released
            // on catch-up/disconnect/timeout. Mirror of the failover drain-gate, on the join path.
            throw new LeaderSyncingException(
                    "Leader is quiescing for a joining follower (catch-up in progress), write rejected");
        }
        if (!coordinator.hasValidLease()) {
            throw new LeaseExpiredException("Leader lease expired, write rejected to prevent data divergence");
        }
        ReplicationHandler handler = handlers.get(topic);
        if (handler == null) {
            throw new IllegalArgumentException("No replication handler registered for topic: " + topic);
        }
        UUID operationId = UUID.randomUUID();
        PendingOperation operation = new PendingOperation(operationId, topic, wirePayload, localApplyPayload,
                coordinator.getLeaderEpoch(), requiredQuorum(quorumOverride));
        pending.put(operationId, operation);
        log.put(operationId, new ReplicatedRecord(operationId, topic, wirePayload, OperationStatus.PENDING));

        // Local node acknowledges receipt, but defers application until quorum
        operation.ack(transport.local().nodeId());

        replicateToFollowers(operation);
        return operation.future();
    }

    private int requiredQuorum(Integer override) {
        if (isStreamMode()) {
            // RELAY_STREAM is ASYNC: the leader commits on its own durable op-log (the binlog) and
            // never blocks on followers, which pull the stream at their own pace. (Semi-sync, which
            // waits for a follower receipt before acking the client, is a separate opt-in.)
            return 1;
        }
        int requested = override != null ? override : config.quorum();
        if (requested < 1) {
            requested = 1;
        }
        if (config.strictConsistency()) {
            return requested;
        }
        int members = coordinator.activeMembers().size();
        if (members == 0) {
            members = 1;
        }
        return Math.max(1, Math.min(requested, members));
    }

    private void replicateToFollowers(PendingOperation operation) {
        ReentrantLock emissionLock = leaderEmissionLock(operation.topic);
        emissionLock.lock();
        try {
            globalSequence.incrementAndGet();
            long seq = nextSequenceForTopic(operation.topic);
            operation.sequence = seq;

            // Persist sequence state for leader recovery (coalesced; flushed off the hot path)
            sequenceStateDirty = true;

            // RELAY_STREAM: the durable op-log (binlog) is the ONLY delivery path to followers, so it
            // must be written BEFORE the write is acked. If the append fails, rewind the per-topic
            // sequence (held under the emission lock, so safe and contiguous — no permanent hole that
            // would stall followers) and fail the write instead of acking a record that is absent from
            // the stream source.
            if (isStreamMode() && !appendStreamOpLog(operation, seq)) {
                rollbackTopicSequence(operation.topic, seq);
                failOperation(operation, new IllegalStateException(
                        "RELAY_STREAM op-log append failed (seq " + seq + ", topic " + operation.topic
                                + "); write not durable in the stream source"));
                return;
            }

            // RELAY_STREAM does NOT push: the leader records the op in its durable op-log (the binlog,
            // appended above and indexed in completeOperation) and followers PULL it as a sequential
            // stream via RELAY_STREAM_FETCH. The local node already acked on receipt, so checkCompletion
            // commits the (async, quorum-1) write here.
            checkCompletion(operation);
        } finally {
            emissionLock.unlock();
        }
    }

    /**
     * Appends a committed operation's frame to the durable op-log (binlog) for RELAY_STREAM, returning
     * {@code false} on failure so the caller can fail the write rather than ack a record absent from
     * the stream source. Called under the per-topic emission lock.
     */
    private boolean appendStreamOpLog(PendingOperation operation, long seq) {
        ResendLogStore store = resendLogStore;
        if (store == null) {
            return false; // stream mode always initializes the op-log; defensive
        }
        ReplicationHandler handler = handlers.get(operation.topic);
        if (handler == null) {
            return false;
        }
        try {
            byte[] payloadBytes = handler.encodePayload(operation.payload);
            RelayEntry entry = new RelayEntry(operation.epoch, seq, operation.topic, operation.operationId,
                    payloadBytes);
            return store.append(operation.topic, seq, System.currentTimeMillis(), RelayEntryCodec.encode(entry));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "RELAY_STREAM op-log append failed for seq " + seq
                    + " (topic " + operation.topic + ")", e);
            return false;
        }
    }

    /**
     * Rewinds the per-topic sequence counter from {@code seq} to {@code seq-1} so a failed emission does
     * not burn the sequence (which would leave a permanent op-log hole). Safe only under the per-topic
     * emission lock, where no other emission for the topic can race.
     */
    private void rollbackTopicSequence(String topic, long seq) {
        java.util.concurrent.atomic.AtomicLong counter = sequenceByTopic.get(topic);
        if (counter != null) {
            counter.compareAndSet(seq, seq - 1);
        }
        sequenceStateDirty = true;
    }

    private ReentrantLock leaderEmissionLock(String topic) {
        return leaderEmissionLocksByTopic.computeIfAbsent(topic, ignored -> new ReentrantLock());
    }

    private void retryPending() {
        try {
            if (!running || !coordinator.isLeader()) {
                return;
            }
            // RELAY_STREAM does not push: followers pull committed ops from the durable op-log, so the
            // only retry work here is to re-drive the (async, quorum-1) commit of any still-pending op.
            for (PendingOperation operation : pending.values()) {
                if (operation.isDone()) {
                    continue;
                }
                checkCompletion(operation);
            }
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "Unexpected error in retryPending loop", t);
        }
    }

    /**
     * Acquires {@link #sequenceBufferLock} with a bounded wait. Callers MUST follow the
     * {@code acquireSequenceLock(); try { ... } finally { sequenceBufferLock.unlock(); }} idiom: on a
     * timeout this throws BEFORE the {@code try}, so the {@code finally} never runs and no spurious
     * unlock happens. A timeout indicates a stuck/orphaned lock; the caller aborts and the operation
     * is retried or recovered via re-sync — far better than parking forever and freezing the node.
     */
    private void acquireSequenceLock() {
        try {
            if (!sequenceBufferLock.tryLock(LOCK_ACQUIRE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                throw new IllegalStateException("Timed out after " + LOCK_ACQUIRE_TIMEOUT_MS
                        + "ms acquiring sequenceBufferLock (possible orphaned lock); aborting to recover");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while acquiring sequenceBufferLock", e);
        }
    }

    private void checkCompletion(PendingOperation operation) {
        if (operation.isCommitted()) {
            return;
        }
        if (operation.ackCount() >= operation.quorum) {
            if (!config.leaderLocalApply()) {
                // External engine owns the authoritative state (delta-shipping op-log): skip the
                // redundant leader-local apply and commit + INDEX the operation SYNCHRONOUSLY now
                // that quorum is met, so it is immediately resendable to a catching-up follower.
                // Indexing at the end of the async apply (the leaderLocalApply=true path below) let
                // the resend index lag the send frontier by the whole apply backlog under high
                // throughput, which made frontier resends impossible → perpetual snapshot fallback.
                if (operation.markCommitStarted()) {
                    long appliedSeq = appliedSequence.updateAndGet(c -> Math.max(c, operation.sequence));
                    lastAppliedSequence = appliedSeq;
                    operation.markLocalApplied();
                    completeOperation(operation);
                }
                return;
            }
            ReplicationHandler handler = handlers.get(operation.topic);
            if (handler != null) {
                if (operation.markLocalApplyStarted()) {
                    // LEADER PATH: Execute apply ASYNCHRONOUSLY to avoid deadlock.
                    // Uses localApplyPayload so leader-local by-reference maps keep the
                    // original value instance locally (defaults to the wire payload).
                    executor.submit(() -> {
                        try {
                            handler.apply(operation.operationId, operation.localApplyPayload);
                            recordApplied();
                            acquireSequenceLock();
                            try {
                                applied.add(operation.operationId);
                                trimApplied();
                            } finally {
                                sequenceBufferLock.unlock();
                            }
                            operation.markLocalApplied();

                            // Complete operation after successful apply
                            completeOperation(operation);
                        } catch (Throwable e) {
                            // Throwable (not Exception): an Error (OOM/StackOverflow) must not kill
                            // the pool worker silently nor skip cleanup — log it and fail the op.
                            LOGGER.log(Level.SEVERE, "Failed to apply committed operation locally", e);
                            failOperation(operation, e);
                        }
                    });
                }
                return; // Completion happens in callback
            }

            completeOperation(operation);
        }
    }

    private void completeOperation(PendingOperation operation) {
        operation.complete(OperationStatus.COMMITTED);
        log.computeIfPresent(operation.operationId, (id, record) -> {
            record.status(OperationStatus.COMMITTED);
            return record;
        });
        // Only committed operations are eligible for resend.
        ReplicationPayload committedPayload = new ReplicationPayload(
                operation.operationId,
                operation.sequence,
                operation.epoch,
                operation.topic,
                operation.payload);
        indexReplicationPayload(operation.topic, operation.sequence, committedPayload);
        pending.remove(operation.operationId);
    }

    @Override
    public void onPeerConnected(NodeInfo peer) {
        // No-op
    }

    @Override
    public void onPeerDisconnected(NodeId peerId) {
        // Join-quiesce (#129): a follower that vanishes mid-join must not freeze the leader — drop it
        // from the wait set and release the gate if it was the last one we were waiting on.
        knownActiveMembers.remove(peerId);
        followerAppliedByNode.remove(peerId);
        if (quiescingFor.remove(peerId)) {
            maybeReleaseJoinQuiesce();
        }
        for (PendingOperation operation : pending.values()) {
            if (operation.isDone()) {
                continue;
            }
            // Dynamically adjust quorum based on reachable members
            if (!config.strictConsistency()) {
                int newQuorum = computeQuorumForReachable(operation.originalQuorum);
                operation.updateQuorum(newQuorum);
                LOGGER.info(() -> String.format(
                        "Peer %s disconnected, adjusted quorum from %d to %d for operation %s",
                        peerId, operation.originalQuorum, newQuorum, operation.operationId));
            }
            checkCompletion(operation);
        }
    }

    @Override
    public void onMessage(ClusterMessage message) {
        if (message.type() == MessageType.SYNC_REQUEST) {
            handleSyncRequest(message);
        } else if (message.type() == MessageType.SYNC_RESPONSE) {
            handleSyncResponse(message);
        } else if (message.type() == MessageType.RELAY_STREAM_FETCH) {
            handleRelayStreamFetch(message);
        } else if (message.type() == MessageType.RELAY_STREAM_BATCH) {
            handleRelayStreamBatch(message);
        } else if (message.type() == MessageType.FOLLOWER_PROGRESS) {
            handleFollowerProgress(message);
        } else if (message.type() == MessageType.USER_BROADCAST) {
            handleBroadcast(message);
        }
    }

    private void handleSyncRequest(ClusterMessage message) {
        SyncRequestPayload payload = message.payload(SyncRequestPayload.class);
        if (!coordinator.isLeader() && !payload.allowFollowerResponse()) {
            return;
        }
        ReplicationHandler handler = handlers.get(payload.topic());
        if (handler == null)
            return;

        // Watermark capture for a (possibly multi-chunk) snapshot:
        // - Read it BEFORE capturing chunk 0 so it is a safe LOWER BOUND for the snapshot content
        //   (on the leader the state mutation precedes the sequence assignment in
        //   replicateToFollowers(), so a sequence read before the capture is reflected by a snapshot
        //   taken afterwards). Reading it AFTER the capture could label the snapshot with a sequence
        //   ABOVE its content → the follower skips entries never in the snapshot and never resent
        //   (the permanent phantom gap on topic=cardinal-state).
        // - REUSE the chunk-0 watermark for every subsequent chunk, so a large snapshot transferred
        //   over many chunks still lands the follower's nextExpected on a value consistent with the
        //   captured content, even though the leader keeps producing during the transfer.
        String syncKey = message.source() + "::" + payload.topic();
        long seq;
        if (payload.chunkIndex() == 0) {
            seq = getSyncSequenceForTopic(payload.topic());
            activeSyncWatermark.put(syncKey, seq);
        } else {
            seq = activeSyncWatermark.getOrDefault(syncKey, getSyncSequenceForTopic(payload.topic()));
        }
        ReplicationHandler.SnapshotChunk chunk = handler.getSnapshotChunk(payload.chunkIndex());
        if (chunk == null) {
            activeSyncWatermark.remove(syncKey);
            return;
        }
        if (!chunk.hasMore()) {
            activeSyncWatermark.remove(syncKey);
        }

        SyncResponsePayload responsePayload = new SyncResponsePayload(payload.topic(), seq, payload.chunkIndex(),
                chunk.hasMore(), chunk.data());
        ClusterMessage response = new ClusterMessage(UUID.randomUUID(),
                message.messageId(),
                MessageType.SYNC_RESPONSE,
                message.qualifier(),
                transport.local().nodeId(),
                message.source(),
                responsePayload,
                5);
        transport.send(response);
    }

    private void handleSyncResponse(ClusterMessage message) {
        SyncResponsePayload payload = message.payload(SyncResponsePayload.class);
        ReplicationHandler handler = handlers.get(payload.topic());
        if (handler == null)
            return;

        executor.submit(() -> {
            try {
                // Mark sync activity so the stuck-sync janitor does not kill a healthy in-flight
                // multi-chunk transfer — nextExpected only advances on the final chunk, so without
                // this a large byte-sliced snapshot would be torn down mid-way (resetState) and the
                // follower would never converge.
                lastSyncActivityByTopic.put(payload.topic(), System.currentTimeMillis());
                long currentNext;
                acquireSequenceLock();
                try {
                    currentNext = nextExpectedSequenceByTopic.getOrDefault(payload.topic(), 1L);
                } finally {
                    sequenceBufferLock.unlock();
                }
                long currentApplied = Math.max(0L, currentNext - 1L);
                if (payload.sequence() < currentApplied) {
                    LOGGER.warning(() -> "Ignoring stale sync for " + payload.topic()
                            + " (sequence=" + payload.sequence() + ", current=" + currentApplied + ")");
                    syncingTopics.remove(payload.topic());
                    // The local (newly promoted) leader already holds newer state than the peer's
                    // snapshot, so this topic is effectively caught up. Release its leader-sync guard
                    // too — otherwise leaderSyncing stays true forever (retryLeaderSync keeps pulling
                    // the same older snapshot) and the write gate in replicate() rejects every write
                    // even though the leader has the latest state.
                    if (leaderSyncTopics.remove(payload.topic()) && leaderSyncTopics.isEmpty()) {
                        leaderSyncing.set(false);
                    }
                    return;
                }
                if (payload.chunkIndex() == 0) {
                    LOGGER.info(() -> "Starting sync for " + payload.topic() + " at sequence " + payload.sequence());
                    handler.resetState();
                }
                handler.installSnapshot(payload.data());

                if (payload.hasMore()) {
                    requestSync(payload.topic(), payload.chunkIndex() + 1);
                } else {
                    // Last chunk installed: let the handler reassemble/decode a multi-chunk
                    // (byte-sliced) snapshot before the follower is considered caught up.
                    handler.onSnapshotInstalled();
                    LOGGER.info(
                            () -> "Sync completed for " + payload.topic() + ". Final sequence: " + payload.sequence());
                    completeSnapshotCutover(payload.topic(), payload.sequence());
                    syncingTopics.remove(payload.topic());
                    if (leaderSyncTopics.remove(payload.topic()) && leaderSyncTopics.isEmpty()) {
                        leaderSyncing.set(false);
                    }
                }
            } catch (Throwable e) {
                // Throwable (not Exception): an Error here (e.g. OOM decoding a large snapshot) must
                // not kill the pool worker silently; log it and release the sync guard to allow retry.
                LOGGER.log(Level.SEVERE, "Failed to install snapshot chunk", e);
                syncingTopics.remove(payload.topic()); // allow retry
            }
        });
    }

    private void completeSnapshotCutover(String topic, long watermark) {
        appliedSequence.updateAndGet(current -> Math.max(current, watermark));
        lastAppliedSequence = appliedSequence.get();
        relayPendingBootstrap.remove(topic);

        acquireSequenceLock();
        try {
            nextExpectedSequenceByTopic.put(topic, watermark + 1);
            sequenceStateDirty = true;
        } finally {
            sequenceBufferLock.unlock();
        }

        // RELAY_STREAM: re-anchor the pull cursor at the snapshot watermark so the fetch loop resumes
        // streaming from watermark+1 instead of a stale pre-snapshot cursor (which would re-pull below
        // the retained window and bounce on needSnapshot). Let it fetch immediately.
        relayStreamCursor(topic).set(watermark);
        relayFetchPendingUntilByTopic.put(topic, 0L);
        signalFetch(topic);

        signalRelay(topic);
    }

    private void requestSync(String topic) {
        requestSync(topic, 0);
    }

    private void requestSync(String topic, int chunkIndex) {
        coordinator.leaderInfo().ifPresent(leader -> {
            if (chunkIndex == 0) {
                syncRequestCount.incrementAndGet();
                LOGGER.info(() -> "Lag detected (" + (coordinator.getTrackedLeaderHighWatermark() - lastAppliedSequence)
                        + "). Requesting sync for " + topic);
            }
            SyncRequestPayload payload = new SyncRequestPayload(topic, chunkIndex);
            ClusterMessage request = ClusterMessage.request(MessageType.SYNC_REQUEST,
                    "sync",
                    transport.local().nodeId(),
                    leader.nodeId(),
                    payload);
            transport.send(request);
        });
    }

    // ── Relay-stream ingestion (RELAY_STREAM) ────────────────────────────────────

    /**
     * True when replication is configured (RELAY_STREAM, the only ingestion mode since 5.0.0). Acts
     * as the "replication configured" guard — the test {@link #ReplicationManager()} constructor
     * leaves {@code config} null.
     */
    private boolean isStreamMode() {
        return config != null && config.followerIngestMode() == FollowerIngestMode.RELAY_STREAM;
    }

    private NQueue<byte[]> relayFor(String topic) {
        RelayStore store = relayStore;
        if (store == null) {
            throw new IllegalStateException("Relay store not initialized");
        }
        return store.relayFor(topic);
    }

    private void signalRelay(String topic) {
        Object sig = relaySignals.get(topic);
        if (sig != null) {
            synchronized (sig) {
                sig.notifyAll();
            }
        }
    }

    /** Starts the per-topic apply consumer once (idempotent). */
    private synchronized void ensureRelayApplyLoop(String topic) {
        if (!running || relayStore == null) {
            return;
        }
        relaySignals.computeIfAbsent(topic, k -> new Object());
        relayApplyThreads.computeIfAbsent(topic, t -> {
            Thread thread = new Thread(() -> relayApplyLoop(topic), "ngrid-relay-apply-" + topic);
            thread.setDaemon(true);
            thread.start();
            return thread;
        });
    }

    /** Starts the per-topic RELAY_STREAM fetch loop once (idempotent). */
    private synchronized void ensureRelayFetchLoop(String topic) {
        if (!running || relayStore == null || !isStreamMode()) {
            return;
        }
        relayFetchSignals.computeIfAbsent(topic, k -> new Object());
        relayFetchThreads.computeIfAbsent(topic, t -> {
            Thread thread = new Thread(() -> relayFetchLoop(topic), "ngrid-relay-fetch-" + topic);
            thread.setDaemon(true);
            thread.start();
            return thread;
        });
    }

    /**
     * Per-topic IO loop (RELAY_STREAM): drives the pull of the leader op-log from this follower's
     * durable cursor. It issues a fetch when eligible, then waits — woken early when a batch arrives
     * (to pull the next run back-to-back) or on the poll interval (caught-up long-poll). Persisting
     * and ordering happen in {@link #handleRelayStreamBatch}; applying happens in the apply loop.
     */
    private void relayFetchLoop(String topic) {
        Object signal = relayFetchSignals.computeIfAbsent(topic, k -> new Object());
        long pollMs = Math.max(1L, config.relayStreamPollInterval().toMillis());
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                maybeSendFetch(topic);
                synchronized (signal) {
                    signal.wait(pollMs);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                LOGGER.log(Level.WARNING, "Relay fetch loop error for topic " + topic + "; retrying", t);
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /** Issues one RELAY_STREAM_FETCH when eligible (follower, leader known, not syncing, backlog ok). */
    private void maybeSendFetch(String topic) {
        if (coordinator.isLeader()) {
            return; // the leader serves the stream, it does not pull
        }
        if (syncingTopics.contains(topic) || relayPendingBootstrap.contains(topic)) {
            return; // a snapshot/bootstrap is installing; do not stream over it
        }
        NodeId leaderId = coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null);
        if (leaderId == null) {
            return; // no leader known yet
        }
        if (getRelaySize(topic) >= config.relayMaxBacklog()) {
            return; // flow control: let the apply loop drain before pulling more
        }
        long now = System.currentTimeMillis();
        Long pendingUntil = relayFetchPendingUntilByTopic.get(topic);
        if (pendingUntil != null && now < pendingUntil) {
            return; // a fetch is in flight, or we are in the caught-up poll gap
        }
        long from = nextFetchSequence(topic);
        transport.send(ClusterMessage.request(MessageType.RELAY_STREAM_FETCH, "stream",
                transport.local().nodeId(), leaderId,
                new RelayStreamFetchPayload(topic, from, config.relayStreamFetchBatch())));
        relayFetchPendingUntilByTopic.put(topic, now + config.relayStreamFetchTimeout().toMillis());
    }

    /** The next sequence to pull = one past the highest sequence already persisted to the relay. */
    private long nextFetchSequence(String topic) {
        return relayStreamCursor(topic).get() + 1L;
    }

    private java.util.concurrent.atomic.AtomicLong relayStreamCursor(String topic) {
        return relayStreamCursorByTopic.computeIfAbsent(topic,
                k -> new java.util.concurrent.atomic.AtomicLong(
                        Math.max(currentNextExpected(topic) - 1L, relayTailSequence(topic))));
    }

    /**
     * Highest sequence currently persisted in the topic relay (0 if empty). On a clean restart the
     * relay durably holds the not-yet-applied tail; anchoring the cursor there resumes the stream from
     * tail+1 instead of re-pulling (and re-storing) entries already on disk.
     */
    private long relayTailSequence(String topic) {
        try {
            long size = getRelaySize(topic);
            if (size <= 0) {
                return 0L;
            }
            int last = (int) Math.min(Integer.MAX_VALUE - 1L, size - 1L);
            List<byte[]> tail = relayFor(topic).readRange(last, 1).items();
            if (tail.isEmpty()) {
                return 0L;
            }
            return RelayEntryCodec.decode(tail.get(0)).sequence();
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Could not read relay tail for topic " + topic, e);
            return 0L;
        }
    }

    private void signalFetch(String topic) {
        Object sig = relayFetchSignals.get(topic);
        if (sig != null) {
            synchronized (sig) {
                sig.notifyAll();
            }
        }
    }

    /** Stops all RELAY_STREAM fetch loops (interrupt + wake + join), before the apply loops quiesce. */
    private void stopRelayFetchLoops() {
        List<Thread> threads = new ArrayList<>(relayFetchThreads.values());
        for (Map.Entry<String, Thread> e : relayFetchThreads.entrySet()) {
            e.getValue().interrupt();
            signalFetch(e.getKey());
        }
        for (Thread t : threads) {
            try {
                t.join(2000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
        relayFetchThreads.clear();
    }

    /**
     * Ingests a RELAY_STREAM_BATCH on the follower: persists the contiguous run to the relay IN ORDER
     * (advancing the durable cursor), wakes the apply loop, and pulls the next run. A need-snapshot
     * signal triggers a bootstrap. Because the leader only sends contiguous runs and the cursor only
     * advances on a durable append, re-fetching the same range is idempotent (duplicates are skipped).
     */
    private void handleRelayStreamBatch(ClusterMessage message) {
        RelayStreamBatchPayload batch = message.payload(RelayStreamBatchPayload.class);
        String topic = batch.topic();
        // Fencing by leader IDENTITY (Fase 0.2): accept the stream only from the currently agreed
        // leader. An in-flight fetch can straddle a leadership change, and a delayed batch from a
        // superseded leader must NOT advance the cursor, update watermarks or apply stale ops. This
        // source check is the SOLE and authoritative fence — frames are NOT re-fenced by epoch
        // magnitude (which would wrongly drop the same leader's earlier-epoch ops after a convergence).
        NodeId agreedLeader = coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null);
        if (agreedLeader == null || !agreedLeader.equals(message.source())) {
            LOGGER.fine(() -> "Ignoring RELAY_STREAM_BATCH from non-leader " + message.source()
                    + " (agreed leader: " + agreedLeader + ")");
            return;
        }
        leaderHwmByTopic.put(topic, batch.leaderHighWatermark());
        leaderOldestByTopic.put(topic, batch.oldestSequence());
        if (!batch.frames().isEmpty()) {
            long bytes = 0L;
            for (byte[] f : batch.frames()) {
                bytes += f.length;
            }
            streamBytesInByTopic.computeIfAbsent(topic, k -> new java.util.concurrent.atomic.AtomicLong())
                    .addAndGet(bytes);
        }

        if (batch.needSnapshot()) {
            // Below the leader's retained window: bootstrap, then resume streaming from the watermark.
            relayFetchPendingUntilByTopic.put(topic,
                    System.currentTimeMillis() + config.relayStreamFetchTimeout().toMillis());
            if (syncingTopics.add(topic)) {
                requestSync(topic);
            }
            return;
        }
        ReplicationHandler handler = handlers.get(topic);
        if (handler == null) {
            relayFetchPendingUntilByTopic.remove(topic); // allow re-fetch once the handler registers
            return;
        }
        java.util.concurrent.atomic.AtomicLong cursor = relayStreamCursor(topic);
        int persisted = 0;
        for (byte[] frame : batch.frames()) {
            RelayEntry entry;
            try {
                entry = RelayEntryCodec.decode(frame);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to decode relay-stream frame for topic " + topic, e);
                break;
            }
            // No epoch-magnitude fence here: the batch is already fenced by leader IDENTITY above
            // (Fase 0.2), so every frame is from the agreed leader. An earlier-epoch frame (the same
            // leader, before a convergence re-stamp) is legitimate and contiguous — fencing it by
            // magnitude would skip it without advancing the cursor and wedge the stream.
            long expected = cursor.get() + 1L;
            if (entry.sequence() < expected) {
                continue; // duplicate / already persisted
            }
            if (entry.sequence() != expected) {
                // A hole must not occur in stream mode (the leader sends contiguous runs). Stop and
                // let the next fetch re-pull from the cursor.
                long got = entry.sequence();
                LOGGER.warning(() -> "Non-contiguous relay-stream frame topic=" + topic + " seq=" + got
                        + " expected=" + expected);
                break;
            }
            try {
                relayFor(topic).offer(frame);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Relay offer failed topic=" + topic + " seq=" + entry.sequence()
                        + "; will re-fetch", e);
                break; // cursor not advanced → the next fetch retries this sequence
            }
            cursor.incrementAndGet();
            persisted++;
        }
        if (persisted > 0) {
            // Got data: allow the next fetch immediately and wake both loops.
            relayFetchPendingUntilByTopic.put(topic, 0L);
            signalRelay(topic);
            signalFetch(topic);
        } else {
            // Caught up: wait the poll interval before the next fetch to avoid busy-polling.
            relayFetchPendingUntilByTopic.put(topic,
                    System.currentTimeMillis() + Math.max(1L, config.relayStreamPollInterval().toMillis()));
        }
    }

    private void relayApplyLoop(String topic) {
        NQueue<byte[]> relay = relayFor(topic);
        Object signal = relaySignals.computeIfAbsent(topic, k -> new Object());
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                boolean progressed = drainRelayOnce(topic, relay);
                if (!progressed) {
                    synchronized (signal) {
                        signal.wait(100);
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                LOGGER.log(Level.WARNING, "Relay apply loop error for topic " + topic + "; retrying head", t);
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    /**
     * One drain step: discard any stale/duplicate prefix, then apply the contiguous run at the relay
     * head and advance the durable apply frontier. The RELAY_STREAM relay is contiguous by
     * construction (the fetch loop persists only contiguous runs and advances the cursor on durable
     * append), so the head is never a forward gap in steady state. Returns true when it made progress
     * (so the loop keeps going without waiting).
     */
    private boolean drainRelayOnce(String topic, NQueue<byte[]> relay) throws Exception {
        if (relayPendingBootstrap.contains(topic)) {
            // Unclean restart: do not apply this topic's relay until it is bootstrapped, or the decision
            // is made that no bootstrap is needed.
            if (coordinator.isLeader()) {
                // Promoted/leader: there is no peer to bootstrap from; the failover drain-gate + sequence
                // fencing recover the backlog. (A lost frontier on a promoted-with-backlog node is the
                // residual edge case that needs crash-safe frontier co-location — out of scope here.)
                relayPendingBootstrap.remove(topic);
            } else if (coordinator.leaderInfo().isPresent()) {
                // Follower with a known leader: pull a fresh snapshot first (replaces local state, so a
                // stale/lost frontier cannot duplicate the non-idempotent queue OFFER).
                relayPendingBootstrap.remove(topic);
                if (syncingTopics.add(topic)) {
                    requestSync(topic);
                }
                return false;
            } else {
                return false; // no leader known yet — wait before replaying anything
            }
        }
        if (syncingTopics.contains(topic)) {
            return false; // a sync/bootstrap is installing a snapshot for this topic; pause apply
        }

        ReplicationHandler handler = handlers.get(topic);
        if (handler == null) {
            return false; // handler not ready (shutdown/registration race): retry later
        }

        if (discardStaleRelayPrefix(topic, relay)) {
            return true;
        }

        // Batch-peek the relay head WITHOUT consuming (#128). The consumer stays single-threaded and
        // applies in strict sequence order; batching only amortizes the per-op lock/flush/peek-poll
        // overhead. readRange reads from the consumer offset (index 0 = oldest unapplied).
        List<byte[]> frames = relay.readRange(0, Math.max(1, config.relayApplyBatchSize())).items();
        if (frames.isEmpty()) {
            // Relay fully drained for this topic — release the failover drain-gate if one is held
            // (no-op otherwise).
            maybeReleaseRelayDrainGate(topic);
            return false;
        }

        long nextExpected = nextExpectedSequenceByTopic.getOrDefault(topic, 1L);
        List<RelayEntry> appliedBatch = new ArrayList<>();
        int consumed = 0;            // head frames to poll (stale-discarded + successfully applied)
        Exception applyError = null;
        for (byte[] f : frames) {
            RelayEntry entry = RelayEntryCodec.decode(f);
            if (entry.sequence() < nextExpected) {
                // Duplicate/already-applied sequence (re-peek after crash, or a resend copy): discard.
                // Sequence-only dedup is what makes the non-idempotent queue OFFER effectively-once.
                // NOTE: do NOT fence by epoch magnitude here. The relay only holds frames from the
                // agreed leader (identity-fenced at persist in handleRelayStreamBatch, Fase 0.2). After
                // an epoch convergence re-stamp the SAME leader's earlier-epoch ops carry a lower epoch
                // but are legitimate and contiguous — fencing them by magnitude would drop a real op and
                // wedge the apply frontier (a lower-epoch entry consumed without advancing nextExpected).
                consumed++;
                continue;
            }
            if (entry.sequence() == nextExpected) {
                try {
                    handler.apply(entry.operationId(), handler.decodePayload(entry.payloadBytes()));
                } catch (Exception e) {
                    // Commit the prefix applied so far, poll it, then rethrow so the apply loop backs
                    // off and retries this failing entry (it stays at the relay head).
                    applyError = e;
                    break;
                }
                appliedBatch.add(entry);
                nextExpected++;
                consumed++;
                continue;
            }
            // head.sequence() > nextExpected: a forward gap. This cannot happen in RELAY_STREAM (the
            // fetch loop only persists contiguous runs), so it would indicate corruption. Stop the
            // batch at the prefix applied so far; the cursor stays put and the fetch loop re-pulls.
            long got = entry.sequence();
            long want = nextExpected;
            LOGGER.warning(() -> "Non-contiguous relay head for topic " + topic + " seq=" + got
                    + " expected=" + want + "; stopping batch (fetch loop will re-pull from cursor)");
            break;
        }

        // One locked frontier commit for the whole applied prefix, THEN poll the consumed head frames.
        // Never poll an entry before its apply() returned AND its frontier advance is committed.
        if (!appliedBatch.isEmpty()) {
            commitRelayBatch(topic, appliedBatch);
        }
        for (int i = 0; i < consumed; i++) {
            relay.poll();
        }
        if (applyError != null) {
            throw applyError;
        }
        return consumed > 0;
    }

    private boolean discardStaleRelayPrefix(String topic, NQueue<byte[]> relay) throws Exception {
        long nextExpected = nextExpectedSequenceByTopic.getOrDefault(topic, 1L);
        List<byte[]> frames = relay.readRange(0, RELAY_STALE_DISCARD_BATCH).items();
        if (frames.isEmpty()) {
            return false;
        }
        int stale = 0;
        for (byte[] frame : frames) {
            RelayEntry entry = RelayEntryCodec.decode(frame);
            // Sequence-only dedup (no epoch-magnitude fence): see the note in drainRelayOnce. An
            // earlier-epoch entry from the agreed leader (after a convergence re-stamp) is legitimate.
            if (entry.sequence() >= nextExpected) {
                break;
            }
            stale++;
        }
        for (int i = 0; i < stale; i++) {
            relay.poll();
        }
        if (stale > 0) {
            int discarded = stale;
            LOGGER.fine(() -> "Discarded " + discarded + " stale relay entries for topic " + topic
                    + " below nextExpected=" + nextExpected);
        }
        return stale > 0;
    }

    /**
     * Advances the durable apply frontier for a batch of already-applied, strictly contiguous entries
     * (#128) under ONE lock acquisition — the throughput lever over the per-operation commit. The
     * entries must be in ascending, gap-free sequence order ending at the batch's last sequence.
     */
    private void commitRelayBatch(String topic, List<RelayEntry> entries) {
        RelayEntry last = entries.get(entries.size() - 1);
        acquireSequenceLock();
        try {
            nextExpectedSequenceByTopic.merge(topic, last.sequence() + 1,
                    (cur, candidate) -> Math.max(cur, candidate));
            for (RelayEntry e : entries) {
                applied.add(e.operationId());
            }
            trimApplied();
            recordApplied(entries.size()); // global applied-op counter (drives the lag metric)
            sequenceStateDirty = true;
        } finally {
            sequenceBufferLock.unlock();
        }
        mirrorAppliedToOpLog(topic, entries);
    }

    /**
     * RELAY_STREAM: mirror an applied run into this node's own op-log (binlog) so that, if it is later
     * promoted to leader, it can serve the full history as a sequential stream instead of forcing
     * followers into a snapshot. Append-only and failure-tolerant — a miss only degrades a future
     * fetch into the existing snapshot path, never the commit.
     */
    private void mirrorAppliedToOpLog(String topic, List<RelayEntry> entries) {
        if (!isStreamMode()) {
            return;
        }
        ResendLogStore store = resendLogStore;
        if (store == null) {
            return;
        }
        long now = System.currentTimeMillis();
        for (RelayEntry e : entries) {
            try {
                store.append(topic, e.sequence(), now, RelayEntryCodec.encode(e));
            } catch (Exception ex) {
                LOGGER.log(Level.WARNING, "Op-log mirror append failed topic=" + topic
                        + " seq=" + e.sequence(), ex);
            }
        }
    }

    // ──────────────────────────────────────────────────────────
    // Leader op-log indexing (binlog) + resend
    // ──────────────────────────────────────────────────────────

    /**
     * Indexes a replication payload in the sequence-based log (leader-side).
     * Enforces retention by evicting oldest entries beyond the configured limit.
     */
    private void indexReplicationPayload(String topic, long sequence, ReplicationPayload payload) {
        long now = System.currentTimeMillis();
        java.util.NavigableMap<Long, TimedPayload> topicLog = replicationLogBySequence
                .computeIfAbsent(topic, k -> java.util.Collections.synchronizedNavigableMap(new java.util.TreeMap<>()));
        topicLog.put(sequence, new TimedPayload(payload, now));

        // Count-based retention (memory cap): evict oldest beyond the configured limit. In hybrid mode
        // (#127) this cap governs ONLY the heap hot cache — the deep window lives on disk below.
        int retention = config.replicationLogRetention();
        while (topicLog.size() > retention) {
            topicLog.pollFirstEntry();
        }
        // Time-based retention (backlog window): opportunistic head eviction on each commit.
        // Complementary to the count cap — whichever limit is reached first evicts.
        evictExpiredFromTopicLog(topicLog);
        // The durable op-log (binlog) disk tier is appended synchronously on the emission path
        // (appendStreamOpLog), so there is nothing to mirror here.
    }

    /**
     * Evicts the contiguous prefix of resend-log entries older than the configured temporal
     * retention window. Entries are inserted in (sequence, time)-monotonic order, so the head
     * (lowest sequence) is always the oldest — mirrors {@code NQueue.skipExpiredRecordsLocked}.
     * No-op when temporal retention is disabled ({@link Duration#ZERO}).
     *
     * @param topicLog the per-topic synchronized resend log
     * @return the number of entries evicted
     */
    private int evictExpiredFromTopicLog(java.util.NavigableMap<Long, TimedPayload> topicLog) {
        long retentionMillis = config.replicationLogRetentionTime().toMillis();
        if (retentionMillis <= 0) {
            return 0; // temporal eviction disabled
        }
        long now = System.currentTimeMillis();
        int evicted = 0;
        synchronized (topicLog) {
            java.util.Map.Entry<Long, TimedPayload> head;
            while ((head = topicLog.firstEntry()) != null
                    && now - head.getValue().indexedAtMillis() > retentionMillis) {
                topicLog.pollFirstEntry();
                evicted++;
            }
        }
        if (evicted > 0) {
            replicationLogTimeEvictedCount.addAndGet(evicted);
        }
        return evicted;
    }

    /**
     * Serves a follower's RELAY_STREAM_FETCH from the durable op-log (the leader's binlog): a
     * strictly contiguous run starting at the requested sequence, or a need-snapshot signal when the
     * follower is below the retained window. Only the leader answers; the op-log is the single source
     * of truth, so the response is driven by what is actually persisted (no separate high-watermark to
     * race the commit/append).
     */
    private void handleRelayStreamFetch(ClusterMessage message) {
        if (!coordinator.isLeader()) {
            LOGGER.fine(() -> "Ignoring RELAY_STREAM_FETCH: not the leader.");
            return;
        }
        ResendLogStore store = resendLogStore;
        if (store == null) {
            LOGGER.warning("RELAY_STREAM_FETCH received but the op-log is not initialized");
            return;
        }
        RelayStreamFetchPayload request = message.payload(RelayStreamFetchPayload.class);
        String topic = request.topic();
        long from = Math.max(1L, request.fromSequence());
        int maxBatch = Math.min(Math.max(1, request.maxBatch()), config.resendLogReadBatchMax());

        ResendLog log = store.logFor(topic);
        long oldest = log.oldestSequence();
        long to = from + (long) maxBatch - 1L;
        List<RelayEntry> entries = log.read(from, to);
        List<byte[]> frames = takeContiguousFrames(entries, from);

        // Below the retained window with nothing contiguous to send → the follower must bootstrap.
        boolean needSnapshot = frames.isEmpty() && oldest > 0 && from < oldest;
        long hwm = currentLeaderTopicSequence(topic);

        RelayStreamBatchPayload batch = new RelayStreamBatchPayload(topic, from, frames, hwm, oldest, needSnapshot);
        transport.send(ClusterMessage.request(MessageType.RELAY_STREAM_BATCH, "stream",
                transport.local().nodeId(), message.source(), batch));
        if (needSnapshot || !frames.isEmpty()) {
            int sent = frames.size();
            LOGGER.fine(() -> String.format("Served RELAY_STREAM_FETCH from %s topic=%s from=%d: %d frames%s",
                    message.source(), topic, from, sent, needSnapshot ? " (needSnapshot)" : ""));
        }
    }

    /** Takes the leading contiguous run of frames starting at {@code from} (stops at the first hole). */
    private List<byte[]> takeContiguousFrames(List<RelayEntry> entries, long from) {
        List<byte[]> out = new ArrayList<>(entries.size());
        long expected = from;
        for (RelayEntry entry : entries) {
            if (entry.sequence() < expected) {
                continue; // duplicate / below the cursor
            }
            if (entry.sequence() != expected) {
                break; // hole — a stream run must be contiguous
            }
            out.add(RelayEntryCodec.encode(entry));
            expected++;
        }
        return out;
    }

    /** Best-effort highest assigned sequence for a topic (advisory lag metric carried to the follower). */
    private long currentLeaderTopicSequence(String topic) {
        java.util.concurrent.atomic.AtomicLong counter = sequenceByTopic.get(topic);
        return counter == null ? 0L : counter.get();
    }

    private long currentNextExpected(String topic) {
        acquireSequenceLock();
        try {
            return nextExpectedSequenceByTopic.getOrDefault(topic, 1L);
        } finally {
            sequenceBufferLock.unlock();
        }
    }

    /**
     * Janitor that releases a sync guard ({@link #syncingTopics}) that has been held without any
     * progress (no advance in {@code nextExpected}) for {@link #SYNC_STUCK_TIMEOUT_MS}. Without this,
     * a sync that never completes — a lost {@code SYNC_RESPONSE}, a silently dropped chunk, or a
     * stale-sync that keeps being ignored — would keep {@code syncingTopics.add(topic)} returning
     * {@code false} forever, so gap detection could never request a fresh sync and the follower
     * would loop logging "Large gap" without ever recovering.
     */
    private void checkStuckSyncs() {
        if (!running || coordinator.isLeader()) {
            return;
        }
        long now = System.currentTimeMillis();
        for (String topic : syncingTopics) {
            // A sync stays "alive" as long as chunks keep arriving. Seed the stamp the first time a
            // guard is observed so a sync that never receives a single chunk (lost SYNC_REQUEST) is
            // still eventually released; healthy multi-chunk transfers refresh it on every chunk.
            long lastActivity = lastSyncActivityByTopic.computeIfAbsent(topic, k -> now);
            if (now - lastActivity > SYNC_STUCK_TIMEOUT_MS) {
                long stuckMs = now - lastActivity;
                LOGGER.warning(() -> String.format(
                        "Sync for topic=%s stuck without a chunk for %dms; releasing sync guard to allow a fresh sync.",
                        topic, stuckMs));
                syncingTopics.remove(topic);
                lastSyncActivityByTopic.remove(topic);
            }
        }
        // Drop activity entries for topics that are no longer syncing.
        lastSyncActivityByTopic.keySet().removeIf(t -> !syncingTopics.contains(t));
    }

    // ──────────────────────────────────────────────────────────
    // Metrics API
    // ──────────────────────────────────────────────────────────

    /**
     * Number of out-of-order gaps the follower detected. RELAY_STREAM pulls the op-log contiguously by
     * construction, so this is always {@code 0} — retained for the stable observability API.
     *
     * @return always {@code 0} since 5.0.0
     */
    public long getGapsDetected() {
        return gapsDetected.get();
    }

    /**
     * Total entries held in the legacy in-memory sequence buffer (the removed INLINE ingestion path).
     * RELAY_STREAM has no in-memory reorder/sequence buffer, so this is always {@code 0} — retained for
     * the stable observability API.
     *
     * @return always {@code 0} since 5.0.0
     */
    public long getInlineSequenceBufferSize() {
        return 0L;
    }

    public long getResendSuccessCount() {
        return resendSuccessCount.get();
    }

    public long getSnapshotFallbackCount() {
        return snapshotFallbackCount.get();
    }

    /**
     * Number of snapshot/sync requests this node has initiated. In RELAY_STREAM this stays flat in
     * regime under lag (the stream/relay absorbs it); a snapshot is only the cold-bootstrap path.
     *
     * @return the count of initiated sync requests
     */
    public long getSyncRequestCount() {
        return syncRequestCount.get();
    }

    /**
     * Number of operations the follower has skipped past because they were evicted from the leader's
     * resend log (unfillable head-of-line gaps). A non-zero, growing value signals the follower fell
     * far enough behind to lose strong ordering for those ops (recovered to eventual LWW).
     *
     * @return total evicted sequences skipped
     */
    public long getEvictedSkipCount() {
        return evictedSkipCount.get();
    }

    /**
     * Number of resend-log entries evicted by the temporal retention window
     * ({@link ReplicationConfig#replicationLogRetentionTime()}). Complements the count-based
     * retention; zero when temporal retention is disabled.
     *
     * @return total entries evicted by time across all topics
     */
    public long getReplicationLogTimeEvictedCount() {
        return replicationLogTimeEvictedCount.get();
    }

    /**
     * Current number of entries in the leader-side in-heap replication-log index for the given topic
     * — the count-/time-governed window sized by {@link ReplicationConfig#replicationLogRetention()}
     * and {@link ReplicationConfig#replicationLogRetentionTime()}. The durable streamable op-log (the
     * binlog) is a separate, segment-governed store. Intended for testing and observability.
     *
     * @param topic the topic
     * @return the replication-log index size for the topic (0 if none)
     */
    public int getReplicationLogSize(String topic) {
        java.util.NavigableMap<Long, TimedPayload> topicLog = replicationLogBySequence.get(topic);
        return topicLog == null ? 0 : topicLog.size();
    }

    /**
     * Number of entries currently buffered in the follower relay-log for {@code topic} (#128). This is
     * the durable apply backlog: it grows when the leader produces faster than the follower applies and
     * shrinks as the apply consumer drains. {@code 0} when replication is not configured. Intended for
     * lag observability and alarming (the cardinal could only infer lag from sequence deltas before).
     *
     * @param topic the replication topic
     * @return the relay backlog size for the topic
     */
    public long getRelaySize(String topic) {
        RelayStore store = relayStore;
        if (store == null) {
            return 0L;
        }
        try {
            return store.relayFor(topic).size(true);
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Relay size read failed for topic " + topic, e);
            return 0L;
        }
    }

    /** RELAY_STREAM: highest sequence this follower has persisted to the topic relay (the pull cursor). */
    public long getRelayStreamCursor(String topic) {
        java.util.concurrent.atomic.AtomicLong cursor = relayStreamCursorByTopic.get(topic);
        return cursor == null ? 0L : cursor.get();
    }

    /**
     * Leader high-watermark for a topic: the leader's own highest assigned sequence, or the value a
     * follower learned from its last RELAY_STREAM_BATCH ({@code 0} if unknown yet).
     */
    public long getLeaderHighWatermark(String topic) {
        if (coordinator.isLeader()) {
            return currentLeaderTopicSequence(topic);
        }
        return leaderHwmByTopic.getOrDefault(topic, 0L);
    }

    /** Leader's retained-window floor for a topic (oldest sequence still streamable); {@code -1} if unknown. */
    public long getLeaderOldestSequence(String topic) {
        if (coordinator.isLeader()) {
            ResendLogStore store = resendLogStore;
            return store == null ? -1L : store.logFor(topic).oldestSequence();
        }
        return leaderOldestByTopic.getOrDefault(topic, -1L);
    }

    /** Replication lag for a topic on a follower: leader high-watermark minus the applied frontier. */
    public long getReplicationLag(String topic) {
        long hwm = getLeaderHighWatermark(topic);
        if (hwm <= 0L) {
            return 0L;
        }
        return Math.max(0L, hwm - (currentNextExpected(topic) - 1L));
    }

    /** Cumulative stream bytes pulled for a topic (RELAY_STREAM throughput). */
    public long getStreamBytesIn(String topic) {
        java.util.concurrent.atomic.AtomicLong bytes = streamBytesInByTopic.get(topic);
        return bytes == null ? 0L : bytes.get();
    }

    /** True when this node is actively streaming the topic from the leader (RELAY_STREAM follower). */
    public boolean isStreaming(String topic) {
        return isStreamMode() && !coordinator.isLeader()
                && relayFetchThreads.containsKey(topic) && !syncingTopics.contains(topic);
    }

    /**
     * Age, in milliseconds, of the oldest unapplied relay entry for {@code topic} (#128) — i.e. how
     * long the follower's apply frontier has lagged the relay head. {@code 0} when the relay is empty
     * or replication is not configured. A growing head age signals the apply consumer is falling behind.
     *
     * @param topic the replication topic
     * @return the relay head age in milliseconds
     */
    public long getRelayHeadAgeMillis(String topic) {
        long headTimestamp = relayHeadTimestamp(topic);
        return headTimestamp == Long.MAX_VALUE ? 0L : Math.max(0L, System.currentTimeMillis() - headTimestamp);
    }

    /**
     * Relay backlog size per registered topic (#128). Snapshot map for dashboards; empty when
     * replication is not configured.
     *
     * @return a map of topic to relay backlog size
     */
    public Map<String, Long> getRelaySizes() {
        Map<String, Long> sizes = new java.util.HashMap<>();
        if (relayStore == null) {
            return sizes;
        }
        for (String topic : handlers.keySet()) {
            sizes.put(topic, getRelaySize(topic));
        }
        return sizes;
    }

    public Map<String, TopicReplicationStatus> getTopicReplicationStatuses() {
        Map<String, TopicReplicationStatus> statuses = new java.util.HashMap<>();
        for (String topic : handlers.keySet()) {
            statuses.put(topic, new TopicReplicationStatus(
                    topic,
                    currentNextExpected(topic),
                    relayHeadSequence(topic),
                    getRelaySize(topic),
                    syncingTopics.contains(topic),
                    relayPendingBootstrap.contains(topic),
                    false, // resendPending: RELAY_STREAM has no NAK/resend protocol (always false)
                    getRelayStreamCursor(topic),
                    getLeaderHighWatermark(topic),
                    getLeaderOldestSequence(topic),
                    getReplicationLag(topic),
                    getStreamBytesIn(topic),
                    isStreaming(topic)));
        }
        return statuses;
    }

    private long relayHeadSequence(String topic) {
        RelayStore store = relayStore;
        if (store == null) {
            return 0L;
        }
        try {
            return store.relayFor(topic).peekRecord()
                    .map(record -> RelayEntryCodec.decode(record.payload()).sequence())
                    .orElse(0L);
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Relay head sequence read failed for topic " + topic, e);
            return 0L;
        }
    }

    public double getAverageConvergenceTimeMs() {
        long count = convergenceCount.get();
        return count > 0 ? (double) totalConvergenceTimeMs.get() / count : 0.0;
    }

    /**
     * Returns the number of replication operations currently awaiting quorum
     * acknowledgement.
     *
     * @return pending operations count
     */
    public int getPendingOperationsCount() {
        return pending.size();
    }

    @Override
    public void close() throws IOException {
        stop();
        executor.shutdownNow();
        timeoutScheduler.shutdownNow();
        try {
            executor.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            timeoutScheduler.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        RelayStore store = relayStore;
        if (store != null) {
            store.close();
            relayStore = null;
        }
        ResendLogStore resendStore = resendLogStore;
        if (resendStore != null) {
            resendStore.close();
            resendLogStore = null;
        }
    }

    private int reachableMembersCount() {
        int reachable = 1; // local node
        for (NodeInfo member : coordinator.activeMembers()) {
            NodeId id = member.nodeId();
            if (id.equals(transport.local().nodeId())) {
                continue;
            }
            if (transport.isReachable(id)) {
                reachable++;
            }
        }
        return reachable;
    }

    /**
     * Computes the effective quorum based on currently reachable members.
     * In non-strict consistency mode, this allows operations to complete
     * with a reduced quorum when peers disconnect (e.g., during failover).
     *
     * @param originalQuorum the originally requested quorum
     * @return the adjusted quorum, at least 1 and at most originalQuorum
     */
    private int computeQuorumForReachable(int originalQuorum) {
        if (config.strictConsistency()) {
            return originalQuorum;
        }
        int reachable = reachableMembersCount();
        return Math.max(1, Math.min(originalQuorum, reachable));
    }

    private void checkTimeouts() {
        try {
            if (!running) {
                return;
            }
            Duration timeout = config.operationTimeout();
            if (timeout.isZero() || timeout.isNegative()) {
                return;
            }
            Instant now = Instant.now();
            List<Failure> toFail = new ArrayList<>();
            for (PendingOperation operation : pending.values()) {
                if (operation.isDone()) {
                    continue;
                }
                if (operation.isExpired(now, timeout)) {
                    TimeoutException ex = new TimeoutException(String.format(
                            "Replication operation timed out operationId=%s timeout=%s",
                            operation.operationId,
                            timeout));
                    toFail.add(new Failure(operation, ex));
                }
            }
            for (Failure failure : toFail) {
                failOperation(failure.operation(), failure.error());
            }
        } catch (Throwable t) {
            LOGGER.log(Level.SEVERE, "Unexpected error in checkTimeouts loop", t);
        }
    }

    private void failOperation(PendingOperation operation, Throwable error) {
        if (!pending.remove(operation.operationId, operation)) {
            return;
        }
        operation.fail(error);
        log.computeIfPresent(operation.operationId, (id, record) -> {
            record.status(OperationStatus.REJECTED);
            return record;
        });
    }

    @Override
    public void onLeaderChanged(NodeId newLeader) {
        NodeId localId = transport.local().nodeId();
        NodeId previousLeader = lastLeader.getAndSet(newLeader);
        if (!localId.equals(newLeader)) {
            if (previousLeader != null && previousLeader.equals(localId)) {
                leaderSyncing.set(false);
                leaderSyncTopics.clear();
                clearJoinQuiesce();
            }
            failAllPending("Lost leadership to " + newLeader);
            return;
        }
        leaderSyncTopics.clear();
        leaderSyncTopics.addAll(handlers.keySet());
        leaderSyncing.set(!leaderSyncTopics.isEmpty());
        // Notify handlers of leadership promotion so they can clean up stale data
        // (e.g., truncate queues with persisted data from a previous epoch).
        for (ReplicationHandler handler : handlers.values()) {
            try {
                handler.onBecameLeader();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Handler onBecameLeader failed", e);
            }
        }
        // RELAY_STREAM failover drain-gate (generalizes the #123 sync-before-lead): the promoted node
        // holds writes (leaderSyncing) until its relay backlog is fully applied. The per-topic apply
        // consumers drain and release the gate (maybeReleaseRelayDrainGate) — release depends on
        // "relay drained", not on a peer snapshot, so a node promoted without a reachable peer (and
        // with a full relay) stays gated until it drains rather than leading with a stale state. The
        // promoted node serves the stream from its own op-log; it never pulls a snapshot to lead.
    }

    /**
     * Called by a topic's apply consumer when its relay has fully drained. While a failover drain-gate
     * is held (leaderSyncing), this releases the topic; once all gated topics have drained, the write
     * gate opens and the promoted node may lead.
     */
    private void maybeReleaseRelayDrainGate(String topic) {
        if (!leaderSyncing.get()) {
            return;
        }
        if (isLoneBootLeadership()) {
            // Boot self-election guard (#129 3c): a node that self-elects alone in pair mode must not
            // advertise empty state as a ready, caught-up leadership by releasing the gate on an empty
            // relay. Hold the gate through the peer-discovery window; if a peer appears it will step this
            // node down to follower (clearing the gate) and the proactive cold-join sync (3a) converges
            // it — otherwise the window lapses and the gate releases below on the next drained tick.
            return;
        }
        if (leaderSyncTopics.remove(topic) && leaderSyncTopics.isEmpty()) {
            leaderSyncing.set(false);
            LOGGER.info(() -> "Relay drained on promotion; releasing write gate for leadership.");
        }
    }

    /**
     * True while a fresh node that self-elected alone is still within the peer-discovery window
     * (#129 3c) — used to defer the empty-relay drain-gate release so a transient boot self-election
     * does not mark empty state as synced.
     */
    private boolean isLoneBootLeadership() {
        long window = config.joinPeerDiscoveryWindow().toMillis();
        return isStreamMode() && window > 0
                && (System.currentTimeMillis() - startedAtMillis) < window
                && coordinator.getActiveMembersCount() <= 1;
    }

    // ── Join convergence (#129) ──────────────────────────────────────────────────

    /** True while the leader is pausing production for a joining, not-caught-up follower (#129 3b). */
    public boolean isJoinQuiescing() {
        return joinQuiescing.get();
    }

    @Override
    public void onMembershipChanged() {
        if (!running || !config.leaderPauseOnJoin()) {
            return;
        }
        Set<NodeId> current = new HashSet<>();
        for (NodeInfo member : coordinator.activeMembers()) {
            current.add(member.nodeId());
        }
        NodeId localId = transport.local().nodeId();
        if (coordinator.isLeader()) {
            long leaderSeq = globalSequence.get();
            long threshold = config.joinSyncLagThreshold();
            for (NodeId member : current) {
                if (member.equals(localId) || knownActiveMembers.contains(member)) {
                    continue; // only newly-joined members
                }
                long applied = followerAppliedByNode.getOrDefault(member, -1L);
                if (applied < 0 || applied < leaderSeq - threshold) {
                    // Behind (or unknown): pause production until it catches up / disconnects / times out.
                    quiescingFor.add(member);
                }
            }
            if (!quiescingFor.isEmpty() && joinQuiescing.compareAndSet(false, true)) {
                joinQuiesceStartedMs = System.currentTimeMillis();
                LOGGER.info(() -> "Leader pausing production for joining follower(s) " + quiescingFor);
            }
        }
        // Drop wait-set entries for members that left, then re-evaluate the gate.
        quiescingFor.retainAll(current);
        knownActiveMembers.clear();
        knownActiveMembers.addAll(current);
        maybeReleaseJoinQuiesce();
    }

    private void handleFollowerProgress(ClusterMessage message) {
        if (!coordinator.isLeader()) {
            return; // only the leader tracks follower progress
        }
        FollowerProgressPayload payload = message.payload(FollowerProgressPayload.class);
        NodeId source = message.source();
        followerAppliedByNode.put(source, payload.appliedSequence());
        if (quiescingFor.contains(source)) {
            long leaderSeq = globalSequence.get();
            if (payload.appliedSequence() >= leaderSeq - config.joinSyncLagThreshold()) {
                quiescingFor.remove(source);
                maybeReleaseJoinQuiesce();
            }
        }
    }

    /** Follower-side: periodically report apply progress so the leader can release its join-quiesce. */
    private void sendFollowerProgress() {
        if (!running || coordinator.isLeader()) {
            return;
        }
        coordinator.leaderInfo().ifPresent(leader -> {
            FollowerProgressPayload payload = new FollowerProgressPayload(lastAppliedSequence,
                    coordinator.getTrackedLeaderEpoch());
            transport.send(ClusterMessage.request(MessageType.FOLLOWER_PROGRESS, "follower-progress",
                    transport.local().nodeId(), leader.nodeId(), payload));
        });
    }

    /** Periodically bounds the join-quiesce: release on timeout (a joiner that died cannot freeze us). */
    private void checkJoinQuiesce() {
        if (!running || !joinQuiescing.get()) {
            return;
        }
        if (System.currentTimeMillis() - joinQuiesceStartedMs > config.joinQuiesceMaxDuration().toMillis()) {
            LOGGER.warning(() -> "Join-quiesce exceeded max duration; releasing write gate (joiner(s) "
                    + quiescingFor + " did not catch up in time)");
            clearJoinQuiesce();
            return;
        }
        // Drop joiners that are no longer active or have since caught up.
        long leaderSeq = globalSequence.get();
        long threshold = config.joinSyncLagThreshold();
        quiescingFor.removeIf(node -> followerAppliedByNode.getOrDefault(node, -1L) >= leaderSeq - threshold);
        maybeReleaseJoinQuiesce();
    }

    private void maybeReleaseJoinQuiesce() {
        if (joinQuiescing.get() && quiescingFor.isEmpty()) {
            joinQuiescing.set(false);
            LOGGER.info(() -> "Join-quiesce released; resuming production.");
        }
    }

    private void clearJoinQuiesce() {
        quiescingFor.clear();
        joinQuiescing.set(false);
    }

    // ── User-level broadcast messaging (broadcastMessage API) ─────────────────────

    /**
     * Broadcasts a small, best-effort message to every node in the cluster, including this one
     * (loopback). Listeners registered via {@link #addBroadcastListener(BroadcastListener)} receive
     * it with the producer's {@link NodeId}. Fire-and-forget: not ordered, not durable, not
     * guaranteed — for guaranteed/ordered delivery use a replicated queue.
     *
     * @param message the message body (must not be {@code null})
     */
    public void broadcastMessage(String message) {
        Objects.requireNonNull(message, "message");
        NodeId local = transport.local().nodeId();
        transport.broadcast(ClusterMessage.request(MessageType.USER_BROADCAST, "broadcast", local, null,
                new BroadcastMessagePayload(message)));
        // Loopback: deliver to local listeners too, on a worker thread to mirror the remote path and
        // avoid invoking a user listener inline on the caller's thread.
        try {
            executor.submit(() -> dispatchBroadcast(local, message));
        } catch (java.util.concurrent.RejectedExecutionException ignored) {
            // shutting down — the remote sends already went out; skip local loopback
        }
    }

    /** Registers a listener for user-level broadcast messages (idempotent). */
    public void addBroadcastListener(BroadcastListener listener) {
        broadcastListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /** Removes a previously registered broadcast listener. */
    public void removeBroadcastListener(BroadcastListener listener) {
        broadcastListeners.remove(listener);
    }

    private void handleBroadcast(ClusterMessage message) {
        BroadcastMessagePayload payload = message.payload(BroadcastMessagePayload.class);
        dispatchBroadcast(message.source(), payload.body());
    }

    private void dispatchBroadcast(NodeId producer, String body) {
        for (BroadcastListener listener : broadcastListeners) {
            try {
                listener.onMsgBroadcasted(producer, body);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Broadcast listener failed", e);
            }
        }
    }

    private void failAllPending(String reason) {
        if (pending.isEmpty()) {
            return;
        }
        IllegalStateException ex = new IllegalStateException(reason);
        List<PendingOperation> toFail = new ArrayList<>(pending.values());
        for (PendingOperation op : toFail) {
            failOperation(op, ex);
        }
    }

    private static final class PendingOperation {
        private final UUID operationId;
        private final String topic;
        private final Object payload;
        /**
         * Payload used for the leader's own local apply. Defaults to {@link #payload}
         * (the wire payload). When a caller provides a distinct value — e.g. a
         * {@code DistributedMap} in leader-local by-reference mode passing the live
         * command object — the leader applies that instance locally while still
         * shipping {@link #payload} (the serialized form) to followers and the resend
         * log. See {@link ReplicationManager#replicate(String, Object, Object, Integer)}.
         */
        private final Object localApplyPayload;
        private final long epoch;
        private final int originalQuorum;
        private volatile int quorum;
        private volatile long sequence;
        private final Set<NodeId> acknowledgements = ConcurrentHashMap.newKeySet();
        private final CompletableFuture<ReplicationResult> future = new CompletableFuture<>();
        private volatile OperationStatus status = OperationStatus.PENDING;
        private final Instant createdAt = Instant.now();
        private final java.util.concurrent.atomic.AtomicBoolean localApplied = new java.util.concurrent.atomic.AtomicBoolean(
                false);
        private final java.util.concurrent.atomic.AtomicBoolean localApplyStarted = new java.util.concurrent.atomic.AtomicBoolean(
                false);
        private final java.util.concurrent.atomic.AtomicBoolean commitStarted = new java.util.concurrent.atomic.AtomicBoolean(
                false);

        private PendingOperation(UUID operationId, String topic, Object payload, long epoch, int quorum) {
            this(operationId, topic, payload, payload, epoch, quorum);
        }

        private PendingOperation(UUID operationId, String topic, Object payload, Object localApplyPayload,
                long epoch, int quorum) {
            this.operationId = operationId;
            this.topic = topic;
            this.payload = payload;
            this.localApplyPayload = localApplyPayload;
            this.epoch = epoch;
            this.originalQuorum = quorum;
            this.quorum = quorum;
        }

        /**
         * Updates the quorum to a new value, typically when peers disconnect.
         * The new quorum cannot exceed the original quorum and must be at least 1.
         */
        void updateQuorum(int newQuorum) {
            this.quorum = Math.max(1, Math.min(newQuorum, originalQuorum));
        }

        void ack(NodeId nodeId) {
            acknowledgements.add(nodeId);
        }

        int ackCount() {
            return acknowledgements.size();
        }

        Set<NodeId> acknowledgementsSnapshot() {
            return Set.copyOf(acknowledgements);
        }

        boolean isAcked(NodeId nodeId) {
            return acknowledgements.contains(nodeId);
        }

        boolean isCommitted() {
            return status == OperationStatus.COMMITTED;
        }

        boolean isDone() {
            return future.isDone();
        }

        boolean markLocalApplyStarted() {
            return localApplyStarted.compareAndSet(false, true);
        }

        boolean markCommitStarted() {
            return commitStarted.compareAndSet(false, true);
        }

        void markLocalApplied() {
            localApplied.set(true);
        }

        void complete(OperationStatus status) {
            if (isCommitted()) {
                return;
            }
            this.status = status;
            future.complete(new ReplicationResult(operationId, status));
        }

        void fail(Throwable error) {
            if (future.isDone()) {
                return;
            }
            this.status = OperationStatus.REJECTED;
            future.completeExceptionally(error);
        }

        boolean isExpired(Instant now, Duration timeout) {
            return createdAt.plus(timeout).isBefore(now);
        }

        CompletableFuture<ReplicationResult> future() {
            return future;
        }
    }

    private long nextSequenceForTopic(String topic) {
        java.util.concurrent.atomic.AtomicLong counter = sequenceByTopic.computeIfAbsent(topic,
                key -> new java.util.concurrent.atomic.AtomicLong(0));
        long baseline = nextExpectedSequenceByTopic.getOrDefault(topic, 1L) - 1;
        counter.updateAndGet(current -> Math.max(current, baseline));
        return counter.incrementAndGet();
    }

    private void recordApplied() {
        lastAppliedSequence = appliedSequence.incrementAndGet();
    }

    private void recordApplied(int n) {
        lastAppliedSequence = appliedSequence.addAndGet(n);
    }

    /**
     * Evicts the oldest entries from the {@code applied} dedup set when it exceeds
     * {@link ReplicationConfig#appliedSetMaxSize()}.
     *
     * <p>
     * Must be called while {@link #sequenceBufferLock} is held.
     */
    private void trimApplied() {
        int maxSize = config.appliedSetMaxSize();
        while (applied.size() > maxSize) {
            Iterator<UUID> it = applied.iterator();
            if (it.hasNext()) {
                it.next();
                it.remove();
            } else {
                break;
            }
        }
    }

    /**
     * Removes committed entries from the operation audit log ({@code log}) when
     * its size exceeds {@link ReplicationConfig#operationLogMaxSize()}.
     * Scheduled periodically by the timeout scheduler.
     */
    private void trimLog() {
        try {
            trimOperationAuditLog();
            trimReplicationLogByTime();
        } catch (Throwable t) {
            LOGGER.log(Level.WARNING, "Unexpected error during trimLog", t);
        }
    }

    private void trimOperationAuditLog() {
        int maxSize = config.operationLogMaxSize();
        if (log.size() <= maxSize) {
            return;
        }
        // Remove only COMMITTED entries — PENDING entries must not be evicted
        log.entrySet().removeIf(e -> e.getValue().status() == OperationStatus.COMMITTED
                && log.size() > maxSize);
        LOGGER.fine(() -> "Operation log trimmed to " + log.size() + " entries");
    }

    /**
     * Sweeps every per-topic resend log for entries past the temporal retention window. This is the
     * eviction path for IDLE topics (no new commits to trigger the opportunistic eviction in
     * {@link #indexReplicationPayload}); without it a topic that stopped writing would never release
     * its backlog within the configured window. No-op when temporal retention is disabled.
     */
    private void trimReplicationLogByTime() {
        if (config.replicationLogRetentionTime().toMillis() <= 0) {
            return;
        }
        for (java.util.NavigableMap<Long, TimedPayload> topicLog : replicationLogBySequence.values()) {
            evictExpiredFromTopicLog(topicLog);
        }
    }

    private long getSyncSequenceForTopic(String topic) {
        if (coordinator.isLeader()) {
            java.util.concurrent.atomic.AtomicLong counter = sequenceByTopic.get(topic);
            return counter != null ? counter.get() : 0L;
        }
        acquireSequenceLock();
        try {
            long next = nextExpectedSequenceByTopic.getOrDefault(topic, 1L);
            return Math.max(0L, next - 1L);
        } finally {
            sequenceBufferLock.unlock();
        }
    }

    /**
     * Loads the last saved sequence state from disk.
     */
    private void loadSequenceState() {
        if (!Files.exists(sequenceStatePath)) {
            LOGGER.info(() -> "No saved sequence state found, starting from sequence 1");
            return;
        }

        try (java.io.ObjectInputStream ois = new java.io.ObjectInputStream(
                Files.newInputStream(sequenceStatePath))) {
            @SuppressWarnings("unchecked")
            Map<String, Long> loaded = (Map<String, Long>) ois.readObject();
            nextExpectedSequenceByTopic.putAll(loaded);

            // Load globalSequence (stored as "_global" key for compatibility)
            Long savedGlobal = loaded.get("_global");
            if (savedGlobal != null) {
                globalSequence.set(savedGlobal);
            }

            // Load per-topic sequences (keys starting with "_topic:")
            loaded.entrySet().stream()
                    .filter(e -> e.getKey().startsWith("_topic:"))
                    .forEach(e -> {
                        String topic = e.getKey().substring(7); // Remove "_topic:" prefix
                        sequenceByTopic.computeIfAbsent(topic,
                                k -> new java.util.concurrent.atomic.AtomicLong(0))
                                .set(e.getValue());
                    });

            LOGGER.info(() -> "Loaded sequence state: global=" + globalSequence.get() +
                    ", topics=" + sequenceByTopic.keySet());
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to load sequence state, starting from 1", e);
        }
    }

    /**
     * Scheduled flush of the coalesced sequence state. Runs OFF the replication lock; reads only
     * thread-safe concurrent structures. Clears the dirty flag before writing so a concurrent
     * dirty-mark during the write re-arms it for the next flush (no lost update).
     */
    private void flushSequenceStateIfDirty() {
        if (!sequenceStateDirty) {
            return;
        }
        sequenceStateDirty = false;
        saveSequenceState();
    }

    /**
     * Saves the current sequence state to disk.
     * Saves:
     * - nextExpectedSequenceByTopic (for followers)
     * - globalSequence (for leader, stored as "_global" key)
     * - sequenceByTopic (for leader, stored as "_topic:{topic}" keys)
     */
    private void saveSequenceState() {
        if (sequenceStatePath == null) {
            return; // Test mode, no persistence
        }
        try {
            Files.createDirectories(sequenceStatePath.getParent());
            Map<String, Long> toSave = new HashMap<>(nextExpectedSequenceByTopic);

            // Add global sequence
            toSave.put("_global", globalSequence.get());

            // Add per-topic sequences
            sequenceByTopic.forEach((topic, seq) -> toSave.put("_topic:" + topic, seq.get()));

            try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(
                    Files.newOutputStream(sequenceStatePath))) {
                oos.writeObject(toSave);
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to save sequence state", e);
        }
    }
}
