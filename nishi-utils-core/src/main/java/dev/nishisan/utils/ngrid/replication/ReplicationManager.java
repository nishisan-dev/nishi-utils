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
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.OperationStatus;
import dev.nishisan.utils.ngrid.common.ReplicationAckPayload;
import dev.nishisan.utils.ngrid.common.ReplicationPayload;
import dev.nishisan.utils.ngrid.common.SequenceResendRequestPayload;
import dev.nishisan.utils.ngrid.common.SequenceResendResponsePayload;
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
public class ReplicationManager implements TransportListener, LeadershipListener, Closeable {
    private static final Logger LOGGER = Logger.getLogger(ReplicationManager.class.getName());
    private static final long SYNC_THRESHOLD = 500; // Trigger sync if lag > 500 ops
    // For small lag (<= SYNC_THRESHOLD), only trigger snapshot sync if lag is
    // stalled for a while, avoiding aggressive sync while follower is progressing.
    private static final Duration SMALL_LAG_STALL_SYNC_TIMEOUT = Duration.ofSeconds(4);

    private final Transport transport;
    private final ClusterCoordinator coordinator;
    private final ReplicationConfig config;
    private final Map<String, ReplicationHandler> handlers = new ConcurrentHashMap<>();
    private final Map<UUID, PendingOperation> pending = new ConcurrentHashMap<>();
    private final Map<UUID, ReplicatedRecord> log = new ConcurrentHashMap<>();
    private final LinkedHashSet<UUID> applied = new LinkedHashSet<>();
    private final Set<UUID> processing = ConcurrentHashMap.newKeySet();
    private final java.util.concurrent.atomic.AtomicLong globalSequence = new java.util.concurrent.atomic.AtomicLong(0);
    private final Map<String, java.util.concurrent.atomic.AtomicLong> sequenceByTopic = new ConcurrentHashMap<>();
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
    private volatile long lastSeenLeaderEpoch = 0L;
    private volatile long smallLagObservedAppliedSequence = -1L;
    private volatile Instant smallLagObservedAt = null;

    // Multi-thread executor to prevent starvation when callbacks recursively submit
    // tasks
    // (e.g., processSequenceBuffer calling applyReplication which callbacks to
    // processSequenceBuffer)
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

    // Sequence ordering structures - PER TOPIC
    private final Map<String, PriorityQueue<BufferedReplication>> sequenceBufferByTopic = new ConcurrentHashMap<>();
    private final Map<String, Long> nextExpectedSequenceByTopic = new ConcurrentHashMap<>();
    private final Map<String, Map<Long, Instant>> sequenceWaitStartByTopic = new ConcurrentHashMap<>();
    private static final Duration SEQUENCE_WAIT_TIMEOUT = Duration.ofSeconds(1);
    private final Path sequenceStatePath;
    // Sequence-state persistence is a recovery HINT (lost state just triggers a re-sync), so it is
    // coalesced: the hot path marks it dirty (no I/O, no lock cost) and a scheduled flush writes it
    // at most once per interval, OFF the lock. Writing the whole file on every applied op (2k+/s)
    // under sequenceBufferLock made the lock hold time bounded by disk latency — a throughput
    // bottleneck and a freeze hazard on any I/O stall.
    private volatile boolean sequenceStateDirty = false;
    private final ReentrantLock sequenceBufferLock = new ReentrantLock();
    // Max time to wait when acquiring sequenceBufferLock on the replication path. A bounded tryLock
    // (instead of an unbounded lock()) means that if the lock is ever orphaned — e.g. a worker dies
    // leaving the ReentrantLock held with no live owner — the replication path degrades to a
    // recoverable timeout (the operation aborts and is retried / re-synced) instead of parking the
    // whole replication pool forever and freezing the node (which then drops the cluster's leader).
    private static final long LOCK_ACQUIRE_TIMEOUT_MS = 15_000L;
    // Hard cap on the per-topic out-of-order sequence buffer. Unbounded growth (a follower stuck on a
    // gap while the live stream keeps arriving) is what drove the node to OOM — the Error that
    // orphaned the lock and froze it. At the cap we drop to a fresh snapshot instead of buffering
    // without limit.
    private static final int MAX_SEQUENCE_BUFFER = 250_000;

    // Leader-side replication log indexed by sequence (per topic) for resend
    // support. Values carry a leader-local index timestamp (TimedPayload) so the log can be
    // evicted both by count (memory cap) and by time (backlog window) — see indexReplicationPayload.
    private final Map<String, java.util.NavigableMap<Long, TimedPayload>> replicationLogBySequence = new ConcurrentHashMap<>();

    // Disk tier of the hybrid resend op-log (#127): when persistentResendLog is enabled, the heap map
    // above keeps only the freshest window (count-capped) and this durable, segmented, time-governed
    // store holds the deep backlog window off-heap — so a large window costs disk, not heap (the cause
    // of the re-snapshot death spiral under high production). Null when persistence is disabled.
    private volatile ResendLogStore resendLogStore;

    // Resend tracking (follower-side)
    private final Set<String> resendPendingTopics = ConcurrentHashMap.newKeySet();
    private final Map<String, Instant> resendStartByTopic = new ConcurrentHashMap<>();

    // ── Relay-log ingestion (#124, FollowerIngestMode.RELAY_LOG) ─────────────────
    // The follower persists each REPLICATION_REQUEST to a durable relay (one NQueue per
    // topic) and a per-topic consumer applies it at its own pace, decoupling reception
    // from application. nextExpectedSequenceByTopic remains the durable apply frontier.
    private volatile RelayStore relayStore;
    private final Map<String, Thread> relayApplyThreads = new ConcurrentHashMap<>();
    private final Map<String, Object> relaySignals = new ConcurrentHashMap<>();
    // Small in-memory reorder buffer (per topic), used ONLY to bridge transport-hiccup gaps
    // while a resend fills the hole. The large in-order backlog lives on disk (the relay), so
    // this stays small; exceeding the cap means the follower is too far behind a hole and must
    // bootstrap rather than buffer unbounded (the old OOM driver).
    private final Map<String, PriorityQueue<RelayEntry>> relayReorderByTopic = new ConcurrentHashMap<>();
    private static final int RELAY_REORDER_CAP = 50_000;
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

    // Metrics
    private final java.util.concurrent.atomic.AtomicLong gapsDetected = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong evictedSkipCount = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong resendSuccessCount = new java.util.concurrent.atomic.AtomicLong(
            0);
    private final java.util.concurrent.atomic.AtomicLong snapshotFallbackCount = new java.util.concurrent.atomic.AtomicLong(
            0);
    // Total snapshot/sync requests this node has initiated (chunk 0). In RELAY_LOG regime this must
    // stay flat — lag is absorbed by the relay, not by a snapshot.
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
     * Buffered replication operation waiting for its sequence turn.
     */
    private static record BufferedReplication(
            ReplicationPayload payload,
            ClusterMessage originalMessage,
            Instant receivedAt) implements Comparable<BufferedReplication> {
        long sequence() {
            return payload.sequence();
        }

        @Override
        public int compareTo(BufferedReplication other) {
            return Long.compare(this.sequence(), other.sequence());
        }
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
        if (isRelayMode()) {
            detectUncleanRestartAndMarkBootstrap();
            // Relay retention mirrors the leader op-log window (#122): an over-retention
            // backlog is surfaced to the consumer and resolved by bootstrap, never silently
            // dropped (the clamp guarantees that).
            this.relayStore = new RelayStore(config.dataDirectory().resolve("relay"),
                    config.replicationLogRetentionTime(), config.relayDurability(),
                    config.relayGroupCommitInterval());
            // Start consumers for any handler registered before start() (normal flow registers after).
            for (String topic : handlers.keySet()) {
                ensureRelayApplyLoop(topic);
            }
        }
        if (config.persistentResendLog()) {
            // Disk-backed resend op-log (#127). Initialized regardless of role — a follower may be
            // promoted to leader later and must already be serving resends from a durable window.
            this.resendLogStore = new ResendLogStore(config.dataDirectory().resolve("resend-log"),
                    config.resendLogSegmentMaxEntries(), config.resendLogSegmentMaxAge(),
                    config.replicationLogRetentionTime(), config.resendLogMaxEntries(),
                    config.relayDurability() == RelayDurability.ALWAYS);
        }
        transport.addListener(this);
        coordinator.addLeadershipListener(this);
        Duration timeout = config.operationTimeout();
        long periodMs = Math.max(100L, timeout.toMillis() / 2);
        timeoutScheduler.scheduleAtFixedRate(this::checkTimeouts, periodMs, periodMs, TimeUnit.MILLISECONDS);
        Duration retryInterval = config.retryInterval();
        long retryMs = Math.max(100L, retryInterval.toMillis());
        timeoutScheduler.scheduleAtFixedRate(this::retryPending, retryMs, retryMs, TimeUnit.MILLISECONDS);
        timeoutScheduler.scheduleAtFixedRate(this::checkLagAndSync, 2000, 2000, TimeUnit.MILLISECONDS);
        timeoutScheduler.scheduleAtFixedRate(this::retryLeaderSync, 500, 500, TimeUnit.MILLISECONDS);
        timeoutScheduler.scheduleAtFixedRate(this::checkResendTimeouts, 500, 500, TimeUnit.MILLISECONDS);
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
        if (!isRelayMode()) {
            return;
        }
        RelayStore.writeCleanMarker(config.dataDirectory().resolve("relay"));
    }

    public void registerHandler(String topic, ReplicationHandler handler) {
        handlers.put(topic, handler);
        if (isRelayMode()) {
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
        }
    }

    public long getGlobalSequence() {
        return globalSequence.get();
    }

    public long getLastAppliedSequence() {
        return lastAppliedSequence;
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

    private void checkLagAndSync() {
        if (!running || coordinator.isLeader()) {
            return;
        }
        if (isRelayMode()) {
            // RELAY_LOG: a mere lag is absorbed by the durable relay and worked off by the apply
            // consumer — it NEVER triggers a snapshot in regime (the reset+grow-snapshot death spiral
            // #124 removes). Bootstrap is reserved for the unrecoverable cases: unclean restart
            // (registerHandler), an unfillable gap (resend → missing), the reorder-cap, and a relay
            // head older than the retention window (below).
            checkRelayHeadAgeAndBootstrap();
            return;
        }
        long leaderWatermark = coordinator.getTrackedLeaderHighWatermark();
        if (leaderWatermark < 0) {
            return;
        }
        long lag = leaderWatermark - lastAppliedSequence;
        if (lag <= 0) {
            resetSmallLagTracking();
            return;
        }
        if (lag > SYNC_THRESHOLD) {
            resetSmallLagTracking();
            requestSyncForAllTopics();
            return;
        }
        if (shouldSyncForStalledSmallLag()) {
            LOGGER.info(() -> "Small lag stalled at " + lag
                    + " operations. Triggering catch-up sync for registered topics.");
            requestSyncForAllTopics();
        }
    }

    private void requestSyncForAllTopics() {
        for (String topic : handlers.keySet()) {
            if (syncingTopics.add(topic)) {
                requestSync(topic);
            }
        }
    }

    /**
     * RELAY_LOG bootstrap safety valve (decision E): if a topic's relay head is older than the
     * retention window, the follower has fallen so far behind that the relay can no longer carry it
     * to convergence — declare it obsolete and bootstrap from a fresh snapshot, rather than letting
     * the relay grow without bound. Relies on {@code expireAfterWrite} being OFF on the relay (it is),
     * so {@code peekRecord()} returns the true oldest unapplied entry.
     */
    private void checkRelayHeadAgeAndBootstrap() {
        Duration retention = config.replicationLogRetentionTime();
        RelayStore store = relayStore;
        if (store == null || retention == null || retention.isZero()) {
            return; // no temporal bound configured
        }
        long retentionMs = retention.toMillis();
        long marginMs = Math.max(1000L, retentionMs / 10); // bootstrap slightly before the window lapses
        for (String topic : handlers.keySet()) {
            if (syncingTopics.contains(topic)) {
                continue;
            }
            try {
                long headTimestamp = store.relayFor(topic).peekRecord()
                        .map(record -> record.meta().getTimestamp())
                        .orElse(Long.MAX_VALUE);
                if (headTimestamp == Long.MAX_VALUE) {
                    continue; // empty relay
                }
                if (System.currentTimeMillis() - headTimestamp > retentionMs - marginMs) {
                    LOGGER.warning(() -> "Relay head for topic " + topic
                            + " is older than the retention window; bootstrapping (lag exceeded retention)");
                    if (syncingTopics.add(topic)) {
                        snapshotFallbackCount.incrementAndGet();
                        requestSync(topic);
                    }
                }
            } catch (Exception e) {
                LOGGER.log(Level.FINE, "Relay head-age check failed for topic " + topic, e);
            }
        }
    }

    private boolean shouldSyncForStalledSmallLag() {
        long appliedNow = lastAppliedSequence;
        Instant now = Instant.now();

        if (smallLagObservedAt == null || appliedNow != smallLagObservedAppliedSequence) {
            smallLagObservedAppliedSequence = appliedNow;
            smallLagObservedAt = now;
            return false;
        }

        if (Duration.between(smallLagObservedAt, now).compareTo(SMALL_LAG_STALL_SYNC_TIMEOUT) < 0) {
            return false;
        }

        // Rate-limit repeated fallback sync attempts while lag stays unchanged.
        smallLagObservedAt = now;
        return true;
    }

    private void resetSmallLagTracking() {
        smallLagObservedAppliedSequence = lastAppliedSequence;
        smallLagObservedAt = null;
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
        globalSequence.incrementAndGet();
        long seq = nextSequenceForTopic(operation.topic);
        operation.sequence = seq;

        // Persist sequence state for leader recovery (coalesced; flushed off the hot path)
        sequenceStateDirty = true;

        ReplicationPayload payload = new ReplicationPayload(operation.operationId, seq,
                coordinator.getLeaderEpoch(), operation.topic, operation.payload);

        for (NodeInfo member : coordinator.activeMembers()) {
            if (member.nodeId().equals(transport.local().nodeId())) {
                continue;
            }
            ClusterMessage message = ClusterMessage.request(MessageType.REPLICATION_REQUEST,
                    operation.topic,
                    transport.local().nodeId(),
                    member.nodeId(),
                    payload);
            transport.send(message);
        }
        checkCompletion(operation);
    }

    private void retryPending() {
        try {
            if (!running || !coordinator.isLeader()) {
                return;
            }
            for (PendingOperation operation : pending.values()) {
                if (operation.isDone()) {
                    continue;
                }
                checkCompletion(operation);
                if (operation.isDone()) {
                    continue;
                }
                ReplicationPayload payload = new ReplicationPayload(operation.operationId, operation.sequence,
                        coordinator.getLeaderEpoch(), operation.topic, operation.payload);
                for (NodeInfo member : coordinator.activeMembers()) {
                    NodeId nodeId = member.nodeId();
                    if (nodeId.equals(transport.local().nodeId())) {
                        continue;
                    }
                    if (operation.isAcked(nodeId)) {
                        continue;
                    }
                    if (!transport.isReachable(nodeId)) {
                        continue;
                    }
                    ClusterMessage message = ClusterMessage.request(MessageType.REPLICATION_REQUEST,
                            operation.topic,
                            transport.local().nodeId(),
                            nodeId,
                            payload);
                    transport.send(message);
                }
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
        if (message.type() == MessageType.REPLICATION_REQUEST) {
            handleReplicationRequest(message);
        } else if (message.type() == MessageType.REPLICATION_ACK) {
            handleReplicationAck(message);
        } else if (message.type() == MessageType.SYNC_REQUEST) {
            handleSyncRequest(message);
        } else if (message.type() == MessageType.SYNC_RESPONSE) {
            handleSyncResponse(message);
        } else if (message.type() == MessageType.SEQUENCE_RESEND_REQUEST) {
            handleSequenceResendRequest(message);
        } else if (message.type() == MessageType.SEQUENCE_RESEND_RESPONSE) {
            handleSequenceResendResponse(message);
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
                    appliedSequence.updateAndGet(current -> Math.max(current, payload.sequence()));
                    lastAppliedSequence = appliedSequence.get();
                    acquireSequenceLock();
                    try {
                        long watermark = payload.sequence();
                        nextExpectedSequenceByTopic.put(payload.topic(), watermark + 1);
                        // Tail-replay: keep the buffered tail above the watermark so the contiguous
                        // run (watermark+1, +2, ...) buffered during the round-trip is still applied.
                        // The entries already covered (seq <= watermark) are discarded cheaply by
                        // processSequenceBuffer's stale-discard loop below (O(log n) each), not by an
                        // O(n^2) PriorityQueue.removeIf here.
                        Map<Long, Instant> waitStart = sequenceWaitStartByTopic.get(payload.topic());
                        if (waitStart != null) {
                            waitStart.entrySet().removeIf(e -> e.getKey() <= watermark);
                        }
                        sequenceStateDirty = true;
                        // Apply the contiguous tail the snapshot did not cover (called under the
                        // lock, per processSequenceBuffer's contract).
                        processSequenceBuffer(payload.topic());
                    } finally {
                        sequenceBufferLock.unlock();
                    }
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

    private void requestSyncFrom(NodeId target, String topic, int chunkIndex, boolean allowFollowerResponse) {
        if (target == null) {
            return;
        }
        SyncRequestPayload payload = new SyncRequestPayload(topic, chunkIndex, allowFollowerResponse);
        ClusterMessage request = ClusterMessage.request(MessageType.SYNC_REQUEST,
                "sync",
                transport.local().nodeId(),
                target,
                payload);
        transport.send(request);
    }

    private void handleReplicationRequest(ClusterMessage message) {
        ReplicationPayload payload = message.payload(ReplicationPayload.class);
        UUID opId = payload.operationId();
        long seq = payload.sequence();
        String localNodeId = transport.local().nodeId().value();

        LOGGER.fine(() -> String.format(
                "[%s] Replication request opId=%s seq=%d topic=%s from=%s",
                localNodeId, opId, seq, payload.topic(), message.source()));

        // Already applied previously - send ACK and skip (checked below under lock)

        // FENCING: Reject commands from stale leader epochs
        long payloadEpoch = payload.epoch();
        if (payloadEpoch < lastSeenLeaderEpoch) {
            LOGGER.warning(() -> String.format(
                    "[%s] Rejecting stale replication from epoch %d (current: %d) opId=%s",
                    localNodeId, payloadEpoch, lastSeenLeaderEpoch, opId));
            return;
        }
        if (payloadEpoch > lastSeenLeaderEpoch) {
            lastSeenLeaderEpoch = payloadEpoch;
        }

        // RELAY_LOG: persist the op to the durable relay and ack on durable receipt; the per-topic
        // consumer applies it at its own pace. This replaces the in-memory buffer + inline apply
        // path below (which is the INLINE mode).
        if (isRelayMode()) {
            handleReplicationRelay(payload, message);
            return;
        }

        // Already being processed - skip entirely (dedup guard)
        if (processing.contains(opId)) {
            LOGGER.fine(() -> String.format(
                    "[%s] Skipping already-processing opId=%s", localNodeId, opId));
            return;
        }

        // Lock to manipulate buffer and sequence
        String topic = payload.topic();
        acquireSequenceLock();
        try {
            // Already applied previously - send ACK and skip (safe check under lock)
            if (applied.contains(opId)) {
                LOGGER.fine(() -> String.format(
                        "[%s] Skipping already-applied opId=%s", localNodeId, opId));
                sendAck(opId, message.source());
                return;
            }

            // Initialize per-topic structures if needed
            long nextExpected = nextExpectedSequenceByTopic.computeIfAbsent(topic, k -> 1L);
            PriorityQueue<BufferedReplication> buffer = sequenceBufferByTopic.computeIfAbsent(
                    topic, k -> new PriorityQueue<>());
            Map<Long, Instant> waitStart = sequenceWaitStartByTopic.computeIfAbsent(
                    topic, k -> new ConcurrentHashMap<>());

            if (seq == nextExpected) {
                // Create callback to execute AFTER successful apply
                Runnable onSuccess = () -> {
                    acquireSequenceLock();
                    try {
                        // Update state ONLY if still the expected sequence (idempotency check)
                        long current = nextExpectedSequenceByTopic.get(topic);
                        if (current == nextExpected) {
                            recordApplied();
                            log.putIfAbsent(opId, new ReplicatedRecord(
                                    opId, payload.topic(), payload.data(), OperationStatus.COMMITTED));
                            applied.add(opId);
                            trimApplied();

                            // SEND ACK (only after successful apply)
                            sendAck(opId, message.source());

                            // Advance sequence
                            nextExpectedSequenceByTopic.put(topic, nextExpected + 1);
                            sequenceStateDirty = true;

                            // Process buffer recursively
                            processSequenceBuffer(topic);
                        }
                    } finally {
                        sequenceBufferLock.unlock();
                    }
                };

                // Apply asynchronously with callback
                applyReplication(payload, message, onSuccess);
            } else if (seq > nextExpected) {
                // Future sequence: buffer it. We do NOT scan the buffer for duplicates here (that was
                // O(n) per op and, with a large buffer under load, monopolized the lock). A duplicate
                // future seq is harmless — it stays until processSequenceBuffer reaches that sequence,
                // applies one copy, and discards the rest as stale (seq < nextExpected) in O(log n).
                if (buffer.size() >= MAX_SEQUENCE_BUFFER) {
                    // Hopelessly behind with a persistent gap: buffering without limit would OOM.
                    // Drop to a fresh snapshot (tail-replayed at the new watermark discards the stale
                    // buffer) instead of growing the buffer until the heap is exhausted.
                    LOGGER.warning(() -> String.format(
                            "[%s] Sequence buffer for topic=%s hit cap (%d); requesting snapshot to recover.",
                            localNodeId, topic, MAX_SEQUENCE_BUFFER));
                    snapshotFallbackCount.incrementAndGet();
                    if (syncingTopics.add(topic)) {
                        requestSync(topic);
                    }
                    return;
                }
                BufferedReplication buffered = new BufferedReplication(
                        payload, message, Instant.now());
                buffer.add(buffered);
                waitStart.putIfAbsent(seq, Instant.now());

                LOGGER.fine(() -> String.format(
                        "[%s] Buffered future sequence opId=%s seq=%d (expecting=%d) for topic=%s",
                        localNodeId, opId, seq, nextExpected, topic));

                // Check if we have gaps and if timeout expired
                checkForMissingSequences(topic);
            } else {
                // Old sequence (duplicate or already processed), ignore
                LOGGER.warning(() -> String.format(
                        "Received old sequence seq=%d (expecting=%d) for topic=%s, ignoring",
                        seq, nextExpected, topic));
            }
        } finally {
            sequenceBufferLock.unlock();
        }
    }

    // ── Relay-log ingestion (#124) ───────────────────────────────────────────────

    private boolean isRelayMode() {
        return config != null && config.followerIngestMode() == FollowerIngestMode.RELAY_LOG;
    }

    /**
     * IO path (RELAY_LOG): encode + persist the op to the topic relay, then ack on durable
     * receipt. Apply happens asynchronously in {@link #relayApplyLoop}. Never drops silently:
     * if the handler is missing or the offer fails, it does NOT ack and the leader resends.
     */
    private void handleReplicationRelay(ReplicationPayload payload, ClusterMessage message) {
        String topic = payload.topic();
        UUID opId = payload.operationId();
        ReplicationHandler handler = handlers.get(topic);
        if (handler == null) {
            LOGGER.fine(() -> "Relay: no handler yet for topic " + topic + ", deferring op " + opId
                    + " (leader resend will redeliver)");
            return;
        }
        try {
            byte[] payloadBytes = handler.encodePayload(payload.data());
            RelayEntry entry = new RelayEntry(payload.epoch(), payload.sequence(), topic, opId, payloadBytes);
            relayFor(topic).offer(RelayEntryCodec.encode(entry));
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Relay offer failed for topic " + topic + "; not acking op " + opId, e);
            return; // no ack -> leader resends; no silent loss
        }
        sendAck(opId, message.source());
        signalRelay(topic);
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

    private void relayApplyLoop(String topic) {
        NQueue<byte[]> relay = relayFor(topic);
        PriorityQueue<RelayEntry> reorder = relayReorderByTopic.computeIfAbsent(topic,
                k -> new PriorityQueue<>(Comparator.comparingLong(RelayEntry::sequence)));
        Object signal = relaySignals.computeIfAbsent(topic, k -> new Object());
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                boolean progressed = drainRelayOnce(topic, relay, reorder);
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
     * One drain step: apply any now-contiguous buffered entries, then inspect the relay head and
     * apply / discard / buffer-on-gap. Returns true when it made progress (so the loop keeps going
     * without waiting).
     */
    private boolean drainRelayOnce(String topic, NQueue<byte[]> relay, PriorityQueue<RelayEntry> reorder)
            throws Exception {
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
        boolean progressed = drainReorderContiguous(topic, reorder);

        Optional<byte[]> head = relay.peek();
        if (head.isEmpty()) {
            if (reorder.isEmpty()) {
                // Relay + reorder buffer fully drained for this topic — release the failover drain-gate
                // if one is held (no-op otherwise). Safe to read reorder here: this is its owner thread.
                maybeReleaseRelayDrainGate(topic);
            }
            return progressed;
        }
        RelayEntry entry = RelayEntryCodec.decode(head.get());
        long nextExpected = nextExpectedSequenceByTopic.getOrDefault(topic, 1L);

        if (entry.epoch() < lastSeenLeaderEpoch || entry.sequence() < nextExpected) {
            // Stale epoch, or a duplicate/already-applied sequence (re-peek after crash, or a resend
            // copy): discard. Fencing on (epoch, sequence) is what makes the non-idempotent queue
            // OFFER effectively-once.
            relay.poll();
            return true;
        }
        if (entry.sequence() == nextExpected) {
            applyRelayEntry(topic, entry);
            relay.poll();
            return true;
        }

        // Gap: head.sequence() > nextExpected. Pull the head into the reorder buffer to expose the
        // entries behind it (including resent ops that arrive at the relay tail), and ask the leader
        // to resend the missing prefix.
        reorder.add(entry);
        relay.poll();
        gapsDetected.incrementAndGet();
        if (reorder.size() > RELAY_REORDER_CAP) {
            LOGGER.warning(() -> "Relay reorder buffer cap hit for topic " + topic
                    + "; backlog too far ahead of an unfilled gap — requesting snapshot");
            snapshotFallbackCount.incrementAndGet();
            reorder.clear();
            if (syncingTopics.add(topic)) {
                requestSync(topic);
            }
        } else if (!resendPendingTopics.contains(topic)) {
            requestSequenceResend(topic, nextExpected, entry.sequence() - 1);
        }
        return true;
    }

    /** Applies buffered entries that have become contiguous with nextExpected. */
    private boolean drainReorderContiguous(String topic, PriorityQueue<RelayEntry> reorder) throws Exception {
        boolean any = false;
        while (!reorder.isEmpty()) {
            long nextExpected = nextExpectedSequenceByTopic.getOrDefault(topic, 1L);
            RelayEntry min = reorder.peek();
            if (min.sequence() < nextExpected) {
                reorder.poll(); // stale duplicate buffered earlier
                any = true;
                continue;
            }
            if (min.sequence() != nextExpected) {
                break; // still a hole
            }
            applyRelayEntry(topic, reorder.poll());
            any = true;
        }
        return any;
    }

    /** Applies one relay entry to the handler and advances the durable apply frontier. */
    private void applyRelayEntry(String topic, RelayEntry entry) throws Exception {
        ReplicationHandler handler = handlers.get(topic);
        if (handler == null) {
            return; // handler vanished (shutdown): leave nextExpected untouched to retry later
        }
        Object payload = handler.decodePayload(entry.payloadBytes());
        handler.apply(entry.operationId(), payload);

        acquireSequenceLock();
        try {
            nextExpectedSequenceByTopic.merge(topic, entry.sequence() + 1,
                    (cur, candidate) -> Math.max(cur, candidate));
            applied.add(entry.operationId());
            trimApplied();
            recordApplied(); // global applied-op counter (drives the lag metric vs leader watermark)
            sequenceStateDirty = true;
        } finally {
            sequenceBufferLock.unlock();
        }
    }

    /**
     * Applies a replication operation ASYNCHRONOUSLY (must be called with
     * sequenceBufferLock held, but releases it before applying).
     * Executes handler.apply() in the executor thread pool, then invokes the
     * onSuccess callback to advance sequences.
     */
    private void applyReplication(ReplicationPayload payload, ClusterMessage message, Runnable onSuccess) {
        UUID opId = payload.operationId();
        String localNodeId = transport.local().nodeId().value();

        if (!processing.add(opId)) {
            LOGGER.fine(() -> String.format(
                    "[%s] applyReplication: already processing opId=%s", localNodeId, opId));
            return; // Already being processed
        }

        ReplicationHandler handler = handlers.get(payload.topic());
        if (handler == null) {
            processing.remove(opId);
            LOGGER.log(Level.WARNING, "No handler registered for topic {0}", payload.topic());
            return;
        }

        // Submit to executor and return IMMEDIATELY (without holding any locks)
        executor.submit(() -> {
            try {
                // Execute apply WITHOUT holding sequenceBufferLock
                handler.apply(opId, payload.data());

                // CALLBACK: re-acquire lock and advance state ONLY after successful apply
                if (onSuccess != null) {
                    onSuccess.run();
                }

                LOGGER.fine(() -> String.format(
                        "[%s] Applied replication opId=%s seq=%d topic=%s",
                        localNodeId, opId, payload.sequence(), payload.topic()));
            } catch (Throwable e) {
                // Throwable (not Exception): an Error must not kill the pool worker silently nor skip
                // the processing-set cleanup in the finally below; log it and recover via re-sync.
                LOGGER.log(Level.SEVERE, "Failed to apply replicated operation", e);
                if (syncingTopics.add(payload.topic())) {
                    LOGGER.warning(() -> "Apply failed for topic " + payload.topic()
                            + ", requesting sync to recover");
                    requestSync(payload.topic());
                }
            } finally {
                processing.remove(opId);
            }
        });
    }

    /**
     * Processes buffered sequences in order (must be called with sequenceBufferLock
     * held). Recursively processes ONE buffered item at a time via callbacks.
     */
    private void processSequenceBuffer(String topic) {
        PriorityQueue<BufferedReplication> buffer = sequenceBufferByTopic.get(topic);
        Map<Long, Instant> waitStart = sequenceWaitStartByTopic.get(topic);

        if (buffer == null || buffer.isEmpty()) {
            return;
        }

        long nextExpected = nextExpectedSequenceByTopic.get(topic);
        // Discard buffered entries already covered (seq < nextExpected): duplicates, or the part of
        // the buffer below a snapshot watermark. Each poll is O(log n) — this replaces the O(n)
        // per-insert duplicate scan and the O(n^2) PriorityQueue.removeIf on tail-replay, both of
        // which monopolized the lock under a large buffer and starved the apply callbacks (lock
        // acquire timeouts), stalling convergence.
        while (!buffer.isEmpty() && buffer.peek().sequence() < nextExpected) {
            BufferedReplication stale = buffer.poll();
            if (waitStart != null) {
                waitStart.remove(stale.sequence());
            }
        }
        if (buffer.isEmpty()) {
            return;
        }
        BufferedReplication next = buffer.peek();

        if (next.sequence() != nextExpected) {
            // Next in buffer is not the expected one, stop
            return;
        }

        // Remove from buffer
        buffer.poll();
        if (waitStart != null) {
            waitStart.remove(next.sequence());
        }

        // Create callback for recursive processing
        Runnable onSuccess = () -> {
            acquireSequenceLock();
            try {
                // Update state ONLY if still the expected sequence (idempotency check)
                long current = nextExpectedSequenceByTopic.get(topic);
                if (current == nextExpected) {
                    recordApplied();
                    log.putIfAbsent(next.payload().operationId(),
                            new ReplicatedRecord(next.payload().operationId(),
                                    next.payload().topic(), next.payload().data(),
                                    OperationStatus.COMMITTED));
                    applied.add(next.payload().operationId());
                    trimApplied();

                    // SEND ACK
                    sendAck(next.payload().operationId(), next.originalMessage().source());

                    // Advance sequence
                    nextExpectedSequenceByTopic.put(topic, nextExpected + 1);
                    sequenceStateDirty = true;

                    LOGGER.fine(() -> String.format(
                            "Processed buffered sequence seq=%d for topic=%s", next.sequence(), topic));

                    // RECURSION: Process next buffered item
                    processSequenceBuffer(topic);
                }
            } finally {
                sequenceBufferLock.unlock();
            }
        };

        // Apply asynchronously with recursive callback
        applyReplication(next.payload(), next.originalMessage(), onSuccess);
    }

    /**
     * Checks for missing sequences and manages hybrid recovery:
     * 1. If gap <= resendGapThreshold and no resend in-flight: send
     * SEQUENCE_RESEND_REQUEST
     * 2. If gap > resendGapThreshold: fallback directly to snapshot sync
     * (must be called with sequenceBufferLock held).
     */
    private void checkForMissingSequences(String topic) {
        // A snapshot sync already in progress will recover this topic at its new watermark; firing
        // resends underneath it only hammers the leader (the hot-loop that starved its lease) and
        // races the tail-replay. Let the sync settle first.
        if (syncingTopics.contains(topic)) {
            return;
        }
        PriorityQueue<BufferedReplication> buffer = sequenceBufferByTopic.get(topic);
        Map<Long, Instant> waitStart = sequenceWaitStartByTopic.get(topic);

        if (buffer == null || buffer.isEmpty() || waitStart == null) {
            return;
        }

        Instant now = Instant.now();
        long nextInBuffer = buffer.peek().sequence();
        long nextExpected = nextExpectedSequenceByTopic.get(topic);
        long gap = nextInBuffer - nextExpected;

        if (gap <= 0) {
            return;
        }

        // Check timeout for initial wait
        Instant waitStartTime = waitStart.get(nextInBuffer);
        if (waitStartTime == null) {
            return;
        }
        Duration waited = Duration.between(waitStartTime, now);
        if (waited.compareTo(SEQUENCE_WAIT_TIMEOUT) <= 0) {
            return; // Still within initial wait period
        }

        // Already have a resend in-flight for this topic? Skip.
        if (resendPendingTopics.contains(topic)) {
            return;
        }

        if (gap <= config.resendGapThreshold()) {
            gapsDetected.incrementAndGet();
            // Small gap: try resend first
            LOGGER.info(() -> String.format(
                    "Gap detected for topic=%s: expecting=%d, nextInBuffer=%d (gap=%d). Attempting resend.",
                    topic, nextExpected, nextInBuffer, gap));
            requestSequenceResend(topic, nextExpected, nextInBuffer - 1);
        } else {
            gapsDetected.incrementAndGet();
            // Large gap: fallback directly to snapshot sync
            LOGGER.warning(() -> String.format(
                    "Large gap detected for topic=%s: expecting=%d, nextInBuffer=%d (gap=%d > threshold=%d). Falling back to snapshot sync.",
                    topic, nextExpected, nextInBuffer, gap, config.resendGapThreshold()));
            snapshotFallbackCount.incrementAndGet();
            if (syncingTopics.add(topic)) {
                requestSync(topic);
            }
        }
    }

    // ──────────────────────────────────────────────────────────
    // Sequence Resend Protocol
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

        // Disk tier (#127): mirror the entry to the durable, time-governed resend op-log so the backlog
        // window survives off-heap. A disk failure here must NOT fail the commit — it only degrades a
        // future resend into the existing snapshot-fallback path.
        ResendLogStore diskStore = resendLogStore;
        if (diskStore != null) {
            ReplicationHandler handler = handlers.get(topic);
            if (handler != null) {
                try {
                    byte[] payloadBytes = handler.encodePayload(payload.data());
                    RelayEntry entry = new RelayEntry(payload.epoch(), sequence, topic, payload.operationId(),
                            payloadBytes);
                    diskStore.append(topic, sequence, now, RelayEntryCodec.encode(entry));
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING,
                            "Resend op-log disk append failed for seq " + sequence + " (topic " + topic + ")", e);
                }
            }
        }
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
     * Sends a SEQUENCE_RESEND_REQUEST to the leader (follower-side).
     */
    private void requestSequenceResend(String topic, long fromSequence, long toSequence) {
        coordinator.leaderInfo().ifPresentOrElse(leader -> {
            resendPendingTopics.add(topic);
            resendStartByTopic.put(topic, Instant.now());

            SequenceResendRequestPayload payload = new SequenceResendRequestPayload(topic, fromSequence, toSequence);
            ClusterMessage request = ClusterMessage.request(
                    MessageType.SEQUENCE_RESEND_REQUEST,
                    "resend",
                    transport.local().nodeId(),
                    leader.nodeId(),
                    payload);

            LOGGER.info(() -> String.format(
                    "Sending SEQUENCE_RESEND_REQUEST to leader %s for topic=%s, range=[%d..%d]",
                    leader.nodeId(), topic, fromSequence, toSequence));

            transport.send(request);
        }, () -> {
            LOGGER.warning(
                    () -> "Cannot send SEQUENCE_RESEND_REQUEST: no leader known. Falling back to snapshot sync.");
            snapshotFallbackCount.incrementAndGet();
            if (syncingTopics.add(topic)) {
                requestSync(topic);
            }
        });
    }

    /**
     * Handles an incoming SEQUENCE_RESEND_REQUEST on the leader.
     * Looks up the requested sequence range in the replication log and responds.
     */
    private void handleSequenceResendRequest(ClusterMessage message) {
        if (!coordinator.isLeader()) {
            LOGGER.fine(() -> "Ignoring SEQUENCE_RESEND_REQUEST: not the leader.");
            return;
        }

        SequenceResendRequestPayload request = (SequenceResendRequestPayload) message.payload();
        String topic = request.topic();
        long from = request.fromSequence();
        long to = request.toSequence();

        LOGGER.info(() -> String.format(
                "Received SEQUENCE_RESEND_REQUEST from %s for topic=%s, range=[%d..%d]",
                message.source(), topic, from, to));

        if (from <= 0 || to < from) {
            LOGGER.warning(() -> String.format(
                    "Invalid SEQUENCE_RESEND_REQUEST for topic=%s: range=[%d..%d]. Requesting snapshot fallback.",
                    topic, from, to));
            sendSequenceResendResponse(topic, message.source(), List.of(), List.of(from));
            return;
        }

        long baseMaxRange = Math.max(1000L, (long) config.replicationLogRetention());
        // The disk window (#127) can be far larger than the heap cap; allow a bigger single response.
        final long maxRange = config.persistentResendLog()
                ? Math.max(baseMaxRange, config.resendLogReadBatchMax())
                : baseMaxRange;
        long rangeSize = (to - from) + 1L;
        if (rangeSize > maxRange) {
            LOGGER.warning(() -> String.format(
                    "SEQUENCE_RESEND_REQUEST range too large for topic=%s: size=%d (max=%d). Requesting snapshot fallback.",
                    topic, rangeSize, maxRange));
            sendSequenceResendResponse(topic, message.source(), List.of(), List.of(from));
            return;
        }

        java.util.NavigableMap<Long, TimedPayload> topicLog = replicationLogBySequence.get(topic);

        // Disk tier (#127): materialize the requested range from the durable op-log once and index it
        // by sequence, so a heap miss can be served from disk before being reported missing.
        java.util.Map<Long, RelayEntry> diskBySeq = java.util.Collections.emptyMap();
        ResendLogStore diskStore = resendLogStore;
        if (diskStore != null) {
            List<RelayEntry> diskEntries = diskStore.read(topic, from, to);
            if (!diskEntries.isEmpty()) {
                diskBySeq = new java.util.HashMap<>(diskEntries.size() * 2);
                for (RelayEntry e : diskEntries) {
                    diskBySeq.put(e.sequence(), e);
                }
            }
        }
        ReplicationHandler diskHandler = diskBySeq.isEmpty() ? null : handlers.get(topic);

        List<ReplicationPayload> operations = new ArrayList<>();
        List<Long> missingSequences = new ArrayList<>();

        for (long seq = from; seq <= to; seq++) {
            // A payload only enters replicationLogBySequence via indexReplicationPayload, which is
            // called exclusively from completeOperation (commit). So its mere presence here already
            // implies it is committed. The synchronizedNavigableMap serves each get() atomically.
            TimedPayload entry = topicLog != null ? topicLog.get(seq) : null;
            if (entry != null) {
                operations.add(entry.payload());
                continue;
            }
            // Heap miss: try the disk tier before declaring the sequence missing. A reconstructed
            // payload re-encodes the stored bytes through the handler, matching the heap path's shape.
            RelayEntry diskEntry = diskBySeq.get(seq);
            if (diskEntry != null && diskHandler != null) {
                try {
                    operations.add(new ReplicationPayload(diskEntry.operationId(), diskEntry.sequence(),
                            diskEntry.epoch(), topic, diskHandler.decodePayload(diskEntry.payloadBytes())));
                    continue;
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING,
                            "Resend disk decode failed for seq " + seq + " (topic " + topic + ")", e);
                }
            }
            // Absent from heap AND disk: never indexed, or evicted by the temporal window. The follower
            // receives it as a missing sequence → gap-detection → snapshot fallback.
            missingSequences.add(seq);
        }

        sendSequenceResendResponse(topic, message.source(), operations, missingSequences);
    }

    private void sendSequenceResendResponse(String topic, NodeId destination, List<ReplicationPayload> operations,
            List<Long> missingSequences) {
        SequenceResendResponsePayload response = new SequenceResendResponsePayload(topic, operations, missingSequences);
        ClusterMessage responseMessage = ClusterMessage.request(
                MessageType.SEQUENCE_RESEND_RESPONSE,
                "resend",
                transport.local().nodeId(),
                destination,
                response);

        LOGGER.info(() -> String.format(
                "Responding to SEQUENCE_RESEND_REQUEST for topic=%s: %d operations, %d missing",
                topic, operations.size(), missingSequences.size()));

        transport.send(responseMessage);
    }

    /**
     * Recovers from an UNFILLABLE head-of-line gap: when the leader reports the requested
     * sequence(s) as missing (evicted from its resend log) and the follower already holds higher
     * sequences in the buffer, the missing range was produced-then-evicted and will never arrive.
     * Advancing {@code nextExpected} past the hole to the buffer head and draining the contiguous
     * tail lets the follower converge in bulk and go live, instead of re-requesting the same
     * sequence forever (the hot-loop that saturated the leader and starved its lease).
     *
     * <p>This trades strong consistency for liveness (eventual LWW): keys touched ONLY in the
     * skipped range keep their last-known value until the next update or a fresh snapshot. It only
     * fires on a confirmed-evicted gap (the leader said "missing"), never during normal small gaps
     * whose sequences are still resendable.
     *
     * @return {@code true} if a gap was skipped and the buffer drain was kicked off
     */
    private boolean skipEvictedGapAndDrain(String topic, java.util.List<Long> missingSequences) {
        acquireSequenceLock();
        try {
            PriorityQueue<BufferedReplication> buffer = sequenceBufferByTopic.get(topic);
            if (buffer == null || buffer.isEmpty()) {
                return false; // nothing buffered above the hole to jump to
            }
            long nextExpected = nextExpectedSequenceByTopic.getOrDefault(topic, 1L);
            long bufferHead = buffer.peek().sequence();
            if (bufferHead <= nextExpected) {
                return false; // buffer head is not above the hole; ordinary processing applies
            }
            long maxMissing = Long.MIN_VALUE;
            for (long m : missingSequences) {
                maxMissing = Math.max(maxMissing, m);
            }
            if (maxMissing < nextExpected) {
                return false; // stale response for an already-advanced position
            }
            long skipFrom = nextExpected;
            long skipTo = bufferHead - 1;
            long skipped = skipTo - skipFrom + 1;
            nextExpectedSequenceByTopic.put(topic, bufferHead);
            evictedSkipCount.addAndGet(skipped);
            LOGGER.warning(() -> String.format(
                    "Skipping %d evicted sequence(s) [%d..%d] for topic=%s (gone from the leader's "
                            + "resend log); advancing to buffered %d and draining the tail.",
                    skipped, skipFrom, skipTo, topic, bufferHead));
            Map<Long, Instant> waitStart = sequenceWaitStartByTopic.get(topic);
            if (waitStart != null) {
                waitStart.entrySet().removeIf(e -> e.getKey() < bufferHead);
            }
            sequenceStateDirty = true;
            processSequenceBuffer(topic);
            return true;
        } finally {
            sequenceBufferLock.unlock();
        }
    }

    /**
     * Handles an incoming SEQUENCE_RESEND_RESPONSE on the follower.
     * Applies resent operations or falls back to snapshot sync.
     */
    private void handleSequenceResendResponse(ClusterMessage message) {
        SequenceResendResponsePayload response = (SequenceResendResponsePayload) message.payload();
        String topic = response.topic();

        // Clear resend-pending state
        resendPendingTopics.remove(topic);
        Instant startTime = resendStartByTopic.remove(topic);

        if (!response.missingSequences().isEmpty()) {
            // The leader cannot resend these sequences. With synchronous indexing on commit, an op is
            // resendable the instant it is produced, so a "missing" sequence was produced earlier and
            // then EVICTED from the bounded resend log (the follower fell more than the retention
            // window behind). It will never come. If we already hold higher sequences in the buffer,
            // skip the evicted hole and drain the buffered tail to converge IN BULK — instead of
            // head-of-line blocking on one unfillable sequence and re-requesting it forever.
            if (skipEvictedGapAndDrain(topic, response.missingSequences())) {
                return;
            }
            // No buffered tail above the hole to jump to: a full snapshot is the only recovery.
            LOGGER.warning(() -> String.format(
                    "Leader reported %d missing sequences for topic=%s and no buffered tail to skip. "
                            + "Falling back to snapshot sync.", response.missingSequences().size(), topic));
            snapshotFallbackCount.incrementAndGet();
            if (syncingTopics.add(topic)) {
                requestSync(topic);
            }
            return;
        }

        if (response.operations().isEmpty()) {
            LOGGER.fine(() -> "Received empty SEQUENCE_RESEND_RESPONSE for topic=" + topic);
            return;
        }

        LOGGER.info(() -> String.format(
                "Received SEQUENCE_RESEND_RESPONSE for topic=%s with %d operations. Applying...",
                topic, response.operations().size()));

        // Re-inject operations as replication requests — the existing sequence/buffer
        // logic handles ordering
        for (ReplicationPayload payload : response.operations()) {
            ClusterMessage synthetic = ClusterMessage.request(
                    MessageType.REPLICATION_REQUEST,
                    null,
                    message.source(),
                    transport.local().nodeId(),
                    payload);
            handleReplicationRequest(synthetic);
        }

        // Record convergence metrics
        resendSuccessCount.incrementAndGet();
        if (startTime != null) {
            long elapsedMs = Duration.between(startTime, Instant.now()).toMillis();
            totalConvergenceTimeMs.addAndGet(elapsedMs);
            convergenceCount.incrementAndGet();
        }

        LOGGER.info(() -> String.format(
                "Successfully applied %d resent operations for topic=%s.",
                response.operations().size(), topic));
    }

    /**
     * Periodically checks if resend requests have timed out and falls back to
     * snapshot sync.
     */
    private void checkResendTimeouts() {
        if (!running || coordinator.isLeader()) {
            return;
        }

        Instant now = Instant.now();
        for (String topic : resendPendingTopics) {
            Instant start = resendStartByTopic.get(topic);
            if (start != null && Duration.between(start, now).compareTo(config.resendTimeout()) > 0) {
                LOGGER.warning(() -> String.format(
                        "Resend request timed out for topic=%s (timeout=%s). Falling back to snapshot sync.",
                        topic, config.resendTimeout()));

                resendPendingTopics.remove(topic);
                resendStartByTopic.remove(topic);
                snapshotFallbackCount.incrementAndGet();
                if (syncingTopics.add(topic)) {
                    requestSync(topic);
                }
            }
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

    public long getGapsDetected() {
        return gapsDetected.get();
    }

    /**
     * Total entries held in the legacy in-memory sequence buffer (the INLINE ingestion path). In
     * {@link FollowerIngestMode#RELAY_LOG} this must stay {@code 0}: the durable relay-log fully
     * replaces the in-memory buffer (cutover, decision A) — there is no fallback to it.
     *
     * @return the total number of buffered entries across all topics
     */
    public long getInlineSequenceBufferSize() {
        acquireSequenceLock();
        try {
            long total = 0;
            for (PriorityQueue<BufferedReplication> buffer : sequenceBufferByTopic.values()) {
                total += buffer.size();
            }
            return total;
        } finally {
            sequenceBufferLock.unlock();
        }
    }

    public long getResendSuccessCount() {
        return resendSuccessCount.get();
    }

    public long getSnapshotFallbackCount() {
        return snapshotFallbackCount.get();
    }

    /**
     * Number of snapshot/sync requests this node has initiated. In RELAY_LOG regime this stays flat
     * under lag (the relay absorbs it); a snapshot is only requested for unrecoverable cases.
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
     * Current number of entries in the leader-side resend log for the given topic. Intended for
     * testing and observability.
     *
     * @param topic the topic
     * @return the resend-log size for the topic (0 if none)
     */
    public int getReplicationLogSize(String topic) {
        java.util.NavigableMap<Long, TimedPayload> topicLog = replicationLogBySequence.get(topic);
        int heap = topicLog == null ? 0 : topicLog.size();
        ResendLogStore diskStore = resendLogStore;
        if (diskStore == null) {
            return heap;
        }
        // Hybrid (#127): disk holds the deep window; the heap cache is a (possibly overlapping) recent
        // subset. The disk size is the authoritative retained count, so report it when larger.
        return (int) Math.max(heap, Math.min(Integer.MAX_VALUE, diskStore.size(topic)));
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

    private void sendAck(UUID operationId, NodeId destination) {
        String localNodeId = transport.local().nodeId().value();
        LOGGER.fine(() -> String.format(
                "[%s] Sending replication ack for %s to %s",
                localNodeId, operationId, destination));
        ReplicationAckPayload ackPayload = new ReplicationAckPayload(operationId, true);
        ClusterMessage ack = ClusterMessage.request(MessageType.REPLICATION_ACK,
                "ack",
                transport.local().nodeId(),
                destination,
                ackPayload);
        transport.send(ack);
    }

    private void handleReplicationAck(ClusterMessage message) {
        ReplicationAckPayload payload = message.payload(ReplicationAckPayload.class);
        PendingOperation operation = pending.get(payload.operationId());
        if (operation == null) {
            return;
        }
        LOGGER.fine(() -> "Replication ack for " + payload.operationId() + " from " + message.source());
        operation.ack(message.source());
        checkCompletion(operation);
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
        if (isRelayMode()) {
            // RELAY_LOG failover drain-gate (generalizes the #123 sync-before-lead): the promoted node
            // holds writes (leaderSyncing) until its relay backlog is fully applied. The per-topic apply
            // consumers drain and release the gate (maybeReleaseRelayDrainGate) — release depends on
            // "relay drained", not on a peer snapshot, so a node promoted without a reachable peer (and
            // with a full relay) stays gated until it drains rather than leading with a stale state.
            return;
        }
        attemptLeaderSync(previousLeader, localId);
    }

    private NodeId resolveSyncSource(NodeId previousLeader, NodeId localId) {
        Set<NodeId> active = coordinator.activeMembers().stream()
                .map(NodeInfo::nodeId)
                .collect(java.util.stream.Collectors.toSet());
        if (previousLeader != null && !previousLeader.equals(localId) && active.contains(previousLeader)) {
            return previousLeader;
        }
        return active.stream()
                .filter(nodeId -> !nodeId.equals(localId))
                .findFirst()
                .orElse(null);
    }

    private void attemptLeaderSync(NodeId previousLeader, NodeId localId) {
        if (!leaderSyncing.get() || leaderSyncTopics.isEmpty()) {
            return;
        }
        NodeId syncSource = resolveSyncSource(previousLeader, localId);
        if (syncSource == null) {
            // No reachable peer to sync from (single-node cluster, first leader of a brand-new
            // cluster, or the previous leader is gone). resolveSyncSource returns null ONLY when no
            // other active member is reachable, so this node is by definition the most-advanced
            // reachable replica — there is nothing newer to catch up to and it may lead immediately.
            // Clearing the flag here is required so consumers gating on isLeaderSyncing() (and the
            // write gate in replicate()) are not blocked indefinitely.
            leaderSyncTopics.clear();
            leaderSyncing.set(false);
            LOGGER.info("Leader sync skipped: no reachable sync source; clearing leaderSyncing flag.");
            return;
        }
        for (String topic : leaderSyncTopics) {
            requestSyncFrom(syncSource, topic, 0, true);
        }
    }

    private void retryLeaderSync() {
        if (!running || !coordinator.isLeader()) {
            return;
        }
        if (!leaderSyncing.get()) {
            return;
        }
        if (isRelayMode()) {
            // The drain-gate is released by the apply consumers (maybeReleaseRelayDrainGate). No snapshot
            // retry here: a promoted node drains its own relay, it does not pull a snapshot to lead.
            return;
        }
        NodeId localId = transport.local().nodeId();
        attemptLeaderSync(lastLeader.get(), localId);
    }

    /**
     * Called by a topic's apply consumer when its relay (and in-memory reorder buffer) have fully
     * drained. While a failover drain-gate is held (leaderSyncing), this releases the topic; once all
     * gated topics have drained, the write gate opens and the promoted node may lead.
     */
    private void maybeReleaseRelayDrainGate(String topic) {
        if (!leaderSyncing.get()) {
            return;
        }
        if (leaderSyncTopics.remove(topic) && leaderSyncTopics.isEmpty()) {
            leaderSyncing.set(false);
            LOGGER.info(() -> "Relay drained on promotion; releasing write gate for leadership.");
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
