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
import dev.nishisan.utils.ngrid.HandoverListener;
import dev.nishisan.utils.ngrid.common.BroadcastMessagePayload;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.FollowerProgressPayload;
import dev.nishisan.utils.ngrid.common.HandbackAbortPayload;
import dev.nishisan.utils.ngrid.common.HandbackCompletePayload;
import dev.nishisan.utils.ngrid.common.HandbackGrantPayload;
import dev.nishisan.utils.ngrid.common.HandbackRequestPayload;
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
    private final Map<NodeId, FollowerProgress> followerAppliedByNode = new ConcurrentHashMap<>();
    private final Set<NodeId> quiescingFor = ConcurrentHashMap.newKeySet();
    private volatile long joinQuiesceStartedMs;
    // Active members observed on the previous membership change, to detect newly-joined nodes.
    private final Set<NodeId> knownActiveMembers = ConcurrentHashMap.newKeySet();
    // Leader-side reclaim-quiesce gate (issue tems#9, D10b, opt-in via config.leaderPauseOnReclaim):
    // when a HIGHER-affinity candidate approaches the local watermark (within
    // reclaimQuiesceThreshold), the leader pauses production so the candidate can pair up EXACTLY
    // and the affinity handoff completes coordinately. Without it, under continuous production the
    // in-flight delta never zeroes: the strict step-down gate (B) retains forever while the
    // candidate's caught-up latch (gate A) arms against a stale heartbeat watermark — the
    // dual-leader livelock. Separate from joinQuiescing: distinct trigger (proximity, not join),
    // single candidate, exact pairing, own timeout and cooldown.
    private final java.util.concurrent.atomic.AtomicBoolean reclaimQuiescing = new java.util.concurrent.atomic.AtomicBoolean(
            false);
    private volatile NodeId reclaimQuiesceFor;
    private volatile long reclaimQuiesceStartedMs;
    private volatile long reclaimQuiesceCooldownUntilMs;
    // True from the dual-leader yield hook (issue tems#9, D10c) until the bootstrap resync installs:
    // keeps the promoted-leader auto-disarm in drainRelayOnce from undoing the pending bootstrap in
    // the window where this (yielding) node still answers isLeader().
    private volatile boolean dualLeaderYielding = false;

    // ── Orchestrated affinity handback (issue tems#9, D11, opt-in via config.affinityHandbackMode) ──
    // Replaces the lineage-blind watermark reclaim with a stop-the-world snapshot handover. The
    // candidate (returning highest-affinity follower) requests; the interim leader freezes production
    // at a watermark, serves a full snapshot, and demotes; the candidate cuts over (SET — the lineage
    // offset is zeroed) and asserts leadership. The HandoverListener carries the app-facing pause/flush.
    private enum HandbackRole { NONE, CANDIDATE_REQUESTING, CANDIDATE_INSTALLING, LEADER_PREPARING, LEADER_SERVING }
    private final java.util.concurrent.atomic.AtomicReference<HandbackRole> handbackRole =
            new java.util.concurrent.atomic.AtomicReference<>(HandbackRole.NONE);
    // INTERIM-LEADER production freeze: gates replicate() so nothing is produced above the frozen
    // watermark while the candidate installs the snapshot (defense in depth vs the app's consumer pause).
    private final java.util.concurrent.atomic.AtomicBoolean handoverFreezing =
            new java.util.concurrent.atomic.AtomicBoolean(false);
    private volatile long handoverFrozenWatermark = -1L;
    private volatile NodeId handbackPeer;        // candidate (on the leader) / interim leader (on the candidate)
    private volatile long handbackGrantedEpoch;  // candidate side: the incumbent's epoch from the GRANT
    private volatile long handbackStartedMs;
    private volatile long handbackCooldownUntilMs;
    private final Set<HandoverListener> handoverListeners = new java.util.concurrent.CopyOnWriteArraySet<>();

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
    // Unclean restart with prior relay data on disk (issue tems#9, D8): engages the bootstrap gate
    // from the very first heartbeat, before any handler registers and arms the per-topic pending.
    private volatile boolean uncleanWithPriorRelayData = false;
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
    // Serializes ALL relay ingestion and cursor re-anchoring per topic (issue tems#9, D7). The transport
    // dispatches each message on its own virtual thread, so two overlapping RELAY_STREAM_BATCHes (a
    // timeout re-fetch, or an in-flight pre-sync batch racing the snapshot cutover) used to run
    // handleRelayStreamBatch concurrently; the per-frame check→offer→increment is not atomic, so a
    // doubly-approved frame advanced the cursor twice and punched a DURABLE one-op hole in the relay
    // (the second op was never appended) — surfacing much later as a forward gap → snapshot loop.
    private final Map<String, java.util.concurrent.locks.ReentrantLock> relayIngestLockByTopic =
            new ConcurrentHashMap<>();
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
    // RELAY_STREAM forward-gap recovery (relay-log TTL analog): when the relay's write-time TTL
    // (relayExpireAfterWrite) ages an UN-APPLIED head entry out from under the apply consumer, a
    // forward gap surfaces at the relay head. A FOLLOWER recovers cleanly via snapshot bootstrap
    // (requestSync). A PROMOTED LEADER has no peer to bootstrap from, so it SKIPS the gap (advances
    // nextExpected to the head and continues draining) and increments this counter — the surviving
    // entries still apply and the Cardinal re-derives the skipped state idempotently from Kafka. A
    // non-zero, growing value signals async loss for those skipped sequences (recovered to eventual
    // consistency by the upstream replay), exactly the spirit of the removed skipEvictedGapAndDrain.
    private final java.util.concurrent.atomic.AtomicLong evictedSkipCount = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.concurrent.atomic.AtomicLong resendSuccessCount = new java.util.concurrent.atomic.AtomicLong(
            0);
    private final java.util.concurrent.atomic.AtomicLong snapshotFallbackCount = new java.util.concurrent.atomic.AtomicLong(
            0);
    // Forward-gap recoveries resolved by a cursor re-pull from the leader binlog instead of a snapshot
    // (issue tems#9, D7): with the relay TTL off, a "TTL-evicted prefix" is impossible, so the hole is
    // an ingestion artifact (or legacy on-disk damage) and the missing range still lives in the leader's
    // durable op-log — re-fetching it is orders of magnitude cheaper than a snapshot bootstrap.
    private final java.util.concurrent.atomic.AtomicLong relayRefetchCount = new java.util.concurrent.atomic.AtomicLong(
            0);
    // Rate-limit for the not-the-leader fetch refusal warning (issue tems#9, D9).
    private volatile long lastFetchRefusalLogMs = 0L;
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
        //
        // The advertised watermark is the node's TOTAL APPLIED state frontier — for the leader this is
        // max(produced, applied), NOT just globalSequence. A follower that is PROMOTED to leader has a
        // globalSequence that counts only ITS OWN productions (it never produced while a follower), which
        // is BELOW the state it actually applied from the previous leader. Advertising the raw
        // globalSequence would make the promoted leader claim a watermark below its real state — and the
        // sync-before-reclaim gate would then let a returning higher-affinity peer reclaim after matching
        // only that understated frontier, i.e. with the incumbent's tail still unsynced. Using
        // max(globalSequence, lastApplied) keeps the advertised watermark on the same scale as every
        // follower's applied frontier, so the gate compares like with like.
        coordinator.setLeaderHighWatermarkSupplier(this::advertisedLeaderHighWatermark);
        // Sync-before-reclaim (watermark gate): feed the coordinator the local APPLIED frontier and the
        // catch-up tolerance. The coordinator compares this against every peer's advertised watermark
        // (learned from heartbeats) to decide whether a returning higher-affinity node may reclaim
        // leadership (only once caught up) and whether an incumbent may step down (only once the
        // higher-affinity candidate has caught up). The threshold reuses joinSyncLagThreshold (#129).
        coordinator.setReplicationProgressGate(this::localAppliedForReclaim, config.joinSyncLagThreshold());
        coordinator.setLocalLeadershipEligibility(this::isLocallyEligibleForLeadership);
        // Dual-leader resolution (issue tems#9, D10c): when this node is the losing (lower-affinity)
        // side, discard the dual-window lineage and resync from the winner via the D8 machinery.
        coordinator.setDualLeaderYieldHook(this::onDualLeaderYield);
        // Orchestrated affinity handback (issue tems#9, D11): gate the coordinator's watermark reclaim
        // and let it suppress the epoch re-stamp / dual-leader contest while a handover is in flight.
        coordinator.setAffinityHandbackMode(config.affinityHandbackMode());
        coordinator.setHandoverInProgressSupplier(() -> handbackRole.get() != HandbackRole.NONE);
        // Nudge the coordinator to recompute leadership as our applied frontier advances, so a deferred
        // reclaim is promoted promptly once we catch up (membership/eviction ticks would also trigger it).
        timeoutScheduler.scheduleAtFixedRate(this::nudgeLeadershipOnCatchUp, 200, 200, TimeUnit.MILLISECONDS);
        if (isStreamMode()) {
            // Restore the applied progress metric from the durable per-topic frontiers so a clean
            // restart does not report 0 applied (the counter is otherwise per-session). A bootstrap,
            // if one happens, re-anchors it at the snapshot watermark.
            seedAppliedSequenceFromFrontier();
            detectUncleanRestartAndMarkBootstrap();
            markPendingBootstrapForRegisteredHandlers();
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
            timeoutScheduler.scheduleAtFixedRate(this::checkJoinQuiesce, 200, 200, TimeUnit.MILLISECONDS);
        }
        if (config.leaderPauseOnJoin() || config.leaderPauseOnReclaim()) {
            // Both quiesce gates are driven by FOLLOWER_PROGRESS reports.
            long progressMs = Math.max(50L, config.followerProgressInterval().toMillis());
            timeoutScheduler.scheduleAtFixedRate(this::sendFollowerProgress, progressMs, progressMs,
                    TimeUnit.MILLISECONDS);
        }
        if (config.leaderPauseOnReclaim()) {
            // Quiesce-assisted reclaim (issue tems#9, D10b): engage on candidate approach, bound the
            // pause, abort (with cooldown) if the candidate never pairs up or leaves.
            timeoutScheduler.scheduleAtFixedRate(this::checkReclaimQuiesce, 200, 200, TimeUnit.MILLISECONDS);
        }
        if (config.affinityHandbackMode()) {
            // Orchestrated affinity handback (issue tems#9, D11): the candidate periodically checks
            // whether to request a handover; the interim leader bounds an in-flight handover (timeout
            // / candidate gone → abort and retain leadership).
            timeoutScheduler.scheduleAtFixedRate(this::maybeInitiateHandback, 200, 200, TimeUnit.MILLISECONDS);
            timeoutScheduler.scheduleAtFixedRate(this::checkHandover, 200, 200, TimeUnit.MILLISECONDS);
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
        // if every applier actually terminated AND the final flush landed on disk; otherwise leave no
        // marker so the next start bootstraps (the safe side — a clean marker over a stale frontier
        // would replay/skip ops on restart).
        boolean frontierFlushed = flushSequenceStateIfDirty();
        if (allAppliersTerminated && frontierFlushed) {
            writeCleanShutdownMarker();
        } else {
            LOGGER.warning("Skipping clean-shutdown marker (appliersTerminated=" + allAppliersTerminated
                    + ", frontierFlushed=" + frontierFlushed
                    + "); the next start bootstraps as a safety measure.");
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
        // Issue tems#9, D8: the per-topic pending bootstrap only arms in registerHandler (the relay
        // dir names are hashed, so topics cannot be enumerated from disk). In wirings where handlers
        // register AFTER start(), the early heartbeats would otherwise advertise the seeded frontier
        // of a possibly dead lineage as a real watermark. This flag keeps the node un-advertisable
        // and ineligible from the FIRST heartbeat until the first handler arms (or no data exists).
        uncleanWithPriorRelayData = relayUncleanRestart && RelayStore.hasAnyTopicData(relayDir);
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
            markPendingBootstrapIfNeeded(topic);
            // Tie the apply consumer to handler registration, not to start(): an op may reach
            // the relay before the handler exists (it is durably parked on disk until then),
            // but apply must not begin until the handler is available.
            ensureRelayApplyLoop(topic);
            ensureRelayFetchLoop(topic);
        }
    }

    private long advertisedLeaderHighWatermark() {
        if (bootstrapGateEngaged()) {
            return -1L;
        }
        return coordinator.isLeader()
                ? Math.max(getGlobalSequence(), getLastAppliedSequence())
                : getLastAppliedSequence();
    }

    private long localAppliedForReclaim() {
        return bootstrapGateEngaged() ? Long.MIN_VALUE : getLastAppliedSequence();
    }

    private boolean isLocallyEligibleForLeadership() {
        return !bootstrapGateEngaged();
    }

    private boolean hasPendingRelayBootstrap() {
        return isStreamMode() && !relayPendingBootstrap.isEmpty();
    }

    /**
     * True while the node must not advertise its frontier nor accept leadership (issue tems#9, D8):
     * a per-topic bootstrap is pending (unclean restart with prior data, cleared only when the
     * snapshot INSTALLS), or the unclean restart was detected but no handler has registered yet to
     * arm the per-topic pending (the startup window where the seeded frontier of a possibly dead
     * lineage would otherwise leak into the first heartbeats as a real watermark).
     */
    private boolean bootstrapGateEngaged() {
        return hasPendingRelayBootstrap()
                || (isStreamMode() && uncleanWithPriorRelayData && handlers.isEmpty());
    }

    private void markPendingBootstrapForRegisteredHandlers() {
        if (!relayUncleanRestart) {
            return;
        }
        handlers.keySet().forEach(this::markPendingBootstrapIfNeeded);
    }

    private void markPendingBootstrapIfNeeded(String topic) {
        if (!isStreamMode() || !relayUncleanRestart
                || !RelayStore.hasTopicData(config.dataDirectory().resolve("relay"), topic)) {
            return;
        }
        if (relayPendingBootstrap.add(topic)) {
            // Unclean restart and this topic had prior relay data: it must bootstrap before applying
            // (regardless of any saved frontier, which may be lost). The snapshot is requested by the
            // apply loop once a leader is known and this node is a follower.
            LOGGER.info(() -> "Relay bootstrap pending for topic " + topic + " after unclean restart");
            coordinator.reevaluateLeadership();
        }
    }

    public long getGlobalSequence() {
        return globalSequence.get();
    }

    public long getLastAppliedSequence() {
        return lastAppliedSequence;
    }

    /**
     * The watermark this node currently advertises in heartbeats: {@code -1} while the bootstrap
     * gate is engaged (unclean restart with un-bootstrapped prior data — issue tems#9, D8), else
     * the applied frontier (leaders advertise {@code max(produced, applied)}). Observability mirror
     * of the heartbeat supplier — what peers' sync-before-reclaim gates actually see.
     *
     * @return the advertised high watermark, or {@code -1} while not advertisable
     */
    public long getAdvertisedHighWatermark() {
        return advertisedLeaderHighWatermark();
    }

    /**
     * Whether this node currently accepts leadership: {@code false} while the bootstrap gate is
     * engaged (a pending relay bootstrap, or an unclean restart with prior data before any handler
     * registered — issue tems#9, D8). Observability mirror of the coordinator eligibility hook.
     *
     * @return {@code true} when the node may lead
     */
    public boolean isLeadershipEligible() {
        return isLocallyEligibleForLeadership();
    }

    /**
     * Seeds the applied-progress metric from the durable per-topic apply frontiers, so a restart
     * resumes the metric instead of reporting 0 (the counter is per-session). The total applied op
     * count equals the sum of {@code (nextExpected - 1)} across topics.
     */
    private void seedAppliedSequenceFromFrontier() {
        // Two durable measures of how much state this node has APPLIED, restored from sequence-state.dat:
        //  - the per-topic FOLLOWER frontier sum (nextExpectedSequenceByTopic) — non-empty when this node
        //    was a follower for those topics;
        //  - the LEADER-produced count (globalSequence, the "_global" key) — non-zero when this node was
        //    the leader (a leader applies everything it produces, but never populates the follower
        //    frontier, so its frontier sum is 0).
        // A returning ex-LEADER therefore has frontier sum 0 while it actually holds globalSequence ops.
        // Seeding from the frontier alone would advertise applied=0 — making the (higher-affinity)
        // ex-leader look permanently behind the ex-follower, so it defers to sync while the ex-follower
        // defers to it by affinity and the cluster wedges leaderless. Seeding from max(frontierSum,
        // globalSequence) restores the true applied frontier for both roles (they coincide for a single
        // topic), so the election/watermark gate picks the right leader. Synthetic keys stay filtered (D1).
        //
        // D9 (issue tems#9): the FRONTIER itself must be re-anchored on the per-topic PRODUCED counter
        // first. An ex-leader's frontier is stale at the point it last applied as a follower (a leader
        // never advances it), while "_topic:{t}" holds everything it produced — and the node is the
        // source of truth for its own production. Leaving the stale frontier in place made a clean
        // rejoin re-pull (and RE-APPLY!) the node's own produced tail from the new leader's binlog,
        // double-counting it into the applied metric: the inflated counter armed a PREMATURE reclaim,
        // then froze (post-reclaim production restarts below it, so max() never advances), the follower's
        // honest counter crossed it ~20 heartbeats later, and gate A evicted the leader into a leaderless
        // mutual-deferral stalemate.
        acquireSequenceLock();
        try {
            sequenceByTopic.forEach((topic, seq) -> {
                long produced = seq.get();
                if (produced <= 0L) {
                    return;
                }
                Long frontier = nextExpectedSequenceByTopic.get(topic);
                if (frontier == null || frontier < produced + 1L) {
                    nextExpectedSequenceByTopic.put(topic, produced + 1L);
                    sequenceStateDirty = true;
                }
            });
        } finally {
            sequenceBufferLock.unlock();
        }
        long frontierSum = 0L;
        for (Map.Entry<String, Long> e : nextExpectedSequenceByTopic.entrySet()) {
            if (isSyntheticSequenceStateKey(e.getKey())) {
                continue;
            }
            frontierSum += Math.max(0L, e.getValue() - 1L);
        }
        long applied = Math.max(frontierSum, Math.max(0L, globalSequence.get()));
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
     * Periodic nudge for sync-before-reclaim (watermark gate): when this node is a deferring follower
     * that has now caught up to the cluster's max peer watermark, ask the coordinator to recompute
     * leadership so the deferred affinity reclaim is promoted promptly (rather than waiting for the next
     * membership/eviction tick). No-op when we already lead or when no peer is ahead. The authoritative
     * decision still lives in {@link ClusterCoordinator#recomputeLeader()}; this only reduces latency.
     */
    private void nudgeLeadershipOnCatchUp() {
        if (!running || coordinator.isLeader() || hasPendingRelayBootstrap()) {
            return;
        }
        try {
            long maxPeer = coordinator.maxActivePeerHighWatermark();
            if (maxPeer < 0) {
                return; // no peer ahead — nothing deferred (lead-while-alone is handled by recompute)
            }
            long applied = getLastAppliedSequence();
            long threshold = Math.max(0L, config.joinSyncLagThreshold());
            if (applied >= maxPeer - threshold) {
                coordinator.reevaluateLeadership();
            }
        } catch (Throwable t) {
            LOGGER.log(Level.FINE, "nudgeLeadershipOnCatchUp failed", t);
        }
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
        if (config.leaderPauseOnReclaim() && isReclaimQuiescing()) {
            // Quiesce-assisted reclaim (issue tems#9, D10b): a higher-affinity candidate is within the
            // approach threshold; pause production so it pairs up EXACTLY and the handoff completes —
            // under a firehose the in-flight delta would otherwise never zero. Bounded + cooldown.
            throw new LeaderSyncingException(
                    "Leader is quiescing for an affinity handoff (candidate pairing up), write rejected");
        }
        if (handoverFreezing.get()) {
            // Orchestrated affinity handback (issue tems#9, D11): production is frozen at the handover
            // watermark while the returning higher-affinity candidate installs a full snapshot and cuts
            // over. Nothing may get a sequence above the frozen watermark until the handover completes or
            // aborts — defense in depth even if the app's consumer pause races a straggler thread.
            throw new LeaderSyncingException(
                    "Leader is frozen for an affinity handback (snapshot handover in progress), write rejected");
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
        } else if (message.type() == MessageType.HANDBACK_REQUEST) {
            handleHandbackRequest(message);
        } else if (message.type() == MessageType.HANDBACK_GRANT) {
            handleHandbackGrant(message);
        } else if (message.type() == MessageType.HANDBACK_COMPLETE) {
            handleHandbackComplete(message);
        } else if (message.type() == MessageType.HANDBACK_ABORT) {
            handleHandbackAbort(message);
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
        if (coordinator.isLeader()) {
            // Role guard (issue tems#9, D8): an ACTIVE LEADER never installs a peer snapshot. A late
            // chunk from a sync requested while this node was still a follower (the server may answer
            // around a leadership flip) used to resetState() the live leader mid-write — and the
            // chunk chain then died on a self-addressed follow-up request. Drop it and release the
            // sync guard; the leader's own op-log is the source of truth.
            LOGGER.warning(() -> "Ignoring snapshot chunk for " + payload.topic()
                    + " received while LEADER (late response from a pre-promotion sync)");
            syncingTopics.remove(payload.topic());
            return;
        }

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
                // The stale-sync guard NEVER applies while a bootstrap is pending for the topic (issue
                // tems#9, D8/D9): the local lineage is untrusted by definition (unclean restart) and
                // sequence numbers across divergent lineages are incomparable — a returning ex-leader's
                // re-anchored frontier is routinely ABOVE the incumbent's honest watermark, yet the
                // incumbent's snapshot is exactly what must replace the local state.
                if (!relayPendingBootstrap.contains(payload.topic()) && payload.sequence() < currentApplied) {
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
        // The installed snapshot REPLACES the local state (resetState + install), so every counter
        // re-anchors on the serving leader's lineage at the watermark — SET, not max (issue tems#9,
        // D8). The old max() kept the seeded frontier of a dead lineage (an unclean ex-leader whose
        // pre-crash applied was numerically above the incumbent's watermark), so the node kept
        // advertising — and later producing from — a counter the cluster history never contained.
        // Re-anchoring globalSequence/sequenceByTopic also makes a future promotion produce
        // contiguously with the lineage it actually holds (fixes the understated-watermark case the
        // supplier comment in start() documents). The active-leader path never gets here (role guard
        // in handleSyncResponse).
        appliedSequence.set(watermark);
        lastAppliedSequence = watermark;
        globalSequence.updateAndGet(current -> watermark);
        sequenceByTopic.computeIfAbsent(topic, k -> new java.util.concurrent.atomic.AtomicLong())
                .set(watermark);
        boolean completedBootstrap = relayPendingBootstrap.remove(topic);

        acquireSequenceLock();
        try {
            nextExpectedSequenceByTopic.put(topic, watermark + 1);
            sequenceStateDirty = true;
        } finally {
            sequenceBufferLock.unlock();
        }

        // Drop the LOCAL binlog of the replaced lineage (issue tems#9, D8): it still holds the
        // dead tail above the incumbent's watermark and would be served verbatim to a follower
        // fetching that range after a later promotion. The post-cutover stream rebuilds it from
        // watermark+1 on the new lineage.
        ResendLogStore store = resendLogStore;
        if (store != null) {
            store.logFor(topic).truncate();
        }

        // RELAY_STREAM: re-anchor the pull cursor at the snapshot watermark so the fetch loop resumes
        // streaming from watermark+1 instead of a stale pre-snapshot cursor (which would re-pull below
        // the retained window and bounce on needSnapshot). Purge + re-anchor run atomically under the
        // ingest lock (issue tems#9, D7): an in-flight pre-sync batch must not interleave with the
        // cursor reset (it could land its increments on the new anchor and punch a hole), and the old
        // relay content is dropped wholesale — everything ≥ watermark+1 is re-pulled in order anyway,
        // and anything ≤ watermark is stale by definition after the install.
        if (isStreamMode()) {
            java.util.concurrent.locks.ReentrantLock ingest = relayIngestLock(topic);
            ingest.lock();
            try {
                purgeRelay(topic);
                relayStreamCursor(topic).set(watermark);
            } finally {
                ingest.unlock();
            }
        }
        relayFetchPendingUntilByTopic.put(topic, 0L);
        signalFetch(topic);
        if (completedBootstrap && relayPendingBootstrap.isEmpty()) {
            // A dual-leader yield resync (issue tems#9, D10c) completes here: the local state now
            // belongs to the winner's lineage and the drainRelayOnce guard can stand down.
            dualLeaderYielding = false;
            if (handbackRole.get() == HandbackRole.CANDIDATE_INSTALLING) {
                // Orchestrated affinity handback (issue tems#9, D11): the candidate installed the
                // incumbent's full snapshot and re-anchored its counters via the SET above — the lineage
                // offset is now zero. Assert leadership above the granted epoch and signal completion.
                completeHandbackAsCandidate(watermark);
            } else {
                coordinator.reevaluateLeadership();
            }
        }

        signalRelay(topic);
    }

    private void requestSync(String topic) {
        requestSync(topic, 0);
    }

    private void requestSync(String topic, int chunkIndex) {
        coordinator.leaderInfo().ifPresent(leader -> {
            if (leader.nodeId().equals(transport.local().nodeId())) {
                // Never self-sync (issue tems#9, D8): a continuation chunk requested after this node
                // got promoted would be addressed to itself and silently die in the transport
                // ("No route available"), leaving a half-installed state behind.
                return;
            }
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

    /** The per-topic lock serializing relay ingestion and cursor re-anchoring (issue tems#9, D7). */
    private java.util.concurrent.locks.ReentrantLock relayIngestLock(String topic) {
        return relayIngestLockByTopic.computeIfAbsent(topic,
                k -> new java.util.concurrent.locks.ReentrantLock());
    }

    /**
     * Drops the whole on-disk relay content for {@code topic} (close + truncate + reopen, same NQueue
     * instance so every cached reference stays valid). Callers MUST hold the topic's ingest lock and
     * decide by the RESULT: on success the stream cursor is re-anchored and the dropped suffix is
     * re-pulled from the leader binlog; on failure the surviving relay content is still in place, so
     * a gap-fill re-pull would append the re-fetched frames BEHIND the old head and wedge the apply
     * loop on the same gap forever — that caller must fall back to a snapshot instead. (The snapshot
     * cutover caller tolerates a failed purge: everything left is ≤ the installed watermark and the
     * apply loop discards it as a stale prefix.)
     *
     * @return {@code true} when the relay is verifiably empty and reopened
     */
    private boolean purgeRelay(String topic) {
        try {
            NQueue<byte[]> relay = relayFor(topic);
            relay.close();
            relay.truncateAndReopen();
            return true;
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Relay purge failed for topic " + topic, e);
            return false;
        }
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
        if (batch.leaderUnavailable()) {
            // Our ADOPTED leader refuses leadership (issue tems#9, D9): it deferred to someone else
            // (or to us) and cannot serve the stream. Surface the refusal to the coordinator — if we
            // are not behind the refusing node, its escape takes leadership and breaks the mutual-
            // deferral stalemate; otherwise the next recompute re-adopts whoever can actually serve.
            relayFetchPendingUntilByTopic.put(topic,
                    System.currentTimeMillis() + Math.max(1L, config.relayStreamPollInterval().toMillis()));
            coordinator.noteLeaderRefusal(message.source());
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
        // Serialize ingestion per topic (issue tems#9, D7): batches arrive on independent virtual
        // threads, and the per-frame check→offer→increment below is only correct single-writer. Two
        // overlapping batches (timeout re-fetch / pre-sync batch racing the cutover) interleaving here
        // used to double-approve a frame, advance the cursor twice and punch a durable hole in the relay.
        java.util.concurrent.locks.ReentrantLock ingest = relayIngestLock(topic);
        ingest.lock();
        int persisted = 0;
        try {
            if (syncingTopics.contains(topic) || relayPendingBootstrap.contains(topic)) {
                // A snapshot is installing (or a bootstrap is pending): an in-flight batch must not
                // append around the cutover — it would land sequences past the new watermark before the
                // post-cutover refetch re-anchors. Everything is re-pulled from watermark+1 afterwards.
                LOGGER.fine(() -> "Discarding in-flight relay-stream batch for topic " + topic
                        + " while a snapshot install is in progress");
                return;
            }
            java.util.concurrent.atomic.AtomicLong cursor = relayStreamCursor(topic);
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
        } finally {
            ingest.unlock();
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
            // is made that no bootstrap is needed. A dual-leader yield (issue tems#9, D10c) arms the
            // pending BEFORE the demotion lands — the auto-disarm below must not undo it while this
            // node transiently still answers isLeader().
            if (coordinator.isLeader() && !dualLeaderYielding) {
                // Promoted/leader: there is no peer to bootstrap from; the failover drain-gate + sequence
                // fencing recover the backlog. (A lost frontier on a promoted-with-backlog node is the
                // residual edge case that needs crash-safe frontier co-location — out of scope here.)
                relayPendingBootstrap.remove(topic);
            } else if (coordinator.leaderInfo().isPresent()) {
                // Follower with a known leader: pull a fresh snapshot first (replaces local state, so a
                // stale/lost frontier cannot duplicate the non-idempotent queue OFFER). The pending
                // bootstrap is NOT removed here (issue tems#9, D8): it only clears when the snapshot
                // actually INSTALLS (completeSnapshotCutover). Removing it on the mere REQUEST re-armed
                // leadership eligibility and the advertised watermark while the sync could still die
                // silently (e.g. the targeted node stepped down and drops SYNC_REQUEST) — letting a
                // dead-lineage frontier win the reclaim gate at the end of the boot window.
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

        // CONSUME-AND-INSPECT (root-cause fix): drive consumption through peek + a VERIFIED poll, never
        // a readRange(0,N) peek followed by a blind poll(N). Under an active relay TTL
        // (relayExpireAfterWrite), readRange ignores expiry while poll() opportunistically expires the
        // head FIRST — so a poll can silently discard an aged head and consume the NEXT record. The old
        // peek(N)-then-poll(N) pattern thus desynced the apply frontier from the relay head (an entry
        // "applied" in the peek loop was never the entry actually removed), dropping one entry per
        // expired head from an otherwise contiguous relay and surfacing a phantom forward gap. Here we
        // remove the head with pollRecord() and VERIFY the removed sequence is the one we decided on; a
        // mismatch (expiry fired in the peek→poll window) is treated as a forward gap and recovered,
        // never silently absorbed.
        int batch = Math.max(1, config.relayApplyBatchSize());
        long nextExpected = nextExpectedSequenceByTopic.getOrDefault(topic, 1L);
        Exception applyError = null;
        boolean progressed = false;
        for (int i = 0; i < batch; i++) {
            // Peek the REAL (post-expiry) head; do not remove it until we have decided to.
            byte[] headFrame = relay.peek().orElse(null);
            if (headFrame == null) {
                break; // relay drained for now
            }
            RelayEntry entry = RelayEntryCodec.decode(headFrame);

            if (entry.sequence() < nextExpected) {
                // Duplicate/already-applied (re-peek after crash, or a resend copy): drop the real head.
                // Sequence-only dedup is what makes the non-idempotent queue OFFER effectively-once.
                if (!pollExpected(relay, entry.sequence())) {
                    break; // head moved under us (expiry); re-evaluate from the top
                }
                progressed = true;
                continue;
            }

            if (entry.sequence() == nextExpected) {
                try {
                    handler.apply(entry.operationId(), handler.decodePayload(entry.payloadBytes()));
                } catch (Exception e) {
                    // The failing entry stays at the relay head (not yet polled): rethrow so the apply
                    // loop backs off and retries it. Nothing was committed for it.
                    applyError = e;
                    break;
                }
                // Frontier first, THEN remove from the relay: never poll an entry before its apply()
                // returned AND its frontier advance is committed (crash-safety of effectively-once).
                commitRelayBatch(topic, List.of(entry));
                if (!pollExpected(relay, entry.sequence())) {
                    // Expiry fired between our peek and the poll: the head we just applied/committed was
                    // itself aged out and the poll removed a newer entry. The frontier is at entry+1; the
                    // next iteration sees a forward gap and recovers. Stop this batch to re-evaluate.
                    progressed = true;
                    break;
                }
                nextExpected++;
                progressed = true;
                continue;
            }

            // entry.sequence() > nextExpected: a FORWARD GAP at the relay head. In steady state the
            // stream is contiguous, but the relay's write-time TTL (relayExpireAfterWrite) can age an
            // un-applied head entry out from under us, leaving a hole. Recover by role instead of
            // wedging — this is the relay-log-corruption / GTID-auto-position recovery of MySQL.
            recoverFromRelayForwardGap(topic, relay, nextExpected, entry.sequence());
            return true;
        }

        if (applyError != null) {
            throw applyError;
        }
        if (!progressed) {
            // Relay fully drained for this topic — release the failover drain-gate if one is held
            // (no-op otherwise), so a promoted node can finish draining and activate leadership.
            maybeReleaseRelayDrainGate(topic);
        }
        return progressed;
    }

    /**
     * Removes the relay head and confirms it carried {@code expectedSequence}. Returns {@code false}
     * (without having advanced any apply state) when expiry fired in the peek→poll window and a
     * different (newer) record was removed, or the relay drained — letting the caller re-evaluate the
     * real head rather than silently trust a stale peek. This is the atomic guard that closes the
     * readRange↔poll divergence under {@code relayExpireAfterWrite}: {@link NQueue#poll()} expires the
     * aged head prefix FIRST and returns the actual surviving head frame, so a mismatch here is a
     * head-moved-under-us event we surface rather than absorb.
     */
    private boolean pollExpected(NQueue<byte[]> relay, long expectedSequence) throws Exception {
        Optional<byte[]> removed = relay.poll();
        if (removed.isEmpty()) {
            return false;
        }
        long got = RelayEntryCodec.decode(removed.get()).sequence();
        return got == expectedSequence;
    }

    /**
     * Recovers the apply loop from a forward gap at the relay head (a sequence above {@code want} when
     * the entry for {@code want} has been TTL-evicted from the relay). Recovery is by role, mirroring
     * the removed {@code skipEvictedGapAndDrain}/snapshot-bootstrap recovery:
     * <ul>
     * <li><b>Follower</b> (a leader is known): request a fresh snapshot ({@code requestSync}). This
     * re-bootstraps local state cleanly — the only way to recover the lost prefix — and re-anchors the
     * apply frontier and the relay cursor. Re-pulling from the cursor would NOT recover the evicted
     * entry (it is gone from the leader's retained window too), so a bootstrap is mandatory.</li>
     * <li><b>Promoted leader</b> (no peer to bootstrap from): SKIP the gap — advance {@code nextExpected}
     * to the head's sequence and continue draining. The skipped op's effect is re-derived idempotently
     * from the upstream source (e.g. Kafka), so async loss is acceptable; what is NOT acceptable is a
     * permanent wedge that keeps the node in STANDBY and never releases the failover drain-gate.</li>
     * </ul>
     */
    private void recoverFromRelayForwardGap(String topic, NQueue<byte[]> relay, long want, long got)
            throws Exception {
        if (coordinator.isLeader()) {
            // Promoted leader: skip the unrecoverable gap so the drain can finish and leadership activate.
            long skipped = got - want;
            evictedSkipCount.addAndGet(skipped);
            acquireSequenceLock();
            try {
                nextExpectedSequenceByTopic.merge(topic, got, (cur, cand) -> Math.max(cur, cand));
                sequenceStateDirty = true;
            } finally {
                sequenceBufferLock.unlock();
            }
            LOGGER.warning(() -> "Relay forward-gap on promoted leader topic=" + topic + " expected=" + want
                    + " head=" + got + "; skipping " + skipped
                    + " TTL-evicted sequence(s) and continuing drain (state re-derived idempotently upstream)");
            // Do NOT poll the head: it is now == nextExpected and will be applied on the next iteration.
            signalRelay(topic);
            return;
        }
        if (coordinator.leaderInfo().isPresent()) {
            if (config.relayExpireAfterWrite() == null || config.relayExpireAfterWrite().isZero()) {
                // Relay TTL OFF: a "TTL-evicted prefix" is impossible, so the hole is an ingestion
                // artifact (issue tems#9, D7) or legacy on-disk damage. The missing range still lives
                // in the leader's durable binlog, so the clean AND cheap recovery is a cursor re-pull:
                // drop the post-hole suffix (it is re-fetched in order right after) and re-anchor the
                // fetch cursor at the gap. If the leader no longer retains `want`, the next batch
                // answers needSnapshot and the bootstrap path still runs — correct floor, not fallback.
                java.util.concurrent.locks.ReentrantLock ingest = relayIngestLock(topic);
                boolean purged;
                ingest.lock();
                try {
                    purged = purgeRelay(topic);
                    if (purged) {
                        relayStreamCursor(topic).set(want - 1L);
                    }
                } finally {
                    ingest.unlock();
                }
                if (purged) {
                    relayRefetchCount.incrementAndGet();
                    LOGGER.warning(() -> "Relay forward-gap on follower topic=" + topic + " expected="
                            + want + " head=" + got + " with relay TTL off (ingestion hole); re-pulling"
                            + " from the leader binlog at " + want + " instead of snapshot");
                    relayFetchPendingUntilByTopic.put(topic, 0L);
                    signalFetch(topic);
                    return;
                }
                // Purge failed: the old suffix survives at the relay head, so a re-pull would append
                // the re-fetched frames BEHIND it and the apply loop would hit this same gap forever.
                // Fall through to the snapshot bootstrap (install replaces state and the cutover
                // re-anchors the cursor; its own purge failure is tolerated by stale-prefix discard).
                LOGGER.warning(() -> "Relay purge failed during gap re-pull for topic " + topic
                        + "; falling back to snapshot bootstrap");
            }
            // Relay TTL ON (or gap re-pull unavailable): the prefix may genuinely have aged out of the
            // leader's retained window too; the only clean recovery is a snapshot bootstrap. Pause
            // apply until it installs.
            snapshotFallbackCount.incrementAndGet();
            LOGGER.warning(() -> "Relay forward-gap on follower topic=" + topic + " expected=" + want
                    + " head=" + got + "; requesting snapshot bootstrap");
            if (syncingTopics.add(topic)) {
                requestSync(topic);
            }
            return;
        }
        // No leader known yet: leave the head in place and retry on the next tick.
        LOGGER.fine(() -> "Relay forward-gap topic=" + topic + " expected=" + want + " head=" + got
                + " but no leader known yet; deferring recovery");
    }

    private boolean discardStaleRelayPrefix(String topic, NQueue<byte[]> relay) throws Exception {
        long nextExpected = nextExpectedSequenceByTopic.getOrDefault(topic, 1L);
        int stale = 0;
        // Consume-and-inspect: poll the REAL head only while it is a stale duplicate, and VERIFY the
        // removed sequence is the stale one we peeked. A readRange(0,N) peek would ignore TTL expiry and
        // could desync from the subsequent polls; even peek+poll can race (expiry firing in between),
        // so the verified poll guards against discarding a needed (>= nextExpected) entry by accident.
        while (stale < RELAY_STALE_DISCARD_BATCH) {
            byte[] frame = relay.peek().orElse(null);
            if (frame == null) {
                break;
            }
            RelayEntry entry = RelayEntryCodec.decode(frame);
            // Sequence-only dedup (no epoch-magnitude fence): an earlier-epoch entry from the agreed
            // leader (after a convergence re-stamp) is legitimate and must not be skipped.
            if (entry.sequence() >= nextExpected) {
                break;
            }
            if (!pollExpected(relay, entry.sequence())) {
                break; // expiry advanced the head past the stale entry; re-evaluate the real head
            }
            stale++;
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
            // Issue tems#9, D9: NEVER refuse silently. A follower whose adopted leader refuses
            // leadership (a mutual-deferral stalemate) would starve forever waiting for a stream only
            // a leader serves — answer with leaderUnavailable so the requester's coordinator learns
            // the refusal and can break the stalemate (the ahead node takes over).
            RelayStreamFetchPayload request = message.payload(RelayStreamFetchPayload.class);
            long now = System.currentTimeMillis();
            if (now - lastFetchRefusalLogMs > 10_000L) {
                lastFetchRefusalLogMs = now;
                LOGGER.warning(() -> "Refusing RELAY_STREAM_FETCH from " + message.source()
                        + " (topic=" + request.topic() + "): this node is not the leader");
            }
            transport.send(ClusterMessage.request(MessageType.RELAY_STREAM_BATCH, "stream",
                    transport.local().nodeId(), message.source(),
                    new RelayStreamBatchPayload(request.topic(), request.fromSequence(), List.of(),
                            -1L, -1L, false, true)));
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
     * Forward-gap recoveries resolved by a cursor re-pull from the leader binlog instead of a snapshot
     * bootstrap (issue tems#9, D7) — the path taken when the relay TTL is off, where a hole can only be
     * an ingestion artifact (or legacy on-disk damage) and the missing range is still durably retained
     * by the leader. A growing value with a flat {@link #getSnapshotFallbackCount()} means gaps are
     * being healed cheaply by the stream itself.
     *
     * @return the cumulative count of gap recoveries via binlog re-pull
     */
    public long getRelayRefetchCount() {
        return relayRefetchCount.get();
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
     * Number of relay sequences a PROMOTED LEADER has skipped past because the relay's write-time TTL
     * ({@link ReplicationConfig#relayExpireAfterWrite()}) aged an un-applied head entry out from under
     * the apply consumer, opening a forward gap the promoted node cannot bootstrap away (no peer). The
     * node skips the gap to finish draining and activate leadership; the skipped state is re-derived
     * idempotently from the upstream source. A non-zero, growing value signals async loss for those
     * sequences (recovered to eventual consistency). Stays {@code 0} for a follower (which recovers a
     * relay gap cleanly via snapshot bootstrap) and in steady state.
     *
     * @return total relay sequences skipped on promotion to recover a TTL-evicted forward gap
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
                // Handoff observed (issue tems#9, D10b): the reclaim-quiesce did its job — the
                // candidate paired up and took over. Resume production as a follower (forwarding).
                clearReclaimQuiesce();
            }
            failAllPending("Lost leadership to " + newLeader);
            return;
        }
        leaderSyncTopics.clear();
        leaderSyncTopics.addAll(handlers.keySet());
        leaderSyncing.set(!leaderSyncTopics.isEmpty());
        // Abandon any in-flight FOLLOWER-side bootstrap on promotion. A topic can be parked in
        // syncingTopics because, while still a follower, a relay forward-gap (TTL-evicted prefix)
        // requested a snapshot that the now-dead leader never served. A promoted node has no peer to
        // bootstrap from, and drainRelayOnce pauses apply while syncingTopics/relayPendingBootstrap hold
        // the topic — so without clearing them the relay never drains, the drain-gate never releases,
        // and the node wedges in STANDBY forever. Clearing here lets the apply loop drain its own relay
        // and recover any residual forward-gap via the promoted-leader skip path.
        syncingTopics.clear();
        relayPendingBootstrap.clear();
        // A promotion supersedes any in-flight dual-leader yield (issue tems#9, D10c): the rival
        // died mid-yield and this node is the only lineage left — nothing to resync from.
        dualLeaderYielding = false;
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
            for (NodeId member : current) {
                if (member.equals(localId) || knownActiveMembers.contains(member)) {
                    continue; // only newly-joined members
                }
                // A rejoining node's progress from its PREVIOUS session says nothing about the new
                // one (issue tems#9, D10a) — and a kill -9 + fast reconnect may race the disconnect
                // cleanup. Drop any stale entry and hold the gate until a fresh post-join report.
                followerAppliedByNode.remove(member);
                quiescingFor.add(member);
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
        // Issue tems#9, D10a: a bootstrap-gated follower advertises -1 (no usable frontier) and a
        // follower still tracking another lineage's epoch reports progress on an incomparable
        // scale — releasing the quiesce against either value reopens the 1.5s-release hole.
        if (payload.appliedSequence() < 0 || payload.epoch() != coordinator.getLeaderEpoch()) {
            return;
        }
        followerAppliedByNode.put(source,
                new FollowerProgress(payload.appliedSequence(), payload.epoch(), System.currentTimeMillis()));
        if (quiescingFor.contains(source)) {
            if (payload.appliedSequence() >= leaderQuiesceTarget() - config.joinSyncLagThreshold()) {
                quiescingFor.remove(source);
                maybeReleaseJoinQuiesce();
            }
        }
        if (reclaimQuiescing.get() && source.equals(reclaimQuiesceFor)
                && payload.appliedSequence() >= leaderQuiesceTarget()) {
            // The candidate paired up exactly against the frozen watermark (issue tems#9, D10b):
            // surface it to the coordinator NOW — waiting for the candidate's next heartbeat (up to
            // one interval stale) would stretch the pause for nothing. Gate B opens, recompute hands
            // leadership to the candidate, and onLeaderChanged releases this quiesce.
            coordinator.noteFollowerWatermark(source, payload.appliedSequence());
        } else if (config.leaderPauseOnReclaim() && !reclaimQuiescing.get()) {
            // Fresh progress is the approach signal — engaging here beats the 200ms tick.
            maybeEngageReclaimQuiesce();
        }
    }

    /** Follower-side: periodically report apply progress so the leader can release its join-quiesce. */
    private void sendFollowerProgress() {
        if (!running || coordinator.isLeader()) {
            return;
        }
        coordinator.leaderInfo().ifPresent(leader -> {
            // Report the advertised watermark (issue tems#9, D10a): -1 while the bootstrap gate is
            // engaged, so the leader never mistakes a pre-bootstrap frontier for catch-up progress.
            FollowerProgressPayload payload = new FollowerProgressPayload(advertisedLeaderHighWatermark(),
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
        // Drop joiners that have since caught up — judged only on a FRESH report from the current
        // epoch (issue tems#9, D10a): a stale entry must never release the gate.
        long target = leaderQuiesceTarget();
        long threshold = config.joinSyncLagThreshold();
        long freshnessMs = followerProgressFreshnessMs();
        long now = System.currentTimeMillis();
        long currentEpoch = coordinator.getLeaderEpoch();
        quiescingFor.removeIf(node -> {
            FollowerProgress progress = followerAppliedByNode.get(node);
            return progress != null
                    && progress.epoch() == currentEpoch
                    && now - progress.atMillis() <= freshnessMs
                    && progress.applied() >= target - threshold;
        });
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

    /**
     * Leader-side catch-up target for the quiesce gates (issue tems#9, D10a): the same scale the
     * leader advertises in heartbeats. {@code globalSequence} alone understates the frontier of a
     * leader promoted without a snapshot cutover (it is a local production counter), which let the
     * join-quiesce release against a target the joiner had trivially passed (1.5s release while the
     * real catch-up took 64s).
     */
    private long leaderQuiesceTarget() {
        return Math.max(getGlobalSequence(), getLastAppliedSequence());
    }

    /** Max age for a follower progress report to count toward releasing a quiesce gate. */
    private long followerProgressFreshnessMs() {
        return Math.max(50L, config.followerProgressInterval().toMillis()) * 3L;
    }

    /** A follower's apply progress as last reported (issue tems#9, D10a). */
    private record FollowerProgress(long applied, long epoch, long atMillis) {
    }

    // ── Quiesce-assisted reclaim — "leader pause on reclaim" (issue tems#9, D10b) ───────────────

    /** True while the leader is pausing production for an approaching reclaim candidate. */
    public boolean isReclaimQuiescing() {
        return reclaimQuiescing.get();
    }

    /**
     * Periodic guard of the reclaim-quiesce: bounds the pause ({@code reclaimQuiesceMaxDuration}),
     * aborts when the candidate leaves, releases when leadership already changed hands, and
     * re-evaluates engagement otherwise.
     */
    private void checkReclaimQuiesce() {
        if (!running || !config.leaderPauseOnReclaim()) {
            return;
        }
        if (!reclaimQuiescing.get()) {
            maybeEngageReclaimQuiesce();
            return;
        }
        if (!coordinator.isLeader()) {
            // Handoff completed (or leadership otherwise changed) — onLeaderChanged also clears;
            // this is the belt for a missed listener tick.
            clearReclaimQuiesce();
            return;
        }
        NodeId candidate = reclaimQuiesceFor;
        if (System.currentTimeMillis() - reclaimQuiesceStartedMs > config.reclaimQuiesceMaxDuration().toMillis()) {
            LOGGER.warning(() -> "Reclaim-quiesce exceeded max duration; resuming production and"
                    + " retaining leadership (candidate " + candidate + " did not pair up in time)");
            abortReclaimQuiesce();
            return;
        }
        boolean candidateActive = candidate != null && coordinator.activeMembers().stream()
                .anyMatch(m -> candidate.equals(m.nodeId()));
        if (!candidateActive) {
            LOGGER.warning(() -> "Reclaim-quiesce candidate " + candidate
                    + " left the membership; resuming production and retaining leadership");
            abortReclaimQuiesce();
        }
    }

    /**
     * Engages the reclaim-quiesce when a HIGHER-affinity candidate (election order: priority, then
     * NodeId) has fresh, comparable progress within {@code reclaimQuiesceThreshold} of the local
     * watermark. Never engages for a lower-affinity peer (there is no handoff to assist), while the
     * join-quiesce is already pausing production, or during the post-abort cooldown.
     */
    private void maybeEngageReclaimQuiesce() {
        if (!running || !config.leaderPauseOnReclaim() || !coordinator.isLeader()
                || reclaimQuiescing.get() || isJoinQuiescing()
                || System.currentTimeMillis() < reclaimQuiesceCooldownUntilMs) {
            return;
        }
        NodeInfo local = transport.local();
        long target = leaderQuiesceTarget();
        long freshnessMs = followerProgressFreshnessMs();
        long now = System.currentTimeMillis();
        long currentEpoch = coordinator.getLeaderEpoch();
        for (NodeInfo member : coordinator.activeMembers()) {
            if (member.nodeId().equals(local.nodeId()) || member.port() <= 0
                    || !hasHigherAffinity(member, local)) {
                continue;
            }
            FollowerProgress progress = followerAppliedByNode.get(member.nodeId());
            if (progress == null || progress.applied() < 0 || progress.epoch() != currentEpoch
                    || now - progress.atMillis() > freshnessMs) {
                continue; // no fresh, comparable report — not an approach signal
            }
            long delta = target - progress.applied();
            if (delta >= 0 && delta <= config.reclaimQuiesceThreshold()) {
                reclaimQuiesceFor = member.nodeId();
                reclaimQuiesceStartedMs = now;
                if (reclaimQuiescing.compareAndSet(false, true)) {
                    LOGGER.info(() -> "Reclaim-quiesce engaged: pausing production so higher-affinity"
                            + " candidate " + member.nodeId() + " can pair up (delta=" + delta
                            + ", issue tems#9, D10b)");
                }
                return;
            }
        }
    }

    /** Election-order affinity comparison: higher priority wins; NodeId breaks ties. */
    private static boolean hasHigherAffinity(NodeInfo candidate, NodeInfo reference) {
        if (candidate.priority() != reference.priority()) {
            return candidate.priority() > reference.priority();
        }
        return candidate.nodeId().compareTo(reference.nodeId()) > 0;
    }

    /**
     * Losing side of a dual-leader resolution (issue tems#9, D10c), invoked by the coordinator
     * BEFORE the demotion: everything this node produced during the dual window belongs to a
     * DIVERGENT lineage, so every registered topic is marked for bootstrap resync. The existing D8
     * machinery does the rest once the node is a follower of the winner — {@code drainRelayOnce}
     * requests the snapshot, {@code completeSnapshotCutover} installs it (resetState + counter SET +
     * local binlog truncate), and the bootstrap gate keeps the node non-advertisable/ineligible
     * until the install lands. The dual-window tail is discarded by design — the same outcome the
     * operational containment (wipe + cold bootstrap) produced manually, now automatic and bounded.
     */
    private void onDualLeaderYield() {
        if (!isStreamMode()) {
            return;
        }
        dualLeaderYielding = true;
        for (String topic : handlers.keySet()) {
            relayPendingBootstrap.add(topic);
        }
        LOGGER.warning(() -> "Dual-leader yield: topics " + handlers.keySet() + " marked for"
                + " bootstrap resync; the locally produced dual-window tail will be discarded"
                + " (issue tems#9, D10c)");
    }

    /** Abort without handoff: release the pause and arm the re-engagement cooldown. */
    private void abortReclaimQuiesce() {
        clearReclaimQuiesce();
        reclaimQuiesceCooldownUntilMs = System.currentTimeMillis() + config.reclaimQuiesceCooldown().toMillis();
    }

    private void clearReclaimQuiesce() {
        reclaimQuiesceFor = null;
        if (reclaimQuiescing.compareAndSet(true, false)) {
            LOGGER.info(() -> "Reclaim-quiesce released; resuming production.");
        }
    }

    // ── Orchestrated affinity handback (issue tems#9, D11) ────────────────────────────────────────

    /** Registers a listener for the orchestrated handback choreography (idempotent). */
    public void addHandoverListener(HandoverListener listener) {
        handoverListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /** True while this (interim leader) node has frozen production for an in-flight handback. */
    public boolean isHandoverFreezing() {
        return handoverFreezing.get();
    }

    /**
     * CANDIDATE side (scheduled): when this node is the returning highest-affinity follower and a
     * healthy incumbent exists, request an orchestrated handover instead of reclaiming via the
     * lineage-blind watermark gates. Engages at most one handback at a time, after any abort cooldown.
     */
    private void maybeInitiateHandback() {
        if (!running || !config.affinityHandbackMode() || !isStreamMode()) {
            return;
        }
        if (handbackRole.get() != HandbackRole.NONE
                || System.currentTimeMillis() < handbackCooldownUntilMs) {
            return;
        }
        // Must be a STABLE follower: not leader, not bootstrapping, not mid-sync, with handlers ready.
        if (coordinator.isLeader() || bootstrapGateEngaged() || isLeaderSyncing()
                || !relayPendingBootstrap.isEmpty() || handlers.isEmpty()) {
            return;
        }
        if (!coordinator.localIsPreferredLeader() || !coordinator.isAgreedLeaderHealthy()) {
            return; // not the preferred node, or no healthy distinct leader to hand back FROM
        }
        coordinator.leaderInfo().ifPresent(leader -> {
            NodeId localId = transport.local().nodeId();
            if (leader.nodeId().equals(localId)) {
                return;
            }
            if (!handbackRole.compareAndSet(HandbackRole.NONE, HandbackRole.CANDIDATE_REQUESTING)) {
                return;
            }
            handbackPeer = leader.nodeId();
            handbackStartedMs = System.currentTimeMillis();
            LOGGER.info(() -> "Affinity handback: requesting orchestrated handover from leader "
                    + leader.nodeId() + " (issue tems#9, D11)");
            transport.send(ClusterMessage.request(MessageType.HANDBACK_REQUEST, "handback", localId,
                    leader.nodeId(), new HandbackRequestPayload(localId, getLastAppliedSequence(),
                            coordinator.getTrackedLeaderHighWatermark())));
        });
    }

    /** INTERIM-LEADER side: accept a handback request — freeze production, then flush + grant async. */
    private void handleHandbackRequest(ClusterMessage message) {
        NodeId candidate = message.source();
        if (!config.affinityHandbackMode() || !isStreamMode() || !coordinator.isLeader()) {
            sendHandbackAbort(candidate, "not an eligible leader");
            return;
        }
        if (System.currentTimeMillis() < handbackCooldownUntilMs) {
            sendHandbackAbort(candidate, "handback cooling down");
            return;
        }
        NodeInfo local = transport.local();
        NodeInfo candidateInfo = coordinator.activeMembers().stream()
                .filter(m -> m.nodeId().equals(candidate)).findFirst().orElse(null);
        if (candidateInfo == null || !hasHigherAffinity(candidateInfo, local)) {
            sendHandbackAbort(candidate, "candidate is not higher-affinity");
            return;
        }
        if (!handbackRole.compareAndSet(HandbackRole.NONE, HandbackRole.LEADER_PREPARING)) {
            sendHandbackAbort(candidate, "handback already in progress");
            return;
        }
        handbackPeer = candidate;
        handbackStartedMs = System.currentTimeMillis();
        // Stop-the-world: freeze production immediately. replicate() now rejects, so nothing gets a
        // sequence above the watermark we capture after the app drains.
        handoverFreezing.set(true);
        LOGGER.info(() -> "Affinity handback: PREP for candidate " + candidate
                + "; production frozen (issue tems#9, D11)");
        // Flush + grant off the transport thread: onHandoverPrepare may block (bounded) while the app
        // drains its in-flight pipe, and that blocking IS the "frozen, snapshot-ready" signal.
        try {
            executor.submit(() -> prepareAndGrantHandback(candidate));
        } catch (java.util.concurrent.RejectedExecutionException e) {
            abortHandover("executor unavailable for PREP");
        }
    }

    private void prepareAndGrantHandback(NodeId candidate) {
        if (handbackRole.get() != HandbackRole.LEADER_PREPARING || !candidate.equals(handbackPeer)) {
            return; // aborted/superseded while queued
        }
        try {
            for (HandoverListener l : handoverListeners) {
                l.onHandoverPrepare();
            }
        } catch (Throwable t) {
            LOGGER.log(Level.WARNING, "Handback PREP flush failed; aborting handover", t);
            abortHandover("prep flush failed");
            return;
        }
        if (handbackRole.get() != HandbackRole.LEADER_PREPARING) {
            return; // aborted during the (possibly long) flush
        }
        long frozen = leaderQuiesceTarget();
        handoverFrozenWatermark = frozen;
        handbackRole.set(HandbackRole.LEADER_SERVING);
        String topic = primaryTopic();
        LOGGER.info(() -> "Affinity handback: granting handover to " + candidate + " at W=" + frozen
                + " (issue tems#9, D11)");
        transport.send(ClusterMessage.request(MessageType.HANDBACK_GRANT, "handback",
                transport.local().nodeId(), candidate,
                new HandbackGrantPayload(topic, frozen, coordinator.getLeaderEpoch())));
    }

    /** CANDIDATE side: handover granted — arm bootstrap for all topics and pull the full snapshot. */
    private void handleHandbackGrant(ClusterMessage message) {
        HandbackGrantPayload payload = message.payload(HandbackGrantPayload.class);
        if (handbackRole.get() != HandbackRole.CANDIDATE_REQUESTING
                || !message.source().equals(handbackPeer)) {
            return;
        }
        if (!handbackRole.compareAndSet(HandbackRole.CANDIDATE_REQUESTING, HandbackRole.CANDIDATE_INSTALLING)) {
            return;
        }
        handbackGrantedEpoch = payload.leaderEpoch();
        if (handlers.isEmpty()) {
            sendHandbackAbort(message.source(), "candidate has no topics");
            clearCandidateHandback("no topics", true);
            return;
        }
        LOGGER.info(() -> "Affinity handback: GRANT received (frozenWatermark=" + payload.frozenWatermark()
                + ", leaderEpoch=" + payload.leaderEpoch() + "); installing full snapshot (issue tems#9, D11)");
        // Arm bootstrap for every topic we follow — the same machinery as the dual-leader yield: the
        // apply loop requests the snapshot, completeSnapshotCutover installs it (SET — lineage offset
        // zeroed), and the stale-sync guard is skipped while the bootstrap is pending so the incumbent's
        // snapshot replaces our (possibly numerically higher) stale frontier. When the last topic cuts
        // over we assert leadership (completeSnapshotCutover tail → completeHandbackAsCandidate).
        for (String topic : handlers.keySet()) {
            syncingTopics.remove(topic);
            relayPendingBootstrap.add(topic);
            signalRelay(topic);
        }
        coordinator.reevaluateLeadership();
    }

    /** CANDIDATE side: snapshot installed and cut over — assert leadership above the granted epoch. */
    private void completeHandbackAsCandidate(long watermark) {
        long grantedEpoch = handbackGrantedEpoch;
        NodeId interimLeader = handbackPeer;
        handbackRole.set(HandbackRole.NONE);
        handbackPeer = null;
        LOGGER.info(() -> "Affinity handback: cutover complete at " + watermark
                + "; asserting leadership above epoch " + grantedEpoch + " (issue tems#9, D11)");
        long newEpoch = coordinator.assumeLeadershipForHandback(grantedEpoch);
        if (interimLeader != null) {
            transport.send(ClusterMessage.request(MessageType.HANDBACK_COMPLETE, "handback",
                    transport.local().nodeId(), interimLeader,
                    new HandbackCompletePayload(watermark, newEpoch)));
        }
        for (HandoverListener l : handoverListeners) {
            try {
                l.onPromotionComplete();
            } catch (Throwable t) {
                LOGGER.log(Level.WARNING, "onPromotionComplete listener failed", t);
            }
        }
    }

    /** INTERIM-LEADER side: candidate finished cutover and asserted leadership — demote cleanly. */
    private void handleHandbackComplete(ClusterMessage message) {
        HandbackCompletePayload payload = message.payload(HandbackCompletePayload.class);
        if (handbackRole.get() != HandbackRole.LEADER_SERVING
                || !message.source().equals(handbackPeer)) {
            return;
        }
        NodeId newLeader = message.source();
        LOGGER.info(() -> "Affinity handback: candidate " + newLeader + " completed cutover at "
                + payload.cutoverWatermark() + " (epoch " + payload.newEpoch() + "); demoting (issue tems#9, D11)");
        handoverFreezing.set(false);
        handoverFrozenWatermark = -1L;
        handbackRole.set(HandbackRole.NONE);
        handbackPeer = null;
        // Step down to the winner promptly; the candidate's higher-epoch heartbeats are the backstop.
        coordinator.acceptHandbackWinner(newLeader, payload.newEpoch());
        for (HandoverListener l : handoverListeners) {
            try {
                l.onDemotedAfterHandover(newLeader);
            } catch (Throwable t) {
                LOGGER.log(Level.WARNING, "onDemotedAfterHandover listener failed", t);
            }
        }
    }

    /**
     * Periodic guard of an in-flight handback (issue tems#9, D11): bounds the interim leader's freeze
     * (timeout / candidate gone → abort and RETAIN leadership) and the candidate's wait (timeout →
     * abandon and stay follower). Never leaves a leader stuck-frozen or a half-handover dangling.
     */
    private void checkHandover() {
        if (!running || !config.affinityHandbackMode()) {
            return;
        }
        HandbackRole role = handbackRole.get();
        if (role == HandbackRole.NONE) {
            return;
        }
        long now = System.currentTimeMillis();
        if (role == HandbackRole.LEADER_PREPARING || role == HandbackRole.LEADER_SERVING) {
            NodeId candidate = handbackPeer;
            if (now - handbackStartedMs > config.handoverMaxDuration().toMillis()) {
                LOGGER.warning(() -> "Affinity handback exceeded max duration; aborting and retaining"
                        + " leadership (candidate " + candidate + " did not complete in time)");
                abortHandover("handover timed out");
                return;
            }
            boolean candidateActive = candidate != null && coordinator.activeMembers().stream()
                    .anyMatch(m -> candidate.equals(m.nodeId()));
            if (!candidateActive) {
                LOGGER.warning(() -> "Affinity handback candidate " + candidate
                        + " left the membership; aborting and retaining leadership");
                abortHandover("candidate left");
            }
        } else { // CANDIDATE_REQUESTING / CANDIDATE_INSTALLING
            long bound = role == HandbackRole.CANDIDATE_REQUESTING
                    ? config.handoverRequestTimeout().toMillis()
                    : config.handoverSnapshotTimeout().toMillis();
            if (now - handbackStartedMs > bound) {
                NodeId peer = handbackPeer;
                LOGGER.warning(() -> "Affinity handback (candidate) timed out in " + role
                        + "; abandoning attempt and staying follower (issue tems#9, D11)");
                // Tell the incumbent so it un-freezes promptly. If we already armed the bootstrap
                // (INSTALLING), leave it: the normal path still installs the incumbent's snapshot, and
                // with the role cleared completeSnapshotCutover will not assert leadership.
                if (peer != null) {
                    sendHandbackAbort(peer, "candidate timed out");
                }
                clearCandidateHandback("candidate-side timeout", true);
            }
        }
    }

    /** INTERIM-LEADER abort: un-freeze, resume production, retain leadership, arm cooldown, notify peer. */
    private void abortHandover(String reason) {
        finishHandoverAbort(reason, true);
    }

    private void finishHandoverAbort(String reason, boolean notifyPeer) {
        NodeId peer = handbackPeer;
        boolean wasFreezing = handoverFreezing.getAndSet(false);
        handoverFrozenWatermark = -1L;
        handbackRole.set(HandbackRole.NONE);
        handbackPeer = null;
        handbackCooldownUntilMs = System.currentTimeMillis() + config.handoverCooldown().toMillis();
        if (notifyPeer && peer != null) {
            sendHandbackAbort(peer, reason);
        }
        if (wasFreezing) {
            for (HandoverListener l : handoverListeners) {
                try {
                    l.onHandoverAborted(reason);
                } catch (Throwable t) {
                    LOGGER.log(Level.WARNING, "onHandoverAborted listener failed", t);
                }
            }
        }
    }

    private void clearCandidateHandback(String reason, boolean cooldown) {
        handbackRole.set(HandbackRole.NONE);
        handbackPeer = null;
        if (cooldown) {
            handbackCooldownUntilMs = System.currentTimeMillis() + config.handoverCooldown().toMillis();
        }
        LOGGER.fine(() -> "Affinity handback (candidate) cleared: " + reason);
    }

    private void sendHandbackAbort(NodeId target, String reason) {
        transport.send(ClusterMessage.request(MessageType.HANDBACK_ABORT, "handback",
                transport.local().nodeId(), target, new HandbackAbortPayload(reason)));
    }

    private void handleHandbackAbort(ClusterMessage message) {
        HandbackAbortPayload payload = message.payload(HandbackAbortPayload.class);
        HandbackRole role = handbackRole.get();
        if (role == HandbackRole.NONE) {
            return;
        }
        String reason = payload.reason();
        LOGGER.warning(() -> "Affinity handback aborted by peer " + message.source() + ": " + reason
                + " (issue tems#9, D11)");
        if (role == HandbackRole.LEADER_PREPARING || role == HandbackRole.LEADER_SERVING) {
            finishHandoverAbort("aborted by candidate: " + reason, false); // do not echo an abort back
        } else {
            clearCandidateHandback("aborted by leader", true);
        }
    }

    private String primaryTopic() {
        return handlers.keySet().stream().findFirst().orElse("");
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
            loaded.forEach((topic, nextExpected) -> {
                if (!isSyntheticSequenceStateKey(topic)) {
                    nextExpectedSequenceByTopic.put(topic, nextExpected);
                }
            });

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

    private static boolean isSyntheticSequenceStateKey(String key) {
        return "_global".equals(key) || key.startsWith("_topic:");
    }

    /**
     * Scheduled flush of the coalesced sequence state. Runs OFF the replication lock; reads only
     * thread-safe concurrent structures. Clears the dirty flag before writing so a concurrent
     * dirty-mark during the write re-arms it for the next flush (no lost update).
     */
    private boolean flushSequenceStateIfDirty() {
        if (!sequenceStateDirty) {
            return true;
        }
        sequenceStateDirty = false;
        boolean saved = saveSequenceState();
        if (!saved) {
            sequenceStateDirty = true; // re-arm so the next periodic flush retries
        }
        return saved;
    }

    /**
     * Saves the current sequence state to disk.
     * Saves:
     * - nextExpectedSequenceByTopic (for followers)
     * - globalSequence (for leader, stored as "_global" key)
     * - sequenceByTopic (for leader, stored as "_topic:{topic}" keys)
     */
    private boolean saveSequenceState() {
        if (sequenceStatePath == null) {
            return true; // Test mode, no persistence
        }
        try {
            Files.createDirectories(sequenceStatePath.getParent());
            Map<String, Long> toSave = new HashMap<>(nextExpectedSequenceByTopic);

            // Add global sequence
            toSave.put("_global", globalSequence.get());

            // Add per-topic sequences
            sequenceByTopic.forEach((topic, seq) -> toSave.put("_topic:" + topic, seq.get()));

            // Write-to-temp + atomic move (issue tems#9, D7/F3): the periodic flush (1s scheduler) and
            // the final flush in stop() may race on this path, and a crash mid-write must never leave a
            // torn sequence-state behind a clean-shutdown marker.
            Path tmp = sequenceStatePath.resolveSibling(sequenceStatePath.getFileName() + ".tmp");
            try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(
                    Files.newOutputStream(tmp))) {
                oos.writeObject(toSave);
            }
            try {
                Files.move(tmp, sequenceStatePath, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(tmp, sequenceStatePath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            }
            return true;
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to save sequence state", e);
            return false;
        }
    }
}
