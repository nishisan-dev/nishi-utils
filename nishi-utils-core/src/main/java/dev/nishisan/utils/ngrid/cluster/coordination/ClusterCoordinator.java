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

package dev.nishisan.utils.ngrid.cluster.coordination;

import dev.nishisan.utils.ngrid.LeaderElectionListener;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.TopicFrontiers;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Maintains the cluster membership and performs a deterministic leader election
 * based on
 * the highest active {@link NodeId}. The coordinator also emits heartbeat
 * messages using
 * the underlying {@link Transport}.
 */
public final class ClusterCoordinator implements TransportListener, Closeable {
    private static final Logger LOGGER = Logger.getLogger(ClusterCoordinator.class.getName());

    /**
     * Multiplier applied to {@code heartbeatTimeout} that bounds how long a member whose
     * heartbeat is overdue but which is still transport-reachable (possibly only via a
     * proxy) is kept active before eviction. This grace window avoids spurious quorum loss
     * during a brief direct-link flap. Trade-off: counting proxy-only reachability keeps the
     * relaying hub as a partial SPOF, so the window is intentionally bounded — a genuinely
     * dead peer (no route at all, or overdue beyond the window) is still evicted.
     */
    private static final long PROXY_REACHABLE_GRACE_FACTOR = 2;

    private final Transport transport;
    private final ClusterCoordinatorConfig config;
    private final Map<NodeId, ClusterMember> members = new ConcurrentHashMap<>();
    private final Set<LeadershipListener> leadershipListeners = new CopyOnWriteArraySet<>();
    private final Set<LeaderElectionListener> leaderElectionListeners = new CopyOnWriteArraySet<>();
    private final Set<MembershipListener> membershipListeners = new CopyOnWriteArraySet<>();
    private final ScheduledExecutorService scheduler;
    private final AtomicReference<NodeId> leader = new AtomicReference<>();
    private final AtomicLong leaderEpoch = new AtomicLong(0);
    private volatile java.util.function.LongSupplier leaderHighWatermarkSupplier = () -> -1L;
    /** Per-topic applied frontier vector advertised in heartbeats (issue #178); empty = no vector. */
    private volatile java.util.function.Supplier<Map<String, Long>> topicFrontiersSupplier = Map::of;
    /** Topic precedence for the incomparable-vector tie-break (issue #178); empty = name order. */
    private volatile java.util.List<String> topicPriority = java.util.List.of();
    private volatile java.util.function.BooleanSupplier localLeadershipEligibilitySupplier = () -> true;
    private volatile long trackedLeaderHighWatermark = -1L;
    private volatile long trackedLeaderEpoch = 0L;
    private volatile Instant leaseExpiresAt = Instant.MIN;
    private final Path epochPath;
    /** Epoch-millis until which a non-preferred node defers self-election; {@code 0} = no deferral. */
    private volatile long bootDiscoveryDeadlineMs = 0L;

    // ── Sync-before-reclaim by WATERMARK GAP (leader-sync-before-reclaim) ─────────────────────────────
    // The reclaim/step-down gate is driven by the replication HIGH-WATERMARK, not by observing who
    // asserts leadership. Each heartbeat already carries the sender's watermark (leaderHighWatermark =
    // isLeader()? globalSeq : lastApplied), so every node learns every peer's progress. A returning
    // higher-affinity node only takes leadership once its own applied frontier has reached the cluster's
    // highest peer watermark (it has synced the newer state) or the boot-discovery window lapses with no
    // peer ahead (lead-while-alone / AP). Symmetrically, the incumbent does NOT step down to a
    // higher-affinity peer that is still BEHIND its watermark — closing the handoff race on both sides.

    /** Per-peer last-known replication high-watermark, learned from heartbeats. -1 until first heard. */
    private final Map<NodeId, Long> peerHighWatermark = new ConcurrentHashMap<>();
    /**
     * Per-peer send timestamp ({@code HeartbeatPayload.epochMilli}) of the newest heartbeat applied
     * (revisão #178, A3). Heartbeats are dispatched one virtual thread each, so two from the same peer
     * can be applied out of order; an older one must not overwrite the newer assertion/watermark/epoch
     * (a demoted node's {@code leader=false} announcement lost to a periodic {@code leader=true}
     * heartbeat re-adopted it for a whole interval). A regression larger than the heartbeat timeout is
     * a sender clock jump, not a reorder, and is accepted.
     */
    private final Map<NodeId, Long> lastHeartbeatStampMs = new ConcurrentHashMap<>();
    /**
     * Voters that announced their departure (LEAVE) and whose heartbeats are ignored until they
     * reconnect (a new incarnation handshakes → {@link #onPeerConnected}) or the heartbeat timeout
     * elapses (revisão #178, A4): a heartbeat read before the LEAVE but dispatched after it must not
     * reactivate the member and re-adopt it as leader, delaying the failover by a full eviction cycle.
     */
    private final Map<NodeId, Long> leavingUntilMs = new ConcurrentHashMap<>();
    /**
     * Per-peer last-known applied frontier PER TOPIC, learned from heartbeats (issue #178). Absent or
     * empty for peers that advertise no vector (older version, or bootstrap gate engaged): every gate
     * then falls back to the scalar {@link #peerHighWatermark} for that peer.
     */
    private final Map<NodeId, TopicFrontiers> peerTopicFrontiers = new ConcurrentHashMap<>();
    // Last time each peer REFUSED to serve the stream as a non-leader (issue tems#9, D9): fed by the
    // replication layer when a RELAY_STREAM_FETCH addressed to our adopted leader comes back with
    // leaderUnavailable. A recent refusal from the affinity-elected node is the stalemate signal that
    // lets the ahead node take over instead of deferring forever.
    private final Map<NodeId, Long> leaderRefusalAtMs = new ConcurrentHashMap<>();
    // Whether each peer's LATEST heartbeat asserted leadership. A refusal is only a stalemate signal
    // while the refusing node is still not leading: the moment its heartbeat asserts leadership the
    // refusal is stale (it was recorded while the node was deferring, e.g. during the boot dance) and
    // must not arm the D9 escape — otherwise any recompute within heartbeatTimeout of that stale
    // refusal, combined with the normal ≤ one-heartbeat watermark skew, promotes a follower into a
    // genuine dual-leader.
    private final Map<NodeId, Boolean> peerAssertsLeadership = new ConcurrentHashMap<>();
    // Rate-limit for the leader-behind-own-follower warning (issue tems#9, D9).
    private volatile long lastLeaderBehindWarnMs = 0L;

    // ── Dual-leader detection + deterministic resolution (issue tems#9, D10c) ──────────────────
    // Consecutive heartbeats in which a peer asserted leadership WHILE we are leader, per peer.
    // Resolution fires only after DUAL_LEADER_OBSERVATIONS_TO_RESOLVE consecutive observations: the
    // overlap window of a legitimate handoff (both sides answering isLeader for a sub-heartbeat
    // instant) never reaches the debounce, while a genuine stable dual-leader does in ~3 heartbeat
    // intervals. Any heartbeat from the peer WITHOUT the flag zeroes its count.
    private final Map<NodeId, Integer> dualLeaderObservations = new ConcurrentHashMap<>();
    private static final int DUAL_LEADER_OBSERVATIONS_TO_RESOLVE = 3;
    // True while this (leader) node has observed a higher-affinity rival leader and will yield once
    // the debounce completes: suppresses the epoch re-stamp ("above observed") so the losing side
    // stops feeding the epoch ladder from the very first observation.
    private volatile boolean yieldingToDualLeader = false;
    // Hook wired by the ReplicationManager, invoked BEFORE the demotion when this node yields a
    // dual-leader: arms the bootstrap resync that discards the dual-window tail (divergent lineage).
    private volatile Runnable dualLeaderYieldHook;
    // Rate-limit for the winner-side dual-leader warning.
    private volatile long lastDualLeaderRetainWarnMs = 0L;

    // ── Orchestrated affinity handback (issue tems#9, D11) ─────────────────────────────────────
    // When enabled, a returning highest-affinity FOLLOWER does NOT reclaim leadership via the
    // watermark gates (a lineage-blind counter offset can defeat them); the ReplicationManager drives
    // an explicit stop-the-world snapshot handover instead. Genuine-failover election is unchanged.
    private volatile boolean affinityHandbackMode = false;
    // True while THIS node has an in-flight handover (interim leader serving, or candidate installing),
    // supplied by the ReplicationManager. Suppresses the epoch re-stamp and the dual-leader contest so
    // the choreographed transition lands as a single clean leader change; the dual-leader resolver
    // remains the backstop once the handover clears (e.g. a lost completion message).
    private volatile java.util.function.BooleanSupplier handoverInProgressSupplier = () -> false;

    /**
     * Supplies the local node's applied replication frontier (how much state it has). Wired by the
     * {@link dev.nishisan.utils.ngrid.replication.ReplicationManager}. The default {@code MAX_VALUE} means
     * "always caught up" so any assembly that does not wire it (or has no replication) keeps the legacy
     * immediate-by-affinity behaviour — the gate only ever engages once a real frontier is supplied.
     */
    private volatile java.util.function.LongSupplier localAppliedSupplier = () -> Long.MAX_VALUE;
    private volatile boolean replicationProgressGateEnabled = false;

    /**
     * Lag tolerance (in sequences) for the watermark gate: the local node is considered "caught up" to a
     * peer when {@code localApplied >= peerWatermark - threshold}. Absorbs the small moving tail of an
     * incumbent that keeps producing while the returning node converges. {@code 0} = strict equality.
     */
    private volatile long syncReclaimLagThreshold = 0L;

    /**
     * Sticky latch: once the returning node has caught up to the cluster's max peer watermark in this
     * session it is allowed to reclaim, and a few ops the incumbent produces afterwards do not un-ready
     * it (defeats the moving-target / livelock risk). Reset when the node is no longer a viable reclaimer
     * (it leads, or there is no peer ahead).
     */
    private volatile boolean reclaimCaughtUpLatch = false;

    /**
     * Listener notified whenever the cluster membership changes (member joins,
     * leaves, or becomes inactive).
     */
    public interface MembershipListener {
        /**
         * Invoked when the set of active cluster members changes.
         */
        void onMembershipChanged();
    }

    private final Object leaderComputationLock = new Object();
    private final AtomicReference<NodeId> preferredLeader = new AtomicReference<>();
    private volatile long preferredLeaderUntilMs;

    private volatile boolean running;

    /**
     * Creates a new cluster coordinator.
     *
     * @param transport the transport layer used for inter-node communication
     * @param config    coordination configuration (intervals, timeouts, etc.)
     * @param scheduler shared scheduler for periodic heartbeat and eviction tasks
     */
    public ClusterCoordinator(Transport transport, ClusterCoordinatorConfig config,
            ScheduledExecutorService scheduler) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.config = Objects.requireNonNull(config, "config");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.epochPath = config.dataDirectory() != null
                ? config.dataDirectory().resolve("leader-epoch.dat")
                : null;
        loadEpoch();
    }

    /**
     * Registers a supplier that provides the leader's high watermark offset,
     * included in heartbeat payloads so followers can track replication progress.
     *
     * @param supplier the high watermark supplier
     */
    public void setLeaderHighWatermarkSupplier(java.util.function.LongSupplier supplier) {
        this.leaderHighWatermarkSupplier = Objects.requireNonNull(supplier);
    }

    /**
     * Registers the supplier of the local node's applied frontier PER TOPIC (issue #178), carried in
     * every heartbeat next to the scalar watermark so peers compare replication progress topic by
     * topic. An empty map means "no vector" (peers fall back to the scalar watermark).
     *
     * @param supplier the per-topic frontier supplier (never returns {@code null})
     */
    public void setTopicFrontiersSupplier(java.util.function.Supplier<Map<String, Long>> supplier) {
        this.topicFrontiersSupplier = supplier != null ? supplier : Map::of;
    }

    /**
     * Signals that the local applied frontier REGRESSED (a snapshot cutover onto another lineage, a
     * dual-leader yield resync): the reclaim latch earned before is void (revisão #178, A5) — the node
     * must catch up to the incumbent's frontier again before it may reclaim.
     */
    public void noteLocalFrontierRegressed() {
        reclaimCaughtUpLatch = false;
    }

    /**
     * Sets the topic precedence used to order two INCOMPARABLE frontier vectors with the same total
     * (issue #178). Must be identical on every node so they rank a pair the same way.
     *
     * @param priorityTopics topics in precedence order; {@code null}/empty = topic-name order
     */
    public void setTopicPriority(java.util.List<String> priorityTopics) {
        this.topicPriority = priorityTopics == null ? java.util.List.of() : java.util.List.copyOf(priorityTopics);
    }

    /**
     * Registers a local leadership eligibility predicate. This lets subsystems that own durable state
     * keep the local node from taking leadership while that state is known to be unsafe to serve. The
     * predicate is local-only; peers express their readiness indirectly through their advertised
     * watermark.
     *
     * @param supplier {@code true} when the local node may lead
     */
    public void setLocalLeadershipEligibility(java.util.function.BooleanSupplier supplier) {
        this.localLeadershipEligibilitySupplier = supplier != null ? supplier : (() -> true);
    }

    /**
     * Wires the local node's applied replication frontier and the lag tolerance used by the
     * sync-before-reclaim watermark gate. Called by the
     * {@link dev.nishisan.utils.ngrid.replication.ReplicationManager}. Until wired, the gate is inert
     * (the default frontier is {@code Long.MAX_VALUE} ⇒ "always caught up").
     *
     * @param localAppliedSupplier supplies how much state this node has applied (its frontier)
     * @param lagThreshold         catch-up tolerance in sequences ({@code >= 0}); {@code 0} = strict
     */
    public void setReplicationProgressGate(java.util.function.LongSupplier localAppliedSupplier,
            long lagThreshold) {
        this.localAppliedSupplier = localAppliedSupplier != null ? localAppliedSupplier
                : (() -> Long.MAX_VALUE);
        this.replicationProgressGateEnabled = localAppliedSupplier != null;
        this.syncReclaimLagThreshold = Math.max(0L, lagThreshold);
    }

    /**
     * Highest replication high-watermark currently advertised by any ACTIVE, LEADER-ELIGIBLE peer
     * (excluding the local node), or {@code -1} when no such peer has reported one yet. This is the
     * cluster's "newest known state" frontier that a returning node must reach before it may reclaim
     * leadership.
     *
     * <p>A leader-ineligible member ({@link NodeInfo#ROLE_LEADER_INELIGIBLE}, e.g. an ngrrd client) is
     * deliberately ignored, like in {@code highestWatermarkActivePeer} and
     * {@code hasActivePeerWithUnknownWatermark}: it can never lead nor serve the stream, so
     * state that only it holds can never be synced FROM it — waiting for it can never resolve (issue
     * #179: a client that received the last replication frame before the leader died made the affinity
     * winner defer to a peer that was not ahead, the other survivor followed the winner without a D9
     * escape, and the cluster stayed leaderless forever). Every consumer wants the eligible frontier:
     * <ul>
     *   <li>gate A / reclaim ({@code isCaughtUpToCluster}): only an eligible peer can be the
     *       incumbent a reclaiming node must catch up to;</li>
     *   <li>the "leader observes a peer watermark above its own applied" warning: a client's counter is
     *       a mirror of the stream this leader serves, never a rival state;</li>
     *   <li>{@code ReplicationManager#nudgeLeadershipOnCatchUp}: a deferring node is caught up once it
     *       reaches the eligible frontier, so the nudge must fire then.</li>
     * </ul>
     * Only members KNOWN to be ineligible are excluded: a HEARTBEAT placeholder (blank host, roles not
     * learned yet) still counts, conservatively — it may be a storage node that is the incumbent ahead of
     * us, and the gate must not let a behind node reclaim before that is known.
     *
     * @return the max active, leader-eligible peer watermark, or {@code -1} if none is known
     */
    public long maxActivePeerHighWatermark() {
        long max = -1L;
        NodeId localId = transport.local().nodeId();
        for (Map.Entry<NodeId, Long> e : peerHighWatermark.entrySet()) {
            if (e.getKey().equals(localId) || e.getValue() == null) {
                continue;
            }
            ClusterMember member = members.get(e.getKey());
            if (member != null && member.isActive() && member.info().isLeaderEligible()
                    && e.getValue() > max) {
                max = e.getValue();
            }
        }
        return max;
    }

    /**
     * Highest applied frontier any ACTIVE, LEADER-ELIGIBLE peer advertises for {@code topic} (issue
     * #178), or {@code -1} when no such peer advertises a frontier vector. Lets a subsystem fence on a
     * single topic (e.g. the ngrrd catalog before resuming in-flight migrations on a new leader).
     *
     * @param topic the replication topic
     * @return the max eligible peer frontier for the topic, or {@code -1} if unknown
     */
    public long maxActivePeerTopicFrontier(String topic) {
        long max = -1L;
        NodeId localId = transport.local().nodeId();
        for (Map.Entry<NodeId, TopicFrontiers> e : peerTopicFrontiers.entrySet()) {
            if (e.getKey().equals(localId) || e.getValue() == null || e.getValue().isEmpty()) {
                continue;
            }
            ClusterMember member = members.get(e.getKey());
            if (member != null && member.isActive() && member.info().isLeaderEligible()) {
                max = Math.max(max, e.getValue().frontier(topic));
            }
        }
        return max;
    }

    /**
     * True when some ACTIVE, LEADER-ELIGIBLE peer is AHEAD of the local node (issue #178): per topic
     * when both advertise a frontier vector, by the scalar watermark otherwise. This is the predicate
     * behind gate A and the catch-up nudge; a peer that only mirrors the stream (a client) never counts.
     *
     * @return {@code true} while a peer holds state this node has not applied yet
     */
    public boolean localBehindEligiblePeer() {
        return aheadEligiblePeer(transport.local().nodeId()) != null;
    }

    /** The active, eligible peer AHEAD of the local node (the first found), or {@code null}. */
    private NodeId aheadEligiblePeer(NodeId localId) {
        for (Map.Entry<NodeId, Long> e : peerHighWatermark.entrySet()) {
            if (e.getKey().equals(localId) || e.getValue() == null) {
                continue;
            }
            ClusterMember member = members.get(e.getKey());
            if (member != null && member.isActive() && member.info().isLeaderEligible()
                    && peerAheadOfLocal(e.getKey())) {
                return e.getKey();
            }
        }
        return null;
    }

    /** Local frontier vector, or {@code null} when the local node advertises none. */
    private TopicFrontiers localFrontiersOrNull() {
        Map<String, Long> frontiers = safeTopicFrontiers();
        return frontiers.isEmpty() ? null : TopicFrontiers.of(frontiers);
    }

    /** A peer's frontier vector, or {@code null} when it advertises none (older peer / bootstrapping). */
    private TopicFrontiers peerFrontiersOrNull(NodeId peer) {
        TopicFrontiers frontiers = peerTopicFrontiers.get(peer);
        return frontiers == null || frontiers.isEmpty() ? null : frontiers;
    }

    /**
     * Is {@code peer} AHEAD of the local node — does it hold state we have not applied? Per topic when
     * both sides advertise a vector (issue #178), else by the scalar watermark. A peer advertising
     * {@code -1} (bootstrap gate) is never ahead.
     */
    private boolean peerAheadOfLocal(NodeId peer) {
        Long watermark = peerHighWatermark.get(peer);
        if (watermark == null || watermark < 0L) {
            return false;
        }
        TopicFrontiers local = localFrontiersOrNull();
        TopicFrontiers theirs = peerFrontiersOrNull(peer);
        if (local != null && theirs != null) {
            return local.isBehind(theirs, syncReclaimLagThreshold, topicPriority);
        }
        return watermark > safeLocalApplied() + syncReclaimLagThreshold;
    }

    /**
     * Is the local node STRICTLY AHEAD of {@code peer} (beyond the tolerance)? Per topic when both
     * sides advertise a vector, else by the scalar watermark. Used by the D9 stalemate escape.
     */
    private boolean localAheadOfPeer(NodeId peer) {
        Long watermark = peerHighWatermark.get(peer);
        long localApplied = safeLocalApplied();
        if (watermark == null || localApplied < 0L) {
            return false;
        }
        TopicFrontiers local = localFrontiersOrNull();
        TopicFrontiers theirs = peerFrontiersOrNull(peer);
        if (local != null && theirs != null && watermark >= 0L) {
            return local.isAhead(theirs, syncReclaimLagThreshold, topicPriority);
        }
        return localApplied > watermark + syncReclaimLagThreshold;
    }

    /**
     * Orders two peers by advertised state: {@code > 0} when {@code a} holds newer state than {@code b}
     * (per topic when both advertise a vector, else by scalar watermark), affinity (priority, then id)
     * as the tie-break so every node ranks the same pair identically.
     */
    private int compareAdvertisedState(NodeId a, NodeId b) {
        TopicFrontiers fa = peerFrontiersOrNull(a);
        TopicFrontiers fb = peerFrontiersOrNull(b);
        long wa = peerHighWatermark.getOrDefault(a, -1L);
        long wb = peerHighWatermark.getOrDefault(b, -1L);
        if (fa != null && fb != null && wa >= 0L && wb >= 0L) {
            if (fa.isAhead(fb, syncReclaimLagThreshold, topicPriority)) {
                return 1;
            }
            if (fa.isBehind(fb, syncReclaimLagThreshold, topicPriority)) {
                return -1;
            }
        } else if (wa != wb) {
            return Long.compare(wa, wb);
        }
        ClusterMember ma = members.get(a);
        ClusterMember mb = members.get(b);
        int pa = ma == null ? Integer.MIN_VALUE : ma.info().priority();
        int pb = mb == null ? Integer.MIN_VALUE : mb.info().priority();
        if (pa != pb) {
            return Integer.compare(pa, pb);
        }
        return a.compareTo(b);
    }

    /**
     * Returns the active, leader-eligible peer (distinct from {@code localId}) advertising the newest
     * replication state — the node a deferring local node should follow and sync from — or
     * {@code null} if no such peer has reported a watermark yet. Ranked per topic when the peers
     * advertise frontier vectors (issue #178), by scalar watermark otherwise, affinity as tie-break. A
     * leader-ineligible peer is never returned: a deferring node must never adopt an ineligible peer
     * as its local leader.
     */
    private NodeId highestWatermarkActivePeer(NodeId localId) {
        NodeId best = null;
        for (Map.Entry<NodeId, Long> e : peerHighWatermark.entrySet()) {
            if (e.getKey().equals(localId) || e.getValue() == null || e.getValue() < 0L) {
                continue;
            }
            ClusterMember member = members.get(e.getKey());
            if (member == null || !isLeaderCandidate(member)) {
                continue;
            }
            if (best == null || compareAdvertisedState(e.getKey(), best) > 0) {
                best = e.getKey();
            }
        }
        return best;
    }

    /**
     * Returns the active, leader-eligible peer (distinct from {@code localId}) whose LATEST heartbeat
     * asserts leadership — the node that is actually serving — or {@code null} if none does. With more
     * than one asserting peer (a dual-leader still being resolved, or a sub-heartbeat handoff overlap)
     * the highest affinity wins, matching the D10c resolution order so every observer converges on the
     * same node.
     */
    private NodeId assertingLeaderPeer(NodeId localId) {
        return members.values().stream()
                .filter(m -> !m.id().equals(localId) && isLeaderCandidate(m)
                        && Boolean.TRUE.equals(peerAssertsLeadership.get(m.id())))
                .max(Comparator.comparingInt((ClusterMember m) -> m.info().priority())
                        .thenComparing(ClusterMember::id))
                .map(ClusterMember::id)
                .orElse(null);
    }

    /**
     * The node a DEFERRING local node (one that must stay a follower and sync) should adopt as its
     * leader — i.e. the node it will pull the stream / request a snapshot from, so the choice decides
     * whether the catch-up can succeed at all:
     * <ol>
     *   <li>the peer that asserts leadership (it serves the stream);</li>
     *   <li>else the currently adopted leader, if still active and not known to be refusing (its
     *       assertion unknown, e.g. no heartbeat yet) — stickiness avoids flapping during discovery;</li>
     *   <li>else the eligible peer with the highest watermark (the likely incumbent during boot).</li>
     * </ol>
     * The previous order (adopted leader first, then highest watermark) let a deferring node stick to
     * — or pick, on a watermark tie — a peer that is itself a follower: every fetch/sync addressed to it
     * was refused ("not the leader"), the deferring node never caught up, and with three members the
     * cluster wedged in a permanent three-way leader disagreement.
     */
    private NodeId deferralFollowTarget(NodeId localId) {
        NodeId asserting = assertingLeaderPeer(localId);
        if (asserting != null) {
            return asserting;
        }
        NodeId current = leader.get();
        if (current != null && !current.equals(localId) && isActiveMember(current)
                && !Boolean.FALSE.equals(peerAssertsLeadership.get(current))) {
            return current;
        }
        return highestWatermarkActivePeer(localId);
    }

    /**
     * Triggers an out-of-band leadership recomputation. Called by the {@link
     * dev.nishisan.utils.ngrid.replication.ReplicationManager} when the local node's readiness flips to
     * ready (its applied frontier has reached the incumbent's high-watermark), so the deferred
     * affinity reclaim happens promptly rather than waiting for the next membership/eviction tick.
     */
    public void reevaluateLeadership() {
        if (running) {
            recomputeLeader();
        }
    }

    /**
     * Returns the last known leader high watermark received via heartbeat.
     *
     * @return the tracked leader high watermark offset
     */
    public long getTrackedLeaderHighWatermark() {
        return trackedLeaderHighWatermark;
    }

    /**
     * Returns the current leader epoch. The epoch is incremented each time
     * a new leader is elected.
     *
     * @return the current leader epoch
     */
    public long getLeaderEpoch() {
        return leaderEpoch.get();
    }

    /**
     * Returns the last known leader epoch received via heartbeat.
     *
     * @return the tracked leader epoch
     */
    public long getTrackedLeaderEpoch() {
        return trackedLeaderEpoch;
    }

    private void loadEpoch() {
        if (epochPath == null || !Files.exists(epochPath)) {
            // Absent on first boot (or after a wiped data dir): start at 0 and let epoch
            // convergence (observeEpoch) lift the term from peers before this node can emit
            // as leader. The cluster term is a monotonic logical clock, not a per-node count.
            return;
        }
        try {
            String content = Files.readString(epochPath).trim();
            long loaded = Long.parseLong(content);
            // Never regress: adopt the max of the persisted value and whatever we already hold.
            // A lost/relative data dir previously reset the term to 0 and re-elected at 1,
            // regressing it below what followers had seen — which fenced the legitimate leader.
            leaderEpoch.updateAndGet(cur -> Math.max(cur, loaded));
            LOGGER.info(() -> "Loaded leader epoch: " + loaded);
        } catch (Exception e) {
            // Corrupt file: do NOT silently reset to 0 (that is what allowed the 7 -> 1
            // regression). Keep the current term and converge upward from peers instead.
            LOGGER.log(Level.SEVERE, "Corrupt leader-epoch file at " + epochPath
                    + "; keeping current term and converging from peers", e);
        }
    }

    private void persistEpoch(long epoch) {
        if (epochPath == null) {
            return;
        }
        try {
            Files.createDirectories(epochPath.getParent());
            Files.writeString(epochPath, String.valueOf(epoch));
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to persist epoch", e);
        }
    }

    /**
     * Converges the local leader epoch toward a term observed from a peer, making the epoch a
     * monotonic, cluster-wide logical clock rather than a per-node counter. Called for every
     * epoch carried by an inbound heartbeat (and any other epoch-bearing message).
     *
     * <p>Semantics, guarding against runaway escalation:
     * <ul>
     *   <li>{@code observed <= currentTerm}: ignore. In particular this stops the leader from
     *       re-stamping in response to its own term echoed back by a follower's heartbeat.</li>
     *   <li>{@code observed > currentTerm} and this node is a <b>follower</b>: adopt
     *       {@code observed} (track the cluster maximum, so a future election here starts above
     *       every term any peer has seen).</li>
     *   <li>{@code observed > currentTerm} and this node is the <b>leader</b>: re-stamp to
     *       {@code observed + 1} — a fresh unique term strictly above the ghost term a follower
     *       still remembers, so the follower's {@code < trackedLeaderEpoch} fencing accepts this
     *       leader again. This is what unblocks a leader whose persisted term regressed.</li>
     * </ul>
     *
     * @param observed the leader epoch carried by a peer message ({@code <= 0} is ignored)
     */
    private void observeEpoch(long observed) {
        if (observed <= 0) {
            return;
        }
        long prev;
        long next;
        do {
            prev = leaderEpoch.get();
            if (observed <= prev) {
                return; // not newer than ours — ignore (also breaks the follower-echo escalation)
            }
            // A leader re-stamps above the observed term — EXCEPT while it is the losing side of a
            // dual-leader observation (issue tems#9, D10c): re-stamping would feed the infinite
            // epoch ladder (both leaders bumping above each other every heartbeat). The loser
            // adopts the term and yields; only the winner keeps re-stamping.
            next = isLeader() && !yieldingToDualLeader && !handoverInProgress() ? observed + 1 : observed;
        } while (!leaderEpoch.compareAndSet(prev, next));
        persistEpoch(next);
        long converged = next;
        boolean restamped = converged == observed + 1;
        LOGGER.info(() -> "Leader epoch converged to " + converged
                + (restamped ? " (leader re-stamp above observed " + observed + ")" : " (tracked from peer)"));
    }

    /**
     * Starts the cluster coordination process, initializing necessary components
     * and tasks.
     *
     * This method transitions the cluster coordination to a running state if it is
     * not already running.
     * It sets the local node as a cluster member, registers itself as a transport
     * listener, and schedules
     * periodic tasks for sending heartbeats and evicting inactive members.
     * Leadership is also recomputed
     * upon startup to establish the current cluster leader.
     *
     * The following actions are performed:
     * - The `running` flag is set to true, marking the cluster as active.
     * - The local node is added to the list of cluster members.
     * - A listener is added to the transport layer to handle incoming messages and
     * events.
     * - Heartbeat messages are scheduled to be broadcast at regular intervals as
     * defined in the configuration.
     * - A task to evict inactive members is scheduled, removing members that miss
     * multiple heartbeats.
     * - The cluster leader is computed and updated, notifying relevant listeners.
     *
     * If the cluster is already running, the method exits without making any
     * changes.
     */
    public void start() {
        if (running) {
            return;
        }
        running = true;
        // Initialize lease so a freshly started leader has a valid window
        this.leaseExpiresAt = Instant.now().plus(config.leaseTimeout());
        // Arm the boot discovery window: while it is open, a node outranked by a configured but
        // not-yet-active higher-priority peer defers self-election (see recomputeLeader).
        long bootWindowMs = config.bootDiscoveryWindow().toMillis();
        this.bootDiscoveryDeadlineMs = bootWindowMs > 0 ? Instant.now().toEpochMilli() + bootWindowMs : 0L;
        NodeInfo local = transport.local();
        members.put(local.nodeId(), new ClusterMember(local));
        transport.addListener(this);
        scheduler.scheduleAtFixedRate(this::sendHeartbeat,
                0,
                config.heartbeatInterval().toMillis(),
                TimeUnit.MILLISECONDS);
        long evictionIntervalMs = config.heartbeatInterval().toMillis() * 2;
        scheduler.scheduleAtFixedRate(this::evictDeadMembers,
                evictionIntervalMs,
                evictionIntervalMs,
                TimeUnit.MILLISECONDS);
        recomputeLeader();
        if (bootWindowMs > 0) {
            // Re-evaluate leadership the moment the discovery window closes, so a node that deferred
            // self-election (the preferred peer never showed up) takes leadership at the deadline.
            scheduler.schedule(this::recomputeLeader, bootWindowMs + 1, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Stops the cluster coordination process, removing this node as a transport
     * listener. Scheduled heartbeat and eviction tasks remain in the executor
     * but will no-op since the running flag is cleared.
     */
    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        transport.removeListener(this);
    }

    /**
     * Returns whether the local node is the current cluster leader.
     *
     * @return {@code true} if this node is the leader
     */
    public boolean isLeader() {
        NodeId leaderId = leader.get();
        return leaderId != null && leaderId.equals(transport.local().nodeId());
    }

    /**
     * Sets the preferred leader for the cluster with an optional time-to-live
     * (TTL).
     *
     * A preferred leader is a node that is temporarily prioritized as the leader
     * for
     * the duration specified by the TTL. If the TTL is null, zero, or negative,
     * the preferred leader is cleared, and the leader is recomputed immediately.
     *
     * @param leaderId the identifier of the node to be set as the preferred leader;
     *                 may be null to clear the current preference.
     * @param ttl      the duration for which the specified node should be preferred
     *                 as the leader;
     *                 must be non-negative and non-zero, otherwise the preference
     *                 is cleared.
     */
    public void setPreferredLeader(NodeId leaderId, Duration ttl) {
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.isNegative() || ttl.isZero()) {
            preferredLeader.set(null);
            preferredLeaderUntilMs = 0L;
            recomputeLeader();
            return;
        }
        preferredLeader.set(leaderId);
        preferredLeaderUntilMs = Instant.now().plus(ttl).toEpochMilli();
        recomputeLeader();
    }

    /**
     * Retrieves information about the current leader of the cluster.
     *
     * This method attempts to fetch the {@code NodeInfo} of the leader node as
     * identified by
     * the current leader's {@code NodeId}. If the leader is null or the leader's
     * associated
     * {@code ClusterMember} is not active, an empty {@code Optional} is returned.
     * Otherwise,
     * the method returns the {@code NodeInfo} of the active leader wrapped in an
     * {@code Optional}.
     *
     * @return an {@code Optional} containing the {@code NodeInfo} of the leader if
     *         available and active;
     *         otherwise, an empty {@code Optional}.
     */
    public Optional<NodeInfo> leaderInfo() {
        NodeId leaderId = leader.get();
        if (leaderId == null) {
            return Optional.empty();
        }
        ClusterMember member = members.get(leaderId);
        return member != null && member.isActive() ? Optional.of(member.info()) : Optional.empty();
    }

    /**
     * Retrieves a collection of active cluster members.
     *
     * This method filters the cluster members to include only those marked
     * as active and then maps them to their respective {@code NodeInfo} objects.
     *
     * @return a collection of {@code NodeInfo}
     */
    public Collection<NodeInfo> activeMembers() {
        return members.values().stream()
                .filter(ClusterMember::isActive)
                .map(ClusterMember::info)
                .toList();
    }

    /**
     * Returns the number of currently active members in the cluster.
     *
     * @return active member count
     */
    public int getActiveMembersCount() {
        return (int) members.values().stream()
                .filter(ClusterMember::isActive)
                .count();
    }

    /**
     * Waits for the cluster to stabilize with a default timeout of 10 seconds.
     * 
     * A cluster is considered stable when:
     * - A leader has been elected (leaderInfo() is present)
     * - The cluster has at least the minimum required size (activeMembers >=
     * minClusterSize)
     * 
     * @throws InterruptedException  if the thread is interrupted while waiting
     * @throws IllegalStateException if the cluster does not stabilize within the
     *                               timeout
     */
    public void awaitLocalStability() throws InterruptedException {
        awaitLocalStability(Duration.ofSeconds(10));
    }

    /**
     * Waits for the cluster to stabilize with a specified timeout.
     * 
     * A cluster is considered stable when:
     * - A leader has been elected (leaderInfo() is present)
     * - The cluster has at least the minimum required size (activeMembers >=
     * minClusterSize)
     * 
     * @param timeout the maximum time to wait for stability
     * @throws InterruptedException  if the thread is interrupted while waiting
     * @throws IllegalStateException if the cluster does not stabilize within the
     *                               timeout
     */
    public void awaitLocalStability(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("Timeout must be positive");
        }

        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (isStable()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException(String.format(
                "Cluster did not stabilize within %s. Current state: leader=%s, activeMembers=%d,"
                        + " minClusterSize=%d, activeVoters=%d, requiredVoterMajority=%d, pairMode=%s",
                timeout, leaderInfo().map(NodeInfo::nodeId).orElse(null),
                activeMemberCount(), config.minClusterSize(), activeVoterCount(), requiredVoterMajority(),
                config.pairMode()));
    }

    /**
     * Checks if the cluster is currently stable.
     *
     * @return true if a leader is present and the leadership quorum holds (see
     *         {@link #hasLeadershipQuorum()})
     */
    private boolean isStable() {
        return leaderInfo().isPresent() && hasLeadershipQuorum();
    }

    /**
     * Registers a listener to be notified of leadership changes.
     *
     * @param listener the listener to add
     */
    public void addLeadershipListener(LeadershipListener listener) {
        leadershipListeners.add(listener);
    }

    /**
     * Removes a previously registered leadership listener.
     *
     * @param listener the listener to remove
     */
    public void removeLeadershipListener(LeadershipListener listener) {
        leadershipListeners.remove(listener);
    }

    /**
     * Registers a listener to be notified when the local node's leadership
     * status changes.
     *
     * @param listener the listener to add
     */
    public void addLeaderElectionListener(LeaderElectionListener listener) {
        leaderElectionListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /**
     * Removes a previously registered leader election listener.
     *
     * @param listener the listener to remove
     */
    public void removeLeaderElectionListener(LeaderElectionListener listener) {
        leaderElectionListeners.remove(listener);
    }

    /**
     * Registers a listener to be notified when cluster membership changes.
     *
     * @param listener the listener to add
     */
    public void addMembershipListener(MembershipListener listener) {
        membershipListeners.add(Objects.requireNonNull(listener, "listener"));
    }

    /**
     * Removes a previously registered membership listener.
     *
     * @param listener the listener to remove
     */
    public void removeMembershipListener(MembershipListener listener) {
        membershipListeners.remove(listener);
    }

    private void notifyMembershipListeners() {
        membershipListeners.forEach(MembershipListener::onMembershipChanged);
    }

    /**
     * Sends a heartbeat message to all nodes in the cluster.
     *
     * This method constructs a heartbeat message containing a timestamp
     * and broadcasts it to all members of the cluster using the transport layer.
     * The heartbeat message is used by the cluster to confirm that this node
     * is still active. The method exits immediately if the cluster is not marked
     * as running.
     *
     * The message includes the following:
     * - Type: HEARTBEAT, to indicate the nature of the message.
     * - Qualifier: "hb", providing additional context about the message.
     * - Source: The local node's identifier.
     * - Payload: A `HeartbeatPayload` containing the current timestamp.
     */
    private void sendHeartbeat() {
        try {
            if (!running) {
                return;
            }
            HeartbeatPayload payload = HeartbeatPayload.now(leaderHighWatermarkSupplier.getAsLong(),
                    leaderEpoch.get(), isLeader(), safeTopicFrontiers());
            ClusterMessage heartbeat = ClusterMessage.lightweight(MessageType.HEARTBEAT,
                    "hb",
                    transport.local().nodeId(),
                    null,
                    payload);
            transport.broadcast(heartbeat);
        } catch (Throwable t) {
            LOGGER.log(java.util.logging.Level.SEVERE, "Unexpected error in heartbeat task", t);
        }
    }

    /**
     * Scans the cluster members for inactive nodes and evicts them if they have not
     * sent a heartbeat
     * within the configured timeout period.
     *
     * This method checks each cluster member to ensure that it is still active. If
     * a member's last
     * heartbeat timestamp exceeds the configured heartbeat timeout, the member is
     * marked as inactive.
     * The leader is then recomputed to reflect the updated state of the cluster.
     * The local node is
     * excluded from eviction checks.
     *
     * If the cluster is not running, the method exits without performing any
     * actions.
     */
    private void evictDeadMembers() {
        try {
            if (!running) {
                return;
            }

            // Check leader lease expiry before processing members
            if (isLeader() && Instant.now().isAfter(leaseExpiresAt)) {
                LOGGER.warning("Leader lease expired — stepping down to prevent split-brain");
                stepDown();
                return;
            }

            long now = Instant.now().toEpochMilli();
            boolean changed = false;
            for (ClusterMember member : members.values()) {
                if (member.id().equals(transport.local().nodeId())) {
                    continue;
                }
                if (member.isActive() && now - member.lastHeartbeat() > config.heartbeatTimeout().toMillis()) {
                    long overdueMs = now - member.lastHeartbeat();
                    long graceMs = config.heartbeatTimeout().toMillis() * PROXY_REACHABLE_GRACE_FACTOR;
                    // Genuine reachability only: an open direct connection OR an active proxy
                    // route. We must NOT use transport.isReachable() here — it returns true for
                    // any known peer (the optimistic default-direct route), which would grant
                    // grace to dead members and let an isolated leader keep refreshing its lease
                    // instead of stepping down.
                    boolean genuinelyReachable =
                            transport.isConnected(member.id()) || transport.isProxied(member.id());
                    if (overdueMs <= graceMs && genuinelyReachable) {
                        // Heartbeat overdue but the peer is still reachable (possibly only via a
                        // proxy). Keep it active within the bounded grace window to avoid spurious
                        // quorum loss during a transient direct-link flap.
                        LOGGER.fine(() -> "Granting proxy-reachable grace to overdue member: " + member.info());
                        continue;
                    }
                    LOGGER.fine(() -> "Marking member inactive due to missed heartbeat: " + member.info());
                    member.markInactive();
                    // Drop the dead peer's tracked watermark so a deferring higher-affinity node can lead.
                    peerHighWatermark.remove(member.id());
                    peerTopicFrontiers.remove(member.id());
                    leaderRefusalAtMs.remove(member.id());
                    peerAssertsLeadership.remove(member.id());
                    changed = true;
                }
            }
            if (changed) {
                recomputeLeader();
                notifyMembershipListeners();
            }

            // Renew lease only while the node still has enough active members to
            // legitimately hold leadership for the currently known cluster size.
            if (isLeader() && hasLeadershipQuorum()) {
                this.leaseExpiresAt = Instant.now().plus(config.leaseTimeout());
            }
        } catch (Throwable t) {
            LOGGER.log(java.util.logging.Level.SEVERE, "Unexpected error in eviction task", t);
        }
    }

    /**
     * Recomputes the leader of the cluster based on currently active members.
     *
     * The method identifies the highest-ranking active node by comparing their
     * unique identifiers,
     * updates the cluster's leader state, and notifies registered listeners if the
     * leadership
     * status has changed.
     *
     * Leadership change is determined by examining the difference between the
     * previous leader
     * and the newly computed leader. If the local node's leadership state changes,
     * the corresponding
     * listeners are notified.
     */
    private void recomputeLeader() {
        synchronized (leaderComputationLock) {
            if (!hasLeadershipQuorum()) {
                updateLeader(null);
                return;
            }

            NodeId preferred = preferredLeader.get();
            if (preferred != null && preferredLeaderUntilMs > Instant.now().toEpochMilli()) {
                ClusterMember preferredMember = members.get(preferred);
                // A preferred-leader suggestion (e.g. LeaderReelectionService, by write-rate) must not
                // override role-based ineligibility: fall through to the normal affinity election below
                // when the suggested node carries NodeInfo.ROLE_LEADER_INELIGIBLE.
                if (preferredMember != null && isLeaderCandidate(preferredMember)) {
                    updateLeader(preferred);
                    return;
                }
            }
            // Elect by leadership affinity: highest (priority, then NodeId), restricted to members
            // that are not leader-ineligible (NodeInfo.ROLE_LEADER_INELIGIBLE). With all priorities
            // at the default 0 and no ineligible member this reduces to the legacy max(NodeId), so
            // behaviour is unchanged unless priorities/roles are configured. When every active member
            // is ineligible, electedId is null and the cluster ends up leaderless below.
            NodeId electedId = members.values().stream()
                    .filter(ClusterCoordinator::isLeaderCandidate)
                    .max(Comparator.comparingInt((ClusterMember m) -> m.info().priority())
                            .thenComparing(ClusterMember::id))
                    .map(ClusterMember::id)
                    .orElse(null);

            // Boot discovery deferral: if WE would lead only because a configured, higher-affinity
            // peer has not shown up yet, stay a follower until the discovery window elapses (or the
            // peer appears). Avoids grabbing leadership and forcing a churny hand-back. After the
            // window, if the preferred peer never appears, we still lead (lead-while-alone / AP).
            if (electedId != null && electedId.equals(transport.local().nodeId())
                    && Instant.now().toEpochMilli() < bootDiscoveryDeadlineMs
                    && outrankedByAbsentConfiguredPeer()) {
                updateLeader(null);
                return;
            }

            NodeId localId = transport.local().nodeId();
            boolean weWouldLead = electedId != null && electedId.equals(localId);
            boolean localIneligible = weWouldLead && !safeLocalLeadershipEligible();
            if (localIneligible) {
                // Defer (stay a follower and sync) ONLY while there is someone to defer TO — an active
                // peer advertising a KNOWN (>= 0) watermark, i.e. a node able to lead and serve us a
                // snapshot — or while we are still inside the boot-discovery window (peers/incumbent may
                // not have appeared yet). AP escape: once the window lapses and NO active peer is viable
                // (every active peer is itself ineligible/bootstrapping → advertising -1 → no known
                // watermark), do NOT keep deferring — fall through to the affinity election so the
                // highest-affinity node leads and the rest bootstrap from it. Without this escape the
                // whole-cluster unclean restart wedges leaderless forever (both nodes defer to each other).
                boolean withinBootWindow = Instant.now().toEpochMilli() < bootDiscoveryDeadlineMs;
                boolean hasViableLeaderPeer = highestWatermarkActivePeer(localId) != null;
                if (withinBootWindow || hasViableLeaderPeer) {
                    NodeId followTarget = deferralFollowTarget(localId);
                    if (followTarget == null) {
                        followTarget = highestAffinityActivePeer(localId);
                    }
                    updateLeader(followTarget);
                    return;
                }
                // else: AP escape — fall through to the affinity election below.
            }

            // ── Sync-before-reclaim gate A (RECLAIM side) — watermark-driven ──────────────────────────
            // If WE would win by affinity but a peer is AHEAD of us by watermark (it holds newer state we
            // have not yet applied), DEFER taking leadership. This does NOT depend on observing who
            // asserts leadership (the affinity handoff is too fast for that): it depends purely on the
            // replication-watermark gap, which both nodes learn from heartbeats. We stay a follower and
            // the peer that is ahead keeps leading (in pair mode it self-elects locally) until WE catch
            // up — then the latch flips and the next recompute promotes us. Lead-while-alone / AP is
            // preserved: with no active peer ahead, maxActivePeerHighWatermark() is -1 and the gate is a
            // no-op, so the node still leads (after the boot-discovery window, handled above). Only
            // leader-eligible peers count as "ahead" (issue #179): a client can never serve the state it
            // holds, so deferring to its watermark would wedge the cluster leaderless after a failover.
            //
            // Gate A NEVER evicts the CURRENT leader (issue tems#9, D9): it gates the RECLAIM of a
            // returning node; the incumbent's side is gate B. In stream mode the serving leader IS the
            // source of the stream — a leader observing a follower's counter above its own is, by
            // construction, a counter-scale desync (e.g. an inflated restart seed), never a reason to
            // abdicate into a leaderless mutual-deferral stalemate.
            // Only a peer GENUINELY ahead (per topic, issue #178) counts here — never the boot-window
            // deferral inside isCaughtUpToCluster(), which is a reclaim-side rule: the incumbent seeing
            // a follower BEHIND during its own boot window is not a desync and must not log as one.
            boolean leaderBehindOwnFollower = weWouldLead && isLeaderInternal(localId)
                    && aheadEligiblePeer(localId) != null;
            if (leaderBehindOwnFollower) {
                long now = Instant.now().toEpochMilli();
                if (now - lastLeaderBehindWarnMs > 60_000L) {
                    lastLeaderBehindWarnMs = now;
                    LOGGER.warning(() -> "Current leader observes a peer watermark above its own applied ("
                            + safeLocalApplied() + " < " + maxActivePeerHighWatermark()
                            + describeAheadPeerDivergence(localId)
                            + "); retaining leadership (counter-scale desync symptom — see issue tems#9/D9)");
                }
            }
            boolean behindAheadPeer = weWouldLead && !isLeaderInternal(localId) && !isCaughtUpToCluster();
            // Watermark-unknown deferral: if a peer is already ACTIVE, never hand leadership to us before
            // it reports a watermark; that peer may be the incumbent ahead of us. The boot window only
            // bounds waiting for configured peers that have not appeared at all, preserving AP when alone.
            // Like gate A, this deferral gates the RECLAIM of a node that is not leading — it NEVER evicts
            // the CURRENT leader (issue tems#9, D9): a serving leader that sees a new peer connect (a
            // joining client, a returning follower) must not abdicate to its own follower until that
            // peer's first heartbeat lands, only to re-elect itself milliseconds later — that flap bumps
            // the epoch twice and fires the demotion/promotion listeners on every join. A rival that is
            // genuinely ahead resolves through the dual-leader path (D10c), never through this gate.
            boolean bootWindowUnknownPeer = weWouldLead
                    && !isLeaderInternal(localId)
                    && replicationProgressGateEnabled
                    && (hasActivePeerWithUnknownWatermark(localId)
                            || (Instant.now().toEpochMilli() < bootDiscoveryDeadlineMs
                                    && hasConfiguredPeerWithUnknownWatermark(localId)));
            if (behindAheadPeer || bootWindowUnknownPeer) {
                // Behind a peer's state (or still discovering it): do not seize leadership. Adopt the peer
                // that is ahead as our LOCAL leader so we become its follower and the replication layer
                // streams its state to us (the apply/fetch loop needs a known leader to pull from — leaving
                // us leaderless would wedge the sync, never converging). Prefer the current leader if it is
                // already a distinct active node; else the active peer with the highest watermark (the real
                // incumbent). Only fall back to "no leader" when neither is known (pure boot discovery).
                // See deferralFollowTarget: a peer that ASSERTS leadership always comes first.
                updateLeader(deferralFollowTarget(localId)); // may be null during pure boot discovery
                return;
            }

            // ── Affinity handback supersedes the watermark reclaim AND step-down (issue tems#9, D11) ──
            // With orchestrated handback enabled, leadership transfers between the affinity pair ONLY via
            // the explicit handover (assumeLeadershipForHandback / acceptHandbackWinner, which call
            // updateLeader directly, bypassing this election), never via the watermark gates below — a
            // lineage-blind counter offset can defeat them (the permanent applied skew that keeps
            // reclaim-quiesce from ever engaging). The candidate stays a follower until it has installed
            // the incumbent's snapshot and cut over (SET — offset zeroed); the incumbent retains until
            // the candidate completes. Genuine failover is unaffected: these only hold while a DISTINCT,
            // ACTIVE leader/candidate exists — if a node died, the affinity election below promotes the
            // survivor.
            if (affinityHandbackMode) {
                // CANDIDATE side: do not reclaim via watermark; the handshake drives the transition. This
                // only holds while a peer is actually SERVING (asserting leadership in its heartbeats):
                // the node adopted during boot discovery is a watermark-tie pick that may itself be a
                // follower, and following it blindly deadlocked a cold start — every node deferred to the
                // affinity winner, the winner stuck to its boot-time pick, nobody ever led, and the
                // HANDBACK_REQUEST addressed to that pick was aborted ("not an eligible leader") every
                // cooldown. With no serving peer there is nothing to hand back FROM: fall through to the
                // ordinary election (lead-while-alone / AP).
                NodeId serving = weWouldLead && !isLeaderInternal(localId) ? assertingLeaderPeer(localId) : null;
                if (serving != null) {
                    updateLeader(serving);
                    return;
                }
                if (isLeaderInternal(localId) && electedId != null && !electedId.equals(localId)
                        && isActiveMember(electedId)) {
                    // INCUMBENT side: a higher-affinity peer would win by affinity, but the watermark
                    // step-down is replaced by the explicit handback — retain until the candidate completes.
                    updateLeader(localId);
                    return;
                }
            }

            // ── Sync-before-reclaim gate B (STEP-DOWN side) — watermark-driven ───────────────────────
            // If WE are the current leader and the affinity-elected candidate is a DIFFERENT, higher-
            // affinity peer that is still BEHIND our watermark, do NOT step down to it yet — retain
            // leadership until it has caught up to our state. This closes the handoff race from the
            // incumbent's side: even if the returning higher-affinity node momentarily tries to lead, the
            // incumbent will not relinquish until that node has synced, so there is never a window where a
            // behind node leads. Once the candidate catches up (its reported watermark reaches ours), this
            // guard no longer holds and the normal affinity election promotes it.
            if (!weWouldLead && isLeaderInternal(localId)
                    && electedId != null && !electedId.equals(localId)
                    && candidateIsBehindLocalWatermark(electedId)) {
                updateLeader(localId); // retain leadership; the higher-affinity peer is still catching up
                return;
            }

            // ── Leaderless-stalemate escape (issue tems#9, D9) ─────────────────────────────────────────
            // We are about to defer to the affinity-elected peer — but if that peer has RECENTLY refused
            // to serve the stream as a non-leader (it is itself deferring, e.g. behind us via gate A) and
            // we are NOT behind it, deferring would close the mutual-deferral loop: it cannot catch up
            // (only a leader serves the stream) and we never lead (affinity). The ahead node takes over;
            // the behind one converges as its follower and the normal affinity handoff resumes later.
            // A role-ineligible local node (NodeInfo.ROLE_LEADER_INELIGIBLE) must never self-elect via
            // this escape, no matter how far ahead its local state is — role eligibility is checked
            // in addition to the replication-progress gate.
            if (electedId != null && !electedId.equals(localId)
                    && replicationProgressGateEnabled
                    && safeLocalLeadershipEligible()
                    && transport.local().isLeaderEligible()) {
                Long refusedAt = leaderRefusalAtMs.get(electedId);
                boolean recentRefusal = refusedAt != null
                        && Instant.now().toEpochMilli() - refusedAt <= config.heartbeatTimeout().toMillis();
                // The refusal must still be CURRENT: the elected node's latest heartbeat must not assert
                // leadership. A node that is leading cannot be "refusing to lead" — its refusal was
                // recorded while it deferred (boot dance, handoff) and a serving leader's watermark lags
                // its followers' applied frontier by up to one heartbeat, so a stale refusal plus that
                // ordinary skew would read as the stalemate signature and promote a follower against a
                // healthy leader (a self-inflicted dual-leader on every membership recompute).
                boolean electedStillNotLeading = !Boolean.TRUE.equals(peerAssertsLeadership.get(electedId));
                // ... and CONFIRMED by a heartbeat received AFTER the refusal: the refusal is issued on
                // the stream path while the heartbeat that would flip peerAssertsLeadership can be up to
                // one interval away, so in the window between the elected node taking leadership and
                // its first leader heartbeat a just-recorded refusal still reads as "not leading". A
                // recompute in that window (a membership change, a watermark heartbeat from a third
                // node) promoted this node against a node that had just started serving — a third
                // leader under D10c, whose tail is discarded when it yields. Requiring a post-refusal
                // heartbeat that still denies leadership closes the window at the cost of at most one
                // heartbeat interval of delay for the genuine stalemate.
                ClusterMember electedMember = members.get(electedId);
                boolean refusalConfirmedByLaterHeartbeat = refusedAt != null && electedMember != null
                        && electedMember.lastHeartbeat() > refusedAt;
                Long electedWatermark = peerHighWatermark.get(electedId);
                long localApplied = safeLocalApplied();
                // STRICTLY ahead beyond the tolerance: with equal watermarks (every fresh boot is 0/0)
                // the affinity election must win — a transient refusal during an election dance must
                // never invert affinity. The genuine stalemate signature is local state the elected
                // node does not have — compared per topic when both advertise a vector (issue #178).
                // Revisão #178 (A2): the escape is for ONE node. With two non-elected survivors both
                // strictly ahead of a bootstrapping elected node, both used to take leadership at once
                // (a self-inflicted dual-leader resolved by D10c with a discarded tail). Only the best
                // non-elected candidate (newest state, then affinity) escapes, and never while some
                // eligible peer already asserts leadership — that peer is followed instead.
                if (recentRefusal && electedStillNotLeading && refusalConfirmedByLaterHeartbeat
                        && localAheadOfPeer(electedId)
                        && assertingLeaderPeer(localId) == null
                        && localIsBestEscapeCandidate(localId, electedId)) {
                    LOGGER.warning(() -> "[" + localId + "] Affinity-elected " + electedId + " refuses leadership and is not"
                            + " ahead (peer=" + electedWatermark + ", local=" + localApplied
                            + describePeerDivergence(electedId)
                            + "); taking leadership to break the leaderless stalemate (issue tems#9, D9)");
                    updateLeader(localId);
                    return;
                }
            }

            // ── Follow the serving leader until the affinity winner takes over ────────────────────────
            // A non-leading node whose affinity-elected candidate is NOT asserting leadership (it is a
            // returning/joining node still deferring behind the watermark gates, or one we have not even
            // heard from yet) must keep following the peer that IS serving. Adopting the candidate early
            // pointed this node's stream fetches and snapshot requests at a follower — refused as "not the
            // leader" — so with three members the cluster split into a stable three-way disagreement
            // (A follows the candidate, the candidate follows the highest-watermark node, the incumbent
            // keeps leading) and the candidate's catch-up starved. The candidate asserts leadership the
            // moment it reclaims (or the handback completes); the next heartbeat flips this node to it.
            // NON-LEADING nodes only: a serving leader never steps down through this path — a rival
            // asserting leadership against it is a dual-leader, resolved exclusively by D10c (affinity
            // order, observation debounce and the yield hook that arms the lineage resync).
            if (electedId != null && !electedId.equals(localId) && !isLeaderInternal(localId)
                    && !Boolean.TRUE.equals(peerAssertsLeadership.get(electedId))) {
                NodeId serving = assertingLeaderPeer(localId);
                if (serving != null && !serving.equals(electedId)) {
                    updateLeader(serving);
                    return;
                }
            }

            updateLeader(electedId);
        }
    }

    /**
     * Records that {@code node} refused to serve the replication stream because it is not the leader
     * (issue tems#9, D9). Fed by the replication layer when a fetch addressed to the locally adopted
     * leader answers {@code leaderUnavailable}. Triggers a leadership re-evaluation so the stalemate
     * escape in {@link #recomputeLeader()} can run while the refusal is fresh.
     *
     * @param node the peer that refused to serve as leader
     */
    public void noteLeaderRefusal(NodeId node) {
        leaderRefusalAtMs.put(node, Instant.now().toEpochMilli());
        reevaluateLeadership();
    }

    /**
     * Surfaces a follower's just-reported applied watermark into the peer-watermark map (merge-max)
     * and re-evaluates leadership immediately (issue tems#9, D10b). During a reclaim-quiesce the
     * candidate's heartbeat (up to one interval stale) would otherwise delay the step-down gate's
     * release after the candidate has already paired up against the frozen watermark.
     *
     * @param node      the reporting follower
     * @param watermark its applied watermark (ignored when negative)
     */
    public void noteFollowerWatermark(NodeId node, long watermark) {
        noteFollowerWatermark(node, watermark, null);
    }

    /**
     * Same as {@link #noteFollowerWatermark(NodeId, long)}, also merging the follower's per-topic
     * frontier vector (issue #178) when it reported one (per-topic max, never regressing).
     *
     * @param node           the reporting follower
     * @param watermark      its applied watermark (ignored when negative)
     * @param topicFrontiers its per-topic frontiers, or {@code null}/empty when not reported
     */
    public void noteFollowerWatermark(NodeId node, long watermark, Map<String, Long> topicFrontiers) {
        if (node == null || watermark < 0 || node.equals(transport.local().nodeId())) {
            return;
        }
        peerHighWatermark.merge(node, watermark, Math::max);
        if (topicFrontiers != null && !topicFrontiers.isEmpty()) {
            peerTopicFrontiers.merge(node, TopicFrontiers.of(topicFrontiers), (current, reported) -> {
                Map<String, Long> merged = new java.util.HashMap<>(current.byTopic());
                reported.byTopic().forEach((topic, frontier) -> merged.merge(topic, frontier, Math::max));
                return TopicFrontiers.of(merged);
            });
        }
        reevaluateLeadership();
    }

    /**
     * Wires the hook invoked when this node YIELDS a dual-leader resolution (issue tems#9, D10c) —
     * BEFORE the demotion: the replication layer arms the bootstrap resync that replaces the local
     * dual-window lineage (the tail this node produced while both leaders ran) with the winner's.
     *
     * @param hook the yield callback (runs on the heartbeat-processing thread)
     */
    public void setDualLeaderYieldHook(Runnable hook) {
        this.dualLeaderYieldHook = hook;
    }

    // ── Orchestrated affinity handback (issue tems#9, D11) ─────────────────────────────────────

    /**
     * Enables/disables the orchestrated affinity handback gating (issue tems#9, D11). When enabled,
     * {@link #recomputeLeader()} stops promoting a returning highest-affinity follower through the
     * watermark gates while a healthy incumbent exists; the ReplicationManager drives the explicit
     * snapshot handover instead. Wired from {@code ReplicationManager.start()}.
     *
     * @param enabled whether orchestrated handback mode is active
     */
    public void setAffinityHandbackMode(boolean enabled) {
        this.affinityHandbackMode = enabled;
    }

    /**
     * Wires the predicate that reports whether THIS node has an in-flight handover (issue tems#9, D11).
     * While it returns {@code true} the epoch re-stamp and the dual-leader resolver are suppressed so the
     * choreographed transition lands as a single clean leader change.
     *
     * @param supplier the in-progress predicate (must not be {@code null})
     */
    public void setHandoverInProgressSupplier(java.util.function.BooleanSupplier supplier) {
        this.handoverInProgressSupplier = Objects.requireNonNull(supplier, "supplier");
    }

    private boolean handoverInProgress() {
        try {
            return handoverInProgressSupplier.getAsBoolean();
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Whether the local node is the highest-affinity (priority, then NodeId) member among the currently
     * active members — the candidate predicate for initiating an affinity handback (issue tems#9, D11).
     *
     * @return {@code true} when the local node would win the affinity election
     */
    public boolean localIsPreferredLeader() {
        synchronized (leaderComputationLock) {
            NodeId localId = transport.local().nodeId();
            NodeId electedId = members.values().stream()
                    .filter(ClusterCoordinator::isLeaderCandidate)
                    .max(Comparator.comparingInt((ClusterMember m) -> m.info().priority())
                            .thenComparing(ClusterMember::id))
                    .map(ClusterMember::id)
                    .orElse(null);
            return localId.equals(electedId);
        }
    }

    /**
     * Whether a DISTINCT, active agreed leader currently exists (issue tems#9, D11) — the condition that
     * makes an affinity handback applicable (there is an incumbent to hand back FROM). When false (no
     * agreed leader, or the leader died and was evicted) the genuine-failover election path applies.
     *
     * @return {@code true} when a healthy distinct agreed leader exists
     */
    public boolean isAgreedLeaderHealthy() {
        NodeId current = leader.get();
        NodeId localId = transport.local().nodeId();
        // "Healthy" means SERVING: the adopted leader's latest heartbeat asserts leadership. A node
        // adopted during boot discovery that is itself a follower is not an incumbent to hand back
        // FROM — a request to it is aborted ("not an eligible leader") and costs a full cooldown.
        return current != null && !current.equals(localId) && isActiveMember(current)
                && Boolean.TRUE.equals(peerAssertsLeadership.get(current));
    }

    /**
     * CANDIDATE side of a handback (issue tems#9, D11): asserts local leadership at a term strictly above
     * {@code grantedEpoch} (the incumbent's epoch carried in the grant) so the candidate's first leader
     * heartbeat fences the incumbent cleanly, then promotes the local node. Returns the new epoch.
     *
     * <p>Defense in depth (M0 role safety): if the local node is leader-ineligible (role
     * {@link NodeInfo#ROLE_LEADER_INELIGIBLE}), this is a no-op that logs a warning instead of
     * assuming leadership — {@code ReplicationManager} already refuses to reach this point for an
     * ineligible candidate, but this guard holds the invariant regardless of caller.
     *
     * @param grantedEpoch the interim leader's epoch from the {@code HANDBACK_GRANT}
     * @return the local leader epoch after assuming leadership (strictly above {@code grantedEpoch}),
     *         or the current epoch, unchanged, if the local node is leader-ineligible
     */
    public long assumeLeadershipForHandback(long grantedEpoch) {
        synchronized (leaderComputationLock) {
            if (!transport.local().isLeaderEligible()) {
                LOGGER.warning(() -> "Ignoring handback leadership assumption: local node "
                        + transport.local().nodeId() + " is leader-ineligible (issue tems#9, D11)");
                return leaderEpoch.get();
            }
            leaderEpoch.updateAndGet(cur -> Math.max(cur, grantedEpoch));
            updateLeader(transport.local().nodeId()); // increments to >= grantedEpoch+1, fires listeners
            return leaderEpoch.get();
        }
    }

    /**
     * INTERIM-LEADER side of a handback (issue tems#9, D11): steps down to the candidate that completed
     * the cutover and adopts its term. Called on the {@code HANDBACK_COMPLETE} message for a prompt, clean
     * demotion; the candidate's higher-epoch heartbeats are the authoritative backstop.
     *
     * @param newLeader the candidate that took over
     * @param newEpoch  the candidate's asserted epoch
     */
    public void acceptHandbackWinner(NodeId newLeader, long newEpoch) {
        synchronized (leaderComputationLock) {
            if (newLeader == null) {
                return;
            }
            updateLeader(newLeader); // step down to the winner (was leader -> "stepped down")
            observeEpoch(newEpoch);  // now a follower -> adopt the winner's term (no re-stamp)
        }
    }

    /**
     * Tracks a peer's leadership assertion against the local one (issue tems#9, D10c). Both sides
     * run the SAME total-order comparison (priority, then NodeId — the election order), so exactly
     * one of the two yields: the lower-affinity node adopts the rival after
     * {@link #DUAL_LEADER_OBSERVATIONS_TO_RESOLVE} consecutive assertions and resyncs via the yield
     * hook; the higher-affinity node retains and only logs. A rival with unknown {@link NodeInfo}
     * (placeholder) is never resolved against — affinity must not be guessed.
     */
    private void observeLeaderAssertion(NodeId rival, boolean rivalAssertsLeadership) {
        if (handoverInProgress()) {
            // Orchestrated affinity handback (issue tems#9, D11): the choreographed transition demotes
            // the interim leader explicitly; do not let the brief assertion overlap during the handover
            // trip the dual-leader resolver. It stays the backstop once the handover clears.
            return;
        }
        if (!rivalAssertsLeadership || !isLeader()) {
            // Any heartbeat without the assertion breaks the CONSECUTIVE requirement.
            dualLeaderObservations.remove(rival);
            if (dualLeaderObservations.isEmpty()) {
                yieldingToDualLeader = false;
            }
            return;
        }
        ClusterMember member = members.get(rival);
        NodeInfo rivalInfo = member != null ? member.info() : null;
        if (rivalInfo == null || rivalInfo.port() <= 0) {
            rivalInfo = findPeerInfo(rival).orElse(null);
        }
        if (rivalInfo == null || rivalInfo.port() <= 0) {
            return; // unknown affinity — wait for the handshake before deciding anything
        }
        if (!outranks(rivalInfo, transport.local())) {
            // WE win the deterministic order: retain leadership and expect the rival to yield.
            long now = Instant.now().toEpochMilli();
            if (now - lastDualLeaderRetainWarnMs > 10_000L) {
                lastDualLeaderRetainWarnMs = now;
                LOGGER.warning(() -> "Dual-leader detected with " + rival
                        + "; retaining (higher affinity) and expecting the peer to yield"
                        + " (issue tems#9, D10c)");
            }
            return;
        }
        yieldingToDualLeader = true; // stop feeding the epoch ladder from the first observation
        int observations = dualLeaderObservations.merge(rival, 1, Integer::sum);
        if (observations >= DUAL_LEADER_OBSERVATIONS_TO_RESOLVE) {
            resolveDualLeaderYield(rival);
        }
    }

    /** The losing side of a confirmed dual-leader steps down to the rival and arms the resync. */
    private void resolveDualLeaderYield(NodeId rival) {
        synchronized (leaderComputationLock) {
            dualLeaderObservations.clear();
            if (!isLeader()) {
                yieldingToDualLeader = false;
                return; // already demoted by another path
            }
            LOGGER.warning(() -> "Dual-leader resolved: yielding leadership to higher-affinity "
                    + rival + " and resyncing from its lineage — the local dual-window tail is"
                    + " discarded (issue tems#9, D10c)");
            Runnable hook = dualLeaderYieldHook;
            if (hook != null) {
                try {
                    hook.run();
                } catch (RuntimeException e) {
                    LOGGER.log(java.util.logging.Level.SEVERE, "Dual-leader yield hook failed", e);
                }
            }
            updateLeader(rival);
            yieldingToDualLeader = false;
        }
    }

    /** Election-order affinity: delegates to the shared {@link LeadershipAffinity#outranks}. */
    private static boolean outranks(NodeInfo candidate, NodeInfo reference) {
        return LeadershipAffinity.outranks(candidate, reference);
    }

    /**
     * Single leadership-candidacy predicate: an active member with a known host that is also
     * eligible (does not carry role {@link NodeInfo#ROLE_LEADER_INELIGIBLE}). The host check excludes
     * the HEARTBEAT placeholder (blank host, e.g. {@code new NodeInfo(source, "", 0)} created for an
     * unknown source before its real {@link NodeInfo} is learned via handshake). Port {@code 0} alone
     * is not disqualifying: several legitimate test harnesses (and pair-mode/loopback setups) use
     * real, non-placeholder {@code NodeInfo}s with port {@code 0}. Centralizes the eligibility rule so
     * it is not scattered across the coordinator's several election/affinity points.
     */
    private static boolean isLeaderCandidate(ClusterMember m) {
        return m.isActive() && !m.info().host().isBlank() && m.info().isLeaderEligible();
    }

    /** True if {@code nodeId} is the current leader (internal, lock-held variant of {@link #isLeader()}). */
    private boolean isLeaderInternal(NodeId nodeId) {
        NodeId l = leader.get();
        return l != null && l.equals(nodeId);
    }

    /**
     * RECLAIM gate predicate: returns {@code true} if the local node's applied frontier has reached the
     * cluster's highest active-peer watermark (within {@link #syncReclaimLagThreshold}), i.e. it has
     * synced the newest known state and may lead. Sticky: once caught up this session it stays caught up
     * (the latch), so an incumbent that keeps producing a small tail afterwards does not livelock the
     * reclaim. Returns {@code true} when there is no peer ahead (lead-while-alone / AP).
     */
    private boolean isCaughtUpToCluster() {
        long maxPeer = maxActivePeerHighWatermark();
        if (maxPeer < 0) {
            // No active peer has reported a watermark — nothing ahead of us. The latch is only
            // meaningful while catching up to someone (A5): reset it, as the Javadoc promises.
            reclaimCaughtUpLatch = false;
            return true;
        }
        if (reclaimCaughtUpLatch) {
            return true; // already caught up this session; do not chase the incumbent's moving tail
        }
        long localApplied = safeLocalApplied();
        NodeId localId = transport.local().nodeId();
        if (Instant.now().toEpochMilli() < bootDiscoveryDeadlineMs
                && hasActivePeer(localId)
                && maxPeer < localApplied) {
            return false;
        }
        // Issue #178: "caught up" = no eligible active peer holds state we lack — decided PER TOPIC
        // against every peer that advertises a frontier vector (a peer behind on one topic and ahead
        // on another is resolved by the deterministic total tie-break), by scalar watermark against
        // peers that do not.
        if (aheadEligiblePeer(localId) == null) {
            reclaimCaughtUpLatch = true;
            return true;
        }
        return false;
    }

    /**
     * True when no other ACTIVE, LEADER-ELIGIBLE peer (the elected node aside) is a better D9 escape
     * candidate than the local node: none is ahead of it, and none with equal state outranks it by
     * affinity (revisão #178, A2). Every node evaluates the same order, so exactly one escapes.
     */
    private boolean localIsBestEscapeCandidate(NodeId localId, NodeId electedId) {
        NodeInfo local = transport.local();
        for (ClusterMember member : members.values()) {
            NodeId id = member.id();
            if (id.equals(localId) || id.equals(electedId) || !isRealActivePeer(member, localId)
                    || !member.info().isLeaderEligible()) {
                continue;
            }
            Long watermark = peerHighWatermark.get(id);
            if (watermark == null || watermark < 0L) {
                continue; // unheard or bootstrapping: it cannot lead nor be ahead
            }
            if (peerAheadOfLocal(id)) {
                return false;
            }
            if (!localAheadOfPeer(id) && LeadershipAffinity.outranks(member.info(), local)) {
                return false;
            }
        }
        return true;
    }

    /** " (topic=a<b, ...)" for the first eligible peer ahead of the local node, or "" when none. */
    private String describeAheadPeerDivergence(NodeId localId) {
        NodeId ahead = aheadEligiblePeer(localId);
        return ahead == null ? "" : describePeerDivergence(ahead);
    }

    /** " (peer <id>: topic=a<b, ...)" when both sides advertise a vector, or "" otherwise. */
    private String describePeerDivergence(NodeId peer) {
        TopicFrontiers local = localFrontiersOrNull();
        TopicFrontiers theirs = peerFrontiersOrNull(peer);
        if (local == null || theirs == null) {
            return "";
        }
        String divergence = local.describeDivergence(theirs, syncReclaimLagThreshold);
        return divergence.isEmpty() ? "" : " (peer " + peer + ": " + divergence + ")";
    }

    /**
     * STEP-DOWN gate predicate: returns {@code true} if {@code candidate} (a higher-affinity peer) is
     * still behind the LOCAL leader's watermark — so the incumbent must retain leadership rather than
     * hand off to a node that has not yet synced. When the candidate's watermark is unknown it is treated
     * as behind (conservative: do not hand off to a node whose state we cannot confirm).
     */
    private boolean candidateIsBehindLocalWatermark(NodeId candidate) {
        long localWatermark = leaderHighWatermarkSupplier.getAsLong();
        if (localWatermark < 0) {
            return false; // we do not know our own watermark — do not block the handoff on uncertainty
        }
        Long candWatermark = peerHighWatermark.get(candidate);
        if (candWatermark == null) {
            return true; // unheard candidate frontier — conservatively keep leadership until it reports
        }
        TopicFrontiers local = localFrontiersOrNull();
        TopicFrontiers theirs = peerFrontiersOrNull(candidate);
        if (local != null && theirs != null && candWatermark >= 0L) {
            // Issue #178: per topic — a candidate missing the last op of ONE topic is behind, whatever
            // its total says.
            return theirs.isBehind(local, syncReclaimLagThreshold, topicPriority);
        }
        return candWatermark < localWatermark - syncReclaimLagThreshold;
    }

    private long safeLocalApplied() {
        try {
            return localAppliedSupplier.getAsLong();
        } catch (RuntimeException e) {
            // A supplier failure must not crash the election; treat as "unknown frontier = behind".
            return Long.MIN_VALUE;
        }
    }

    /** The local per-topic frontier vector for heartbeats; empty on supplier failure (no vector). */
    private Map<String, Long> safeTopicFrontiers() {
        try {
            Map<String, Long> frontiers = topicFrontiersSupplier.get();
            return frontiers == null ? Map.of() : frontiers;
        } catch (RuntimeException e) {
            return Map.of();
        }
    }

    private boolean safeLocalLeadershipEligible() {
        try {
            return localLeadershipEligibilitySupplier.getAsBoolean();
        } catch (RuntimeException e) {
            // A supplier failure must not promote an unsafe node.
            return false;
        }
    }

    private boolean hasActivePeer(NodeId localId) {
        for (ClusterMember member : members.values()) {
            if (isRealActivePeer(member, localId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if some active, leader-eligible peer with a real listen port has not yet
     * reported a replication watermark. A leader-ineligible peer (a client) is ignored: it can never be
     * the incumbent ahead of us, so its unknown frontier is no reason to defer.
     */
    private boolean hasActivePeerWithUnknownWatermark(NodeId localId) {
        for (ClusterMember member : members.values()) {
            if (isRealActivePeer(member, localId) && member.info().isLeaderEligible()
                    && !peerHighWatermark.containsKey(member.id())) {
                return true;
            }
        }
        return false;
    }

    private NodeId highestAffinityActivePeer(NodeId localId) {
        return members.values().stream()
                .filter(m -> isRealActivePeer(m, localId) && isLeaderCandidate(m))
                .max(Comparator.comparingInt((ClusterMember m) -> m.info().priority())
                        .thenComparing(ClusterMember::id))
                .map(ClusterMember::id)
                .orElse(null);
    }

    private boolean isRealActivePeer(ClusterMember member, NodeId localId) {
        return !member.id().equals(localId) && member.isActive() && member.info().port() > 0;
    }

    /**
     * Returns {@code true} if some CONFIGURED peer (real listen port) has not yet reported a replication
     * watermark to us. Used ONLY inside the boot-discovery window to defer a fresh self-election until
     * every configured peer's state is known — any of them may be an incumbent ahead of us (this is the
     * crux of the pré-prod race: node-1 self-elects before it has even heard node-2). The window is
     * finite, so this can never stall HA: once it lapses, the node leads (lead-while-alone / AP). A peer
     * that has gone down still reports nothing, but the window bounds the wait. Portless gossip/discovery
     * placeholders are ignored.
     */
    private boolean hasConfiguredPeerWithUnknownWatermark(NodeId localId) {
        for (NodeInfo peer : transport.peers()) {
            if (peer.nodeId().equals(localId) || peer.port() <= 0) {
                continue; // self, or a portless gossip/discovery placeholder
            }
            if (!peerHighWatermark.containsKey(peer.nodeId())) {
                return true; // a configured peer we have not yet heard a watermark from
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if a configured peer of strictly higher leadership affinity than the
     * local node ({@code (priority, NodeId)}) is not currently an active member — i.e. the preferred
     * leader is expected by configuration but has not yet been discovered. Used only to gate boot
     * self-election deferral; portless gossip placeholders and leader-ineligible peers (they can
     * never become the preferred leader) are ignored.
     */
    private boolean outrankedByAbsentConfiguredPeer() {
        NodeInfo local = transport.local();
        int localPriority = local.priority();
        NodeId localId = local.nodeId();
        for (NodeInfo peer : transport.peers()) {
            if (peer.nodeId().equals(localId) || peer.port() <= 0 || !peer.isLeaderEligible()) {
                continue; // self, a portless gossip/discovery placeholder, or an ineligible peer
            }
            boolean outranks = peer.priority() > localPriority
                    || (peer.priority() == localPriority && peer.nodeId().compareTo(localId) > 0);
            if (!outranks) {
                continue;
            }
            ClusterMember member = members.get(peer.nodeId());
            if (member == null || !member.isActive()) {
                return true; // a higher-affinity configured peer is not (yet) active
            }
        }
        return false;
    }

    /** Returns {@code true} if {@code nodeId} is a currently active cluster member. */
    private boolean isActiveMember(NodeId nodeId) {
        ClusterMember member = members.get(nodeId);
        return member != null && member.isActive();
    }

    /** Number of currently active members, clients included (the population {@code minClusterSize} is about). */
    private long activeMemberCount() {
        return members.values().stream().filter(ClusterMember::isActive).count();
    }

    /**
     * Number of ACTIVE members that take part in the leadership majority (the "voters"): active members
     * that are leader-eligible (do not carry {@link NodeInfo#ROLE_LEADER_INELIGIBLE}). Compared against
     * {@link #requiredVoterMajority()}, which is computed over the same population, so a
     * leader-ineligible member (a client) neither helps nor hinders the majority. HEARTBEAT
     * placeholders (empty host, created before the handshake) are excluded as well: they never
     * appear in the denominator, so counting them here would inflate the numerator alone.
     */
    private long activeVoterCount() {
        return members.values().stream()
                .filter(m -> m.isActive() && !m.info().host().isBlank() && m.info().isLeaderEligible())
                .count();
    }

    /**
     * Leadership quorum predicate. Two independent requirements, each over its own population:
     * <ul>
     *   <li>{@code minClusterSize} counts ALL active members, clients included — it derives from the
     *       replication quorum ({@code NGridNode}: {@code min(replicationQuorum, peers + 1)}) and a
     *       leader-ineligible member is still a replica, so it legitimately satisfies it;</li>
     *   <li>the dynamic majority (non-pair mode) counts only the VOTERS, on both sides of the comparison —
     *       see {@link #requiredVoterMajority()}.</li>
     * </ul>
     */
    private boolean hasLeadershipQuorum() {
        if (activeMemberCount() < config.minClusterSize()) {
            return false;
        }
        // Pair mode: bypass the dynamic majority — leadership requires only minClusterSize active
        // members (typically 1), so a node that loses its peer still leads. Split-brain during a
        // partition is accepted and reconciled on reconnect by electing the highest NodeId
        // (recomputeLeader already picks max(NodeId); epoch fencing rejects the stale leader's writes).
        if (config.pairMode()) {
            return true;
        }
        return activeVoterCount() >= requiredVoterMajority();
    }

    /**
     * Dynamic majority of the VOTERS known to the transport. {@code transport.peers()} is backed by
     * {@code knownPeers} and already includes the local node. Only peers with a real listen port that are
     * leader-eligible count:
     * <ul>
     *   <li>discovery clients and gossip placeholders carry port 0 and must not inflate the required
     *       majority — an inflated denominator can make a healthy quorum fall short and stall the
     *       election;</li>
     *   <li>leader-ineligible members ({@link NodeInfo#ROLE_LEADER_INELIGIBLE}, e.g. short-lived clients)
     *       are excluded for the same reason. The transport now forgets a departed ephemeral member (its
     *       graceful LEAVE, or a disconnection longer than the transport's forget window), but a client
     *       can still linger in {@code knownPeers} until then, and would keep raising the majority the
     *       eligible survivors must reach — until a single failover, or even plain client churn, left the
     *       cluster leaderless. Clients can never lead, so they belong on neither side of the majority
     *       (see {@link #activeVoterCount()}): counting them on the active side alone would let a node
     *       partitioned away with its clients out-vote the eligible majority.</li>
     *   <li>leader-eligible members (voters) are never forgotten, not even after a graceful LEAVE:
     *       shrinking the denominator without consensus would weaken split-brain safety for the durable
     *       members. Decommissioning a voter for good (e.g. a drained storage that will not return) still
     *       requires operator action.</li>
     * </ul>
     */
    private int requiredVoterMajority() {
        long voters = transport.peers().stream()
                .filter(p -> p.port() > 0 && p.isLeaderEligible())
                .count();
        int totalExpected = (int) Math.max(1, voters);
        return (totalExpected / 2) + 1;
    }

    /**
     * Updates the leader of the cluster and notifies listeners of leadership
     * changes if necessary.
     *
     * This method changes the current leader to the specified {@code newLeaderId}.
     * If the leader changes, registered leadership listeners and leader election
     * listeners
     * are notified. It also determines if the local node's leadership status has
     * changed,
     * and updates the relevant listeners accordingly.
     *
     * @param newLeaderId the identifier of the new leader; may be {@code null} if
     *                    no leader is present.
     */
    private void updateLeader(NodeId newLeaderId) {
        NodeId previous = leader.getAndSet(newLeaderId);
        if (!Objects.equals(previous, newLeaderId)) {
            // Increment epoch on leader change
            NodeId localNodeId = transport.local().nodeId();
            boolean wasLeader = previous != null && previous.equals(localNodeId);
            boolean isNowLeader = newLeaderId != null && newLeaderId.equals(localNodeId);
            if (isNowLeader || wasLeader) {
                long newEpoch = leaderEpoch.incrementAndGet();
                persistEpoch(newEpoch);
                // Attributable and diagnosable: several nodes log into the same stream in in-process
                // clusters, and a step-down to LEADERLESS is only explainable with the quorum figures.
                String quorum = newLeaderId == null
                        ? " to <none>: activeVoters=" + activeVoterCount() + ", requiredVoterMajority="
                                + requiredVoterMajority() + ", activeMembers=" + activeMemberCount()
                                + ", minClusterSize=" + config.minClusterSize()
                        : " to " + newLeaderId;
                LOGGER.info(() -> "[" + localNodeId + "] Leader epoch changed: " + newEpoch
                        + (isNowLeader ? " (elected)" : " (stepped down" + quorum + ")"));
            }

            // Re-arm the lease the moment this node becomes leader. While it was a follower the
            // lease was never renewed (renewal only runs for the active leader, in
            // evictDeadMembers), so a node elected after a long follower period would otherwise
            // inherit a stale, already-expired lease and be stepped down on the very next eviction
            // cycle — that cycle checks lease expiry BEFORE renewing — leaving the cluster
            // leaderless immediately after a failover. Arm a fresh lease window at election time.
            if (isNowLeader && !wasLeader) {
                this.leaseExpiresAt = Instant.now().plus(config.leaseTimeout());
                // We are leading now: any future return as a follower must re-sync before reclaiming, so
                // arm the latch fresh for the NEXT session (it is only meaningful while catching up).
                reclaimCaughtUpLatch = false;
            }
            if (wasLeader && !isNowLeader) {
                // No longer a leader: dual-leader observations are only meaningful between two
                // asserting leaders (issue tems#9, D10c).
                dualLeaderObservations.clear();
                yieldingToDualLeader = false;
                // A5: a demoted node must prove it is caught up again before it may reclaim — the
                // latch earned in the previous catch-up says nothing about the new incumbent's tail.
                reclaimCaughtUpLatch = false;
            }

            if (!isNowLeader && !wasLeader) {
                // A follower changing whom it follows never bumps the epoch, yet it decides where its
                // stream fetches and snapshot requests go — log it so a wrong adoption is attributable.
                LOGGER.info(() -> "[" + localNodeId + "] Leader view changed: " + previous + " -> " + newLeaderId);
            }
            leadershipListeners.forEach(listener -> listener.onLeaderChanged(newLeaderId));

            // Notify LeaderElectionListener if local node's leadership status changed
            if (wasLeader != isNowLeader) {
                leaderElectionListeners.forEach(listener -> listener.onLeadershipChanged(isNowLeader, newLeaderId));
                // Announce the change right away instead of at the next periodic tick: peers decide whom
                // to follow, whether a refusal is stale (D9 escape) and whether a rival is a dual-leader
                // from the leader flag of the LATEST heartbeat, and a full interval of silence after a
                // promotion (3 s at the default cadence) let a third node promote itself against a node
                // that was already serving.
                announceLeadershipChange();
            }
        }
    }

    /** Sends an out-of-band heartbeat carrying the new local leadership flag (best-effort, async). */
    private void announceLeadershipChange() {
        if (!running) {
            return;
        }
        try {
            scheduler.schedule(this::sendHeartbeat, 0, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Scheduler shut down (closing): the periodic heartbeat is gone as well; nothing to announce.
        }
    }

    /**
     * Forces the current leader to step down. Clears the leader reference,
     * increments the epoch, and notifies all listeners.
     * This is called when the leader's lease expires, preventing an isolated
     * leader from accepting writes.
     */
    private void stepDown() {
        synchronized (leaderComputationLock) {
            NodeId previous = leader.getAndSet(null);
            if (previous != null && previous.equals(transport.local().nodeId())) {
                long newEpoch = leaderEpoch.incrementAndGet();
                persistEpoch(newEpoch);
                reclaimCaughtUpLatch = false; // A5: re-sync before any reclaim
                LOGGER.warning(() -> "Leader stepped down. New epoch: " + newEpoch);

                leadershipListeners.forEach(listener -> listener.onLeaderChanged(null));
                leaderElectionListeners.forEach(listener -> listener.onLeadershipChanged(false, null));
                notifyMembershipListeners();
                announceLeadershipChange();
            }
        }
    }

    /**
     * Returns {@code true} if this node is the current leader and holds a valid
     * (non-expired) lease. This method should be checked before accepting write
     * operations to prevent data divergence during network partitions.
     *
     * @return {@code true} if this node is leader with a valid lease
     */
    public boolean hasValidLease() {
        return isLeader() && Instant.now().isBefore(leaseExpiresAt);
    }

    @Override
    public void onPeerConnected(NodeInfo peer) {
        if (peer != null) {
            leavingUntilMs.remove(peer.nodeId()); // a new incarnation (or a reconnect) speaks again
        }
        members.compute(peer.nodeId(), (id, existing) -> {
            // Replace any placeholder member information (e.g. created from a heartbeat
            // before we learned host/port), or update when host/port changes (e.g. peer restarted
            // on a new port). NodeInfo.equals ignores roles/priority (identity is nodeId+host+port),
            // so also replace when either changed — e.g. a client that (re)connects after gaining
            // NodeInfo.ROLE_LEADER_INELIGIBLE, or a priority reconfiguration — so the coordinator's
            // affinity/eligibility decisions always see the peer's latest NodeInfo.
            if (existing == null || !existing.info().equals(peer)
                    || !existing.info().roles().equals(peer.roles())
                    || existing.info().priority() != peer.priority()) {
                return new ClusterMember(peer);
            }
            existing.touch();
            return existing;
        });
        recomputeLeader();
        notifyMembershipListeners();
    }

    @Override
    public void onPeerDisconnected(NodeId peerId) {
        if (members.get(peerId) == null) {
            return;
        }
        // A transport-level disconnect is not proof of death. During a join (a new node, a client) the
        // mesh reshuffles its connections — simultaneous-open tie-breaks and reconnects close sockets to
        // peers that are alive and heartbeating — and marking the member inactive on the spot made the
        // leader lose its majority for an instant, step down to leaderless, and hand the cluster to the
        // D9 escape / dual-leader machinery (whose resolution discards a leader's tail). Give the peer
        // the same grace the eviction path gives an overdue-but-reachable member: one heartbeat
        // interval to be connected again (directly or via a proxy) before it is declared gone. A dead
        // peer is still detected within that interval; a flapping one never leaves the membership.
        long graceMs = running ? config.heartbeatInterval().toMillis() : 0L;
        if (graceMs > 0) {
            try {
                scheduler.schedule(() -> confirmPeerDisconnect(peerId), graceMs, TimeUnit.MILLISECONDS);
                return;
            } catch (java.util.concurrent.RejectedExecutionException e) {
                // Scheduler shut down (closing): fall through to the immediate path.
            }
        }
        confirmPeerDisconnect(peerId);
    }

    /**
     * The transport forgot {@code peerId} for good (a departed ephemeral member: graceful LEAVE of a
     * leader-ineligible or portless peer, or one disconnected for too long). Unlike a disconnect, no
     * return is expected: the member is removed from the membership instead of lingering as inactive,
     * together with every per-peer state {@link #confirmPeerDisconnect(NodeId)} clears. Membership
     * listeners are notified only when the removed member was still active — an already-inactive
     * departure changes nothing they can observe (the ngrrd rebalancer debounces on these events). The
     * transport-level disconnect that may follow finds no member and is a no-op. A later direct
     * handshake from the same id is a new incarnation and re-enters through
     * {@link #onPeerConnected(NodeInfo)}.
     */
    @Override
    public void onPeerLeft(NodeId peerId) {
        if (peerId == null || peerId.equals(transport.local().nodeId())) {
            return;
        }
        ClusterMember removed = members.remove(peerId);
        if (removed == null) {
            return;
        }
        boolean wasActive = removed.isActive();
        forgetPeerState(peerId);
        LOGGER.info(() -> "[" + transport.local().nodeId() + "] Member " + peerId + " left the cluster"
                + (wasActive ? "" : " (already inactive)"));
        recomputeLeader();
        if (wasActive) {
            notifyMembershipListeners();
        }
    }

    /**
     * A leader-eligible peer announced first-hand that it is closing (LEAVE). It stays in the membership
     * and in the transport's known peers (a voter is never forgotten: the majority must not shrink
     * without consensus), but it is declared inactive right away instead of after the disconnect grace:
     * the grace exists for sockets that flap during a join, not for a peer that said it is gone. If it
     * comes back with the same id, its next heartbeat reactivates it ({@link ClusterMember#touch()}).
     * Decommissioning a voter for good (e.g. a drained storage that will not return) still requires
     * operator action.
     */
    @Override
    public void onPeerLeaving(NodeId peerId) {
        if (peerId == null || peerId.equals(transport.local().nodeId())) {
            return;
        }
        LOGGER.info(() -> "[" + transport.local().nodeId() + "] Leader-eligible member " + peerId
                + " announced its departure; confirming the disconnect without the grace");
        // A4: heartbeats of the leaving incarnation still in flight must not reactivate it.
        leavingUntilMs.put(peerId, Instant.now().toEpochMilli() + config.heartbeatTimeout().toMillis());
        confirmPeerDisconnect(peerId);
    }

    /** Clears the per-peer state kept for {@code peerId} (preferred leader, watermark, D9/D10c marks). */
    private void forgetPeerState(NodeId peerId) {
        NodeId preferred = preferredLeader.get();
        if (preferred != null && preferred.equals(peerId)) {
            preferredLeader.set(null);
            preferredLeaderUntilMs = 0L;
        }
        // Drop the disconnected peer's tracked watermark so a deferring higher-affinity node stops
        // waiting on a peer that is genuinely gone (lead-while-alone): with no active peer ahead, the
        // reclaim gate is a no-op and the node leads.
        peerHighWatermark.remove(peerId);
        peerTopicFrontiers.remove(peerId);
        lastHeartbeatStampMs.remove(peerId);
        // A gone peer can neither refuse nor assert leadership (issue tems#9, D9).
        leaderRefusalAtMs.remove(peerId);
        peerAssertsLeadership.remove(peerId);
        // A gone rival can no longer sustain a dual-leader (issue tems#9, D10c).
        dualLeaderObservations.remove(peerId);
        if (dualLeaderObservations.isEmpty()) {
            yieldingToDualLeader = false;
        }
    }

    /**
     * Declares {@code peerId} gone unless the transport reports it connected again (directly or via a
     * proxy) — the deferred half of {@link #onPeerDisconnected(NodeId)}.
     */
    private void confirmPeerDisconnect(NodeId peerId) {
        // NOTE — two liveness policies coexist on purpose (technical debt, to be unified): the heartbeat
        // eviction path (evictDeadMembers) still grants an overdue member the PROXY_REACHABLE_GRACE when
        // transport.isProxied() says a relay route exists, while this path — reached only after OUR
        // socket to the peer closed — trusts direct reachability alone. The eviction path protects a
        // member that is genuinely reachable only through a relay (direct link flapping); this one
        // must not let a gossip-only route keep a dead peer alive after its socket went away.
        // Direct reachability only. A PROXY route is gossip (the relay merely "knows" the peer) and the
        // transport demotes the route to proxy on the very first failed dial after the socket closed —
        // so a dead leader stayed "reachable via proxy" through a client that could not deliver either,
        // dodged this confirmation, and was only evicted by the heartbeat-timeout path plus its
        // proxy-reachable grace (~3× heartbeatTimeout): every failover took ~25 s instead of one
        // heartbeat interval. A live peer whose direct link merely flapped is re-activated by its next
        // heartbeat (touch), whichever route delivers it.
        if (transport.isConnected(peerId)) {
            LOGGER.fine(() -> "Peer " + peerId + " reconnected within the disconnect grace; membership kept");
            return;
        }
        ClusterMember member = members.get(peerId);
        if (member != null && member.isActive()) {
            member.markInactive();
            forgetPeerState(peerId);
            recomputeLeader();
            notifyMembershipListeners();
        }
    }

    @Override
    public void onMessage(ClusterMessage message) {
        if (message.type() == MessageType.HEARTBEAT) {
            HeartbeatPayload payload = message.payload(HeartbeatPayload.class);
            NodeId source = message.source();
            if (transport.isDeparted(source)) {
                // A heartbeat of a member the transport just forgot (read before its LEAVE, dispatched
                // after it): it must not re-create the member it removed. The member's new incarnation
                // lifts the tombstone with its own handshake before its heartbeats arrive.
                return;
            }
            if (!source.equals(transport.local().nodeId())) {
                // A4: a voter that said it is leaving stays quiet until it reconnects or the window lapses.
                Long leavingUntil = leavingUntilMs.get(source);
                if (leavingUntil != null) {
                    if (Instant.now().toEpochMilli() < leavingUntil) {
                        LOGGER.fine(() -> "Ignoring heartbeat from departing voter " + source);
                        return;
                    }
                    leavingUntilMs.remove(source);
                }
                // A3: drop a heartbeat older than the newest one already applied from this peer.
                long stamp = payload.epochMilli();
                Long newest = lastHeartbeatStampMs.get(source);
                if (newest != null && stamp < newest
                        && newest - stamp <= config.heartbeatTimeout().toMillis()) {
                    LOGGER.fine(() -> "Ignoring out-of-order heartbeat from " + source
                            + " (stamp " + stamp + " < " + newest + ")");
                    return;
                }
                lastHeartbeatStampMs.put(source, stamp);
            }

            // FENCING: Reject heartbeats from leaders with stale epochs.
            // This prevents an ex-leader that stepped down from being
            // re-accepted as a valid leader after reconnecting.
            long heartbeatEpoch = payload.leaderEpoch();
            // Dual-leader detection (issue tems#9, D10c) BEFORE epoch convergence: when both sides
            // assert leadership, the lower-affinity one must ADOPT the rival's term instead of
            // re-stamping above it, and yield after the debounce — running first keeps the loser
            // from feeding the epoch ladder and lands the post-yield observeEpoch on the follower
            // (adopt) path.
            boolean fromPeer = !source.equals(transport.local().nodeId());
            if (fromPeer) {
                observeLeaderAssertion(source, payload.leader());
            }
            // Converge the cluster term from every peer heartbeat BEFORE fencing, so a leader
            // whose persisted epoch regressed re-learns the highest term any node has seen and
            // re-stamps above it (re-establishing itself as the legitimate, accepted leader).
            observeEpoch(heartbeatEpoch);

            // Record membership and watermarks before epoch fencing. A stale-epoch heartbeat must not be
            // accepted as leadership, but its sender can still be the incumbent with newer replicated
            // state; the sync-before-reclaim gate needs that watermark to correct a premature reclaim.
            boolean watermarkAdvanced = false;
            if (!source.equals(transport.local().nodeId())) {
                long reported = payload.leaderHighWatermark();
                // Record the peer's watermark even when it is -1 (a node with a pending relay bootstrap
                // advertises -1 to say "my state is not safe to serve yet"). Recording -1 keeps "heard but
                // behind/bootstrapping" DISTINCT from "never heard": the unknown-watermark deferral
                // (hasActivePeerWithUnknownWatermark) must not treat a bootstrapping peer as a possible
                // incumbent ahead of us — otherwise a whole-cluster unclean restart deadlocks (every node
                // defers to a peer that is itself bootstrapping). The first heartbeat (prev == null) also
                // triggers a recompute, so the AP escape fires promptly once the boot window lapses.
                // Scale-safe: maxActivePeerHighWatermark / highestWatermarkActivePeer compare > -1, so a -1
                // peer never counts as "ahead" or as a viable sync source; candidateIsBehindLocalWatermark
                // reads -1 as "behind" (gate B keeps leadership), unchanged.
                if (reported >= -1) {
                    Long prev = peerHighWatermark.put(source, reported);
                    watermarkAdvanced = prev == null || reported != prev;
                    // Issue #178: record the per-topic frontier vector next to the scalar (an empty
                    // vector = "none advertised": older peer or bootstrap gate → scalar fallback).
                    TopicFrontiers reportedFrontiers = TopicFrontiers.of(payload.topicFrontiers());
                    TopicFrontiers prevFrontiers = reportedFrontiers.isEmpty()
                            ? peerTopicFrontiers.remove(source)
                            : peerTopicFrontiers.put(source, reportedFrontiers);
                    if (!Objects.equals(prevFrontiers, reportedFrontiers)
                            && !(prevFrontiers == null && reportedFrontiers.isEmpty())) {
                        watermarkAdvanced = true;
                    }
                    if (watermarkAdvanced && peerAheadOfLocal(source)) {
                        reclaimCaughtUpLatch = false;
                    }
                }
            }
            boolean[] isNewMember = {false};
            members.compute(source, (id, existing) -> {
                if (existing != null) {
                    if (existing.info().host().isBlank()) {
                        // Upgrade a HEARTBEAT placeholder as soon as the transport knows the peer's real
                        // NodeInfo (learned by gossip). A heartbeat can arrive through a proxy before the
                        // direct handshake, and a placeholder (blank host) is not a leadership candidate:
                        // left in place it made this node ignore the serving leader in every election,
                        // defer to a watermark-tie follower and fetch the stream from it forever.
                        Optional<NodeInfo> real = findPeerInfo(source).filter(info -> !info.host().isBlank());
                        if (real.isPresent()) {
                            isNewMember[0] = true;
                            return new ClusterMember(real.get());
                        }
                    }
                    existing.touch();
                    return existing;
                }
                isNewMember[0] = true;
                return new ClusterMember(
                        findPeerInfo(source).orElseGet(() -> new NodeInfo(source, "", 0)));
            });

            // FENCING by leader IDENTITY: the agreed leader (deterministic max NodeId) is
            // authoritative — adopt its term even if it momentarily appears lower than a ghost term
            // we remember, so a converged leader is never fenced into a permanent freeze. Reject a
            // stale epoch only from a source that is NOT the agreed leader (a partitioned ex-leader
            // still broadcasting); leadership itself is still decided by NodeId, not by this filter.
            NodeId currentLeader = leader.get();
            boolean fromAgreedLeader = currentLeader != null && currentLeader.equals(source);
            boolean assertionChanged = false;
            if (!fromAgreedLeader && heartbeatEpoch > 0 && heartbeatEpoch < trackedLeaderEpoch) {
                LOGGER.fine(() -> String.format(
                        "Ignoring heartbeat from non-leader %s with stale epoch %d (current: %d)",
                        source, heartbeatEpoch, trackedLeaderEpoch));
                // A fenced sender (a partitioned ex-leader still broadcasting a stale term) is NOT a
                // serving leader, whatever its heartbeat claims: drop any assertion mark so followers
                // never adopt it through assertingLeaderPeer / deferralFollowTarget.
                if (fromPeer && Boolean.TRUE.equals(peerAssertsLeadership.remove(source))) {
                    assertionChanged = true;
                }
                if (isNewMember[0] || watermarkAdvanced || assertionChanged) {
                    recomputeLeader();
                }
                return;
            }
            boolean confirmsRefusal = false;
            if (fromPeer) {
                // Record the assertion only for heartbeats that passed the epoch fence. A peer starting
                // or stopping to assert leadership changes whom a follower should adopt
                // (assertingLeaderPeer / deferralFollowTarget): recompute below.
                Boolean previousAssertion = peerAssertsLeadership.put(source, payload.leader());
                assertionChanged = previousAssertion == null || previousAssertion != payload.leader();
                if (payload.leader()) {
                    // The peer is leading: any refusal it issued while deferring is stale (D9 escape).
                    leaderRefusalAtMs.remove(source);
                } else {
                    // A heartbeat that still denies leadership AFTER a recorded refusal confirms the
                    // stalemate signal (D9 escape): recompute so the escape can run on it.
                    confirmsRefusal = leaderRefusalAtMs.containsKey(source);
                }
            }
            if (fromAgreedLeader) {
                trackedLeaderHighWatermark = payload.leaderHighWatermark();
                trackedLeaderEpoch = payload.leaderEpoch();
            }
            if (isNewMember[0] || watermarkAdvanced || assertionChanged || confirmsRefusal) {
                // A new member, or a peer whose watermark changed: recompute so the watermark gate can
                // (a) defer our reclaim while a peer is ahead, or (b) release the incumbent's step-down
                // guard the moment a higher-affinity candidate has caught up to our state.
                recomputeLeader();
            }
        }
    }

    private Optional<NodeInfo> findPeerInfo(NodeId id) {
        return transport.peers().stream().filter(info -> info.nodeId().equals(id)).findFirst();
    }

    @Override
    public void close() throws IOException {
        stop();
        scheduler.shutdownNow();
    }
}
