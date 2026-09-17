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

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Three-member convergence rules of {@link ClusterCoordinator}: a node that is not leading must
 * adopt the peer that actually ASSERTS leadership — the one serving the stream and the snapshots —
 * rather than the affinity winner that has not taken over yet (it is still deferring behind the
 * watermark gates) or a watermark-tie follower. Without these rules a three-node cluster split into
 * a stable three-way leader disagreement and the catching-up node starved on refused fetches.
 */
class ServingLeaderAdoptionTest {

    private static final NodeId LOW = NodeId.of("node-1");     // lowest affinity
    private static final NodeId MID = NodeId.of("node-2");     // the serving incumbent
    private static final NodeId HIGH = NodeId.of("node-3");    // highest affinity, joins behind

    private final List<AutoCloseable> closeables = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * R1 — third-party follower: the affinity winner ({@code HIGH}) is active but NOT asserting
     * leadership (it is deferring), while the incumbent ({@code MID}) asserts it. The local follower
     * must keep following the incumbent instead of electing the non-serving affinity winner.
     */
    @Test
    void followerKeepsTheServingIncumbentWhileTheAffinityWinnerIsNotLeading() throws Exception {
        Harness h = harness(LOW, List.of(MID, HIGH));
        h.coord.setReplicationProgressGate(() -> 30L, 0L);
        h.start();
        h.connectAll();
        h.startPeerHeartbeats(MID, 3L, 30L, true);   // incumbent: serving
        h.startPeerHeartbeats(HIGH, 3L, 0L, false);  // affinity winner: behind, deferring

        awaitLeader(h, MID);
        long deadline = System.currentTimeMillis() + 1_000;
        while (System.currentTimeMillis() < deadline) {
            assertEquals(MID, h.coord.leaderInfo().map(NodeInfo::nodeId).orElse(null),
                    "a follower must not adopt an affinity winner that is not asserting leadership");
            Thread.sleep(50);
        }

        // The winner catches up and takes over: its heartbeats now assert leadership. Follow it.
        h.stopPeerHeartbeats(HIGH);
        h.stopPeerHeartbeats(MID);
        h.startPeerHeartbeats(HIGH, 4L, 30L, true);
        h.startPeerHeartbeats(MID, 4L, 30L, false);
        awaitLeader(h, HIGH);
    }

    /**
     * R2 — deferring node: the local node would win by affinity but is behind by watermark, so it
     * defers. Two peers advertise the same (higher) watermark; only one of them asserts leadership.
     * The deferring node must adopt the asserting one (the only node that will serve its catch-up),
     * not the watermark-tie follower.
     */
    @Test
    void deferringNodeAdoptsTheAssertingPeerOverAWatermarkTieFollower() throws Exception {
        Harness h = harness(HIGH, List.of(LOW, MID));
        h.coord.setReplicationProgressGate(() -> 0L, 0L); // local behind (0 < 30)
        h.start();
        h.connectAll();
        h.startPeerHeartbeats(LOW, 3L, 30L, false);  // follower with the same watermark
        h.startPeerHeartbeats(MID, 3L, 30L, true);   // serving incumbent

        awaitLeader(h, MID);
        long deadline = System.currentTimeMillis() + 1_000;
        while (System.currentTimeMillis() < deadline) {
            assertEquals(MID, h.coord.leaderInfo().map(NodeInfo::nodeId).orElse(null),
                    "a deferring node must follow the peer that asserts leadership");
            assertFalse(h.coord.isLeader(), "still behind: must not reclaim");
            Thread.sleep(50);
        }
    }

    /**
     * Guard — serving leader: the "follow the asserting peer" rule is for NON-leading nodes only. A
     * leader that sees a caught-up, higher-affinity candidate that does not assert yet AND a
     * lower-affinity peer asserting leadership (a dual-leader rival) must never step down to that
     * rival through this path: a rival is resolved exclusively by D10c (affinity order, observation
     * debounce, yield hook). Stepping down by affinity to the candidate is the ordinary handoff.
     */
    @Test
    void servingLeaderNeverStepsDownToALowerAffinityAssertingPeer() throws Exception {
        Harness h = harness(MID, List.of(LOW, HIGH), Duration.ZERO);
        h.coord.setReplicationProgressGate(() -> 30L, 0L);
        h.coord.setLeaderHighWatermarkSupplier(() -> 30L);
        h.start();
        h.connect(LOW);
        h.startPeerHeartbeats(LOW, 3L, 30L, false);  // plain follower
        awaitLeader(h, MID);                          // the local node leads and serves

        // A lower-affinity rival starts asserting leadership while a caught-up higher-affinity
        // candidate joins without asserting yet.
        h.stopPeerHeartbeats(LOW);
        h.startPeerHeartbeats(LOW, 4L, 30L, true);
        h.connect(HIGH);
        h.startPeerHeartbeats(HIGH, 4L, 30L, false);

        long deadline = System.currentTimeMillis() + 1_000;
        while (System.currentTimeMillis() < deadline) {
            NodeId observed = h.coord.leaderInfo().map(NodeInfo::nodeId).orElse(null);
            assertFalse(LOW.equals(observed),
                    "a serving leader must never step down to a lower-affinity asserting rival");
            Thread.sleep(50);
        }
    }

    // ---- harness (three-member variant of the LeaderlessStalemateEscapeTest loopback) ----

    private Harness harness(NodeId localId, List<NodeId> peerIds) {
        return harness(localId, peerIds, Duration.ofSeconds(2));
    }

    private Harness harness(NodeId localId, List<NodeId> peerIds, Duration bootWindow) {
        Harness h = new Harness(localId, peerIds, bootWindow);
        closeables.add(h);
        return h;
    }

    private static void awaitLeader(Harness h, NodeId expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (h.coord.leaderInfo().map(NodeInfo::nodeId).filter(expected::equals).isPresent()) {
                return;
            }
            Thread.sleep(25);
        }
        fail("leader did not converge to " + expected
                + " (observed=" + h.coord.leaderInfo().map(NodeInfo::nodeId).orElse(null) + ")");
    }

    private static final class Harness implements AutoCloseable {
        final LoopbackTransport transport;
        final ClusterCoordinator coord;
        final ScheduledExecutorService sched;
        final Map<NodeId, ScheduledFuture<?>> peerTasks = new ConcurrentHashMap<>();

        Harness(NodeId localId, List<NodeId> peerIds, Duration bootWindow) {
            NodeInfo localInfo = new NodeInfo(localId, "127.0.0.1", 1, Collections.emptySet(), 0);
            List<NodeInfo> peers = new ArrayList<>();
            int port = 2;
            for (NodeId peerId : peerIds) {
                peers.add(new NodeInfo(peerId, "127.0.0.1", port++, Collections.emptySet(), 0));
            }
            this.transport = new LoopbackTransport(localInfo, peers);
            // A boot-discovery window keeps the local node from self-electing before its configured
            // peers have reported (the node is JOINING a running cluster, not booting alone).
            ClusterCoordinatorConfig cfg = ClusterCoordinatorConfig.of(
                    Duration.ofMillis(150), Duration.ofMillis(600), Duration.ofSeconds(60), 1, null)
                    .withPairMode(true)
                    .withBootDiscoveryWindow(bootWindow);
            this.sched = Executors.newScheduledThreadPool(3);
            this.coord = new ClusterCoordinator(transport, cfg, sched);
        }

        void start() {
            transport.start();
            coord.start();
        }

        /** Peers become active members through the handshake, as in the real transport. */
        void connect(NodeId peerId) {
            transport.peersOnly().stream().filter(p -> p.nodeId().equals(peerId)).forEach(coord::onPeerConnected);
        }

        void connectAll() {
            transport.peersOnly().forEach(coord::onPeerConnected);
        }

        void startPeerHeartbeats(NodeId source, long epoch, long highWatermark, boolean assertsLeadership) {
            peerTasks.put(source, sched.scheduleAtFixedRate(() -> coord.onMessage(
                    ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", source, null,
                            HeartbeatPayload.now(highWatermark, epoch, assertsLeadership))),
                    0, 100, TimeUnit.MILLISECONDS));
        }

        void stopPeerHeartbeats(NodeId source) {
            ScheduledFuture<?> task = peerTasks.remove(source);
            if (task != null) {
                task.cancel(true);
            }
        }

        @Override
        public void close() {
            peerTasks.values().forEach(t -> t.cancel(true));
            try {
                coord.close();
            } catch (Exception ignored) {
            }
            try {
                transport.close();
            } catch (Exception ignored) {
            }
            sched.shutdownNow();
        }
    }

    private static final class LoopbackTransport implements Transport {
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();

        LoopbackTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
        }

        List<NodeInfo> peersOnly() {
            return peers;
        }

        @Override
        public void start() {
        }

        @Override
        public NodeInfo local() {
            return local;
        }

        @Override
        public Collection<NodeInfo> peers() {
            List<NodeInfo> all = new ArrayList<>();
            all.add(local);
            all.addAll(peers);
            return all;
        }

        @Override
        public void addListener(TransportListener l) {
            listeners.add(l);
        }

        @Override
        public void removeListener(TransportListener l) {
            listeners.remove(l);
        }

        @Override
        public void broadcast(ClusterMessage m) {
        }

        @Override
        public void send(ClusterMessage m) {
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage m) {
            CompletableFuture<ClusterMessage> f = new CompletableFuture<>();
            f.completeExceptionally(new UnsupportedOperationException("not used"));
            return f;
        }

        @Override
        public boolean isConnected(NodeId nodeId) {
            return peers.stream().anyMatch(p -> p.nodeId().equals(nodeId));
        }

        @Override
        public boolean isReachable(NodeId nodeId) {
            return isConnected(nodeId);
        }

        @Override
        public void addPeer(NodeInfo peer) {
        }

        @Override
        public void close() throws IOException {
        }
    }
}
