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
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Item 2 (coverage) — {@link ClusterCoordinator#onPeerConnected(NodeInfo)} must refresh a member's
 * {@link NodeInfo} (not just touch its heartbeat) when roles/priority change for the SAME identity
 * (nodeId+host+port unchanged, so {@code NodeInfo.equals} stays {@code true}) — e.g. a peer that
 * reconnects after gaining {@link NodeInfo#ROLE_LEADER_INELIGIBLE}. The refreshed role must
 * immediately affect the next leader election, driven directly through the coordinator's public
 * API (no transport/heartbeat harness needed).
 */
class OnPeerConnectedRoleUpdateTest {

    private static final NodeId LOCAL_ID = NodeId.of("local");
    private static final NodeId PEER_ID = NodeId.of("peer");

    private ClusterCoordinator coord;
    private ScheduledExecutorService sched;

    @AfterEach
    void tearDown() {
        if (coord != null) {
            try {
                coord.close();
            } catch (Exception ignored) {
            }
        }
        if (sched != null) {
            sched.shutdownNow();
        }
    }

    @Test
    void peerBecomingLeaderIneligibleViaOnPeerConnectedLosesLeadershipImmediately() {
        NodeInfo localInfo = new NodeInfo(LOCAL_ID, "127.0.0.1", 1, Set.of(), 10);
        FakeTransport transport = new FakeTransport(localInfo);

        ClusterCoordinatorConfig cfg = ClusterCoordinatorConfig.of(
                        Duration.ofMillis(200), Duration.ofSeconds(60), Duration.ofSeconds(60), 1, null)
                .withPairMode(true)
                .withBootDiscoveryWindow(Duration.ZERO);
        sched = Executors.newSingleThreadScheduledExecutor();
        coord = new ClusterCoordinator(transport, cfg, sched);
        coord.start();

        // Peer connects, eligible, higher priority: it must be elected leader.
        NodeInfo eligiblePeer = new NodeInfo(PEER_ID, "127.0.0.1", 2, Set.of(), 100);
        coord.onPeerConnected(eligiblePeer);

        assertEquals(PEER_ID, coord.leaderInfo().map(NodeInfo::nodeId).orElse(null),
                "the higher-priority eligible peer should be elected leader");

        // Same identity (nodeId/host/port unchanged -> NodeInfo.equals still true), but now
        // leader-ineligible: onPeerConnected must still refresh the member's roles.
        NodeInfo nowIneligiblePeer = new NodeInfo(PEER_ID, "127.0.0.1", 2,
                Set.of(NodeInfo.ROLE_LEADER_INELIGIBLE), 100);
        coord.onPeerConnected(nowIneligiblePeer);

        NodeInfo peerAsSeenByCoordinator = coord.activeMembers().stream()
                .filter(n -> n.nodeId().equals(PEER_ID))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the peer should still be an active member"));
        assertEquals(Set.of(NodeInfo.ROLE_LEADER_INELIGIBLE), peerAsSeenByCoordinator.roles(),
                "the member's roles should have been refreshed by onPeerConnected");

        assertEquals(LOCAL_ID, coord.leaderInfo().map(NodeInfo::nodeId).orElse(null),
                "once the peer becomes leader-ineligible, the local (lower-priority but eligible)"
                        + " node should be elected instead");
    }

    /** Minimal no-op {@link Transport}: this test drives the coordinator directly, not via wire messages. */
    private static final class FakeTransport implements Transport {
        private final NodeInfo local;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final ConcurrentHashMap<NodeId, Boolean> connected = new ConcurrentHashMap<>();

        FakeTransport(NodeInfo local) {
            this.local = local;
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
            return Boolean.TRUE.equals(connected.get(nodeId));
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
