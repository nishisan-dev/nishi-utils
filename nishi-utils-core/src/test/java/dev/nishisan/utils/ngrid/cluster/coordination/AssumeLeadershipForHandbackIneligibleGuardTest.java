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
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Item 5 (defense in depth) — {@link ClusterCoordinator#assumeLeadershipForHandback(long)} must be a
 * no-op when the local node is leader-ineligible (role {@link NodeInfo#ROLE_LEADER_INELIGIBLE}),
 * even though {@code ReplicationManager} should never call it for an ineligible candidate through
 * the normal orchestrated-handback protocol (the incumbent's {@code handleHandbackRequest} already
 * refuses to grant to an ineligible candidate). This is a direct, coordinator-level test of the
 * last-resort guard, independent of the full handback wire protocol.
 */
class AssumeLeadershipForHandbackIneligibleGuardTest {

    @Test
    void assumeLeadershipForHandbackIsIgnoredWhenLocalIsLeaderIneligible() {
        NodeInfo localInfo = new NodeInfo(NodeId.of("local"), "127.0.0.1", 1,
                Set.of(NodeInfo.ROLE_LEADER_INELIGIBLE), 100);
        FakeTransport transport = new FakeTransport(localInfo);

        ClusterCoordinatorConfig cfg = ClusterCoordinatorConfig.of(
                        Duration.ofMillis(200), Duration.ofSeconds(60), Duration.ofSeconds(60), 1, null)
                .withPairMode(true)
                .withBootDiscoveryWindow(Duration.ZERO);
        ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
        ClusterCoordinator coord = new ClusterCoordinator(transport, cfg, sched);
        try {
            coord.start();
            assertFalse(coord.isLeader(),
                    "a leader-ineligible node alone must never self-elect at start");
            long epochBefore = coord.getLeaderEpoch();

            long returned = coord.assumeLeadershipForHandback(epochBefore + 5);

            assertEquals(epochBefore, returned,
                    "the epoch must stay unchanged when the ineligibility guard rejects the assumption");
            assertFalse(coord.isLeader(),
                    "a leader-ineligible node must never become leader via a handback assumption");
        } finally {
            try {
                coord.close();
            } catch (Exception ignored) {
            }
            sched.shutdownNow();
        }
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
