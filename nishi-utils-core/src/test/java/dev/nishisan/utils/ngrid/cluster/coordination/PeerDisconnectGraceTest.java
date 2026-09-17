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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A transport-level disconnect gets one heartbeat interval of grace before the peer is declared gone.
 * During a join the mesh reshuffles connections (simultaneous-open tie-breaks, reconnects) and closes
 * sockets to peers that are alive; declaring them inactive on the spot made a leader lose its majority
 * for an instant and step down — the trigger of the D9-escape / dual-leader cascade under load.
 */
class PeerDisconnectGraceTest {

    private static final NodeId LOCAL = NodeId.of("node-2");
    private static final NodeId PEER = NodeId.of("node-1");

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

    @Test
    void disconnectOfAPeerThatReconnectsWithinTheGraceKeepsItAnActiveMember() throws Exception {
        Harness h = harness();
        h.start();
        h.transport.connected.set(true);
        h.coord.onPeerConnected(h.transport.peerInfo);
        h.startPeerHeartbeats(PEER, 1L, 0L);
        awaitTrue(() -> h.coord.getActiveMembersCount() == 2, "peer active");

        // The socket flaps but the transport reports the peer connected again before the grace
        // elapses (a duplicate-connection tie-break, a reconnect): membership must not blink.
        h.coord.onPeerDisconnected(PEER);
        long deadline = System.currentTimeMillis() + 700; // > heartbeatInterval (150 ms) grace
        while (System.currentTimeMillis() < deadline) {
            assertEquals(2, h.coord.getActiveMembersCount(),
                    "a peer that is connected again within the grace must stay an active member");
            Thread.sleep(25);
        }
    }

    @Test
    void disconnectOfAPeerThatStaysGoneIsConfirmedAfterTheGrace() throws Exception {
        Harness h = harness();
        h.start();
        h.transport.connected.set(true);
        h.coord.onPeerConnected(h.transport.peerInfo);
        awaitTrue(() -> h.coord.getActiveMembersCount() == 2, "peer active");

        h.transport.connected.set(false); // genuinely gone: no direct connection, no proxy
        h.coord.onPeerDisconnected(PEER);
        assertEquals(2, h.coord.getActiveMembersCount(), "not declared gone on the spot");
        awaitTrue(() -> h.coord.getActiveMembersCount() == 1, "peer declared inactive after the grace");
    }

    // ---- harness ----

    private Harness harness() {
        Harness h = new Harness();
        closeables.add(h);
        return h;
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition, String what)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        fail("condition not met in time: " + what);
    }

    private static final class Harness implements AutoCloseable {
        final LoopbackTransport transport;
        final ClusterCoordinator coord;
        final ScheduledExecutorService sched;
        volatile ScheduledFuture<?> peerTask;

        Harness() {
            NodeInfo localInfo = new NodeInfo(LOCAL, "127.0.0.1", 1, Collections.emptySet(), 0);
            NodeInfo peerInfo = new NodeInfo(PEER, "127.0.0.1", 2, Collections.emptySet(), 0);
            this.transport = new LoopbackTransport(localInfo, peerInfo);
            ClusterCoordinatorConfig cfg = ClusterCoordinatorConfig.of(
                    Duration.ofMillis(150), Duration.ofMillis(600), Duration.ofSeconds(60), 1, null)
                    .withPairMode(true)
                    .withBootDiscoveryWindow(Duration.ZERO);
            this.sched = Executors.newScheduledThreadPool(2);
            this.coord = new ClusterCoordinator(transport, cfg, sched);
        }

        void start() {
            transport.start();
            coord.start();
        }

        void startPeerHeartbeats(NodeId source, long epoch, long highWatermark) {
            peerTask = sched.scheduleAtFixedRate(() -> coord.onMessage(
                    ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", source, null,
                            HeartbeatPayload.now(highWatermark, epoch, false))),
                    0, 100, TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() {
            if (peerTask != null) {
                peerTask.cancel(true);
            }
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
        final NodeInfo peerInfo;
        final AtomicBoolean connected = new AtomicBoolean(false);
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();

        LoopbackTransport(NodeInfo local, NodeInfo peerInfo) {
            this.local = local;
            this.peerInfo = peerInfo;
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
            return List.of(local, peerInfo);
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
            return nodeId.equals(peerInfo.nodeId()) && connected.get();
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
