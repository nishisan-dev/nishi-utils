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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;

/**
 * Revisão #178 (A2): o escape do impasse D9 é de UM nó só. Dois coordinators reais (B e C) num
 * barramento compartilhado, ambos à frente do eleito por afinidade (E, que anuncia {@code -1} — em
 * bootstrap — e recusa servir o stream). Antes, os dois escapavam ao mesmo tempo e o cluster ficava
 * com dois líderes até o D10c descartar a cauda de um deles. Agora só o melhor candidato não-eleito
 * (estado mais novo, depois afinidade) escapa, e o outro o segue.
 */
class StalemateEscapeSingleWinnerTest {

    private static final NodeId E = NodeId.of("node-e"); // afinidade máxima, em bootstrap
    private static final NodeId B = NodeId.of("node-b"); // prio 100
    private static final NodeId C = NodeId.of("node-c"); // prio 50

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
    void onlyTheBestNonElectedSurvivorEscapesTheStalemate() throws Exception {
        Bus bus = new Bus();
        ScheduledExecutorService sched = Executors.newScheduledThreadPool(3);
        closeables.add(sched::shutdownNow);
        NodeInfo infoE = new NodeInfo(E, "127.0.0.1", 1, Collections.emptySet(), 200);
        NodeInfo infoB = new NodeInfo(B, "127.0.0.1", 2, Collections.emptySet(), 100);
        NodeInfo infoC = new NodeInfo(C, "127.0.0.1", 3, Collections.emptySet(), 50);

        Node b = new Node(bus, infoB, List.of(infoE, infoC), sched);
        Node c = new Node(bus, infoC, List.of(infoE, infoB), sched);
        closeables.add(b);
        closeables.add(c);
        b.start();
        c.start();
        // E: em bootstrap (watermark -1, sem vetor), nunca afirma liderança.
        sched.scheduleAtFixedRate(() -> bus.deliver(E, ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", E, null,
                HeartbeatPayload.now(-1L, 7L, false))), 0, 100, TimeUnit.MILLISECONDS);

        // Janela de boot vencida: ambos elegem E por afinidade e deferem (estado do impasse).
        Thread.sleep(700);
        assertEquals(E, b.coord.leaderInfo().map(NodeInfo::nodeId).orElse(null));
        assertEquals(E, c.coord.leaderInfo().map(NodeInfo::nodeId).orElse(null));

        // Os dois recebem a recusa de E (leaderUnavailable) e o heartbeat seguinte a confirma.
        b.coord.noteLeaderRefusal(E);
        c.coord.noteLeaderRefusal(E);
        Thread.sleep(250);
        b.coord.reevaluateLeadership();
        c.coord.reevaluateLeadership();

        awaitLeader(b, B);
        long deadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < deadline) {
            assertFalse(c.coord.isLeader(), "C nunca pode escapar junto: B tem o mesmo estado e afinidade maior");
            Thread.sleep(50);
        }
        assertEquals(B, c.coord.leaderInfo().map(NodeInfo::nodeId).orElse(null), "C segue o líder que serve (B)");
        assertTrue(b.coord.isLeader());
    }

    private static void awaitLeader(Node n, NodeId expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (n.coord.isLeader() && n.coord.leaderInfo().map(NodeInfo::nodeId).filter(expected::equals).isPresent()) {
                return;
            }
            Thread.sleep(25);
        }
        fail("líder não convergiu para " + expected + " (observado=" + n.coord.leaderInfo() + ")");
    }

    /** Um nó real: coordinator + transporte no barramento. */
    private static final class Node implements AutoCloseable {
        final BusTransport transport;
        final ClusterCoordinator coord;

        Node(Bus bus, NodeInfo local, List<NodeInfo> peers, ScheduledExecutorService sched) {
            this.transport = new BusTransport(bus, local, peers);
            ClusterCoordinatorConfig cfg = ClusterCoordinatorConfig.of(
                    Duration.ofMillis(100), Duration.ofMillis(600), Duration.ofSeconds(60), 1, null)
                    .withPairMode(true)
                    .withBootDiscoveryWindow(Duration.ofMillis(300));
            this.coord = new ClusterCoordinator(transport, cfg, sched);
            coord.setReplicationProgressGate(() -> 10L, 0L);
            coord.setLeaderHighWatermarkSupplier(() -> 10L);
            coord.setTopicFrontiersSupplier(() -> Map.of("t", 10L));
        }

        void start() {
            transport.start();
            coord.start();
        }

        @Override
        public void close() {
            try {
                coord.close();
            } catch (Exception ignored) {
            }
        }
    }

    /** Barramento em memória: broadcast/send entregam aos listeners dos outros nós. */
    private static final class Bus {
        final Map<NodeId, BusTransport> transports = new ConcurrentHashMap<>();

        void deliver(NodeId from, ClusterMessage m) {
            transports.forEach((id, t) -> {
                if (!id.equals(from)) {
                    t.listeners.forEach(l -> l.onMessage(m));
                }
            });
        }
    }

    private static final class BusTransport implements Transport {
        private final Bus bus;
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();

        BusTransport(Bus bus, NodeInfo local, List<NodeInfo> peers) {
            this.bus = bus;
            this.local = local;
            this.peers = new ArrayList<>(peers);
            bus.transports.put(local.nodeId(), this);
        }

        @Override public void start() { }
        @Override public NodeInfo local() { return local; }
        @Override public Collection<NodeInfo> peers() {
            List<NodeInfo> all = new ArrayList<>();
            all.add(local);
            all.addAll(peers);
            return all;
        }
        @Override public void addListener(TransportListener l) { listeners.add(l); }
        @Override public void removeListener(TransportListener l) { listeners.remove(l); }
        @Override public void broadcast(ClusterMessage m) { bus.deliver(local.nodeId(), m); }
        @Override public void send(ClusterMessage m) {
            BusTransport target = m.destination() == null ? null : bus.transports.get(m.destination());
            if (target != null) {
                target.listeners.forEach(l -> l.onMessage(m));
            }
        }
        @Override public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage m) {
            CompletableFuture<ClusterMessage> f = new CompletableFuture<>();
            f.completeExceptionally(new UnsupportedOperationException("not used"));
            return f;
        }
        @Override public boolean isConnected(NodeId nodeId) { return true; }
        @Override public boolean isReachable(NodeId nodeId) { return true; }
        @Override public void addPeer(NodeInfo peer) { }
        @Override public void close() throws IOException { }
    }
}
