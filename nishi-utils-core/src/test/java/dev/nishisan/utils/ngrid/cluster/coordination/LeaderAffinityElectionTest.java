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

import dev.nishisan.utils.ngrid.cluster.transport.TcpTransport;
import dev.nishisan.utils.ngrid.cluster.transport.TcpTransportConfig;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Fase 0.4b — eleição por afinidade de prioridade. A configuração define a afinidade do líder;
 * empates são quebrados por NodeId. Um nó de menor prioridade que sobe sem ver o preferido DEFERE
 * a auto-eleição durante a janela de descoberta (em vez de assumir e forçar uma reeleição quando o
 * preferido aparece), mas continua liderando sozinho se o preferido nunca aparecer (AP).
 */
class LeaderAffinityElectionTest {

    private static final NodeId LOW_ID = NodeId.of("node-1");   // NodeId menor
    private static final NodeId HIGH_ID = NodeId.of("node-9");  // NodeId maior

    @Test
    void electsHigherPriorityEvenWithLowerNodeId() throws Exception {
        // Local tem NodeId MENOR mas prioridade MAIOR; o peer tem NodeId maior e prioridade menor.
        try (Harness h = new Harness(LOW_ID, 100, HIGH_ID, 10, true, Duration.ZERO)) {
            h.start();
            h.injectHeartbeat(HIGH_ID, 1L); // torna o peer um membro ativo
            // Prioridade vence o NodeId: o local (prio 100) lidera apesar do NodeId menor.
            h.awaitLeader(LOW_ID);
        }
    }

    @Test
    void tieBreaksByNodeIdWhenPriorityEqual() throws Exception {
        // Prioridades iguais (ambas 0): mantém o comportamento legado max(NodeId) — o peer maior vence.
        try (Harness h = new Harness(LOW_ID, 0, HIGH_ID, 0, true, Duration.ZERO)) {
            h.start();
            h.injectHeartbeat(HIGH_ID, 1L);
            h.awaitLeader(HIGH_ID);
        }
    }

    @Test
    void nonPreferredDefersDuringWindowThenLeadsWhenAlone() throws Exception {
        // Local é não-preferido (prio 10) e o preferido configurado (prio 100) não aparece.
        Duration window = Duration.ofMillis(1200);
        try (Harness h = new Harness(LOW_ID, 10, HIGH_ID, 100, true, window)) {
            h.start();
            // Durante a janela, o local DEFERE: permanece follower (não assume sozinho).
            Thread.sleep(500);
            assertFalse(h.coord.isLeader(), "não-preferido não deveria assumir durante a janela de descoberta");
            // Após a janela, o preferido nunca apareceu → o local assume sozinho (AP / lead-while-alone).
            h.awaitLeader(LOW_ID);
        }
    }

    @Test
    void nonPreferredYieldsWhenPreferredAppearsDuringWindow() throws Exception {
        Duration window = Duration.ofMillis(3000);
        try (Harness h = new Harness(LOW_ID, 10, HIGH_ID, 100, true, window)) {
            h.start();
            Thread.sleep(300);
            assertFalse(h.coord.isLeader(), "deveria estar deferindo");
            // O preferido (prio 100) aparece via heartbeat: ele assume e o local permanece follower.
            h.injectHeartbeat(HIGH_ID, 1L);
            h.awaitLeader(HIGH_ID);
            assertFalse(h.coord.isLeader(), "local de menor afinidade não deveria liderar com o preferido presente");
        }
    }

    // ---- harness ----

    private final class Harness implements AutoCloseable {
        final TcpTransport transport;
        final ClusterCoordinator coord;
        final ScheduledExecutorService sched;

        Harness(NodeId localId, int localPriority, NodeId peerId, int peerPriority,
                boolean pairMode, Duration discoveryWindow) throws Exception {
            NodeInfo localInfo = new NodeInfo(localId, "127.0.0.1", freePort(), Collections.emptySet(), localPriority);
            NodeInfo peerInfo = new NodeInfo(peerId, "127.0.0.1", freePort(), Collections.emptySet(), peerPriority);
            this.transport = new TcpTransport(TcpTransportConfig.builder(localInfo)
                    .reconnectInterval(Duration.ofSeconds(30)).addPeer(peerInfo).build());
            ClusterCoordinatorConfig cfg = ClusterCoordinatorConfig.of(
                    Duration.ofMillis(200), Duration.ofSeconds(60), Duration.ofSeconds(60),
                    pairMode ? 1 : 2, null)
                    .withPairMode(pairMode)
                    .withBootDiscoveryWindow(discoveryWindow);
            this.sched = Executors.newSingleThreadScheduledExecutor();
            this.coord = new ClusterCoordinator(transport, cfg, sched);
        }

        void start() {
            transport.start();
            coord.start();
        }

        void injectHeartbeat(NodeId source, long epoch) {
            coord.onMessage(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", source, null,
                    HeartbeatPayload.now(-1L, epoch)));
        }

        void awaitLeader(NodeId expected) throws InterruptedException {
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                if (coord.leaderInfo().map(NodeInfo::nodeId).filter(expected::equals).isPresent()) {
                    return;
                }
                Thread.sleep(50);
            }
            fail("líder não convergiu para " + expected
                    + " (observado=" + coord.leaderInfo().map(NodeInfo::nodeId).orElse(null) + ")");
        }

        @Override
        public void close() {
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

        private int freePort() throws java.io.IOException {
            try (java.net.ServerSocket s = new java.net.ServerSocket()) {
                s.setReuseAddress(true);
                s.bind(new java.net.InetSocketAddress("127.0.0.1", 0));
                return s.getLocalPort();
            }
        }
    }
}
