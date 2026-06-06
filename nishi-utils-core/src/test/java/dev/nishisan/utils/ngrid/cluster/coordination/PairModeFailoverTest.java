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
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Pair mode (cluster de dois nós active/standby): com {@code pairMode} habilitado e
 * {@code minClusterSize=1}, o nó que perde contato com o peer deve assumir/permanecer líder em vez
 * de fazer step-down por falta de quórum.
 *
 * <p><b>RED→GREEN:</b> sem pair mode, {@code requiredActiveMembersForLeadership()} é
 * {@code max(minClusterSize, (peers/2)+1)} = {@code max(1, 2)} = 2 para dois nós, então o
 * sobrevivente nunca alcança o quórum e o cluster fica sem líder ao matar um nó (foi o que travou o
 * HA em pré-prod). Com pair mode a maioria dinâmica é ignorada (split-brain aceito, reconciliado
 * pelo maior NodeId na reconexão) e o sobrevivente assume.</p>
 */
class PairModeFailoverTest {

    private static final NodeId LOW = NodeId.of("node-1-low");
    private static final NodeId HIGH = NodeId.of("node-2-high");

    @Test
    void survivorBecomesAndStaysLeaderWhenPeerDiesInPairMode() throws Exception {
        int portLow = freePort(Set.of());
        int portHigh = freePort(Set.of(portLow));

        NodeInfo infoLow = new NodeInfo(LOW, "127.0.0.1", portLow);
        NodeInfo infoHigh = new NodeInfo(HIGH, "127.0.0.1", portHigh);

        TcpTransport transLow = new TcpTransport(mesh(infoLow, infoHigh));
        TcpTransport transHigh = new TcpTransport(mesh(infoHigh, infoLow));

        ScheduledExecutorService schedLow = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService schedHigh = Executors.newSingleThreadScheduledExecutor();

        ClusterCoordinator coordLow = new ClusterCoordinator(transLow, pairCfg(), schedLow);
        ClusterCoordinator coordHigh = new ClusterCoordinator(transHigh, pairCfg(), schedHigh);

        try {
            transLow.start();
            transHigh.start();
            coordLow.start();
            coordHigh.start();

            // 1) Cluster estável de dois nós: HIGH (maior NodeId) é líder; LOW o reconhece.
            awaitLeader(coordHigh, HIGH, Duration.ofSeconds(10), "HIGH não virou líder");
            awaitLeader(coordLow, HIGH, Duration.ofSeconds(10), "LOW não viu HIGH como líder");

            // 2) Deixa a lease do boot vencer (lease=2s; ~4s) — o sobrevivente terá de re-armar.
            Thread.sleep(4000);

            // 3) Mata o líder HIGH. Resta só LOW.
            coordHigh.stop();
            transHigh.close();

            // 4) Com pair mode, LOW assume mesmo sozinho (quórum=1) e PERMANECE líder. Sem pair mode
            //    (dynamicMajority=2) ele nunca alcançaria o quórum e o cluster ficaria sem líder.
            awaitLeader(coordLow, LOW, Duration.ofSeconds(20), "LOW não assumiu sozinho em pair mode");
            assertStableLeader(coordLow, Duration.ofSeconds(4),
                    "LOW assumiu mas não se manteve líder sozinho em pair mode");
        } finally {
            close(coordLow, coordHigh, transLow, transHigh);
            schedLow.shutdownNow();
            schedHigh.shutdownNow();
        }
    }

    private static ClusterCoordinatorConfig pairCfg() {
        return ClusterCoordinatorConfig.of(
                Duration.ofMillis(250),    // heartbeat interval
                Duration.ofMillis(1500),   // heartbeat timeout
                Duration.ofSeconds(2),     // lease — vence durante a fase de follower
                1,                         // minClusterSize=1 (quórum solo em pair mode)
                null).withPairMode(true);
    }

    private static TcpTransportConfig mesh(NodeInfo local, NodeInfo... peers) {
        TcpTransportConfig.Builder b = TcpTransportConfig.builder(local)
                .reconnectInterval(Duration.ofMillis(300))
                .routeProbeInterval(Duration.ofMillis(500))
                .connectTimeout(Duration.ofMillis(500));
        for (NodeInfo p : peers) {
            b.addPeer(p);
        }
        return b.build();
    }

    private static void awaitLeader(ClusterCoordinator coord, NodeId expected, Duration timeout, String msg)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            Optional<NodeId> seen = coord.leaderInfo().map(NodeInfo::nodeId);
            if (seen.isPresent() && seen.get().equals(expected)) {
                return;
            }
            Thread.sleep(100);
        }
        fail(msg + " (observado=" + coord.leaderInfo().map(NodeInfo::nodeId).orElse(null)
                + ", esperado=" + expected + ", ativos=" + coord.getActiveMembersCount() + ")");
    }

    private static void assertStableLeader(ClusterCoordinator coord, Duration window, String msg)
            throws InterruptedException {
        long end = System.currentTimeMillis() + window.toMillis();
        while (System.currentTimeMillis() < end) {
            if (!coord.isLeader()) {
                fail(msg + " (isLeader=false; sofreu step-down)");
            }
            Thread.sleep(100);
        }
        assertTrue(coord.isLeader() && coord.hasValidLease(),
                msg + " (final: isLeader=" + coord.isLeader() + ", hasValidLease=" + coord.hasValidLease() + ")");
    }

    private static void close(AutoCloseable... cs) {
        for (AutoCloseable c : cs) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static int freePort(Set<Integer> avoid) throws java.io.IOException {
        for (int i = 0; i < 50; i++) {
            try (java.net.ServerSocket s = new java.net.ServerSocket()) {
                s.setReuseAddress(true);
                s.bind(new java.net.InetSocketAddress("127.0.0.1", 0));
                int p = s.getLocalPort();
                if (p > 0 && !avoid.contains(p)) {
                    return p;
                }
            }
        }
        throw new java.io.IOException("no free port");
    }
}
