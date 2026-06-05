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
 * Regressão do failover quebrado relatado na issue #117 (integração tevent-cardinal): ao matar o
 * líder, o nó eleito em seguida fazia <i>step-down</i> imediato por "lease expirada", deixando o
 * cluster sem líder.
 *
 * <p>
 * <b>Causa-raiz:</b> {@code leaseExpiresAt} só era inicializada no {@code start()} e renovada
 * enquanto o nó era líder. Um nó que passou um bom tempo como <i>follower</i> herdava, ao ser
 * eleito no failover, uma lease já vencida — e {@code evictDeadMembers} checa a expiração ANTES de
 * renovar, então o recém-eleito caía no {@code stepDown()} no primeiro ciclo de eviction.
 *
 * <p>
 * <b>Por que os testes de failover anteriores não pegavam:</b> eles fazem failover poucos segundos
 * após o boot, com a lease do boot ainda válida. Este teste <b>espera a lease do boot expirar</b>
 * antes de matar o líder — exatamente a condição de produção (follower por minutos antes do
 * failover). Pré-correção o assert FALHA (sem líder estável); pós-correção (lease re-armada na
 * eleição) o novo líder se mantém.
 */
class LeaderLeaseRearmOnFailoverTest {

    private static final NodeId LOW = NodeId.of("node-1-low");
    private static final NodeId MID = NodeId.of("node-2-mid");
    private static final NodeId HIGH = NodeId.of("node-3-high");

    @Test
    void electedLeaderKeepsLeadershipWhenItsBootLeaseHadExpiredAsFollower() throws Exception {
        int portLow = freePort(Set.of());
        int portMid = freePort(Set.of(portLow));
        int portHigh = freePort(Set.of(portLow, portMid));

        NodeInfo infoLow = new NodeInfo(LOW, "127.0.0.1", portLow);
        NodeInfo infoMid = new NodeInfo(MID, "127.0.0.1", portMid);
        NodeInfo infoHigh = new NodeInfo(HIGH, "127.0.0.1", portHigh);

        TcpTransport transLow = new TcpTransport(mesh(infoLow, infoMid, infoHigh));
        TcpTransport transMid = new TcpTransport(mesh(infoMid, infoLow, infoHigh));
        TcpTransport transHigh = new TcpTransport(mesh(infoHigh, infoLow, infoMid));

        // Lease CURTA (1s) para que o tempo de follower antes do failover a vença de propósito.
        ScheduledExecutorService schedLow = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService schedMid = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService schedHigh = Executors.newSingleThreadScheduledExecutor();

        ClusterCoordinator coordLow = new ClusterCoordinator(transLow, cfg(), schedLow);
        ClusterCoordinator coordMid = new ClusterCoordinator(transMid, cfg(), schedMid);
        ClusterCoordinator coordHigh = new ClusterCoordinator(transHigh, cfg(), schedHigh);

        try {
            transLow.start();
            transMid.start();
            transHigh.start();
            coordLow.start();
            coordMid.start();
            coordHigh.start();

            // 1) Cluster estável: HIGH é líder; MID e LOW o reconhecem.
            awaitLeader(coordHigh, HIGH, Duration.ofSeconds(10), "HIGH não virou líder");
            awaitLeader(coordMid, HIGH, Duration.ofSeconds(10), "MID não viu HIGH como líder");
            awaitLeader(coordLow, HIGH, Duration.ofSeconds(10), "LOW não viu HIGH como líder");

            // 2) Deixa a lease do boot dos followers VENCER (lease=2s; esperamos ~4s).
            Thread.sleep(4000);

            // 3) Failover: mata o líder HIGH.
            coordHigh.stop();
            transHigh.close();

            // 4) MID (próximo maior NodeId) deve assumir e PERMANECER líder — não fazer step-down
            //    por lease velha. Pré-correção: vira líder e cai no mesmo ciclo → sem líder estável.
            awaitLeader(coordMid, MID, Duration.ofSeconds(20), "MID não assumiu após o failover");
            assertStableLeader(coordMid, Duration.ofSeconds(3),
                    "MID assumiu mas NÃO se manteve líder (step-down por lease vencida no failover)");
            awaitLeader(coordLow, MID, Duration.ofSeconds(10), "LOW não convergiu para o novo líder MID");
        } finally {
            close(coordLow, coordMid, coordHigh, transLow, transMid, transHigh);
            schedLow.shutdownNow();
            schedMid.shutdownNow();
            schedHigh.shutdownNow();
        }
    }

    private static ClusterCoordinatorConfig cfg() {
        // Timing folgado para robustez sob carga (build/CI): a renovação da lease (a cada
        // 2×interval=500ms enquanto há quórum) tem margem ampla contra blips de heartbeat; ainda
        // assim a lease (2s) vence durante a fase de follower (esperamos ~4s) para disparar o bug.
        return ClusterCoordinatorConfig.of(
                Duration.ofMillis(250),    // heartbeat interval
                Duration.ofMillis(1500),   // heartbeat timeout (margem p/ não evictar sob carga)
                Duration.ofSeconds(2),     // lease — vence durante a fase de follower
                2,                         // minClusterSize (quórum)
                null);
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

    /**
     * Exige que o nó permaneça líder de forma CONTÍNUA por toda a janela. {@code isLeader()} é o
     * sinal limpo do bug: o {@code stepDown()} por lease vencida zera a liderança. Ao final também
     * confirma a lease válida (com o fix, ela é re-armada na eleição e renovada a cada ciclo).
     */
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
