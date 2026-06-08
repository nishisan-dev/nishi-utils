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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Fase 0.1 — o leader epoch deve se comportar como um termo de cluster monotônico convergido,
 * não como um contador local por nó. Reproduz a raiz do incidente em pré-prod: o líder perdeu o
 * {@code leader-epoch.dat} e regrediu o epoch (7 → 1), enquanto o follower retinha um epoch maior
 * em memória e passou a descartar (fence) tudo do líder legítimo.
 *
 * <p>Os casos cobrem: persistência não-regressiva ({@code loadEpoch}), o líder re-carimbando acima
 * de um epoch maior observado num heartbeat de peer (o destrave), a guarda contra escalada por eco,
 * e o follower apenas adotando o epoch observado (sem {@code +1}).</p>
 */
class EpochConvergenceTest {

    @Test
    void loadEpochNeverRegressesAndSurvivesCorruptFile(@TempDir Path dir) throws Exception {
        // Arquivo válido: o epoch carregado é adotado.
        Files.writeString(dir.resolve("leader-epoch.dat"), "7");
        try (Harness h = Harness.lonePairLeader(NodeId.of("node-9-leader"), dir, false)) {
            assertEquals(7L, h.coord.getLeaderEpoch(), "epoch persistido deveria ser carregado");
        }

        // Arquivo corrompido: NÃO regride para 0 silenciosamente; mantém o termo corrente (0) sem lançar.
        Files.writeString(dir.resolve("leader-epoch.dat"), "not-a-number");
        try (Harness h = Harness.lonePairLeader(NodeId.of("node-9-leader"), dir, false)) {
            assertEquals(0L, h.coord.getLeaderEpoch(), "arquivo corrompido não deveria derrubar o boot");
        }
    }

    @Test
    void leaderReStampsAboveHigherObservedPeerEpochAndIgnoresEcho() throws Exception {
        // Nó solo em pair mode: torna-se líder sozinho (epoch eleito = 1).
        try (Harness h = Harness.lonePairLeader(NodeId.of("node-9-leader"), null, true)) {
            h.awaitLeader();
            long elected = h.coord.getLeaderEpoch();

            // Peer (NodeId menor) bate heartbeat anunciando epoch 5 — maior do que o do líder.
            // O líder deve re-carimbar para 6 (observed + 1): um termo fresco estritamente acima do
            // "fantasma" que um follower poderia lembrar, restabelecendo-se como líder aceito.
            h.injectHeartbeat(NodeId.of("node-1-peer"), 5L);
            h.awaitEpoch(6L);

            // Guarda contra escalada por eco: observar o próprio termo de volta (6) não sobe nada.
            h.injectHeartbeat(NodeId.of("node-1-peer"), 6L);
            // E um epoch antigo/menor (3) também é ignorado.
            h.injectHeartbeat(NodeId.of("node-1-peer"), 3L);
            Thread.sleep(200);
            assertEquals(6L, h.coord.getLeaderEpoch(),
                    "eco do próprio termo (ou epoch menor) não deveria escalar o epoch");
            // Sanidade: o termo eleito era estritamente menor que o convergido.
            if (elected >= 6L) {
                fail("epoch eleito (" + elected + ") deveria ser menor que o convergido (6)");
            }
        }
    }

    @Test
    void followerAdoptsObservedEpochWithoutReStamp() throws Exception {
        // Nó NÃO-líder (minClusterSize=2, sem pair mode, NodeId menor que o peer): ao observar um
        // heartbeat com epoch 5, apenas ADOTA 5 (rastreia o máximo do cluster), sem o +1 do líder.
        try (Harness h = Harness.follower(NodeId.of("node-1-follower"), NodeId.of("node-9-leader"))) {
            h.injectHeartbeat(NodeId.of("node-9-leader"), 5L);
            h.awaitEpoch(5L);
            assertEquals(5L, h.coord.getLeaderEpoch(), "follower deveria adotar o epoch observado sem +1");
        }
    }

    @Test
    void followerAdoptsAgreedLeaderEpochEvenWhenItRegresses() throws Exception {
        // Fase 0.2 — fencing por IDENTIDADE: uma vez que o nó reconhece HIGH como líder acordado,
        // ele adota o epoch de HIGH mesmo que regrida (5 -> 2), em vez de descartar como "stale".
        // O modelo antigo (só magnitude) ignoraria o epoch 2 e manteria 5 — congelando o follower.
        try (Harness h = Harness.follower(NodeId.of("node-1-follower"), NodeId.of("node-9-leader"))) {
            NodeId leader = NodeId.of("node-9-leader");
            // 1ª batida estabelece HIGH como líder acordado (no fencing ainda não era líder).
            h.injectHeartbeat(leader, 5L);
            h.awaitTrackedLeader(leader);
            // 2ª batida (já como líder acordado) fixa trackedLeaderEpoch=5.
            h.injectHeartbeat(leader, 5L);
            h.awaitTrackedEpoch(5L);
            // Líder regride o termo para 2 — por identidade, o follower ADOTA 2 (não fencia).
            h.injectHeartbeat(leader, 2L);
            h.awaitTrackedEpoch(2L);
            assertEquals(2L, h.coord.getTrackedLeaderEpoch(),
                    "follower deveria adotar o epoch do líder acordado mesmo regredido");
        }
    }

    // ---- harness ----

    private static final class Harness implements AutoCloseable {
        final TcpTransport transport;
        final ClusterCoordinator coord;
        final ScheduledExecutorService sched;

        private Harness(TcpTransport transport, ClusterCoordinator coord, ScheduledExecutorService sched) {
            this.transport = transport;
            this.coord = coord;
            this.sched = sched;
        }

        /** Lone node that leads by itself in pair mode (or, with pairMode=false, a plain single node). */
        static Harness lonePairLeader(NodeId local, Path dataDir, boolean start) throws Exception {
            NodeInfo info = new NodeInfo(local, "127.0.0.1", freePort());
            TcpTransport transport = new TcpTransport(TcpTransportConfig.builder(info)
                    .reconnectInterval(Duration.ofSeconds(30)).build());
            ClusterCoordinatorConfig cfg = ClusterCoordinatorConfig.of(
                    Duration.ofMillis(200), Duration.ofSeconds(60), Duration.ofSeconds(60), 1, dataDir)
                    .withPairMode(true);
            ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
            ClusterCoordinator coord = new ClusterCoordinator(transport, cfg, sched);
            Harness h = new Harness(transport, coord, sched);
            if (start) {
                transport.start();
                coord.start();
            }
            return h;
        }

        /** Node that stays a follower: minClusterSize=2 (no pair mode) and a higher-NodeId peer. */
        static Harness follower(NodeId local, NodeId higherPeer) throws Exception {
            NodeInfo info = new NodeInfo(local, "127.0.0.1", freePort());
            NodeInfo peer = new NodeInfo(higherPeer, "127.0.0.1", freePort());
            TcpTransport transport = new TcpTransport(TcpTransportConfig.builder(info)
                    .reconnectInterval(Duration.ofSeconds(30)).addPeer(peer).build());
            ClusterCoordinatorConfig cfg = ClusterCoordinatorConfig.of(
                    Duration.ofMillis(200), Duration.ofSeconds(60), Duration.ofSeconds(60), 2, null);
            ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
            ClusterCoordinator coord = new ClusterCoordinator(transport, cfg, sched);
            Harness h = new Harness(transport, coord, sched);
            transport.start();
            coord.start();
            return h;
        }

        void injectHeartbeat(NodeId source, long epoch) {
            ClusterMessage hb = ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", source, null,
                    HeartbeatPayload.now(-1L, epoch));
            coord.onMessage(hb);
        }

        void awaitLeader() throws InterruptedException {
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                if (coord.isLeader()) {
                    return;
                }
                Thread.sleep(50);
            }
            fail("nó solo não assumiu liderança em pair mode");
        }

        void awaitEpoch(long expected) throws InterruptedException {
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                if (coord.getLeaderEpoch() == expected) {
                    return;
                }
                Thread.sleep(50);
            }
            assertEquals(expected, coord.getLeaderEpoch(), "epoch não convergiu para o esperado");
        }

        void awaitTrackedLeader(NodeId expected) throws InterruptedException {
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                if (coord.leaderInfo().map(NodeInfo::nodeId).filter(expected::equals).isPresent()) {
                    return;
                }
                Thread.sleep(50);
            }
            fail("líder acordado não convergiu para " + expected
                    + " (observado=" + coord.leaderInfo().map(NodeInfo::nodeId).orElse(null) + ")");
        }

        void awaitTrackedEpoch(long expected) throws InterruptedException {
            long deadline = System.currentTimeMillis() + 5_000;
            while (System.currentTimeMillis() < deadline) {
                if (coord.getTrackedLeaderEpoch() == expected) {
                    return;
                }
                Thread.sleep(50);
            }
            assertEquals(expected, coord.getTrackedLeaderEpoch(), "trackedLeaderEpoch não convergiu");
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

        private static int freePort() throws java.io.IOException {
            try (java.net.ServerSocket s = new java.net.ServerSocket()) {
                s.setReuseAddress(true);
                s.bind(new java.net.InetSocketAddress("127.0.0.1", 0));
                return s.getLocalPort();
            }
        }
    }
}
