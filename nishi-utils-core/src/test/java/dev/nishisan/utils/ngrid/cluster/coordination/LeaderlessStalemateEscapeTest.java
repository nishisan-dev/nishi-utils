/*
 *  Copyright (C) 2020-2026 Lucas Nishimura <lucas.nishimura at gmail.com>
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regressão da issue tems#9/D9 (abdicação do líder + impasse leaderless), no nível do coordinator.
 *
 * <p>Incidente real: um contador de restart inflado fez o gate A — avaliado no PRÓPRIO LÍDER — evictar
 * um líder saudável quando o contador honesto do follower o cruzou (~20 heartbeats após o reclaim), e o
 * cluster travou num impasse de deferência mútua por 62 minutos: o nó atrás (maior afinidade) deferia
 * pelo gate A e não conseguia avançar (só um líder serve o stream — a recusa era um log FINE invisível);
 * o nó à frente deferia por afinidade, sem jamais comparar watermarks nem verificar se o eleito assumiu.
 */
class LeaderlessStalemateEscapeTest {

    private static final NodeId PREFERRED = NodeId.of("node-1"); // afinidade MAIOR (prioridade 100)
    private static final NodeId INCUMBENT = NodeId.of("node-2"); // afinidade MENOR (prioridade 50)

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
     * F2: o gate A nunca evicta o líder corrente. Um líder saudável que observa o CONTADOR de um
     * follower acima do próprio applied (sintoma de dessincronia de escala, ex.: seed inflado) deve
     * RETER a liderança — abdicar derruba o cluster em leaderless (o follower de menor afinidade não
     * se elege; o ex-líder não consegue mais avançar sem um líder servindo o stream).
     */
    @Test
    @DisplayName("Líder corrente nunca abdica por watermark de follower acima do próprio (F2)")
    void leaderNeverStepsDownToCountAheadFollower() throws Exception {
        Harness h = harness(PREFERRED, 100, INCUMBENT, 50, Duration.ZERO);
        h.coord.setReplicationProgressGate(() -> 500L, 0L); // applied local fixo em 500
        h.start();
        awaitLeader(h, PREFERRED); // lidera sozinho (pairMode)

        // O follower passa a reportar contador ACIMA do applied do líder (a assinatura do D9).
        h.startPeerHeartbeats(INCUMBENT, 7L, 1000L);

        long deadline = System.currentTimeMillis() + 1500;
        while (System.currentTimeMillis() < deadline) {
            assertTrue(h.coord.isLeader(),
                    "o líder corrente nunca pode abdicar porque um follower reporta contador maior "
                            + "(gate A é de RECLAIM; abdicar derruba o cluster em leaderless)");
            Thread.sleep(50);
        }
    }

    /**
     * F3 (escape do impasse): o nó À FRENTE, deferindo por afinidade a um eleito que está ATRÁS e que
     * RECUSOU servir o stream como não-líder, assume a liderança em vez de deferir para sempre.
     */
    @Test
    @DisplayName("Nó à frente assume quando o eleito por afinidade recusa liderança e está atrás (F3)")
    void aheadNodeBreaksStalemateOnLeaderRefusal() throws Exception {
        // Janela de descoberta > 0: o local NÃO se auto-elege no boot; ao conhecer o peer de maior
        // afinidade pela janela, a afinidade o elege e o local DEFERE — o estado do impasse.
        Harness h = harness(INCUMBENT, 50, PREFERRED, 100, Duration.ofMillis(400));
        h.coord.setReplicationProgressGate(() -> 1000L, 0L); // local à frente (1000)
        h.start();
        h.startPeerHeartbeats(PREFERRED, 7L, 900L); // peer ativo mas ATRÁS (900)

        Thread.sleep(600); // janela de boot vencida, peer conhecido
        long deadline = System.currentTimeMillis() + 800;
        while (System.currentTimeMillis() < deadline) {
            assertFalse(h.coord.isLeader(), "antes da recusa, o local defere por afinidade (estado do impasse)");
            Thread.sleep(50);
        }

        // O eleito respondeu leaderUnavailable a um fetch (a recusa que era invisível no incidente).
        h.coord.noteLeaderRefusal(PREFERRED);
        awaitLeader(h, INCUMBENT);
    }

    /**
     * F3 (guarda): a recusa do eleito NÃO autoriza um nó ATRÁS a assumir — ele está genuinamente
     * defasado e lideraria perdendo a cauda do cluster. O escape é exclusivo de quem está à frente.
     */
    @Test
    @DisplayName("Escape não promove nó atrás do eleito, mesmo com recusa (guarda do F3)")
    void stalemateEscapeRespectsBehindGuard() throws Exception {
        Harness h = harness(INCUMBENT, 50, PREFERRED, 100, Duration.ofMillis(400));
        h.coord.setReplicationProgressGate(() -> 800L, 0L); // local ATRÁS (800 < 900)
        h.start();
        h.startPeerHeartbeats(PREFERRED, 7L, 900L);
        Thread.sleep(600); // janela de boot vencida, peer conhecido, local deferindo

        h.coord.noteLeaderRefusal(PREFERRED);

        long deadline = System.currentTimeMillis() + 1200;
        while (System.currentTimeMillis() < deadline) {
            assertFalse(h.coord.isLeader(),
                    "um nó atrás do eleito nunca assume pelo escape — apenas o nó à frente destrava o impasse");
            Thread.sleep(50);
        }
    }

    // ---- harness (espelho do LeaderSyncBeforeReclaimTest) ----

    private Harness harness(NodeId localId, int localPriority, NodeId peerId, int peerPriority,
            Duration discoveryWindow) {
        Harness h = new Harness(localId, localPriority, peerId, peerPriority, discoveryWindow);
        closeables.add(h);
        return h;
    }

    private static void awaitLeader(Harness h, NodeId expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (h.coord.leaderInfo().map(NodeInfo::nodeId).filter(expected::equals).isPresent()
                    && (!expected.equals(h.transport.local().nodeId()) || h.coord.isLeader())) {
                return;
            }
            Thread.sleep(25);
        }
        fail("líder não convergiu para " + expected
                + " (observado=" + h.coord.leaderInfo().map(NodeInfo::nodeId).orElse(null) + ")");
    }

    private static final class Harness implements AutoCloseable {
        final LoopbackTransport transport;
        final ClusterCoordinator coord;
        final ScheduledExecutorService sched;
        volatile java.util.concurrent.ScheduledFuture<?> peerTask;

        Harness(NodeId localId, int localPriority, NodeId peerId, int peerPriority, Duration discoveryWindow) {
            NodeInfo localInfo = new NodeInfo(localId, "127.0.0.1", 1, Collections.emptySet(), localPriority);
            NodeInfo peerInfo = new NodeInfo(peerId, "127.0.0.1", 2, Collections.emptySet(), peerPriority);
            this.transport = new LoopbackTransport(localInfo, List.of(peerInfo));
            ClusterCoordinatorConfig cfg = ClusterCoordinatorConfig.of(
                    Duration.ofMillis(150), Duration.ofMillis(600), Duration.ofSeconds(60), 1, null)
                    .withPairMode(true)
                    .withBootDiscoveryWindow(discoveryWindow);
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
                            HeartbeatPayload.now(highWatermark, epoch))),
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
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final ConcurrentHashMap<NodeId, Boolean> connected = new ConcurrentHashMap<>();
        private final List<ClusterMessage> sent = new CopyOnWriteArrayList<>();

        LoopbackTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            peers.forEach(p -> connected.put(p.nodeId(), true));
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
            sent.add(m);
        }

        @Override
        public void send(ClusterMessage m) {
            sent.add(m);
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage m) {
            sent.add(m);
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
