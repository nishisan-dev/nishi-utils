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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Sync-before-reclaim por WATERMARK GAP (leader-sync-before-reclaim), no nível do coordinator. Prova o
 * design correto (não dependente de observar a asserção de liderança): o gate decide pela DISTÂNCIA de
 * high-watermark, que ambos os nós aprendem dos heartbeats.
 *
 * <p>Cenário (step-10 pré-prod): node-1 (prio 100, preferido) volta enquanto node-2 (prio 50) é o
 * incumbente com estado MAIS NOVO (watermark maior). node-1 deve PERMANECER follower e SINCRONIZAR até
 * o watermark do node-2 ANTES de assumir.
 *
 * <p>Os testes cobrem os dois lados do gate e os invariantes:
 * <ul>
 *   <li><b>reclaimDefersWhileBehindPeerWatermark</b>: lado RECLAIM. node-1 está atrás (lastApplied &lt;
 *       watermark do node-2) → não assume; quando seu lastApplied alcança o watermark do node-2 →
 *       assume. (FALHA sem o fix: assume imediato por afinidade.)</li>
 *   <li><b>leadsWhileAloneNoPeerWatermark</b>: invariante AP. Sem peer ativo à frente, node-1 lidera
 *       após a janela de boot — o gate nunca trava a HA.</li>
 *   <li><b>incumbentRetainsLeadershipWhileCandidateBehind</b>: lado STEP-DOWN. node-2 (líder, prio
 *       menor) NÃO cede a node-1 (prio maior) enquanto node-1 está atrás do watermark de node-2;
 *       cede quando node-1 alcança. Fecha a corrida pelos dois lados.</li>
 *   <li><b>bothFreshElectByAffinity</b>: ambos partindo iguais (sem watermark à frente) → eleição
 *       normal por afinidade (node-1 assume), sem deferral espúrio.</li>
 * </ul>
 */
class LeaderSyncBeforeReclaimTest {

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
     * Lado RECLAIM: node-1 (prio maior) volta atrás do watermark do incumbente node-2. Defere até
     * alcançar. Aqui node-1 é o nó LOCAL; node-2 entra por heartbeat com watermark 1000 (estado novo).
     */
    @Test
    void reclaimDefersWhileBehindPeerWatermark() throws Exception {
        AtomicLong localApplied = new AtomicLong(0); // node-1 começa SEM estado aplicado
        Harness h = harness(PREFERRED, 100, INCUMBENT, 50, Duration.ofMillis(500));
        // Gate por watermark: frontier aplicado local controlado pelo teste; threshold 0 (estrito).
        h.coord.setReplicationProgressGate(localApplied::get, 0L);
        h.start();
        // node-2 (incumbente) ativo com watermark 1000 (estado mais novo). Heartbeats periódicos.
        h.startPeerHeartbeats(INCUMBENT, 7L, 1000L);

        // node-1 está ATRÁS (0 < 1000) → deve PERMANECER follower de forma sustentada.
        long deadline = System.currentTimeMillis() + 1500;
        while (System.currentTimeMillis() < deadline) {
            assertFalse(h.coord.isLeader(),
                    "node-1 NÃO pode assumir enquanto está atrás do watermark do incumbente (estado stale)");
            Thread.sleep(50);
        }

        // node-1 SINCRONIZA: seu frontier aplicado alcança o watermark do incumbente (1000).
        localApplied.set(1000);
        h.coord.reevaluateLeadership();
        // Agora o gate libera e a afinidade promove node-1.
        awaitLeader(h, PREFERRED);
    }

    /**
     * Invariante AP / lead-while-alone: sem nenhum peer ativo reportando watermark, node-1 lidera após a
     * janela de boot, MESMO com frontier aplicado 0 (não há ninguém à frente).
     */
    @Test
    void leadsWhileAloneNoPeerWatermark() throws Exception {
        Harness h = harness(PREFERRED, 100, INCUMBENT, 50, Duration.ofMillis(400));
        h.coord.setReplicationProgressGate(() -> 0L, 0L); // frontier 0, mas ninguém à frente
        h.start();
        // Nenhum heartbeat de peer é injetado → maxActivePeerHighWatermark == -1.
        // Após a janela de boot-discovery, node-1 assume sozinho.
        awaitLeader(h, PREFERRED);
    }

    /**
     * Lado STEP-DOWN: aqui o nó LOCAL é o INCUMBENTE node-2 (prio menor), que já lidera com estado novo
     * (seu globalSeq = 1000). node-1 (prio maior) volta ATRÁS (heartbeat watermark 0). node-2 NÃO deve
     * ceder enquanto node-1 está atrás; cede quando node-1 reporta watermark alcançado.
     */
    @Test
    void incumbentRetainsLeadershipWhileCandidateBehind() throws Exception {
        AtomicLong incumbentWatermark = new AtomicLong(1000); // node-2 já avançou o estado
        Harness h = harness(INCUMBENT, 50, PREFERRED, 100, Duration.ZERO);
        // node-2 é o líder local: seu high-watermark = globalSeq (controlado pelo teste).
        h.coord.setLeaderHighWatermarkSupplier(incumbentWatermark::get);
        h.coord.setReplicationProgressGate(incumbentWatermark::get, 0L);
        h.start();
        // node-2 está sozinho primeiro → assume liderança (pairMode, minClusterSize 1).
        awaitLeader(h, INCUMBENT);

        // node-1 (prio MAIOR) volta, mas ATRÁS: heartbeat com watermark 0 (ainda não sincronizou).
        h.startPeerHeartbeats(PREFERRED, 7L, 0L);

        // node-2 deve RETER a liderança de forma sustentada — node-1 está atrás do seu watermark.
        long deadline = System.currentTimeMillis() + 1500;
        while (System.currentTimeMillis() < deadline) {
            assertTrue(h.coord.isLeader(),
                    "incumbente node-2 deve reter a liderança enquanto o candidato de maior afinidade está atrás");
            Thread.sleep(50);
        }

        // node-1 SINCRONIZA: passa a reportar watermark 1000 (alcançou o estado do node-2).
        h.stopPeerHeartbeats();
        h.startPeerHeartbeats(PREFERRED, 7L, 1000L);
        // Agora node-2 pode ceder; a afinidade elege node-1 e node-2 faz step-down.
        awaitLeader(h, PREFERRED);
        assertFalse(h.coord.isLeader(), "node-2 deve ter cedido a liderança ao node-1 sincronizado");
    }

    /**
     * Ambos reiniciando juntos / iguais: nenhum tem watermark à frente do outro → eleição normal por
     * afinidade. node-1 (prio maior) assume sem deferral espúrio.
     */
    @Test
    void bothFreshElectByAffinity() throws Exception {
        Harness h = harness(PREFERRED, 100, INCUMBENT, 50, Duration.ofMillis(400));
        h.coord.setReplicationProgressGate(() -> 0L, 0L); // node-1 com frontier 0
        h.start();
        // node-2 entra também SEM estado à frente (watermark 0) — ninguém ahead.
        h.startPeerHeartbeats(INCUMBENT, 1L, 0L);
        // node-1 (prio maior) assume por afinidade após a janela de boot.
        awaitLeader(h, PREFERRED);
    }

    // ---- harness ----

    private Harness harness(NodeId localId, int localPriority, NodeId peerId, int peerPriority,
            Duration discoveryWindow) {
        Harness h = new Harness(localId, localPriority, peerId, peerPriority, discoveryWindow);
        closeables.add(h);
        return h;
    }

    private static void awaitLeader(Harness h, NodeId expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (h.coord.leaderInfo().map(NodeInfo::nodeId).filter(expected::equals).isPresent()) {
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

        /**
         * Inicia heartbeats periódicos de {@code source} carregando {@code highWatermark} (o estado que
         * o peer reporta). É assim que o gate por watermark aprende a progressão do peer.
         */
        void startPeerHeartbeats(NodeId source, long epoch, long highWatermark) {
            peerTask = sched.scheduleAtFixedRate(() -> coord.onMessage(
                    ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", source, null,
                            HeartbeatPayload.now(highWatermark, epoch))),
                    0, 100, TimeUnit.MILLISECONDS);
        }

        void stopPeerHeartbeats() {
            if (peerTask != null) {
                peerTask.cancel(true);
                peerTask = null;
            }
        }

        @Override
        public void close() {
            stopPeerHeartbeats();
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

    /** Transporte mínimo in-process: registra envios e expõe o peer como conectado/ativo. */
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
        @Override public void broadcast(ClusterMessage m) { sent.add(m); }
        @Override public void send(ClusterMessage m) { sent.add(m); }
        @Override public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage m) {
            sent.add(m);
            CompletableFuture<ClusterMessage> f = new CompletableFuture<>();
            f.completeExceptionally(new UnsupportedOperationException("not used"));
            return f;
        }
        @Override public boolean isConnected(NodeId nodeId) { return Boolean.TRUE.equals(connected.get(nodeId)); }
        @Override public boolean isReachable(NodeId nodeId) { return isConnected(nodeId); }
        @Override public void addPeer(NodeInfo peer) { }
        @Override public void close() throws IOException { }
    }
}
