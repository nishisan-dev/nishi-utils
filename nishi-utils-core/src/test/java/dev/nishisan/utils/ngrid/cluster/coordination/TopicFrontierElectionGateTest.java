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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.TopicFrontiers;

/**
 * Issue #178: os gates de eleição comparam a fronteira aplicada POR TÓPICO (vetor no heartbeat), não
 * um odômetro agregado. Cenário do comentário da issue: o ngrrd replica {@code catalog}, {@code nodes}
 * e {@code geometries}; o tópico de status ({@code nodes}) é escrito a cada tick e domina qualquer
 * agregado, então um seguidor que perdeu a última op do CATÁLOGO parecia "em dia" e era eleito à
 * frente de um peer que a tinha — em RELAY_STREAM (quórum 1) a op confirmada se perdia.
 */
class TopicFrontierElectionGateTest {

    private static final String CATALOG = "map:ngrrd.catalog";
    private static final String NODES = "map:ngrrd.nodes";
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

    /** (a) Mesma soma, tópicos diferentes: quem tem a última op do catálogo lidera. */
    @Test
    void sameTotalButBehindOnCatalogDefersToThePeerThatHoldsIt() throws Exception {
        Harness h = harness(PREFERRED, 100, INCUMBENT, 50, Duration.ofMillis(300));
        h.localFrontiers.set(Map.of(CATALOG, 9L, NODES, 20L)); // total 29, sem a última op do catálogo
        h.start();
        h.startPeerHeartbeats(INCUMBENT, 7L, 29L, Map.of(CATALOG, 10L, NODES, 19L)); // total 29

        long deadline = System.currentTimeMillis() + 1200;
        while (System.currentTimeMillis() < deadline) {
            assertFalse(h.coord.isLeader(),
                    "node-1 NÃO pode assumir: mesma soma, mas está atrás no catálogo (issue #178)");
            Thread.sleep(50);
        }
        assertEquals(INCUMBENT, h.coord.leaderInfo().map(NodeInfo::nodeId).orElse(null),
                "o nó que defere adota como líder o peer à frente no vetor");
        assertTrue(h.coord.localBehindEligiblePeer());

        // node-1 aplica a op do catálogo que faltava: agora domina (10/20 vs 10/19) e a afinidade vence.
        h.localFrontiers.set(Map.of(CATALOG, 10L, NODES, 20L));
        h.coord.reevaluateLeadership();
        awaitLeader(h, PREFERRED);
        assertFalse(h.coord.localBehindEligiblePeer());
    }

    /** (b) Peer antigo, sem vetor: o gate cai no escalar, como na 8.7.0. */
    @Test
    void peerWithoutVectorFallsBackToScalarWatermark() throws Exception {
        Harness h = harness(PREFERRED, 100, INCUMBENT, 50, Duration.ofMillis(300));
        h.localFrontiers.set(Map.of(CATALOG, 10L, NODES, 20L)); // total 30
        h.start();
        h.startPeerHeartbeats(INCUMBENT, 7L, 1000L, Map.of()); // só escalar, à frente

        long deadline = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < deadline) {
            assertFalse(h.coord.isLeader(), "peer sem vetor à frente no escalar → deferir");
            Thread.sleep(50);
        }
        h.stopPeerHeartbeats();
        h.startPeerHeartbeats(INCUMBENT, 7L, 30L, Map.of()); // escalar empatado → afinidade decide
        awaitLeader(h, PREFERRED);
    }

    /** (c) Vetores incomparáveis: a soma decide, deterministicamente. */
    @Test
    void incomparableVectorsAreResolvedByTotal() throws Exception {
        Harness h = harness(PREFERRED, 100, INCUMBENT, 50, Duration.ofMillis(300));
        h.localFrontiers.set(Map.of(CATALOG, 11L, NODES, 18L)); // total 29, à frente no catálogo
        h.start();
        h.startPeerHeartbeats(INCUMBENT, 7L, 30L, Map.of(CATALOG, 10L, NODES, 20L)); // total 30

        long deadline = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < deadline) {
            assertFalse(h.coord.isLeader(), "incomparável com soma menor → deferir");
            Thread.sleep(50);
        }
        // Agora o local tem a soma maior (31 > 30): assume mesmo atrás em `nodes`.
        h.localFrontiers.set(Map.of(CATALOG, 12L, NODES, 19L));
        h.coord.reevaluateLeadership();
        awaitLeader(h, PREFERRED);
    }

    /** (d) O líder corrente NUNCA abdica por um vetor de seguidor acima do seu (F2 do D9 preservado). */
    @Test
    void servingLeaderNeverAbdicatesOnFollowerVectorAhead() throws Exception {
        Harness h = harness(PREFERRED, 100, INCUMBENT, 50, Duration.ofMillis(200));
        h.localFrontiers.set(Map.of(CATALOG, 10L, NODES, 20L));
        h.start();
        awaitLeader(h, PREFERRED); // lidera sozinho após a janela de boot
        h.startPeerHeartbeats(INCUMBENT, 7L, 40L, Map.of(CATALOG, 20L, NODES, 20L)); // "à frente"

        long deadline = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < deadline) {
            assertTrue(h.coord.isLeader(), "gate A é de RECLAIM: o incumbente retém (tems#9/D9 F2)");
            Thread.sleep(50);
        }
    }

    /** Gate B: o incumbente (afinidade menor) não cede ao candidato atrás em UM tópico. */
    @Test
    void incumbentRetainsLeadershipWhileCandidateBehindOnOneTopic() throws Exception {
        Harness h = harness(INCUMBENT, 50, PREFERRED, 100, Duration.ofMillis(200));
        h.localFrontiers.set(Map.of(CATALOG, 10L, NODES, 20L)); // total 30
        h.start();
        awaitLeader(h, INCUMBENT);
        // Candidato preferido com a MESMA soma, mas sem a última op do catálogo.
        h.startPeerHeartbeats(PREFERRED, 7L, 30L, Map.of(CATALOG, 9L, NODES, 21L));
        long deadline = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < deadline) {
            assertTrue(h.coord.isLeader(), "candidato atrás no catálogo: o incumbente não cede");
            Thread.sleep(50);
        }
        h.stopPeerHeartbeats();
        h.startPeerHeartbeats(PREFERRED, 7L, 31L, Map.of(CATALOG, 10L, NODES, 21L)); // em dia
        awaitLeader(h, PREFERRED);
    }

    /** (e) Escape D9 por vetor: o nó à frente no catálogo assume quando o eleito recusa e está atrás. */
    @Test
    void stalemateEscapeIsDecidedPerTopic() throws Exception {
        Harness h = harness(INCUMBENT, 50, PREFERRED, 100, Duration.ofMillis(400));
        h.localFrontiers.set(Map.of(CATALOG, 10L, NODES, 20L)); // total 30
        h.start();
        h.startPeerHeartbeats(PREFERRED, 7L, 30L, Map.of(CATALOG, 11L, NODES, 19L)); // total 30, incomparável

        Thread.sleep(600);
        // Incomparável com soma IGUAL: o desempate por tópico (catalog primeiro, por nome) diz que o
        // eleito está à frente (11 > 10) → o local NÃO escapa, mesmo com a recusa.
        h.coord.noteLeaderRefusal(PREFERRED);
        long deadline = System.currentTimeMillis() + 800;
        while (System.currentTimeMillis() < deadline) {
            assertFalse(h.coord.isLeader(), "recusa + eleito à frente no desempate por tópico: sem escape");
            Thread.sleep(50);
        }

        // O eleito é dominado (9/20 < 10/20): a recusa confirmada por heartbeat posterior libera o escape.
        h.stopPeerHeartbeats();
        h.startPeerHeartbeats(PREFERRED, 7L, 29L, Map.of(CATALOG, 9L, NODES, 20L));
        Thread.sleep(250);
        h.coord.noteLeaderRefusal(PREFERRED);
        awaitLeader(h, INCUMBENT);
    }

    @Test
    void maxPeerTopicFrontierReflectsEligibleActivePeersOnly() throws Exception {
        Harness h = harness(PREFERRED, 100, INCUMBENT, 50, Duration.ofMillis(200));
        h.localFrontiers.set(Map.of(CATALOG, 1L));
        h.start();
        assertEquals(-1L, h.coord.maxActivePeerTopicFrontier(CATALOG));
        h.startPeerHeartbeats(INCUMBENT, 7L, 15L, Map.of(CATALOG, 10L, NODES, 5L));
        long deadline = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < deadline && h.coord.maxActivePeerTopicFrontier(CATALOG) < 10L) {
            Thread.sleep(25);
        }
        assertEquals(10L, h.coord.maxActivePeerTopicFrontier(CATALOG));
        assertEquals(0L, h.coord.maxActivePeerTopicFrontier("map:unknown"),
                "peer com vetor mas sem o tópico → fronteira 0 (não -1)");
    }

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
        final AtomicReference<Map<String, Long>> localFrontiers = new AtomicReference<>(Map.of());
        volatile ScheduledFuture<?> peerTask;

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
            // O escalar é a SOMA do vetor local (mesma derivação do ReplicationManager).
            coord.setReplicationProgressGate(() -> TopicFrontiers.of(localFrontiers.get()).total(), 0L);
            coord.setLeaderHighWatermarkSupplier(() -> TopicFrontiers.of(localFrontiers.get()).total());
            coord.setTopicFrontiersSupplier(localFrontiers::get);
        }

        void start() {
            transport.start();
            coord.start();
        }

        void startPeerHeartbeats(NodeId source, long epoch, long highWatermark, Map<String, Long> frontiers) {
            peerTask = sched.scheduleAtFixedRate(() -> coord.onMessage(
                    ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", source, null,
                            HeartbeatPayload.now(highWatermark, epoch, false, frontiers))),
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
