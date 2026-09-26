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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regressão da issue #179 (impasse leaderless após failover), no nível do coordinator.
 *
 * <p>Cenário real ({@code LeaderFailoverDuringMigrationClusterTest}): três storages e um cliente ngrrd
 * ({@link NodeInfo#ROLE_LEADER_INELIGIBLE}); o líder cai e o cliente, que recebeu o último frame de
 * replicação, anuncia um watermark uma operação à frente dos dois sobreviventes. O gate A (reclaim)
 * contava o watermark do cliente como "estado mais novo do cluster": o sobrevivente eleito por afinidade
 * deferia a um peer elegível que não estava à frente, o outro sobrevivente seguia o eleito (sem escape
 * D9, pois não estava estritamente à frente dele) e ninguém liderava — para sempre. Um membro inelegível
 * nunca lidera nem serve o stream, então esperar por ele jamais resolve.
 */
class IneligibleMemberWatermarkStalemateTest {

    private static final NodeId STORAGE_0 = NodeId.of("storage-0"); // afinidade menor (prioridade 0)
    private static final NodeId STORAGE_1 = NodeId.of("storage-1"); // afinidade maior (prioridade 2)
    private static final NodeId CLIENT = NodeId.of("ngrrd-client-x"); // inelegível (prioridade 3)

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
     * O sobrevivente eleito por afinidade lidera quando o único peer à frente dele é inelegível: o
     * watermark do cliente não entra na fronteira do gate A.
     */
    @Test
    @DisplayName("Eleito por afinidade lidera quando só um membro inelegível está à frente (#179)")
    void electedSurvivorLeadsWhenOnlyIneligibleMemberIsAhead() throws Exception {
        Node storage1 = node(STORAGE_1, 2, List.of(eligible(STORAGE_0, 0), client(CLIENT, 3)));
        storage1.coord.setReplicationProgressGate(() -> 10L, 0L); // applied local = 10
        storage1.start(); // sozinho não forma quorum (2 votantes elegíveis conhecidos): ainda sem líder
        // O cliente é registrado ANTES de storage-0 aparecer: o primeiro recompute com quorum já enxerga
        // o watermark 11 do cliente (sem isso, storage-1 poderia se eleger antes e o caso não provaria nada).
        storage1.injectHeartbeat(CLIENT, 7L, 11L); // cliente inelegível: 11 (uma op à frente)
        storage1.startPeerHeartbeats(CLIENT, 7L, 11L);
        storage1.startPeerHeartbeats(STORAGE_0, 7L, 10L); // storage-0: 10, não lidera

        awaitLeader(storage1, STORAGE_1);
        assertEquals(10L, storage1.coord.maxActivePeerHighWatermark(),
                "a fronteira do gate A considera apenas peers elegíveis; o watermark do cliente fica de fora");
    }

    /**
     * Guarda: um storage ELEGÍVEL à frente continua bloqueando o reclaim, mesmo com o cliente presente —
     * a correção exclui só os inelegíveis, não desliga o gate A.
     */
    @Test
    @DisplayName("Storage elegível à frente continua adiando o reclaim do eleito por afinidade (guarda #179)")
    void eligiblePeerAheadStillDefersElectedSurvivor() throws Exception {
        Node storage1 = node(STORAGE_1, 2, List.of(eligible(STORAGE_0, 0), client(CLIENT, 3)));
        storage1.coord.setReplicationProgressGate(() -> 10L, 0L);
        storage1.start();
        storage1.startPeerHeartbeats(STORAGE_0, 7L, 11L); // storage-0 elegível à frente
        storage1.startPeerHeartbeats(CLIENT, 7L, 11L);

        awaitLeader(storage1, STORAGE_0); // defere e segue quem está à frente para sincronizar
        long deadline = System.currentTimeMillis() + 1000;
        while (System.currentTimeMillis() < deadline) {
            assertFalse(storage1.coord.isLeader(),
                    "um peer elegível à frente mantém o gate A fechado: o eleito não pode liderar atrás dele");
            Thread.sleep(50);
        }
        assertEquals(11L, storage1.coord.maxActivePeerHighWatermark());
    }

    /**
     * Ponta a ponta com dois coordinators reais (os sobreviventes) trocando heartbeats e o cliente
     * inelegível à frente: um líder emerge — o eleito por afinidade — e o outro sobrevivente o segue.
     *
     * <p>Como no failover real, os dois sobreviventes já conhecem o watermark 11 do cliente ANTES de
     * formarem quorum entre si: cada um sobe sozinho (sem quorum, sem líder), recebe o heartbeat do
     * cliente e só então passa a ouvir o outro storage. Antes da correção os dois ficavam sem líder
     * indefinidamente: storage-1 deferia a storage-0 pelo gate A (o cliente "à frente"), e storage-0
     * seguia storage-1 sem escape D9 (não estava estritamente à frente dele).</p>
     */
    @Test
    @DisplayName("Sobreviventes convergem para um líder com o cliente inelegível à frente (#179)")
    void survivorsConvergeOnALeaderWithIneligibleMemberAhead() throws Exception {
        Node storage0 = node(STORAGE_0, 0, List.of(eligible(STORAGE_1, 2), client(CLIENT, 3)));
        Node storage1 = node(STORAGE_1, 2, List.of(eligible(STORAGE_0, 0), client(CLIENT, 3)));
        for (Node n : List.of(storage0, storage1)) {
            n.coord.setReplicationProgressGate(() -> 10L, 0L);
            n.coord.setLeaderHighWatermarkSupplier(() -> 10L);
        }
        storage0.start();
        storage1.start();
        for (Node n : List.of(storage0, storage1)) {
            n.injectHeartbeat(CLIENT, 0L, 11L); // o cliente inelegível está uma op à frente
            n.startPeerHeartbeats(CLIENT, 0L, 11L);
            assertFalse(n.coord.isLeader(), "um storage sozinho não forma quorum e não se elege");
        }
        // Só agora os sobreviventes passam a se ouvir e podem formar quorum.
        storage0.transport.linkTo(storage1);
        storage1.transport.linkTo(storage0);

        awaitLeader(storage1, STORAGE_1);
        awaitLeader(storage0, STORAGE_1);
        assertFalse(storage0.coord.isLeader(), "storage-0 segue o eleito por afinidade, sem dual-leader");
    }

    // ---- harness (espelho do LeaderlessStalemateEscapeTest, sem pair mode) ----

    private static NodeInfo eligible(NodeId id, int priority) {
        return new NodeInfo(id, "127.0.0.1", portOf(id), Collections.emptySet(), priority);
    }

    private static NodeInfo client(NodeId id, int priority) {
        return new NodeInfo(id, "127.0.0.1", portOf(id), Set.of(NodeInfo.ROLE_LEADER_INELIGIBLE), priority);
    }

    private static int portOf(NodeId id) {
        if (STORAGE_0.equals(id)) {
            return 1;
        }
        return STORAGE_1.equals(id) ? 2 : 3;
    }

    private Node node(NodeId localId, int localPriority, List<NodeInfo> peers) {
        Node n = new Node(new NodeInfo(localId, "127.0.0.1", portOf(localId), Collections.emptySet(),
                localPriority), peers);
        closeables.add(n);
        return n;
    }

    private static void awaitLeader(Node n, NodeId expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (n.coord.leaderInfo().map(NodeInfo::nodeId).filter(expected::equals).isPresent()
                    && (!expected.equals(n.transport.local().nodeId()) || n.coord.isLeader())) {
                return;
            }
            Thread.sleep(25);
        }
        fail("[" + n.transport.local().nodeId() + "] líder não convergiu para " + expected
                + " (observado=" + n.coord.leaderInfo().map(NodeInfo::nodeId).orElse(null) + ")");
    }

    private static final class Node implements AutoCloseable {
        final LoopbackTransport transport;
        final ClusterCoordinator coord;
        final ScheduledExecutorService sched;
        final List<ScheduledFuture<?>> peerTasks = new CopyOnWriteArrayList<>();

        Node(NodeInfo localInfo, List<NodeInfo> peers) {
            this.transport = new LoopbackTransport(localInfo, peers);
            // Sem pair mode: 2 votantes elegíveis conhecidos (local + o outro storage) → maioria 2.
            ClusterCoordinatorConfig cfg = ClusterCoordinatorConfig.of(
                    Duration.ofMillis(150), Duration.ofMillis(600), Duration.ofSeconds(60), 1, null)
                    .withBootDiscoveryWindow(Duration.ZERO);
            this.sched = Executors.newScheduledThreadPool(3);
            this.coord = new ClusterCoordinator(transport, cfg, sched);
        }

        void start() {
            transport.start();
            coord.start();
        }

        void startPeerHeartbeats(NodeId source, long epoch, long highWatermark) {
            peerTasks.add(sched.scheduleAtFixedRate(() -> injectHeartbeat(source, epoch, highWatermark),
                    0, 100, TimeUnit.MILLISECONDS));
        }

        /** Entrega um heartbeat (sem afirmar liderança) de {@code source} de forma síncrona. */
        void injectHeartbeat(NodeId source, long epoch, long highWatermark) {
            coord.onMessage(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", source, null,
                    HeartbeatPayload.now(highWatermark, epoch, false)));
        }

        /** Entrega uma mensagem a este nó na thread do próprio scheduler (sem reentrância no emissor). */
        void deliver(ClusterMessage m) {
            try {
                sched.execute(() -> coord.onMessage(m));
            } catch (RejectedExecutionException ignored) {
                // nó já encerrado
            }
        }

        @Override
        public void close() {
            peerTasks.forEach(t -> t.cancel(true));
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
        private final List<Node> linked = new CopyOnWriteArrayList<>();

        LoopbackTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            peers.forEach(p -> connected.put(p.nodeId(), true));
        }

        /** Faz os broadcasts deste nó (os heartbeats) chegarem ao coordinator de {@code peer}. */
        void linkTo(Node peer) {
            linked.add(peer);
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
            linked.forEach(peer -> peer.deliver(m));
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
