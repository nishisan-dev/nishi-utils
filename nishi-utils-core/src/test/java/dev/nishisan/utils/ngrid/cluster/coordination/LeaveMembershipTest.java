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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Saída de membros no coordenador: um membro efêmero esquecido pelo transporte ({@code onPeerLeft})
 * sai da membership na hora — sem esperar o grace de disconnect — e heartbeats dele ainda em voo não
 * o recriam enquanto o id estiver em tombstone.
 */
class LeaveMembershipTest {

    private static final NodeId LOCAL = NodeId.of("node-2");
    private static final NodeId CLIENT = NodeId.of("client-1");
    private static final NodeId VOTER = NodeId.of("node-1");
    // Long heartbeat interval: the disconnect grace (one interval) is clearly distinguishable from an
    // immediate removal, and the periodic eviction never interferes.
    private static final Duration HEARTBEAT = Duration.ofSeconds(3);

    private final List<Harness> harnesses = new ArrayList<>();

    @AfterEach
    void tearDown() {
        harnesses.forEach(Harness::close);
    }

    @Test
    void leftEphemeralMemberIsRemovedAtOnceAndMembershipIsNotifiedOnce() throws Exception {
        Harness h = harness();
        h.join(h.client);
        awaitTrue(() -> h.activeIds().contains(CLIENT), "cliente ativo");
        int before = h.membershipEvents.get();

        h.transport.departed.add(CLIENT);
        h.transport.known.remove(h.client);
        h.coord.onPeerLeft(CLIENT);

        assertFalse(h.activeIds().contains(CLIENT), "o membro que saiu deveria sair na hora, sem grace");
        Thread.sleep(200);
        assertEquals(before + 1, h.membershipEvents.get(), "uma única notificação de membership");
        // The transport-level disconnect that follows is not news.
        h.coord.onPeerDisconnected(CLIENT);
        Thread.sleep(200);
        assertEquals(before + 1, h.membershipEvents.get(), "o disconnect posterior não notifica de novo");
    }

    @Test
    void leftMemberAlreadyInactiveDoesNotNotifyMembership() throws Exception {
        Harness h = harness();
        h.join(h.client);
        awaitTrue(() -> h.activeIds().contains(CLIENT), "cliente ativo");
        h.transport.connected.remove(CLIENT);
        h.coord.onPeerDisconnected(CLIENT);
        awaitTrue(() -> !h.activeIds().contains(CLIENT), "cliente inativo após o grace");
        int before = h.membershipEvents.get();

        h.transport.departed.add(CLIENT);
        h.coord.onPeerLeft(CLIENT);

        Thread.sleep(200);
        assertEquals(before, h.membershipEvents.get(), "sair já inativo não muda nada observável");
    }

    @Test
    void heartbeatOfADepartedPeerDoesNotRecreateTheMember() throws Exception {
        Harness h = harness();
        h.join(h.client);
        awaitTrue(() -> h.activeIds().contains(CLIENT), "cliente ativo");

        h.transport.departed.add(CLIENT);
        h.coord.onPeerLeft(CLIENT);
        h.heartbeat(CLIENT); // read before the LEAVE, dispatched after it

        assertFalse(h.activeIds().contains(CLIENT), "heartbeat atrasado recriou o membro que saiu");
    }

    /**
     * LEAVE de um membro elegível (votante): não é esquecido — segue na membership e em knownPeers —, mas
     * fica inativo na hora, sem esperar o grace de disconnect; se o mesmo id voltar, o próximo heartbeat
     * o reativa.
     */
    @Test
    void leavingVoterIsMarkedInactiveAtOnceAndAHeartbeatReactivatesIt() throws Exception {
        Harness h = harness();
        h.join(h.voter);
        awaitTrue(() -> h.activeIds().contains(VOTER), "votante ativo");
        int before = h.membershipEvents.get();

        h.transport.connected.remove(VOTER); // the transport closed the leaver's connection
        h.coord.onPeerLeaving(VOTER);

        assertFalse(h.activeIds().contains(VOTER), "o votante que anunciou a saída deveria ficar inativo na hora");
        assertEquals(before + 1, h.membershipEvents.get());

        // Revisão #178 (A4): um heartbeat da encarnação que SAIU, lido antes do LEAVE e despachado
        // depois, não pode reativar o membro (re-adotava o líder rebaixado por um ciclo inteiro).
        h.heartbeat(VOTER);
        assertFalse(h.activeIds().contains(VOTER), "heartbeat em voo da encarnação que saiu não reativa o votante");

        // Quem volta com o mesmo id fala de novo pelo handshake (onPeerConnected) — aí o heartbeat reativa.
        h.transport.connected.add(VOTER);
        h.coord.onPeerConnected(h.voter);
        h.heartbeat(VOTER);
        assertTrue(h.activeIds().contains(VOTER), "o heartbeat de quem voltou com o mesmo id o reativa");
    }

    // ---- harness ----

    private Harness harness() {
        Harness h = new Harness();
        harnesses.add(h);
        h.start();
        return h;
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition, String what)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        fail("condition not met in time: " + what);
    }

    static final class Harness {
        final NodeInfo local = new NodeInfo(LOCAL, "127.0.0.1", 1, Set.of(), 0);
        final NodeInfo client = new NodeInfo(CLIENT, "127.0.0.1", 3,
                Set.of("client", NodeInfo.ROLE_LEADER_INELIGIBLE), 0);
        final NodeInfo voter = new NodeInfo(VOTER, "127.0.0.1", 2, Set.of(), 0);
        final LoopbackTransport transport = new LoopbackTransport(local);
        final ScheduledExecutorService sched = Executors.newScheduledThreadPool(2);
        final ClusterCoordinator coord;
        final AtomicInteger membershipEvents = new AtomicInteger();

        Harness() {
            ClusterCoordinatorConfig cfg = ClusterCoordinatorConfig.of(
                    HEARTBEAT, HEARTBEAT.multipliedBy(3), Duration.ofSeconds(60), 1, null)
                    .withPairMode(true)
                    .withBootDiscoveryWindow(Duration.ZERO);
            coord = new ClusterCoordinator(transport, cfg, sched);
            coord.addMembershipListener(membershipEvents::incrementAndGet);
        }

        void start() {
            coord.start();
        }

        void join(NodeInfo peer) {
            transport.known.add(peer);
            transport.connected.add(peer.nodeId());
            coord.onPeerConnected(peer);
            heartbeat(peer.nodeId());
        }

        void heartbeat(NodeId source) {
            coord.onMessage(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", source, null,
                    HeartbeatPayload.now(0L, 0L, false)));
        }

        Set<NodeId> activeIds() {
            Set<NodeId> ids = new java.util.HashSet<>();
            coord.activeMembers().forEach(m -> ids.add(m.nodeId()));
            return ids;
        }

        void close() {
            try {
                coord.close();
            } catch (Exception ignored) {
                // best-effort cleanup
            }
            sched.shutdownNow();
        }
    }

    static final class LoopbackTransport implements Transport {
        private final NodeInfo local;
        final Set<NodeInfo> known = new CopyOnWriteArraySet<>();
        final Set<NodeId> connected = new CopyOnWriteArraySet<>();
        final Set<NodeId> departed = new CopyOnWriteArraySet<>();

        LoopbackTransport(NodeInfo local) {
            this.local = local;
            known.add(local);
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
            return List.copyOf(known);
        }

        @Override
        public void addListener(TransportListener l) {
        }

        @Override
        public void removeListener(TransportListener l) {
        }

        @Override
        public void broadcast(ClusterMessage m) {
        }

        @Override
        public void send(ClusterMessage m) {
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage m) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("not used"));
        }

        @Override
        public boolean isConnected(NodeId nodeId) {
            return connected.contains(nodeId);
        }

        @Override
        public boolean isReachable(NodeId nodeId) {
            return isConnected(nodeId);
        }

        @Override
        public boolean isDeparted(NodeId nodeId) {
            return departed.contains(nodeId);
        }

        @Override
        public void addPeer(NodeInfo peer) {
        }

        @Override
        public void close() throws IOException {
        }
    }
}
