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
import java.nio.file.Files;
import java.nio.file.Path;
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

/**
 * Revisão #178 (C3, C8, B10): endurecimentos do coordinator — isolamento de listeners, persistência
 * do epoch, step-down no {@code stop()}, votante com porta 0 fora do numerador do quórum,
 * reativação por heartbeat notificando a membership e evicção atômica com {@code touch()}.
 */
class CoordinatorHardeningTest {

    private static final NodeId LOCAL = NodeId.of("node-local");
    private static final Duration HB = Duration.ofMillis(100);

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

    /** C3: um listener que lança não impede os seguintes nem a notificação de eleição. */
    @Test
    void throwingLeadershipListenerDoesNotSkipTheOthers() throws Exception {
        Harness h = new Harness(null, true, List.of());
        AtomicInteger secondCalls = new AtomicInteger();
        h.coord.addLeadershipListener(leader -> {
            throw new IllegalStateException("listener quebrado");
        });
        h.coord.addLeadershipListener(leader -> secondCalls.incrementAndGet());
        AtomicReference<Boolean> electionSeen = new AtomicReference<>(false);
        h.coord.addLeaderElectionListener((isLeader, leader) -> electionSeen.set(isLeader));
        h.start();
        awaitTrue(() -> h.coord.isLeader(), "líder sozinho");
        awaitTrue(() -> secondCalls.get() > 0, "segundo listener chamado apesar do primeiro lançar");
        awaitTrue(electionSeen::get, "listener de eleição notificado");
    }

    /** C8: o epoch é persistido no diretório de dados e nunca regride no restart. */
    @Test
    void leaderEpochIsPersistedAndRestoredOnRestart() throws Exception {
        Path dir = Files.createTempDirectory("epoch-persist");
        Harness first = new Harness(dir, true, List.of());
        first.start();
        awaitTrue(() -> first.coord.isLeader(), "líder sozinho");
        long epochBefore = first.coord.getLeaderEpoch();
        assertTrue(epochBefore >= 1L);
        first.close();

        Harness second = new Harness(dir, true, List.of());
        assertTrue(second.coord.getLeaderEpoch() >= epochBefore,
                "epoch carregado do disco antes mesmo do start (era " + epochBefore + ")");
        second.start();
        awaitTrue(() -> second.coord.isLeader(), "líder de novo");
        assertTrue(second.coord.getLeaderEpoch() > epochBefore, "a nova eleição avança acima do epoch persistido");
    }

    /** B10: stop() rebaixa o líder — nada mais responde isLeader()/hasValidLease(). */
    @Test
    void stopStepsDownAndClearsTheLeader() throws Exception {
        Harness h = new Harness(null, true, List.of());
        AtomicReference<NodeId> lastLeader = new AtomicReference<>(LOCAL);
        h.coord.addLeadershipListener(lastLeader::set);
        h.start();
        awaitTrue(() -> h.coord.isLeader(), "líder sozinho");
        h.coord.stop();
        assertFalse(h.coord.isLeader(), "coordinator parado não é líder");
        assertFalse(h.coord.hasValidLease(), "coordinator parado não tem lease");
        assertEquals(null, lastLeader.get(), "listeners viram o step-down");
    }

    /** C9: o líder que não ouve NENHUM votante por um heartbeatTimeout inteiro rebaixa na hora. */
    @Test
    void isolatedLeaderStepsDownWithinOneHeartbeatTimeout() throws Exception {
        // Afinidade menor que a do local (prio 0): o local é o eleito por afinidade.
        NodeInfo v1 = new NodeInfo(NodeId.of("node-v1"), "127.0.0.1", 2, Set.of(), -10);
        NodeInfo v2 = new NodeInfo(NodeId.of("node-v2"), "127.0.0.1", 3, Set.of(), -10);
        Harness h = new Harness(null, false, List.of(v1, v2)); // não-pair: maioria 2 de 3
        h.start();
        java.util.concurrent.ScheduledFuture<?> beats = h.sched.scheduleAtFixedRate(() -> {
            h.heartbeat(v1.nodeId());
            h.heartbeat(v2.nodeId());
        }, 0, 50, java.util.concurrent.TimeUnit.MILLISECONDS);
        awaitTrue(() -> h.coord.isLeader(), "líder com quórum (maior afinidade: id local)");
        assertTrue(h.coord.hasValidLease());

        // Partição: os dois votantes somem de uma vez (o transporte ainda os diz "conectados", como
        // um link half-open — a evicção por membro esperaria o grace de proxy, ~3× o timeout).
        beats.cancel(true);
        long t0 = System.currentTimeMillis();
        awaitTrue(() -> !h.coord.isLeader(), "líder isolado rebaixa");
        long took = System.currentTimeMillis() - t0;
        assertTrue(took <= HB.multipliedBy(9).toMillis(),
                "step-down proativo em ~1 heartbeatTimeout (3×HB), não após o grace (levou " + took + "ms)");
        assertFalse(h.coord.hasValidLease(), "isolado: sem lease, escritas rejeitadas");

        // Os votantes voltam: o step-down agendou a reavaliação e o nó reassume sem evento externo.
        java.util.concurrent.ScheduledFuture<?> back = h.sched.scheduleAtFixedRate(() -> {
            h.heartbeat(v1.nodeId());
            h.heartbeat(v2.nodeId());
        }, 0, 50, java.util.concurrent.TimeUnit.MILLISECONDS);
        awaitTrue(() -> h.coord.isLeader(), "reassume quando o quórum volta");
        back.cancel(true);
    }

    /** B10: um membro elegível com porta 0 não conta como votante ativo (mesma população do denominador). */
    @Test
    void portZeroMemberDoesNotCountTowardsTheVoterMajority() throws Exception {
        NodeInfo realVoter = new NodeInfo(NodeId.of("node-real"), "127.0.0.1", 2, Set.of(), 0);
        NodeInfo portZero = new NodeInfo(NodeId.of("node-zero"), "127.0.0.1", 0, Set.of(), 0);
        Harness h = new Harness(null, false, List.of(realVoter, portZero)); // não-pair: maioria de 2 votantes
        h.start();
        // Só o membro de porta 0 dá sinal de vida: antes, ele inflava o numerador (2 ≥ 2) e o local
        // liderava sem o votante real — split-brain em potencial.
        long deadline = System.currentTimeMillis() + 1_200;
        while (System.currentTimeMillis() < deadline) {
            h.heartbeat(portZero.nodeId());
            assertFalse(h.coord.isLeader(), "sem o votante real não há maioria (porta 0 não vota)");
            Thread.sleep(50);
        }
        // O votante real aparece: maioria 2/2, o local (maior id? não — maior afinidade decide) elege.
        h.heartbeat(realVoter.nodeId());
        awaitTrue(() -> h.coord.leaderInfo().isPresent(), "com o votante real há maioria e um líder");
    }

    /** B10: reativação por heartbeat (sem handshake) notifica os listeners de membership. */
    @Test
    void reactivationByHeartbeatNotifiesMembership() throws Exception {
        NodeInfo peer = new NodeInfo(NodeId.of("node-peer"), "127.0.0.1", 2, Set.of(), 0);
        Harness h = new Harness(null, true, List.of(peer));
        AtomicInteger events = new AtomicInteger();
        h.coord.addMembershipListener(events::incrementAndGet);
        h.start();
        h.heartbeat(peer.nodeId());
        awaitTrue(() -> h.activeIds().contains(peer.nodeId()), "peer ativo");
        awaitTrue(() -> !h.activeIds().contains(peer.nodeId()), "peer evictado sem heartbeats");
        int before = events.get();
        h.heartbeat(peer.nodeId());
        awaitTrue(() -> h.activeIds().contains(peer.nodeId()), "peer reativado pelo heartbeat");
        assertTrue(events.get() > before, "a reativação é uma mudança de membership visível");
    }

    /** B10: a evicção não sobrescreve um heartbeat que chegou entre a checagem e a marcação. */
    @Test
    void clusterMemberIsNotMarkedInactiveWhenTouchedAfterTheCheck() {
        ClusterMember member = new ClusterMember(new NodeInfo(NodeId.of("m"), "127.0.0.1", 2));
        long threshold = System.currentTimeMillis() + 1_000; // "stale" if last heartbeat <= threshold
        member.touch();
        assertFalse(member.markInactiveIfStale(member.lastHeartbeat() - 1), "heartbeat fresco: não evicta");
        assertTrue(member.markInactiveIfStale(threshold), "heartbeat velho: evicta");
        assertFalse(member.isActive());
        assertTrue(member.touchAndReportReactivation(), "touch reporta a reativação");
        assertFalse(member.touchAndReportReactivation(), "já ativo: sem reativação");
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        fail("condição não atingida: " + what);
    }

    private final class Harness implements AutoCloseable {
        final LoopbackTransport transport;
        final ClusterCoordinator coord;
        final ScheduledExecutorService sched = Executors.newScheduledThreadPool(2);

        Harness(Path dataDir, boolean pairMode, List<NodeInfo> peers) {
            NodeInfo local = new NodeInfo(LOCAL, "127.0.0.1", 1, Set.of(), 0);
            this.transport = new LoopbackTransport(local, peers);
            ClusterCoordinatorConfig cfg = ClusterCoordinatorConfig.of(HB, HB.multipliedBy(3),
                    Duration.ofSeconds(60), 1, dataDir)
                    .withPairMode(pairMode)
                    .withBootDiscoveryWindow(Duration.ZERO);
            this.coord = new ClusterCoordinator(transport, cfg, sched);
            closeables.add(this);
        }

        void start() {
            coord.start();
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

        @Override
        public void close() {
            try {
                coord.close();
            } catch (Exception ignored) {
            }
            sched.shutdownNow();
        }
    }

    private static final class LoopbackTransport implements Transport {
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();

        LoopbackTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
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
        @Override public void broadcast(ClusterMessage m) { }
        @Override public void send(ClusterMessage m) { }
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
