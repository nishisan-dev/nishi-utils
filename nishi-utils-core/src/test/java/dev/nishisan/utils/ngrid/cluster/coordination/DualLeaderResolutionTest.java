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
import org.junit.jupiter.api.Timeout;

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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Resolução determinística de dual-leader (issue tems#9, D10c), no nível do coordinator.
 *
 * <p>Incidente real: dois líderes estáveis sob produção contínua, cada um re-stampando o epoch
 * "above observed" a cada heartbeat (escada 22→105+) e ambos consumindo o consumer group — sem
 * nenhum mecanismo que comparasse afinidade para decidir quem cede. A resolução: ambos os lados
 * rodam a MESMA ordem total da eleição (prioridade, depois NodeId); o de MENOR afinidade cede após
 * {@code DUAL_LEADER_OBSERVATIONS_TO_RESOLVE} heartbeats consecutivos com a flag de líder do rival,
 * invoca o hook de resync (descarte da linhagem da janela dual) e adota o rival — em vez de
 * alimentar a escada de epochs.
 */
class DualLeaderResolutionTest {

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

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("O líder de menor afinidade cede após o debounce e invoca o hook de resync 1x")
    void lowerAffinityLeaderYieldsAfterDebounce() throws Exception {
        Harness h = harness(INCUMBENT, 50, PREFERRED, 100);
        AtomicInteger yieldHookInvocations = new AtomicInteger();
        h.coord.setDualLeaderYieldHook(yieldHookInvocations::incrementAndGet);
        h.start();
        awaitSelfLeadership(h);

        // O rival (maior afinidade) também se afirma líder a cada heartbeat — dual estável.
        long deadline = System.currentTimeMillis() + 10_000;
        while (h.coord.isLeader() && System.currentTimeMillis() < deadline) {
            h.deliverHeartbeat(PREFERRED, 7L, 900L, true);
            Thread.sleep(50);
        }

        assertFalse(h.coord.isLeader(), "o lado de menor afinidade deve ceder");
        assertEquals(PREFERRED, h.coord.leaderInfo().map(NodeInfo::nodeId).orElse(null),
                "o perdedor adota o vencedor como líder local");
        assertEquals(1, yieldHookInvocations.get(),
                "o hook de resync (descarte da linhagem dual) roda exatamente uma vez");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("O líder de maior afinidade NUNCA cede — exatamente um lado resolve")
    void higherAffinityLeaderNeverYields() throws Exception {
        Harness h = harness(PREFERRED, 100, INCUMBENT, 50);
        AtomicInteger yieldHookInvocations = new AtomicInteger();
        h.coord.setDualLeaderYieldHook(yieldHookInvocations::incrementAndGet);
        h.start();
        awaitSelfLeadership(h);

        long deadline = System.currentTimeMillis() + 1_500;
        while (System.currentTimeMillis() < deadline) {
            h.deliverHeartbeat(INCUMBENT, 7L, 900L, true);
            assertTrue(h.coord.isLeader(),
                    "o vencedor da ordem determinística retém — se ambos cedessem, leaderless");
            Thread.sleep(50);
        }
        assertEquals(0, yieldHookInvocations.get(), "o vencedor jamais invoca o hook de yield");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("Flap da flag zera o debounce: só observações CONSECUTIVAS resolvem")
    void interruptedAssertionsNeverResolve() throws Exception {
        Harness h = harness(INCUMBENT, 50, PREFERRED, 100);
        AtomicInteger yieldHookInvocations = new AtomicInteger();
        h.coord.setDualLeaderYieldHook(yieldHookInvocations::incrementAndGet);
        h.start();
        awaitSelfLeadership(h);

        // Padrão de handoff legítimo: a janela de overlap é sub-heartbeat — a flag não persiste.
        for (int round = 0; round < 4; round++) {
            h.deliverHeartbeat(PREFERRED, 7L, 900L, true);
            h.deliverHeartbeat(PREFERRED, 7L, 900L, true);
            h.deliverHeartbeat(PREFERRED, 7L, 900L, false); // quebra a consecutividade
            assertTrue(h.coord.isLeader(),
                    "observações não-consecutivas jamais resolvem (anti-flapping)");
        }
        assertEquals(0, yieldHookInvocations.get());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("O perdedor adota o epoch observado (sem re-stamp) — a escada morre na primeira observação")
    void epochLadderIsSuppressedOnTheLosingSide() throws Exception {
        Harness h = harness(INCUMBENT, 50, PREFERRED, 100);
        h.coord.setDualLeaderYieldHook(() -> {
        });
        h.start();
        awaitSelfLeadership(h);

        // Primeira observação do rival-líder com epoch acima: o perdedor ADOTA (sem +1).
        h.deliverHeartbeat(PREFERRED, 40L, 900L, true);
        assertEquals(40L, h.coord.getLeaderEpoch(),
                "o lado perdedor adota o epoch observado em vez de re-stampar acima (mata a escada)");

        h.deliverHeartbeat(PREFERRED, 42L, 900L, true);
        assertEquals(42L, h.coord.getLeaderEpoch(),
                "cada observação subsequente também é adotada, nunca re-stampada");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("Empate de prioridade resolve por NodeId — a mesma ordem total da eleição")
    void priorityTieBreaksByNodeIdConsistentWithElection() throws Exception {
        // Mesma prioridade: o comparator da eleição dá a liderança ao MAIOR NodeId. node-1 < node-2,
        // então o node-1 cede e o node-2 retém — empates jamais invertem a ordem da eleição.
        Harness loser = harness(PREFERRED, 50, INCUMBENT, 50); // local node-1 (menor id)
        AtomicInteger loserHook = new AtomicInteger();
        loser.coord.setDualLeaderYieldHook(loserHook::incrementAndGet);
        loser.start();
        awaitSelfLeadership(loser);
        long deadline = System.currentTimeMillis() + 10_000;
        while (loser.coord.isLeader() && System.currentTimeMillis() < deadline) {
            loser.deliverHeartbeat(INCUMBENT, 7L, 900L, true);
            Thread.sleep(50);
        }
        assertFalse(loser.coord.isLeader(), "no empate de prioridade, o menor NodeId cede");
        assertEquals(1, loserHook.get());

        Harness winner = harness(INCUMBENT, 50, PREFERRED, 50); // local node-2 (maior id)
        AtomicInteger winnerHook = new AtomicInteger();
        winner.coord.setDualLeaderYieldHook(winnerHook::incrementAndGet);
        winner.start();
        awaitSelfLeadership(winner);
        deadline = System.currentTimeMillis() + 1_200;
        while (System.currentTimeMillis() < deadline) {
            winner.deliverHeartbeat(PREFERRED, 7L, 900L, true);
            assertTrue(winner.coord.isLeader(), "no empate de prioridade, o maior NodeId retém");
            Thread.sleep(50);
        }
        assertEquals(0, winnerHook.get());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("Rival com NodeInfo desconhecida (placeholder) nunca dispara a resolução")
    void unknownRivalInfoNeverResolves() throws Exception {
        // O rival NÃO está nos peers configurados: heartbeats criam apenas um placeholder (port 0).
        Harness h = harnessWithoutConfiguredPeer(INCUMBENT, 50);
        AtomicInteger yieldHookInvocations = new AtomicInteger();
        h.coord.setDualLeaderYieldHook(yieldHookInvocations::incrementAndGet);
        h.start();
        awaitSelfLeadership(h);

        NodeId stranger = NodeId.of("node-9");
        long deadline = System.currentTimeMillis() + 1_200;
        while (System.currentTimeMillis() < deadline) {
            h.deliverHeartbeat(stranger, 7L, 900L, true);
            assertTrue(h.coord.isLeader(),
                    "afinidade desconhecida nunca decide uma cessão de liderança");
            Thread.sleep(50);
        }
        assertEquals(0, yieldHookInvocations.get());
    }

    // ---- harness (espelho do LeaderlessStalemateEscapeTest, com flag de líder) ----

    private Harness harness(NodeId localId, int localPriority, NodeId peerId, int peerPriority) {
        Harness h = new Harness(localId, localPriority, peerId, peerPriority);
        closeables.add(h);
        return h;
    }

    private Harness harnessWithoutConfiguredPeer(NodeId localId, int localPriority) {
        Harness h = new Harness(localId, localPriority, null, 0);
        closeables.add(h);
        return h;
    }

    private static void awaitSelfLeadership(Harness h) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (h.coord.isLeader()) {
                return;
            }
            Thread.sleep(25);
        }
        fail("o nó local não se elegeu (pairMode/lead-while-alone)");
    }

    private static final class Harness implements AutoCloseable {
        final LoopbackTransport transport;
        final ClusterCoordinator coord;
        final ScheduledExecutorService sched;

        Harness(NodeId localId, int localPriority, NodeId peerId, int peerPriority) {
            NodeInfo localInfo = new NodeInfo(localId, "127.0.0.1", 1, Collections.emptySet(), localPriority);
            List<NodeInfo> peers = peerId == null
                    ? List.of()
                    : List.of(new NodeInfo(peerId, "127.0.0.1", 2, Collections.emptySet(), peerPriority));
            this.transport = new LoopbackTransport(localInfo, peers);
            ClusterCoordinatorConfig cfg = ClusterCoordinatorConfig.of(
                    Duration.ofMillis(150), Duration.ofSeconds(60), Duration.ofSeconds(60), 1, null)
                    .withPairMode(true)
                    .withBootDiscoveryWindow(Duration.ZERO);
            this.sched = Executors.newScheduledThreadPool(2);
            this.coord = new ClusterCoordinator(transport, cfg, sched);
            // Gate B do incumbente: watermark local conhecido e ACIMA do anunciado pelo rival (900),
            // para reter a liderança como no incidente — sem isso a eleição normal cederia antes da
            // detecção e não haveria dual a resolver.
            coord.setLeaderHighWatermarkSupplier(() -> 1_000L);
            coord.setReplicationProgressGate(() -> 1_000L, 0L);
        }

        void start() {
            transport.start();
            coord.start();
        }

        void deliverHeartbeat(NodeId source, long epoch, long highWatermark, boolean leader) {
            coord.onMessage(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", source, null,
                    HeartbeatPayload.now(highWatermark, epoch, leader)));
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
