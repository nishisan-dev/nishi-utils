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
package dev.nishisan.utils.ngrid.replication;

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinatorConfig;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.FollowerProgressPayload;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Quiesce-assisted reclaim — "leader pause on reclaim" (issue tems#9, D10b): quando um candidato de
 * MAIOR afinidade se aproxima do watermark do incumbente (dentro de {@code reclaimQuiesceThreshold}),
 * o líder pausa a produção para o candidato emparelhar EXATO e o handoff por afinidade completar de
 * forma coordenada — sob produção contínua o delta in-flight nunca zeraria e o gate B estrito
 * reteria a liderança para sempre (metade do livelock dual-leader do D10).
 */
class ReclaimQuiesceTest {

    private static final String TOPIC = "cardinal-state";
    private static final NodeId LOCAL = NodeId.of("zzz-incumbent");
    private static final NodeId CANDIDATE = NodeId.of("aaa-candidate");

    private static final int LOCAL_PRIORITY = 50;
    private static final int CANDIDATE_PRIORITY = 100;

    /** Perfil "líder promovido": produziu 10, aplicou 10.000 — watermark real 10.000. */
    private static final long PROMOTED_GLOBAL = 10L;
    private static final long PROMOTED_APPLIED = 10_000L;
    private static final long APPROACH_THRESHOLD = 500L;

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("reclaim-quiesce");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("Aproximação engaja, replicate() pausa e o emparelhamento exato completa o handoff")
    void approachEngagesPauseAndExactPairingHandsOff() throws Exception {
        writePromotedLeaderSequenceState();
        try (Harness h = new Harness(tempDir, scheduler, Duration.ofSeconds(10), Duration.ofSeconds(60), false)) {
            h.startAsLeader();
            h.connectCandidate(CANDIDATE_PRIORITY, 5_000L);
            h.assertNotQuiescing(500);

            // Candidato entra na janela de aproximação (delta 300 <= 500): engaja e pausa produção.
            h.deliverProgress(PROMOTED_APPLIED - 300L, h.coordinator.getLeaderEpoch());
            h.awaitReclaimQuiescing(true, 5_000);
            LeaderSyncingException pause = assertThrows(LeaderSyncingException.class,
                    () -> h.manager.replicate(TOPIC, "blocked-during-pause"));
            assertTrue(pause.getMessage().contains("affinity handoff"),
                    "a pausa deve ser atribuída ao reclaim-quiesce, não a outro gate");

            // Emparelhamento exato contra o watermark congelado: gate B abre e o incumbente cede.
            h.deliverProgress(PROMOTED_APPLIED, h.coordinator.getLeaderEpoch());
            h.awaitHandoffToCandidate(5_000);
            h.awaitReclaimQuiescing(false, 5_000);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("Não engaja: delta acima do threshold, epoch divergente ou candidato de menor afinidade")
    void neverEngagesOutsideApproachWindowOrForIncomparableProgress() throws Exception {
        writePromotedLeaderSequenceState();
        try (Harness h = new Harness(tempDir, scheduler, Duration.ofSeconds(10), Duration.ofSeconds(60), false)) {
            h.startAsLeader();
            h.connectCandidate(CANDIDATE_PRIORITY, 5_000L);

            // Longe (delta 1.000 > 500): catch-up segue assíncrono, produção segue ligada.
            h.deliverProgress(PROMOTED_APPLIED - 1_000L, h.coordinator.getLeaderEpoch());
            h.assertNotQuiescing(600);

            // Perto mas com epoch divergente: escala incomparável — nunca engaja.
            h.deliverProgress(PROMOTED_APPLIED - 100L, h.coordinator.getLeaderEpoch() + 7);
            h.assertNotQuiescing(600);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("Candidato de menor afinidade nunca engaja o quiesce (não há handoff a assistir)")
    void lowerAffinityCandidateNeverEngages() throws Exception {
        writePromotedLeaderSequenceState();
        try (Harness h = new Harness(tempDir, scheduler, Duration.ofSeconds(10), Duration.ofSeconds(60), false)) {
            h.startAsLeader();
            // Prioridade menor que a local: mesmo emparelhado, não há cessão por afinidade.
            h.connectCandidate(10, 5_000L);
            h.deliverProgress(PROMOTED_APPLIED, h.coordinator.getLeaderEpoch());
            h.assertNotQuiescing(600);
            assertTrue(h.coordinator.isLeader(), "o incumbente segue líder — nada a ceder");
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("Timeout libera retendo a liderança e o cooldown impede o loop de re-engage")
    void timeoutReleasesRetainingLeadershipAndCooldownPreventsLoop() throws Exception {
        writePromotedLeaderSequenceState();
        try (Harness h = new Harness(tempDir, scheduler, Duration.ofMillis(700), Duration.ofSeconds(30), false)) {
            h.startAsLeader();
            h.connectCandidate(CANDIDATE_PRIORITY, 5_000L);

            // Engaja mas o candidato nunca emparelha: o timeout solta a pausa (disponibilidade).
            h.deliverProgress(PROMOTED_APPLIED - 300L, h.coordinator.getLeaderEpoch());
            h.awaitReclaimQuiescing(true, 5_000);
            h.awaitReclaimQuiescing(false, 5_000);
            assertTrue(h.coordinator.isLeader(), "abort sem handoff retém a liderança");

            // Nova aproximação dentro do cooldown: não re-engaja (anti-loop de pausas).
            h.deliverProgress(PROMOTED_APPLIED - 200L, h.coordinator.getLeaderEpoch());
            h.assertNotQuiescing(1_000);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("Join-quiesce ativo tem precedência: o reclaim-quiesce não engaja por cima")
    void joinQuiesceTakesPrecedenceOverReclaimQuiesce() throws Exception {
        writePromotedLeaderSequenceState();
        try (Harness h = new Harness(tempDir, scheduler, Duration.ofSeconds(10), Duration.ofSeconds(60), true)) {
            h.startAsLeader();
            // O candidato entra como joiner: o join-quiesce engaja primeiro (produção já pausada).
            h.connectCandidate(CANDIDATE_PRIORITY, 5_000L);
            h.awaitJoinQuiescing(true, 5_000);

            h.deliverProgress(PROMOTED_APPLIED - 300L, h.coordinator.getLeaderEpoch());
            h.assertNotQuiescing(600);
        }
    }

    /** Fabrica o sequence-state.dat exatamente como {@code saveSequenceState} persiste. */
    private void writePromotedLeaderSequenceState() throws IOException {
        Map<String, Long> persisted = new HashMap<>();
        persisted.put(TOPIC, PROMOTED_APPLIED + 1L);
        persisted.put("_global", PROMOTED_GLOBAL);
        persisted.put("_topic:" + TOPIC, PROMOTED_GLOBAL);
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(
                Files.newOutputStream(tempDir.resolve("sequence-state.dat")))) {
            oos.writeObject(persisted);
        }
    }

    // ---- harness ----

    private static final class Harness implements AutoCloseable {
        final RecordingTransport transport;
        final ClusterCoordinator coordinator;
        final ReplicationManager manager;
        final ReplicationHandler handler = new ReplicationHandler() {
            @Override
            public void apply(UUID operationId, Object payload) {
            }

            @Override
            public void resetState() {
            }

            @Override
            public void installSnapshot(Object snapshot) {
            }
        };

        Harness(Path dataDir, ScheduledExecutorService scheduler, Duration reclaimMaxDuration,
                Duration reclaimCooldown, boolean leaderPauseOnJoin) {
            NodeInfo localNode = new NodeInfo(LOCAL, "127.0.0.1", 1, Set.of(), LOCAL_PRIORITY);
            this.transport = new RecordingTransport(localNode, List.of());
            this.coordinator = new ClusterCoordinator(transport,
                    ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(60), 1),
                    scheduler);
            coordinator.start();
            this.manager = new ReplicationManager(transport, coordinator,
                    ReplicationConfig.builder(1)
                            .strictConsistency(false)
                            .leaderLocalApply(false)
                            .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                            .operationTimeout(Duration.ofSeconds(5))
                            .dataDirectory(dataDir)
                            .leaderPauseOnJoin(leaderPauseOnJoin)
                            .joinQuiesceMaxDuration(Duration.ofSeconds(25))
                            .leaderPauseOnReclaim(true)
                            .reclaimQuiesceThreshold(APPROACH_THRESHOLD)
                            .reclaimQuiesceMaxDuration(reclaimMaxDuration)
                            .reclaimQuiesceCooldown(reclaimCooldown)
                            .build());
        }

        /** Start + auto-eleição + drain-gate liberado (replicate() utilizável). */
        void startAsLeader() throws InterruptedException {
            manager.registerHandler(TOPIC, handler);
            manager.start();
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                if (coordinator.isLeader() && !manager.isLeaderSyncing()) {
                    return;
                }
                Thread.sleep(50);
            }
            fail("o nó não assumiu liderança utilizável (leader + drain-gate liberado)");
        }

        void connectCandidate(int priority, long announcedWatermark) {
            // Espelha o fluxo real: o primeiro heartbeat chega junto do connect, então o watermark
            // do peer é conhecido quando a membership muda (sem deferral de watermark-desconhecido).
            transport.deliverToListeners(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb",
                    CANDIDATE, null, HeartbeatPayload.now(announcedWatermark, 0L)));
            transport.notifyPeerConnected(new NodeInfo(CANDIDATE, "127.0.0.1", 2, Set.of(), priority));
        }

        void deliverProgress(long applied, long epoch) {
            transport.deliverToListeners(ClusterMessage.request(MessageType.FOLLOWER_PROGRESS,
                    "follower-progress", CANDIDATE, LOCAL, new FollowerProgressPayload(applied, epoch)));
        }

        void awaitReclaimQuiescing(boolean expected, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                if (manager.isReclaimQuiescing() == expected) {
                    return;
                }
                Thread.sleep(25);
            }
            fail("reclaim-quiesce não atingiu o estado esperado: " + expected);
        }

        void awaitJoinQuiescing(boolean expected, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                if (manager.isJoinQuiescing() == expected) {
                    return;
                }
                Thread.sleep(25);
            }
            fail("join-quiesce não atingiu o estado esperado: " + expected);
        }

        /** Asserta que o reclaim-quiesce NÃO engaja durante toda a janela. */
        void assertNotQuiescing(long windowMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + windowMs;
            while (System.currentTimeMillis() < deadline) {
                assertFalse(manager.isReclaimQuiescing(),
                        "o reclaim-quiesce engajou fora da janela de aproximação válida");
                Thread.sleep(50);
            }
        }

        void awaitHandoffToCandidate(long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                boolean handedOff = !coordinator.isLeader()
                        && CANDIDATE.equals(coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null));
                if (handedOff) {
                    return;
                }
                Thread.sleep(25);
            }
            fail("o incumbente não cedeu ao candidato emparelhado (gate B não abriu)");
        }

        @Override
        public void close() {
            try {
                manager.close();
            } catch (Exception ignored) {
            }
            try {
                coordinator.close();
            } catch (Exception ignored) {
            }
        }
    }

    private static final class RecordingTransport implements Transport {
        private final NodeInfo local;
        private final List<NodeInfo> peers;
        private final CopyOnWriteArraySet<TransportListener> listeners = new CopyOnWriteArraySet<>();
        private final ConcurrentHashMap<NodeId, Boolean> connected = new ConcurrentHashMap<>();
        private final List<ClusterMessage> sent = new CopyOnWriteArrayList<>();

        RecordingTransport(NodeInfo local, List<NodeInfo> peers) {
            this.local = local;
            this.peers = new ArrayList<>(peers);
            for (NodeInfo p : peers) {
                connected.put(p.nodeId(), true);
            }
        }

        void notifyPeerConnected(NodeInfo peer) {
            if (peers.stream().noneMatch(p -> p.nodeId().equals(peer.nodeId()))) {
                peers.add(peer);
            }
            connected.put(peer.nodeId(), true);
            for (TransportListener l : listeners) {
                l.onPeerConnected(peer);
            }
        }

        void deliverToListeners(ClusterMessage message) {
            for (TransportListener l : listeners) {
                l.onMessage(message);
            }
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
        public void addPeer(NodeInfo peer) {
            peers.add(peer);
            connected.put(peer.nodeId(), true);
        }

        @Override
        public void addListener(TransportListener listener) {
            listeners.add(listener);
        }

        @Override
        public void removeListener(TransportListener listener) {
            listeners.remove(listener);
        }

        @Override
        public void broadcast(ClusterMessage message) {
            sent.add(message);
        }

        @Override
        public void send(ClusterMessage message) {
            sent.add(message);
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            sent.add(message);
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
        public void close() throws IOException {
        }
    }
}
