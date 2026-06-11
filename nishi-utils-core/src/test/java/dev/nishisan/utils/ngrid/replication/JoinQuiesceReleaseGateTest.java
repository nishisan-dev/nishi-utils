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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regressão da issue tems#9/D10a (liberação precoce do join-quiesce): no incidente de pré-prod o
 * quiesce liberou em 1,5s enquanto o catch-up real do candidato levou 64s, porque a condição de
 * release comparava o progresso reportado contra o {@code globalSequence} CRU do líder — um
 * contador de produção local que, num líder PROMOVIDO de follower, fica muito abaixo do watermark
 * real ({@code max(globalSequence, lastApplied)}, a mesma fórmula do watermark anunciado em
 * heartbeat). Endurecimentos cobertos:
 *
 * <ul>
 * <li><b>H1</b> — o release usa a escala do watermark anunciado, não o {@code globalSequence};</li>
 * <li><b>H2</b> — progresso com epoch divergente ou frontier não-anunciável ({@code -1}) nunca
 * libera o gate;</li>
 * <li><b>H3</b> — a entrada de progresso da SESSÃO ANTERIOR de um nó que religa (crash + reconexão
 * rápida, sem disconnect observado) não pré-satisfaz o gate — o release exige report fresco.</li>
 * </ul>
 */
class JoinQuiesceReleaseGateTest {

    private static final String TOPIC = "cardinal-state";
    private static final NodeId LOCAL = NodeId.of("zzz-leader");
    private static final NodeId JOINER = NodeId.of("aaa-joiner");

    /** Perfil "líder promovido de follower": aplicou 10.000 ops, produziu só 10. */
    private static final long PROMOTED_GLOBAL = 10L;
    private static final long PROMOTED_APPLIED = 10_000L;

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("join-quiesce-gate");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("H1: o release compara contra o watermark real do líder, não o globalSequence cru")
    void releaseTargetsAdvertisedWatermarkNotRawGlobalSequence() throws Exception {
        writePromotedLeaderSequenceState();
        try (Harness h = new Harness(tempDir, scheduler, Duration.ofSeconds(60))) {
            h.manager.start();
            h.awaitSelfLeadership(10_000);
            assertEquals(PROMOTED_GLOBAL, h.manager.getGlobalSequence(),
                    "perfil do incidente: contador de produção local baixo");
            assertEquals(PROMOTED_APPLIED, h.manager.getLastAppliedSequence(),
                    "perfil do incidente: frontier aplicado na escala da linhagem do cluster");

            h.connectJoiner();
            h.awaitQuiescing(true, 5_000);

            // Reproduz o release de 1,5s: progresso ACIMA do globalSequence (10) mas muito ABAIXO
            // do watermark real (10.000). O gate não pode liberar.
            h.deliverFollowerProgress(9_000L, h.coordinator.getLeaderEpoch());
            h.assertQuiescingHolds(1_000);

            // Catch-up legítimo: emparelhou com o watermark real — libera.
            h.deliverFollowerProgress(PROMOTED_APPLIED, h.coordinator.getLeaderEpoch());
            h.awaitQuiescing(false, 5_000);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("H2: progresso com epoch divergente ou frontier -1 nunca libera o gate")
    void staleEpochOrGatedFrontierNeverReleases() throws Exception {
        writePromotedLeaderSequenceState();
        try (Harness h = new Harness(tempDir, scheduler, Duration.ofSeconds(60))) {
            h.manager.start();
            h.awaitSelfLeadership(10_000);
            h.connectJoiner();
            h.awaitQuiescing(true, 5_000);

            // Epoch de outra linhagem/termo: a escala do progresso é incomparável — segura o gate.
            h.deliverFollowerProgress(PROMOTED_APPLIED, h.coordinator.getLeaderEpoch() + 7);
            h.assertQuiescingHolds(1_000);

            // Frontier não-anunciável (bootstrap gate engajado no follower) — segura o gate.
            h.deliverFollowerProgress(-1L, h.coordinator.getLeaderEpoch());
            h.assertQuiescingHolds(1_000);

            // Report comparável e emparelhado — libera.
            h.deliverFollowerProgress(PROMOTED_APPLIED, h.coordinator.getLeaderEpoch());
            h.awaitQuiescing(false, 5_000);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("H3: rejoin com entrada stale da sessão anterior re-engaja o quiesce até report fresco")
    void rejoinDiscardsStaleProgressFromPreviousSession() throws Exception {
        writePromotedLeaderSequenceState();
        // heartbeatTimeout curto para o joiner "morrer" rápido sem evento de disconnect.
        try (Harness h = new Harness(tempDir, scheduler, Duration.ofMillis(800))) {
            h.manager.start();
            h.awaitSelfLeadership(10_000);

            // Sessão 1: joiner entra, emparelha e libera o quiesce (entrada fica no mapa do líder).
            h.connectJoiner();
            h.awaitQuiescing(true, 5_000);
            h.deliverFollowerProgress(PROMOTED_APPLIED, h.coordinator.getLeaderEpoch());
            h.awaitQuiescing(false, 5_000);

            // Crash sem disconnect observado: heartbeats param, o sweep o marca inativo.
            h.transport.setConnected(JOINER, false);
            h.awaitJoinerInactive(15_000);

            // Sessão 2: reconexão rápida. A entrada stale (emparelhada) da sessão anterior NÃO
            // pode pré-satisfazer o gate — o quiesce re-engaja e espera report fresco.
            h.connectJoiner();
            h.awaitQuiescing(true, 5_000);

            h.deliverFollowerProgress(PROMOTED_APPLIED, h.coordinator.getLeaderEpoch());
            h.awaitQuiescing(false, 5_000);
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

        Harness(Path dataDir, ScheduledExecutorService scheduler, Duration heartbeatTimeout) {
            NodeInfo localNode = new NodeInfo(LOCAL, "127.0.0.1", 1);
            this.transport = new RecordingTransport(localNode, List.of());
            // minClusterSize=1 sem peers: o nó local se elege sozinho (lead-while-alone).
            this.coordinator = new ClusterCoordinator(transport,
                    ClusterCoordinatorConfig.of(Duration.ofMillis(100), heartbeatTimeout, 1),
                    scheduler);
            coordinator.start();
            this.manager = new ReplicationManager(transport, coordinator,
                    ReplicationConfig.builder(1)
                            .strictConsistency(false)
                            .leaderLocalApply(false)
                            .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                            .operationTimeout(Duration.ofSeconds(5))
                            .dataDirectory(dataDir)
                            .leaderPauseOnJoin(true)
                            .joinQuiesceMaxDuration(Duration.ofSeconds(25))
                            .build());
        }

        void connectJoiner() {
            // Espelha o fluxo real: o primeiro heartbeat do joiner chega junto do connect, então o
            // watermark do peer já é conhecido quando a membership notifica (o deferral de
            // watermark-desconhecido do recomputeLeader não derruba o líder do harness).
            transport.deliverToListeners(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb",
                    JOINER, null, HeartbeatPayload.now(9_000L, 0L)));
            transport.notifyPeerConnected(new NodeInfo(JOINER, "127.0.0.1", 2));
        }

        void deliverFollowerProgress(long applied, long epoch) {
            transport.deliverToListeners(ClusterMessage.request(MessageType.FOLLOWER_PROGRESS,
                    "follower-progress", JOINER, LOCAL, new FollowerProgressPayload(applied, epoch)));
        }

        void awaitSelfLeadership(long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                if (coordinator.isLeader()) {
                    return;
                }
                Thread.sleep(50);
            }
            fail("o nó não se elegeu sozinho (lead-while-alone)");
        }

        void awaitQuiescing(boolean expected, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                if (manager.isJoinQuiescing() == expected) {
                    return;
                }
                Thread.sleep(25);
            }
            fail("join-quiesce não atingiu o estado esperado: " + expected);
        }

        /** Asserta que o quiesce permanece engajado durante toda a janela (sem release indevido). */
        void assertQuiescingHolds(long windowMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + windowMs;
            while (System.currentTimeMillis() < deadline) {
                assertTrue(manager.isJoinQuiescing(),
                        "o quiesce liberou contra um progresso não-emparelhado/incomparável");
                Thread.sleep(50);
            }
        }

        void awaitJoinerInactive(long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                boolean active = coordinator.activeMembers().stream()
                        .anyMatch(m -> JOINER.equals(m.nodeId()));
                if (!active) {
                    return;
                }
                Thread.sleep(50);
            }
            fail("o joiner não foi marcado inativo pelo sweep de heartbeat-timeout");
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

        void setConnected(NodeId nodeId, boolean isConnected) {
            connected.put(nodeId, isConnected);
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
