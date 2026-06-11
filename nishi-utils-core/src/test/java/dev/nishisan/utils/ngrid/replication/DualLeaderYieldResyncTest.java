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
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.SyncResponsePayload;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Pós-step-down do perdedor de um dual-leader (issue tems#9, D10c), no nível do ReplicationManager:
 * o yield arma o pending bootstrap de TODOS os tópicos registrados (a linhagem produzida na janela
 * dual é divergente por definição), o pending sobrevive até o INSTALL (maquinaria D8 — anuncia -1,
 * inelegível, requisita snapshot do vencedor) e o cutover re-ancora os contadores na linhagem do
 * vencedor, descartando a cauda dual.
 */
class DualLeaderYieldResyncTest {

    private static final String TOPIC = "cardinal-state";
    private static final NodeId LOCAL = NodeId.of("zzz-incumbent"); // prioridade 50 — perde
    private static final NodeId RIVAL = NodeId.of("aaa-rival");     // prioridade 100 — vence

    private static final long LOCAL_APPLIED = 10_000L;
    private static final long WINNER_WATERMARK = 12_000L;

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("dual-leader-yield");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Yield arma pending para todos os tópicos, sobrevive ao REQUEST e o cutover re-ancora")
    void yieldArmsPendingBootstrapAndCutoverReanchorsOnWinnerLineage() throws Exception {
        // Estado local acima do anunciado pelo rival nos heartbeats (900): o gate B retém a
        // liderança — o estado dual estável do incidente — até a resolução determinística ceder.
        writeSequenceState(LOCAL_APPLIED);
        try (Harness h = new Harness(tempDir, scheduler)) {
            h.manager.registerHandler(TOPIC, h.handler);
            h.manager.start();
            h.awaitSelfLeadership(10_000);

            // O rival (maior afinidade) se afirma líder a cada heartbeat: dual-leader estável até
            // o debounce resolver — o local (menor afinidade) cede e arma o resync.
            h.connectRival();
            long deadline = System.currentTimeMillis() + 10_000;
            while (h.coordinator.isLeader() && System.currentTimeMillis() < deadline) {
                h.deliverRivalHeartbeat(true);
                Thread.sleep(50);
            }
            assertFalse(h.coordinator.isLeader(), "o perdedor deve ceder ao rival de maior afinidade");
            assertEquals(RIVAL, h.coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null));

            // O yield armou o pending: estado local é linhagem dual — não-anunciável e inelegível.
            assertTrue(h.manager.getTopicReplicationStatuses().get(TOPIC).relayPendingBootstrap(),
                    "o yield arma o pending bootstrap do tópico registrado");
            assertEquals(-1L, h.manager.getAdvertisedHighWatermark(),
                    "linhagem dual não é anunciável até o resync instalar");
            assertFalse(h.manager.isLeadershipEligible(),
                    "o perdedor não pode reclamar liderança antes do resync");

            // O apply loop requisita o snapshot do vencedor — e o pending SOBREVIVE ao request
            // (D8: só o install desarma).
            h.awaitSyncRequested(10_000);
            assertTrue(h.manager.getTopicReplicationStatuses().get(TOPIC).relayPendingBootstrap(),
                    "o pending sobrevive ao REQUEST do snapshot");

            // O vencedor serve o snapshot: o cutover re-ancora TUDO na linhagem dele (SET) e a
            // cauda produzida na janela dual morre.
            h.deliverSyncResponse(WINNER_WATERMARK);
            h.awaitPendingCleared(10_000);
            assertTrue(h.resetCalled.get(), "o install reseta o estado local (linhagem substituída)");
            assertEquals(WINNER_WATERMARK, h.manager.getLastAppliedSequence(),
                    "applied re-ancora no watermark do vencedor (SET, não max)");
            assertEquals(WINNER_WATERMARK, h.manager.getGlobalSequence(),
                    "produção futura é contígua à linhagem instalada");
            assertEquals(WINNER_WATERMARK, h.manager.getAdvertisedHighWatermark());
            assertTrue(h.manager.isLeadershipEligible(), "instalado o snapshot, volta a ser elegível");
            assertFalse(h.coordinator.isLeader(),
                    "mesmo elegível, o perdedor (menor afinidade) segue follower do vencedor");
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Rival morto mid-yield: a re-promoção desarma o pending (única linhagem restante)")
    void rePromotionSupersedesPendingYield() throws Exception {
        writeSequenceState(LOCAL_APPLIED);
        try (Harness h = new Harness(tempDir, scheduler)) {
            h.manager.registerHandler(TOPIC, h.handler);
            h.manager.start();
            h.awaitSelfLeadership(10_000);

            h.connectRival();
            long deadline = System.currentTimeMillis() + 10_000;
            while (h.coordinator.isLeader() && System.currentTimeMillis() < deadline) {
                h.deliverRivalHeartbeat(true);
                Thread.sleep(50);
            }
            assertFalse(h.coordinator.isLeader());
            assertTrue(h.manager.getTopicReplicationStatuses().get(TOPIC).relayPendingBootstrap());

            // O vencedor morre antes de servir o snapshot: o local é a única linhagem viva.
            h.transport.notifyPeerDisconnected(RIVAL);
            h.awaitSelfLeadership(15_000);

            // A re-promoção supersede o yield: sem peer para ressincar, o pending desarma e o nó
            // volta a operar sobre a própria linhagem (drain-gate do failover cobre o backlog).
            long clearDeadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < clearDeadline) {
                if (!h.manager.getTopicReplicationStatuses().get(TOPIC).relayPendingBootstrap()) {
                    return;
                }
                Thread.sleep(50);
            }
            fail("a re-promoção deveria desarmar o pending do yield (não há vencedor para ressincar)");
        }
    }

    /** Fabrica sequence-state.dat com frontier aplicado alto (linhagem local pré-dual). */
    private void writeSequenceState(long applied) throws IOException {
        Map<String, Long> persisted = new HashMap<>();
        persisted.put(TOPIC, applied + 1L);
        persisted.put("_global", applied);
        persisted.put("_topic:" + TOPIC, applied);
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
        final AtomicBoolean resetCalled = new AtomicBoolean(false);
        final ReplicationHandler handler = new ReplicationHandler() {
            @Override
            public void apply(UUID operationId, Object payload) {
            }

            @Override
            public void resetState() {
                resetCalled.set(true);
            }

            @Override
            public void installSnapshot(Object snapshot) {
            }
        };

        Harness(Path dataDir, ScheduledExecutorService scheduler) {
            NodeInfo localNode = new NodeInfo(LOCAL, "127.0.0.1", 1, Set.of(), 50);
            this.transport = new RecordingTransport(localNode, List.of());
            this.coordinator = new ClusterCoordinator(transport,
                    ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofMillis(800),
                            Duration.ofSeconds(60), 1, null).withPairMode(true),
                    scheduler);
            coordinator.start();
            this.manager = new ReplicationManager(transport, coordinator,
                    ReplicationConfig.builder(1)
                            .strictConsistency(false)
                            .leaderLocalApply(false)
                            .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                            .operationTimeout(Duration.ofSeconds(5))
                            .dataDirectory(dataDir)
                            .build());
        }

        void connectRival() {
            transport.notifyPeerConnected(new NodeInfo(RIVAL, "127.0.0.1", 2, Set.of(), 100));
        }

        void deliverRivalHeartbeat(boolean leader) {
            // Watermark anunciado ABAIXO do estado local: o gate B retém (o dual estável do
            // incidente) até a resolução determinística decidir.
            transport.deliverToListeners(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb",
                    RIVAL, null, HeartbeatPayload.now(900L, 0L, leader)));
        }

        void deliverSyncResponse(long watermark) {
            transport.deliverToListeners(ClusterMessage.request(MessageType.SYNC_RESPONSE, "sync",
                    RIVAL, LOCAL, new SyncResponsePayload(TOPIC, watermark, new byte[0])));
        }

        void awaitSelfLeadership(long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                if (coordinator.isLeader()) {
                    return;
                }
                Thread.sleep(50);
            }
            fail("o nó não assumiu a liderança no prazo");
        }

        void awaitSyncRequested(long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                for (ClusterMessage m : transport.getSentMessages()) {
                    if (m.type() == MessageType.SYNC_REQUEST) {
                        return;
                    }
                }
                Thread.sleep(25);
            }
            fail("o apply loop não requisitou o snapshot do vencedor");
        }

        void awaitPendingCleared(long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                ReplicationManager.TopicReplicationStatus status =
                        manager.getTopicReplicationStatuses().get(TOPIC);
                if (status != null && !status.relayPendingBootstrap()) {
                    return;
                }
                Thread.sleep(25);
            }
            fail("o pending bootstrap não desarmou após o install do snapshot");
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

        void notifyPeerDisconnected(NodeId peer) {
            connected.put(peer, false);
            for (TransportListener l : listeners) {
                l.onPeerDisconnected(peer);
            }
        }

        void deliverToListeners(ClusterMessage message) {
            for (TransportListener l : listeners) {
                l.onMessage(message);
            }
        }

        List<ClusterMessage> getSentMessages() {
            return sent;
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
