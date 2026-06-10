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
import java.util.List;
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
 * Regressão da issue tems#9/D8 (linhagem divergente): um ex-líder morto em kill -9 com o frontier
 * numericamente À FRENTE do incumbente reassumia sem nunca instalar o snapshot, porque (1) o
 * pending bootstrap era desarmado ao REQUISITAR o sync (não ao completar) e o SYNC_REQUEST podia
 * morrer em silêncio; (2) na janela de startup os heartbeats anunciavam o frontier semeado da
 * linhagem morta antes de qualquer handler armar o {@code -1}; (3) um SYNC_RESPONSE tardio resetava
 * o estado de um líder ATIVO; (4) o cutover preservava (max) o contador da linhagem morta e o
 * binlog local com a cauda morta.
 */
class DivergentLineageBootstrapGateTest {

    private static final String TOPIC = "cardinal-state";
    private static final NodeId FOLLOWER = NodeId.of("aaa-follower");
    private static final NodeId LEADER = NodeId.of("zzz-leader");
    private static final long EPOCH = 5L;

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("divergent-lineage-gate");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("Boot unclean com dados prévios: -1/inelegível desde antes do registerHandler (F-B)")
    void uncleanBootWithPriorDataGatesBeforeHandlerRegistration() throws Exception {
        // Run anterior morto sem marker, com dados do tópico no relay.
        Files.createDirectories(tempDir.resolve("relay").resolve(RelayStore.dirName(TOPIC)));

        try (Harness h = Harness.follower(tempDir, scheduler)) {
            h.manager.start();
            // Janela de startup (wiring que registra handler DEPOIS do start): o nó não pode
            // anunciar o frontier semeado da linhagem possivelmente morta nem aceitar liderança.
            assertEquals(-1L, h.manager.getAdvertisedHighWatermark(),
                    "sem handler registrado, um boot unclean com dados prévios anuncia -1");
            assertFalse(h.manager.isLeadershipEligible(),
                    "sem handler registrado, um boot unclean com dados prévios é inelegível");

            // O registro do handler migra o gate para o pending por tópico — continua engajado.
            h.manager.registerHandler(TOPIC, h.handler);
            assertTrue(h.manager.getTopicReplicationStatuses().get(TOPIC).relayPendingBootstrap(),
                    "registro do handler arma o pending por tópico");
            assertEquals(-1L, h.manager.getAdvertisedHighWatermark());
            assertFalse(h.manager.isLeadershipEligible());
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Pending sobrevive ao REQUEST e só desarma no cutover, que re-ancora a linhagem (F-A/F-C3)")
    void pendingBootstrapClearsOnlyOnCutoverAndReanchorsLineage() throws Exception {
        // Run anterior unclean: relay com dados E binlog local com a cauda da linhagem morta.
        Files.createDirectories(tempDir.resolve("relay").resolve(RelayStore.dirName(TOPIC)));
        Path binlogDir = tempDir.resolve("resend-log").resolve(RelayStore.dirName(TOPIC));
        ResendLog deadBinlog = new ResendLog(binlogDir, 1024, Duration.ofMinutes(5), 0L,
                Duration.ofHours(1), 1_000_000L, 0, false);
        for (long seq = 901; seq <= 903; seq++) {
            deadBinlog.append(seq, 1L, RelayEntryCodec.encode(
                    new RelayEntry(EPOCH - 1, seq, TOPIC, UUID.randomUUID(), ("dead-" + seq).getBytes())));
        }
        deadBinlog.close();

        try (Harness h = Harness.follower(tempDir, scheduler)) {
            // Wiring Cardinal: handler ANTES do start (o backfill arma o pending no start).
            h.manager.registerHandler(TOPIC, h.handler);
            h.manager.start();
            h.awaitAgreedLeader();

            // O apply loop conhece o líder e REQUISITA o snapshot — e o pending DEVE permanecer
            // armado (a regressão D8: era removido aqui, e o request podia morrer em silêncio).
            h.awaitSyncRequested(10_000);
            assertTrue(h.manager.getTopicReplicationStatuses().get(TOPIC).relayPendingBootstrap(),
                    "o pending bootstrap deve sobreviver ao REQUEST do snapshot (só o install desarma)");
            assertEquals(-1L, h.manager.getAdvertisedHighWatermark(),
                    "enquanto o snapshot não instala, o nó segue não-anunciável");
            assertFalse(h.manager.isLeadershipEligible(),
                    "enquanto o snapshot não instala, o nó segue inelegível");

            // O líder serve o snapshot no watermark 500 — o install desarma e RE-ANCORA a linhagem.
            h.deliverSyncResponse(500L);
            h.awaitPendingCleared(10_000);
            assertTrue(h.resetCalled.get(), "chunk 0 deve resetar o estado antes do install");
            assertEquals(500L, h.manager.getLastAppliedSequence(),
                    "applied re-ancora NO watermark do snapshot (SET, não max)");
            assertEquals(500L, h.manager.getGlobalSequence(),
                    "globalSequence re-ancora na linhagem instalada (produção futura contígua)");
            assertEquals(500L, h.manager.getAdvertisedHighWatermark());
            assertTrue(h.manager.isLeadershipEligible(),
                    "instalado o snapshot, o nó volta a ser elegível (reclaim por afinidade)");

            // O binlog local da linhagem morta foi truncado — a cauda dead-* não pode mais ser
            // servida a um follower após uma promoção futura.
            try (var files = Files.list(binlogDir)) {
                assertTrue(files.noneMatch(f -> f.getFileName().toString().startsWith("seg-")),
                        "o cutover deve truncar o binlog local da linhagem substituída");
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @DisplayName("Líder ATIVO ignora SYNC_RESPONSE tardio — nunca reseta o próprio estado (F-C1)")
    void activeLeaderIgnoresLateSyncResponse() throws Exception {
        try (Harness h = Harness.soleLeader(tempDir, scheduler)) {
            h.manager.registerHandler(TOPIC, h.handler);
            h.manager.start();
            h.awaitSelfLeadership(10_000);

            h.deliverSyncResponse(999L);
            // A resposta tardia não pode resetar o estado nem re-ancorar contadores do líder ativo.
            Thread.sleep(300);
            assertFalse(h.resetCalled.get(),
                    "um líder ativo nunca instala snapshot de peer (reset do estado vivo)");
            assertEquals(0L, h.manager.getLastAppliedSequence(),
                    "contadores do líder ativo permanecem intactos");
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

        private Harness(Path dataDir, ScheduledExecutorService scheduler, int minClusterSize,
                boolean withPeer) {
            NodeInfo localNode = new NodeInfo(FOLLOWER, "127.0.0.1", 1);
            NodeInfo leaderNode = new NodeInfo(LEADER, "127.0.0.1", 2);
            this.transport = new RecordingTransport(localNode,
                    withPeer ? List.of(leaderNode) : List.of());
            this.coordinator = new ClusterCoordinator(transport,
                    ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(60),
                            Duration.ofSeconds(60), minClusterSize, null),
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

        /** minClusterSize=2: o nó local nunca se elege; o líder acordado vem por heartbeat. */
        static Harness follower(Path dataDir, ScheduledExecutorService scheduler) {
            return new Harness(dataDir, scheduler, 2, true);
        }

        /** minClusterSize=1 e NENHUM peer configurado: o nó se elege sozinho (lead-while-alone). */
        static Harness soleLeader(Path dataDir, ScheduledExecutorService scheduler) {
            return new Harness(dataDir, scheduler, 1, false);
        }

        void awaitAgreedLeader() throws InterruptedException {
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                coordinator.onMessage(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", LEADER, null,
                        HeartbeatPayload.now(900L, EPOCH)));
                if (LEADER.equals(coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null))) {
                    return;
                }
                Thread.sleep(50);
            }
            fail("o líder acordado não convergiu para " + LEADER);
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
            fail("o apply loop não requisitou o snapshot");
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

        void deliverSyncResponse(long watermark) {
            transport.deliverToListeners(ClusterMessage.request(MessageType.SYNC_RESPONSE, "sync",
                    LEADER, FOLLOWER, new SyncResponsePayload(TOPIC, watermark, new byte[0])));
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

        List<ClusterMessage> getSentMessages() {
            return sent;
        }

        void deliverToListeners(ClusterMessage message) {
            for (TransportListener l : listeners) {
                l.onMessage(message);
            }
        }

        @Override
        public void close() throws IOException {
        }
    }
}
