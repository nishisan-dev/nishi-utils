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
import dev.nishisan.utils.ngrid.common.RelayStreamBatchPayload;
import dev.nishisan.utils.ngrid.common.RelayStreamFetchPayload;
import dev.nishisan.utils.ngrid.common.SyncResponsePayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regressão da issue tems#9/D7: a ingestão de {@code RELAY_STREAM_BATCH} rodava CONCORRENTE consigo
 * mesma (o transporte despacha cada mensagem numa virtual thread própria) e o check→offer→increment
 * por frame não era atômico. Dois batches sobrepostos (re-fetch por timeout, ou batch em voo cruzando
 * o cutover de snapshot) aprovavam o MESMO frame duas vezes, avançavam o cursor 2× e perfuravam um
 * buraco DURÁVEL de 1 op no relay — que o apply só encontrava dezenas de milhares de ops depois, como
 * forward-gap, e "recuperava" com um snapshot que recriava a corrida → loop infinito sob firehose
 * (observado em pré-prod: fallbacks de snapshot a ~2/min com buracos de 1-3 ops).
 * <p>
 * Cobre os três reparos: (F1) ingestão serializada por tópico + batch em voo descartado durante o
 * install com purge atômico no cutover; (F2) forward-gap com TTL de relay DESLIGADO recupera por
 * re-pull do binlog do líder (re-ancora o cursor) em vez de snapshot.
 */
class RelayStreamConcurrentIngestTest {

    private static final String TOPIC = "cardinal-state";
    private static final NodeId FOLLOWER = NodeId.of("aaa-follower");
    private static final NodeId LEADER = NodeId.of("zzz-leader");
    private static final long EPOCH = 7L;

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("relay-concurrent-ingest");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    @DisplayName("Batches sobrepostos entregues concorrentemente nunca perfuram o relay (F1)")
    void concurrentOverlappingBatchesNeverPunchHole() throws Exception {
        try (Follower f = new Follower(tempDir)) {
            f.awaitAgreedLeader();

            int total = 2000;
            int window = 100;
            int step = 50; // janelas 50% sobrepostas: o mesmo frame chega em batches distintos
            int writers = 3; // simula as virtual threads do transporte entregando em paralelo

            ExecutorService pool = Executors.newFixedThreadPool(writers);
            try {
                for (int start = 1; start <= total; start += step) {
                    int from = start;
                    int to = Math.min(start + window - 1, total);
                    List<byte[]> frames = new ArrayList<>();
                    for (int seq = from; seq <= to; seq++) {
                        frames.add(frame(seq));
                    }
                    RelayStreamBatchPayload batch =
                            new RelayStreamBatchPayload(TOPIC, from, frames, total, 1L, false);
                    CyclicBarrier barrier = new CyclicBarrier(writers);
                    List<Future<?>> deliveries = new ArrayList<>();
                    for (int w = 0; w < writers; w++) {
                        deliveries.add(pool.submit(() -> {
                            barrier.await();
                            f.deliverBatch(batch);
                            return null;
                        }));
                    }
                    for (Future<?> d : deliveries) {
                        d.get(20, TimeUnit.SECONDS);
                    }
                }
            } finally {
                pool.shutdownNow();
            }

            // Sem o lock de ingestão, um frame duplamente aprovado avança o cursor sem append e o
            // apply trava num forward-gap (snapshot que o harness não responde) — timeout aqui.
            f.awaitApplied(total, 30_000);
            assertEquals(total, f.applied.size(), "toda op deve aplicar exatamente uma vez, em ordem");
            for (int i = 1; i <= total; i++) {
                assertEquals("op-" + i, f.applied.get(i - 1), "apply deve ser contíguo e ordenado");
            }
            assertEquals(total, f.manager.getRelayStreamCursor(TOPIC),
                    "o cursor durável deve cobrir exatamente o que foi appendado");
            assertEquals(0L, f.manager.getSnapshotFallbackCount(),
                    "ingestão concorrente não pode degenerar em snapshot");
            assertEquals(0L, f.manager.getRelayRefetchCount(),
                    "com a ingestão serializada não deve sobrar buraco para o re-pull curar");
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Batch em voo durante o install é descartado e o cutover purga o relay (F1)")
    void inFlightBatchDuringSnapshotInstallIsDiscarded() throws Exception {
        try (Follower f = new Follower(tempDir)) {
            f.awaitAgreedLeader();

            // Regime normal: [1..10] aplica via stream.
            f.deliverBatch(batchOf(1, 10, 10));
            f.awaitApplied(10, 10_000);

            // O líder sinaliza needSnapshot (follower abaixo da janela retida) → sync engata.
            f.deliverBatch(new RelayStreamBatchPayload(TOPIC, 11L, List.of(), 100L, 50L, true));
            f.awaitSyncRequested(5_000);

            // Batch em voo chegando DURANTE o install: era ele que aterrissava sequências em volta do
            // cutover e perfurava o relay. Agora é descartado integralmente.
            f.deliverBatch(batchOf(11, 20, 100));
            assertEquals(10L, f.manager.getRelayStreamCursor(TOPIC),
                    "batch em voo durante o install não pode avançar o cursor");

            // Install do snapshot no watermark 25 → cutover purga o relay e re-ancora o cursor.
            f.deliverSyncResponse(25L);
            f.awaitCursor(25L, 10_000);
            assertEquals(25L, f.manager.getLastAppliedSequence(),
                    "o cutover deve assumir o watermark do snapshot como fronteira");

            // O stream recomeça exatamente de watermark+1, sem resto do batch descartado no caminho.
            f.deliverBatch(batchOf(26, 30, 30));
            f.awaitApplied(30, 10_000);
            assertEquals(0L, f.manager.getSnapshotFallbackCount(),
                    "nenhum forward-gap pode nascer do cutover");
            for (String v : f.applied) {
                assertTrue(!v.startsWith("op-1") || Integer.parseInt(v.substring(3)) <= 10
                                || Integer.parseInt(v.substring(3)) >= 26,
                        "nenhuma op do batch descartado (11..20) pode ter sido aplicada: " + v);
            }
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Buraco legado no relay com TTL off cura por re-pull do binlog, sem snapshot (F2)")
    void legacyRelayHoleHealsByBinlogRefetchNotSnapshot() throws Exception {
        // Forja um relay durável já perfurado (o estrago que o D7 deixou em campo): [1,2,4,5] — o 3
        // sumiu. Marker de shutdown limpo presente: o boot NÃO deve armar bootstrap por unclean.
        Path relayDir = tempDir.resolve("relay");
        RelayStore store = new RelayStore(relayDir, Duration.ofHours(1));
        store.relayFor(TOPIC).offer(frame(1));
        store.relayFor(TOPIC).offer(frame(2));
        store.relayFor(TOPIC).offer(frame(4));
        store.relayFor(TOPIC).offer(frame(5));
        store.close();
        RelayStore.writeCleanMarker(relayDir);

        try (Follower f = new Follower(tempDir)) {
            f.awaitAgreedLeader();

            // O apply drena 1,2 e encontra head=4 esperando 3: forward-gap. Com TTL OFF o prefixo não
            // pode ter sido evicted — a recuperação é re-ancorar o cursor e re-puxar do binlog (barato),
            // não um snapshot (caro, e que em pré-prod realimentava o loop).
            f.awaitRefetch(1L, 15_000);
            assertEquals(0L, f.manager.getSnapshotFallbackCount(),
                    "com TTL off o forward-gap não pode degenerar em snapshot");
            f.awaitFetchFrom(3L, 10_000);

            // O líder responde o range que faltava; o follower converge sem perder nem duplicar nada.
            f.deliverBatch(batchOf(3, 5, 5));
            f.awaitApplied(5, 10_000);
            assertEquals(List.of("op-1", "op-2", "op-3", "op-4", "op-5"), f.applied,
                    "a cura por re-pull deve preservar ordem e unicidade");
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    @DisplayName("Gap re-pull com purge falho cai para snapshot — nunca re-fetch sobre relay sujo")
    void gapRepullFallsBackToSnapshotWhenPurgeFails() throws Exception {
        // Relay perfurado [1,2,4,5] como no caso legado, mas com o diretório do tópico SEM permissão
        // de escrita: o truncate do purge não consegue apagar os arquivos. Re-anchorar o cursor assim
        // mesmo appendaria os frames re-fetchados ATRÁS do head antigo e o apply ficaria preso no
        // mesmo gap para sempre — o caminho correto é degradar para snapshot bootstrap.
        Path relayDir = tempDir.resolve("relay");
        RelayStore store = new RelayStore(relayDir, Duration.ofHours(1));
        store.relayFor(TOPIC).offer(frame(1));
        store.relayFor(TOPIC).offer(frame(2));
        store.relayFor(TOPIC).offer(frame(4));
        store.relayFor(TOPIC).offer(frame(5));
        store.close();
        RelayStore.writeCleanMarker(relayDir);
        Path topicDir = relayDir.resolve(RelayStore.dirName(TOPIC));
        java.nio.file.Files.setPosixFilePermissions(topicDir,
                java.nio.file.attribute.PosixFilePermissions.fromString("r-xr-xr-x"));

        try (Follower f = new Follower(tempDir)) {
            f.awaitAgreedLeader();
            // O apply drena 1,2 e bate no gap; o purge falha (dir read-only) → snapshot bootstrap.
            f.awaitSyncRequested(15_000);
            assertEquals(1L, f.manager.getSnapshotFallbackCount(),
                    "purge falho deve degradar o gap re-pull para snapshot");
            assertEquals(0L, f.manager.getRelayRefetchCount(),
                    "sem purge não pode haver re-anchor/re-fetch (o relay ainda tem o head antigo)");
        } finally {
            java.nio.file.Files.setPosixFilePermissions(topicDir,
                    java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }

    private static RelayStreamBatchPayload batchOf(int from, int to, long leaderHwm) {
        List<byte[]> frames = new ArrayList<>();
        for (int seq = from; seq <= to; seq++) {
            frames.add(frame(seq));
        }
        return new RelayStreamBatchPayload(TOPIC, from, frames, leaderHwm, 1L, false);
    }

    private static byte[] frame(long sequence) {
        return RelayEntryCodec.encode(new RelayEntry(EPOCH, sequence, TOPIC, UUID.randomUUID(),
                ("op-" + sequence).getBytes(StandardCharsets.UTF_8)));
    }

    // ---- follower harness (espelho do RelayStreamEpochConvergenceApplyTest) ----

    private final class Follower implements AutoCloseable {
        final RecordingTransport transport;
        final ClusterCoordinator coordinator;
        final ReplicationManager manager;
        final List<String> applied = new CopyOnWriteArrayList<>();

        Follower(Path dataDir) {
            NodeInfo followerNode = new NodeInfo(FOLLOWER, "127.0.0.1", 1);
            NodeInfo leaderNode = new NodeInfo(LEADER, "127.0.0.1", 2);
            this.transport = new RecordingTransport(followerNode, List.of(leaderNode));
            // minClusterSize=2, sem pair mode: o nó local permanece follower; com o leader (maior
            // NodeId) ativo, recomputeLeader elege o leader como líder acordado.
            this.coordinator = new ClusterCoordinator(transport,
                    ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(60),
                            Duration.ofSeconds(60), 2, null),
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
            manager.registerHandler(TOPIC, new ReplicationHandler() {
                @Override
                public void apply(UUID operationId, Object payload) {
                    applied.add(new String((byte[]) payload, StandardCharsets.UTF_8));
                }

                @Override
                public void installSnapshot(Object snapshot) {
                    // estado externo irrelevante para o protocolo em teste
                }
            });
            manager.start();
        }

        /** Faz o coordinator reconhecer o líder acordado (maior NodeId) via heartbeat. */
        void awaitAgreedLeader() throws InterruptedException {
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                coordinator.onMessage(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", LEADER, null,
                        HeartbeatPayload.now(-1L, EPOCH)));
                if (LEADER.equals(coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null))) {
                    return;
                }
                Thread.sleep(50);
            }
            fail("o líder acordado não convergiu para " + LEADER);
        }

        void deliverBatch(RelayStreamBatchPayload batch) {
            transport.deliverToListeners(ClusterMessage.request(MessageType.RELAY_STREAM_BATCH, "stream",
                    LEADER, FOLLOWER, batch));
        }

        void deliverSyncResponse(long watermark) {
            transport.deliverToListeners(ClusterMessage.request(MessageType.SYNC_RESPONSE, "sync",
                    LEADER, FOLLOWER, new SyncResponsePayload(TOPIC, watermark, new byte[0])));
        }

        void awaitApplied(int target, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (manager.getLastAppliedSequence() < target) {
                if (System.currentTimeMillis() > deadline) {
                    fail("o follower travou: applied=" + manager.getLastAppliedSequence()
                            + " target=" + target + " cursor=" + manager.getRelayStreamCursor(TOPIC)
                            + " fallbacks=" + manager.getSnapshotFallbackCount()
                            + " refetches=" + manager.getRelayRefetchCount());
                }
                Thread.sleep(50);
            }
        }

        void awaitCursor(long expected, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (manager.getRelayStreamCursor(TOPIC) != expected) {
                if (System.currentTimeMillis() > deadline) {
                    fail("cursor não re-ancorou: esperado=" + expected
                            + " atual=" + manager.getRelayStreamCursor(TOPIC));
                }
                Thread.sleep(50);
            }
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
            fail("needSnapshot não disparou SYNC_REQUEST");
        }

        void awaitRefetch(long expectedCount, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (manager.getRelayRefetchCount() < expectedCount) {
                if (System.currentTimeMillis() > deadline) {
                    fail("re-pull do binlog não engatou: refetches=" + manager.getRelayRefetchCount()
                            + " fallbacks=" + manager.getSnapshotFallbackCount()
                            + " applied=" + manager.getLastAppliedSequence());
                }
                Thread.sleep(50);
            }
        }

        void awaitFetchFrom(long expectedFrom, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                for (ClusterMessage m : transport.getSentMessages()) {
                    if (m.type() == MessageType.RELAY_STREAM_FETCH
                            && m.payload(RelayStreamFetchPayload.class).fromSequence() == expectedFrom) {
                        return;
                    }
                }
                Thread.sleep(25);
            }
            fail("nenhum RELAY_STREAM_FETCH com from=" + expectedFrom + " foi emitido após o re-anchor");
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

        /** Entrega a mensagem aos listeners NA THREAD CHAMADORA — espelha o despacho concorrente real. */
        void deliverToListeners(ClusterMessage message) {
            for (TransportListener l : listeners) {
                l.onMessage(message);
            }
        }

        @Override
        public void close() {
        }
    }
}
