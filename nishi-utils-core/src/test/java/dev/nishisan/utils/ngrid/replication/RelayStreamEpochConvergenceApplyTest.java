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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Regressão (RELAY_STREAM, Fase 0.1/0.2) — após uma <strong>convergência de epoch</strong> o líder
 * acordado re-carimba seu termo (ex.: epoch 1 → 3 quando um follower que entra lembra epoch 2). As
 * ops que o MESMO líder já havia produzido no epoch anterior carregam um epoch <em>menor</em> no
 * frame do relay. Essas ops são legítimas e contíguas em sequência; o follower DEVE aplicar TODAS —
 * as de epoch menor (pré re-stamp) e as de epoch maior (pós re-stamp).
 *
 * <p>O bug corrigido em {@code 76904e1} fenciava por MAGNITUDE de epoch
 * ({@code entry.epoch() < lastSeenLeaderEpoch}) no persist ({@code handleRelayStreamBatch}) e no apply
 * ({@code drainRelayOnce}/{@code discardStaleRelayPrefix}). Com o fence ativo e um
 * {@code lastSeenLeaderEpoch} já elevado pelo epoch novo, os frames de epoch menor eram descartados
 * SEM avançar o cursor/{@code nextExpected}; a próxima seq (epoch novo) virava um 'forward gap'
 * eterno e o follower travava ('Non-contiguous relay head ... expected=N got=N+1'), com
 * {@code gapsDetected} crescendo e sem convergir (observado em pré-prod sob cold-join + firehose).
 *
 * <p>Este teste exercita o caminho de apply do follower DIRETO (sem subir um cluster TCP): monta um
 * {@link ReplicationManager} follower cujo {@link ClusterCoordinator} reconhece um líder acordado
 * separado, e entrega um {@code RELAY_STREAM_BATCH} desse líder com um run contíguo de epochs MISTOS
 * (1..K em epoch-baixo, K+1..N em epoch-alto). A asserção: todas as N ops aplicam, em ordem, com
 * {@code gapsDetected==0} e {@code snapshotFallbackCount==0} — nada de wedge.
 *
 * <p>Prova de regressão: com o fence-por-magnitude reintroduzido (com {@code lastSeenLeaderEpoch}
 * mantido a partir do epoch dos frames), este teste TRAVA — só as primeiras K ops (epoch-baixo)
 * persistem e o apply nunca alcança a seq K+1, estourando o timeout.
 */
class RelayStreamEpochConvergenceApplyTest {

    private static final String TOPIC = "cardinal-state";
    // O líder acordado é o maior NodeId; o follower local é o menor.
    private static final NodeId LEADER = NodeId.of("zzz-leader");
    private static final NodeId FOLLOWER = NodeId.of("aaa-follower");

    // Epochs antes/depois da convergência. lowEpoch < highEpoch e ambos > 0 (frames reais nunca
    // carregam epoch negativo) — é exatamente o caso que o fence-por-magnitude quebrava.
    private static final long LOW_EPOCH = 1L;
    private static final long HIGH_EPOCH = 3L;

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("relay-stream-epoch-converge");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void followerAppliesAllMixedEpochOpsAfterLeaderReStamp() throws Exception {
        int lowEpochOps = 5;   // seq 1..5  produzidas no epoch antigo (pré re-stamp)
        int highEpochOps = 5;  // seq 6..10 produzidas no epoch novo (pós re-stamp)
        int total = lowEpochOps + highEpochOps;

        try (Follower f = new Follower()) {
            f.awaitAgreedLeader();

            // O líder acordado serve um run estritamente contíguo (seq 1..N), mas com epochs MISTOS:
            // a cauda do binlog que sobreviveu ao re-stamp ainda carrega o epoch ANTIGO (menor), e as
            // ops novas carregam o epoch novo (maior). Tudo do MESMO líder acordado.
            List<byte[]> frames = new ArrayList<>();
            for (int i = 1; i <= lowEpochOps; i++) {
                frames.add(frame(LOW_EPOCH, i, "op-" + i));
            }
            for (int i = lowEpochOps + 1; i <= total; i++) {
                frames.add(frame(HIGH_EPOCH, i, "op-" + i));
            }

            f.deliverBatch(new RelayStreamBatchPayload(TOPIC, 1L, frames, total, 1L, false));

            // O follower deve aplicar TODAS as N ops — as de epoch menor (pré re-stamp) inclusive —
            // contíguas e em ordem. Com o fence-por-magnitude, ele travaria em seq <= lowEpochOps.
            f.awaitApplied(total, 15_000);

            assertEquals(total, f.applied.size(),
                    "o follower deve aplicar todas as ops contíguas (epoch baixo + epoch alto)");
            for (int i = 1; i <= total; i++) {
                assertEquals("op-" + i, f.applied.get(i - 1),
                        "as ops devem aplicar em ordem estrita de sequência");
            }
            assertEquals((long) total, f.manager.getLastAppliedSequence(),
                    "a fronteira de apply deve avançar até a última sequência");
            assertEquals((long) total, f.manager.getRelayStreamCursor(TOPIC),
                    "o cursor de pull deve cobrir todo o run contíguo (nenhum frame de epoch menor pulado)");
            assertEquals(0L, f.manager.getGapsDetected(),
                    "um run contíguo do líder acordado nunca é um gap, mesmo com epochs mistos");
            assertEquals(0L, f.manager.getSnapshotFallbackCount(),
                    "convergência de epoch não deve disparar fallback de snapshot");
        }
    }

    private static byte[] frame(long epoch, long sequence, String payload) {
        return RelayEntryCodec.encode(new RelayEntry(epoch, sequence, TOPIC, UUID.randomUUID(),
                payload.getBytes(StandardCharsets.UTF_8)));
    }

    // ---- follower harness ----

    private final class Follower implements AutoCloseable {
        final RecordingTransport transport;
        final ClusterCoordinator coordinator;
        final ReplicationManager manager;
        final List<String> applied = new CopyOnWriteArrayList<>();

        Follower() {
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
                            .dataDirectory(tempDir)
                            .build());
            manager.registerHandler(TOPIC, new ReplicationHandler() {
                @Override
                public void apply(UUID operationId, Object payload) {
                    applied.add(new String((byte[]) payload, StandardCharsets.UTF_8));
                }
            });
            manager.start();
        }

        /** Faz o coordinator reconhecer o líder acordado (maior NodeId) via heartbeat. */
        void awaitAgreedLeader() throws InterruptedException {
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                // Heartbeat do líder: torna-o membro ativo; com o nó local também ativo (2 membros) e
                // o maior NodeId, ele é eleito líder acordado.
                coordinator.onMessage(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", LEADER, null,
                        HeartbeatPayload.now(-1L, HIGH_EPOCH)));
                if (LEADER.equals(coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null))) {
                    return;
                }
                Thread.sleep(50);
            }
            fail("o líder acordado não convergiu para " + LEADER + " (observado="
                    + coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null) + ")");
        }

        void deliverBatch(RelayStreamBatchPayload batch) {
            transport.deliverToListeners(ClusterMessage.request(MessageType.RELAY_STREAM_BATCH, "stream",
                    LEADER, FOLLOWER, batch));
        }

        void awaitApplied(int target, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (manager.getLastAppliedSequence() < target) {
                if (System.currentTimeMillis() > deadline) {
                    fail("o follower travou (wedge): applied=" + manager.getLastAppliedSequence()
                            + " target=" + target + " cursor=" + manager.getRelayStreamCursor(TOPIC)
                            + " gaps=" + manager.getGapsDetected());
                }
                Thread.sleep(50);
            }
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
        public void addPeer(NodeInfo peer) {
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
