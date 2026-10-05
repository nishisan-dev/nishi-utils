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
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HandbackCompletePayload;
import dev.nishisan.utils.ngrid.common.HandbackGrantPayload;
import dev.nishisan.utils.ngrid.common.HandbackRequestPayload;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.SyncRequestPayload;
import dev.nishisan.utils.ngrid.common.SyncResponsePayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 8.10.1 — um {@code HANDBACK_COMPLETE} sem vetor por tópico (candidato anterior à 8.8.0) não pode
 * aplicar o escalar de um tópico em outro.
 *
 * <p>Incidente do CTP: o snapshot de {@code map:ngrrd.catalog} (~13,58M) saiu rotulado com 4462137, o
 * valor de {@code map:_ngrid-queue-offsets}. Até a 8.7.0 o incumbente rebaixado de um handback fazia SET
 * do contador de produção, da fronteira e do cursor de um tópico "primário" arbitrário (a primeira chave
 * do mapa de handlers) com o escalar do candidato — a marca d'água de cutover do ÚLTIMO tópico que ele
 * instalou. Na 8.8.0+ esse caminho continuava ativo quando o vetor chegava vazio. O contador
 * contaminado sobrevivia no {@code _topic:} persistido (só a produção o eleva), e o nó, ao liderar,
 * rotulava o snapshot do catálogo com ele.
 *
 * <p>Os nomes dos tópicos são escolhidos para que, na ordem de iteração do {@code ConcurrentHashMap}, o
 * tópico grande ({@code map:catalog}) seja o "primário" e o escalar venha do pequeno
 * ({@code map:offsets}): com o fallback antigo o catálogo cai de 13578849 para 4462137.
 */
class LegacyHandbackCompleteCrossTopicTest {

    private static final String SMALL_TOPIC = "map:offsets";
    private static final String LARGE_TOPIC = "map:catalog";
    private static final long SMALL_FRONTIER = 4_462_137L;
    private static final long LARGE_FRONTIER = 13_578_849L;
    private static final NodeId LOCAL = NodeId.of("mmm-incumbent");
    private static final NodeId CANDIDATE = NodeId.of("aaa-candidate");

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("legacy-handback-cross-topic");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void completeSemVetorDeCandidatoAntigoNaoContaminaOTopicoGrandeComOEscalarDoPequeno() throws Exception {
        Map<String, Long> state = new HashMap<>();
        state.put(SMALL_TOPIC, SMALL_FRONTIER + 1L);
        state.put(LARGE_TOPIC, LARGE_FRONTIER + 1L);
        state.put("_topic:" + SMALL_TOPIC, SMALL_FRONTIER);
        state.put("_topic:" + LARGE_TOPIC, LARGE_FRONTIER);
        state.put("_global", LARGE_FRONTIER);
        writeSequenceState(state);

        ScriptedTransport transport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1, Set.of(), 50),
                List.of());
        ClusterCoordinator coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(5),
                        Duration.ofSeconds(60), 1, null).withPairMode(true),
                scheduler);
        ReplicationManager manager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .strictConsistency(false)
                        .leaderLocalApply(false)
                        .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                        .operationTimeout(Duration.ofSeconds(5))
                        .affinityHandbackMode(true)
                        .handoverMaxDuration(Duration.ofSeconds(30))
                        .dataDirectory(tempDir)
                        .build());
        try {
            manager.registerHandler(SMALL_TOPIC, new SnapshotHandler());
            manager.registerHandler(LARGE_TOPIC, new SnapshotHandler());
            manager.start();
            coordinator.start();
            awaitCondition(() -> coordinator.isLeader() && !manager.isLeaderSyncing(), 15_000,
                    "o incumbente deve liderar sozinho");

            // O candidato (maior afinidade) entra atrasado: o incumbente segue líder e aceita o handback.
            transport.connect(new NodeInfo(CANDIDATE, "127.0.0.1", 2, Set.of(), 100));
            for (int i = 0; i < 5; i++) {
                candidateHeartbeat(transport, false, 0L);
                Thread.sleep(50);
            }
            awaitCondition(coordinator::isLeader, 5_000, "o candidato atrasado não pode tomar a liderança");
            transport.deliver(ClusterMessage.request(MessageType.HANDBACK_REQUEST, "handback", CANDIDATE, LOCAL,
                    new HandbackRequestPayload(CANDIDATE, 0L, LARGE_FRONTIER)));
            ClusterMessage grant = awaitSent(transport, MessageType.HANDBACK_GRANT, 10_000);
            long grantEpoch = grant.payload(HandbackGrantPayload.class).leaderEpoch();

            // O candidato antigo instala os snapshots servidos e responde só com o escalar do último tópico
            // que instalou (o pequeno), sem o vetor por tópico.
            assertEquals(SMALL_FRONTIER, snapshotLabel(transport, SMALL_TOPIC), "rótulo do tópico pequeno");
            assertEquals(LARGE_FRONTIER, snapshotLabel(transport, LARGE_TOPIC), "rótulo do tópico grande");
            long newEpoch = grantEpoch + 1L;
            transport.deliver(ClusterMessage.request(MessageType.HANDBACK_COMPLETE, "handback", CANDIDATE, LOCAL,
                    new HandbackCompletePayload(SMALL_FRONTIER, newEpoch)));
            awaitCondition(() -> {
                candidateHeartbeat(transport, true, newEpoch);
                return !coordinator.isLeader()
                        && CANDIDATE.equals(coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null));
            }, 10_000, "o incumbente deve ser rebaixado para o candidato");

            assertEquals(LARGE_FRONTIER, frontier(manager, LARGE_TOPIC),
                    "a fronteira do tópico grande não pode receber o escalar do pequeno");
            assertEquals(LARGE_FRONTIER, manager.getRelayStreamCursor(LARGE_TOPIC),
                    "o cursor do tópico grande não pode receber o escalar do pequeno");
            assertEquals(SMALL_FRONTIER, frontier(manager, SMALL_TOPIC));
            assertEquals(SMALL_FRONTIER, manager.getRelayStreamCursor(SMALL_TOPIC));
        } finally {
            try {
                manager.close();
            } catch (Exception ignored) {
                // teardown best-effort
            }
            try {
                coordinator.close();
            } catch (Exception ignored) {
                // teardown best-effort
            }
        }
        // O contador de produção persistido (a fonte do rótulo contaminado na 8.8.0) segue o próprio tópico.
        Map<String, Long> persisted = readSequenceState();
        assertEquals(LARGE_FRONTIER, persisted.get("_topic:" + LARGE_TOPIC),
                "o _topic: do tópico grande não pode ser sobrescrito com o escalar do pequeno");
        assertEquals(SMALL_FRONTIER, persisted.get("_topic:" + SMALL_TOPIC));
    }

    // ---- apoio ----

    private static void candidateHeartbeat(ScriptedTransport transport, boolean leader, long epoch) {
        transport.deliver(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", CANDIDATE, null,
                HeartbeatPayload.now(0L, epoch, leader)));
    }

    /** Pede o snapshot de um tópico (chunk 0) como o candidato e devolve o rótulo servido. */
    private static long snapshotLabel(ScriptedTransport transport, String topic) throws InterruptedException {
        transport.clearSent();
        transport.deliver(ClusterMessage.request(MessageType.SYNC_REQUEST, "sync", CANDIDATE, LOCAL,
                new SyncRequestPayload(topic, 0)));
        return awaitSent(transport, MessageType.SYNC_RESPONSE, 5_000).payload(SyncResponsePayload.class).sequence();
    }

    private static long frontier(ReplicationManager manager, String topic) {
        return manager.getTopicReplicationStatuses().get(topic).nextExpectedSequence() - 1L;
    }

    private static ClusterMessage awaitSent(ScriptedTransport transport, MessageType type, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            List<ClusterMessage> sent = transport.sentOfType(type);
            if (!sent.isEmpty()) {
                return sent.get(0);
            }
            Thread.sleep(20);
        }
        throw new AssertionError("o nó não enviou " + type + " em " + timeoutMs + " ms");
    }

    private static void awaitCondition(BooleanSupplier condition, long timeoutMs, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(25);
        }
        fail(message);
    }

    private void writeSequenceState(Map<String, Long> state) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(
                Files.newOutputStream(tempDir.resolve("sequence-state.dat")))) {
            oos.writeObject(new HashMap<>(state));
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Long> readSequenceState() throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(
                Files.newInputStream(tempDir.resolve("sequence-state.dat")))) {
            return (Map<String, Long>) ois.readObject();
        }
    }

    /** Handler que serve um snapshot qualquer. */
    private static final class SnapshotHandler implements ReplicationHandler {
        @Override
        public void apply(UUID operationId, Object payload) {
        }

        @Override
        public Object getSnapshot() {
            return new byte[] {1};
        }
    }
}
