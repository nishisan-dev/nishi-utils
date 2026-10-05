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
import dev.nishisan.utils.ngrid.common.HandbackAbortPayload;
import dev.nishisan.utils.ngrid.common.HandbackRequestPayload;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.RelayStreamBatchPayload;
import dev.nishisan.utils.ngrid.common.RelayStreamFetchPayload;
import dev.nishisan.utils.ngrid.common.SyncRequestPayload;
import dev.nishisan.utils.ngrid.common.SyncResponsePayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 8.10.1 — o rótulo do snapshot servido por um líder recém-promovido é a fronteira aplicada, não o
 * contador de produção cru.
 *
 * <p>Incidente do CTP: um líder que ainda não tinha produzido no tópico neste mandato rotulava o
 * {@code SYNC_RESPONSE} com o contador do último mandato em que produziu ({@code _topic:} persistido),
 * bem abaixo do que ele aplicou como seguidor. Quem instalava o snapshot fazia SET para baixo e, ao
 * liderar, voltava a numerar a partir daí; os seguidores fora da troca ficavam "à frente" e descartavam
 * as operações novas como duplicadas.
 */
class SnapshotLabelOnPromotedLeaderTest {

    private static final String TOPIC = "map:catalog";
    private static final NodeId LOCAL = NodeId.of("aaa-local");
    private static final NodeId OLD_LEADER = NodeId.of("zzz-leader");
    private static final NodeId REQUESTER = NodeId.of("mmm-requester");
    private static final NodeId CANDIDATE = NodeId.of("bbb-candidate");
    private static final String OTHER_TOPIC = "map:other";

    private Path tempDir;
    private final List<ScheduledExecutorService> schedulers = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("snapshot-label-promoted");
    }

    @AfterEach
    void tearDown() {
        schedulers.forEach(ScheduledExecutorService::shutdownNow);
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void liderPromovidoRotulaOSnapshotComAFronteiraENaoComOContadorDoMandatoAntigo() throws Exception {
        // Contador de produção de um mandato antigo (100) e fronteira aplicada como seguidor (500).
        writeSequenceState(Map.of(TOPIC, 501L, "_global", 100L, "_topic:" + TOPIC, 100L));
        ScriptedTransport transport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1), List.of());
        ClusterCoordinator coordinator = soloCoordinator(transport);
        ReplicationManager manager = newManager(transport, coordinator);
        try {
            manager.registerHandler(TOPIC, new SnapshotHandler(null));
            manager.start();
            coordinator.start();
            awaitCondition(coordinator::isLeader, 15_000, "o nó sozinho deve assumir a liderança");

            assertEquals(500L, requestSnapshotLabel(transport),
                    "o rótulo deve ser a fronteira aplicada (500), não o contador cru do mandato antigo (100)");
        } finally {
            closeQuietly(manager, coordinator);
        }
        // A promoção normaliza o contador cru: o _topic: persistido acompanha a fronteira.
        assertEquals(500L, readSequenceState().get("_topic:" + TOPIC),
                "a promoção eleva o contador de produção persistido à fronteira aplicada");
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void fronteiraQueSobeDrenandoORelayDepoisDaPromocaoEntraNoRotulo() throws Exception {
        int appliedAsFollower = 20;
        int received = 50;

        prepareUnappliedRelayTail(appliedAsFollower, received);

        // Fase 2: reinicia sozinho e é promovido; o drain-gate aplica 21..50 SEM produzir nada.
        CountDownLatch releaseDrain = new CountDownLatch(1);
        ScriptedTransport transport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1), List.of());
        ClusterCoordinator coordinator = soloCoordinator(transport);
        ReplicationManager manager = newManager(transport, coordinator);
        try {
            manager.registerHandler(TOPIC, new SnapshotHandler(seq -> releaseDrain.await()));
            manager.start();
            coordinator.start();
            awaitCondition(coordinator::isLeader, 15_000, "o nó deve ser promovido");
            assertTrue(manager.isLeaderSyncing(), "com o relay por drenar, o drain-gate segura a liderança");

            // Durante a drenagem o líder não anuncia high-watermark (a fronteira ainda vai subir).
            assertEquals(-1L, fetchLeaderHighWatermark(transport, appliedAsFollower + 1L),
                    "um líder em leaderSyncing anuncia leaderHighWatermark=-1 (desconhecido)");

            releaseDrain.countDown();
            awaitCondition(() -> frontier(manager) == received, 15_000, "o drain aplica 21..50");
            awaitCondition(() -> !manager.isLeaderSyncing(), 15_000, "o drain-gate libera a escrita");

            assertEquals(received, requestSnapshotLabel(transport),
                    "o rótulo deve incluir o que o líder aplicou depois da promoção, sem produzir");
            assertEquals(received, fetchLeaderHighWatermark(transport, received + 1L),
                    "drenado o relay, o high-watermark volta a ser anunciado");
        } finally {
            closeQuietly(manager, coordinator);
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void syncPedidoDuranteADrenagemRecebeAFronteiraAplicadaComoRotulo() throws Exception {
        int appliedAsFollower = 20;
        int received = 50;
        prepareUnappliedRelayTail(appliedAsFollower, received);

        CountDownLatch releaseDrain = new CountDownLatch(1);
        ScriptedTransport transport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1), List.of());
        ClusterCoordinator coordinator = soloCoordinator(transport);
        ReplicationManager manager = newManager(transport, coordinator);
        try {
            manager.registerHandler(TOPIC, new SnapshotHandler(seq -> releaseDrain.await()));
            manager.start();
            coordinator.start();
            awaitCondition(coordinator::isLeader, 15_000, "o nó deve ser promovido");
            assertTrue(manager.isLeaderSyncing(), "com o relay por drenar, o drain-gate segura a liderança");

            // Comportamento definido: durante a drenagem o rótulo é a fronteira APLICADA no momento do
            // pedido (20) — limite inferior seguro do conteúdo capturado em seguida. Nem o cursor do relay
            // (50, operações ainda não aplicadas, que o snapshot não contém), nem -1/0 (o HWM anunciado na
            // drenagem é "desconhecido", mas o rótulo de um snapshot precisa ser concreto). O que o líder
            // aplicar depois chega ao instalador pelo stream, a partir de 21, espelhado no op-log.
            assertEquals(appliedAsFollower, requestSnapshotLabel(transport),
                    "durante a drenagem o rótulo é a fronteira aplicada, não o cursor do relay");
            releaseDrain.countDown();
            awaitCondition(() -> !manager.isLeaderSyncing(), 15_000, "o drain-gate libera a escrita");
            assertEquals(received, requestSnapshotLabel(transport), "drenado o relay, o rótulo acompanha a fronteira");
        } finally {
            releaseDrain.countDown();
            closeQuietly(manager, coordinator);
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void handbackPedidoDuranteADrenagemEAbortado() throws Exception {
        prepareUnappliedRelayTail(20, 50);

        CountDownLatch releaseDrain = new CountDownLatch(1);
        ScriptedTransport transport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1, Set.of(), 50),
                List.of());
        ClusterCoordinator coordinator = soloCoordinator(transport);
        ReplicationManager manager = newManager(transport, coordinator, true);
        try {
            manager.registerHandler(TOPIC, new SnapshotHandler(seq -> releaseDrain.await()));
            manager.start();
            coordinator.start();
            awaitCondition(coordinator::isLeader, 15_000, "o nó deve ser promovido");

            // Um candidato de maior afinidade, ainda atrás, pede o handback enquanto o líder drena.
            transport.connect(new NodeInfo(CANDIDATE, "127.0.0.1", 2, Set.of(), 100));
            for (int i = 0; i < 5; i++) {
                transport.deliver(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", CANDIDATE, null,
                        HeartbeatPayload.now(0L, 0L, false)));
                Thread.sleep(50);
            }
            assertTrue(coordinator.isLeader(), "o candidato atrasado não toma a liderança");
            assertTrue(manager.isLeaderSyncing(), "o líder ainda drena o relay");
            transport.clearSent();
            transport.deliver(ClusterMessage.request(MessageType.HANDBACK_REQUEST, "handback", CANDIDATE, LOCAL,
                    new HandbackRequestPayload(CANDIDATE, 0L, 20L)));

            ClusterMessage abort = awaitSent(transport, MessageType.HANDBACK_ABORT, 5_000);
            assertEquals("leader still draining", abort.payload(HandbackAbortPayload.class).reason());
            assertFalse(manager.isHandoverFreezing(), "o líder em drenagem não congela a produção");
            assertTrue(transport.sentOfType(MessageType.HANDBACK_GRANT).isEmpty(), "nenhum GRANT é enviado");
        } finally {
            releaseDrain.countDown();
            closeQuietly(manager, coordinator);
        }
    }

    @Test
    @Timeout(value = 90, unit = TimeUnit.SECONDS)
    void topicoRegistradoDepoisDaPromocaoComRelayPorDrenarNaoDisparaODetectorDoLider() throws Exception {
        prepareUnappliedRelayTail(20, 50);

        CountDownLatch releaseDrain = new CountDownLatch(1);
        ScriptedTransport transport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1), List.of());
        ClusterCoordinator coordinator = soloCoordinator(transport);
        ReplicationManager manager = newManager(transport, coordinator);
        try {
            // A promoção acontece só com outro tópico registrado: o drain-gate não cobre o TOPIC.
            manager.registerHandler(OTHER_TOPIC, new SnapshotHandler(null));
            manager.start();
            coordinator.start();
            awaitCondition(() -> coordinator.isLeader() && !manager.isLeaderSyncing(), 15_000,
                    "o nó deve liderar com o drain-gate liberado");
            manager.replicate(OTHER_TOPIC, "1".getBytes(StandardCharsets.UTF_8)).get(5, TimeUnit.SECONDS);

            // O handler do TOPIC registra depois da promoção e drena o relay (21..50) fora do gate.
            manager.registerHandler(TOPIC, new SnapshotHandler(seq -> releaseDrain.await()));
            awaitCondition(() -> frontier(manager) == 20L, 10_000, "o TOPIC aplica até 20 e trava no 21");
            assertFalse(manager.isLeaderSyncing(), "o gate de promoção não cobre o TOPIC");

            // Um seguidor que já recebeu até 30 do líder anterior está à frente da fronteira (20), mas
            // legitimamente: o líder ainda vai aplicar 21..50. Nada de needSnapshot, e HWM desconhecido.
            RelayStreamBatchPayload batch = fetchBatch(transport, 31L);
            assertFalse(batch.needSnapshot(), "um tópico ainda drenando não dispara o detector do líder");
            assertEquals(-1L, batch.leaderHighWatermark(), "durante a drenagem do tópico o HWM é desconhecido");

            releaseDrain.countDown();
            awaitCondition(() -> frontier(manager) == 50L, 10_000, "o TOPIC drena até 50");
            assertEquals(50L, fetchBatch(transport, 51L).leaderHighWatermark(), "drenado, o HWM volta");
        } finally {
            releaseDrain.countDown();
            closeQuietly(manager, coordinator);
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void topicoSemHandlerNoLiderNaoDisparaODetector() throws Exception {
        // Sequence-state com o TOPIC em 100, mas sem handler registrado para ele neste líder.
        writeSequenceState(Map.of(TOPIC, 101L, "_topic:" + TOPIC, 100L, "_global", 100L));
        ScriptedTransport transport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1), List.of());
        ClusterCoordinator coordinator = soloCoordinator(transport);
        ReplicationManager manager = newManager(transport, coordinator);
        try {
            manager.registerHandler(OTHER_TOPIC, new SnapshotHandler(null));
            manager.start();
            coordinator.start();
            awaitCondition(() -> coordinator.isLeader() && !manager.isLeaderSyncing(), 15_000,
                    "o nó deve liderar com o drain-gate liberado");
            manager.replicate(OTHER_TOPIC, "1".getBytes(StandardCharsets.UTF_8)).get(5, TimeUnit.SECONDS);

            // Sem handler o líder não responderia o SYNC_REQUEST: mandar o seguidor ao bootstrap o deixaria
            // inelegível e parado indefinidamente.
            RelayStreamBatchPayload batch = fetchBatch(transport, 151L);
            assertFalse(batch.needSnapshot(), "sem handler para o tópico o líder não manda bootstrap");
        } finally {
            closeQuietly(manager, coordinator);
        }
    }

    // ---- apoio ----

    /**
     * Fase de seguidor: recebe {@code 1..received} no relay e só aplica {@code 1..appliedAsFollower} (o
     * apply trava na seguinte). O nó para com o relay por drenar.
     */
    private void prepareUnappliedRelayTail(int appliedAsFollower, int received) throws InterruptedException {
        ScriptedTransport followerTransport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1),
                List.of(new NodeInfo(OLD_LEADER, "127.0.0.1", 2)));
        ClusterCoordinator followerCoordinator = new ClusterCoordinator(followerTransport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(60),
                        Duration.ofSeconds(60), 2, null),
                newScheduler());
        ReplicationManager follower = newManager(followerTransport, followerCoordinator);
        CountDownLatch neverReleased = new CountDownLatch(1);
        try {
            follower.registerHandler(TOPIC, new SnapshotHandler(seq -> {
                if (seq > appliedAsFollower) {
                    neverReleased.await();
                }
            }));
            follower.start();
            followerCoordinator.start();
            awaitCondition(() -> {
                followerCoordinator.onMessage(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", OLD_LEADER,
                        null, HeartbeatPayload.now(received, 1L)));
                return OLD_LEADER.equals(followerCoordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null));
            }, 10_000, "o seguidor deve adotar o líder antigo");
            List<byte[]> frames = new ArrayList<>();
            for (int i = 1; i <= received; i++) {
                frames.add(frame(i));
            }
            followerTransport.deliver(ClusterMessage.request(MessageType.RELAY_STREAM_BATCH, "stream", OLD_LEADER,
                    LOCAL, new RelayStreamBatchPayload(TOPIC, 1L, frames, received, 1L, false)));
            awaitCondition(() -> follower.getRelayStreamCursor(TOPIC) == received
                            && frontier(follower) == appliedAsFollower,
                    10_000, "o relay deve guardar 1..50 com só 1..20 aplicados");
        } finally {
            closeQuietly(follower, followerCoordinator);
        }

    }


    /**
     * Coordinator de um nó sozinho (pair mode), ainda não iniciado: como no {@code NGridNode}, o
     * {@link ReplicationManager} inicia antes e já está registrado quando a promoção acontece.
     */
    private ClusterCoordinator soloCoordinator(ScriptedTransport transport) {
        return new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofMillis(800),
                        Duration.ofSeconds(60), 1, null).withPairMode(true),
                newScheduler());
    }

    /** O {@code close()} do coordinator encerra o scheduler: cada coordinator ganha o seu. */
    private ScheduledExecutorService newScheduler() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        schedulers.add(scheduler);
        return scheduler;
    }

    private ReplicationManager newManager(ScriptedTransport transport, ClusterCoordinator coordinator) {
        return newManager(transport, coordinator, false);
    }

    private ReplicationManager newManager(ScriptedTransport transport, ClusterCoordinator coordinator,
            boolean affinityHandbackMode) {
        return new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .strictConsistency(false)
                        .leaderLocalApply(false)
                        .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                        .operationTimeout(Duration.ofSeconds(5))
                        .affinityHandbackMode(affinityHandbackMode)
                        .dataDirectory(tempDir)
                        .build());
    }

    /** Envia um SYNC_REQUEST (chunk 0) ao nó e devolve o rótulo do SYNC_RESPONSE. */
    private static long requestSnapshotLabel(ScriptedTransport transport) throws InterruptedException {
        transport.clearSent();
        transport.deliver(ClusterMessage.request(MessageType.SYNC_REQUEST, "sync", REQUESTER, LOCAL,
                new SyncRequestPayload(TOPIC, 0)));
        ClusterMessage response = awaitSent(transport, MessageType.SYNC_RESPONSE, 5_000);
        return response.payload(SyncResponsePayload.class).sequence();
    }

    /** Envia um RELAY_STREAM_FETCH do TOPIC ao nó e devolve o lote de resposta. */
    private static RelayStreamBatchPayload fetchBatch(ScriptedTransport transport, long from)
            throws InterruptedException {
        transport.clearSent();
        transport.deliver(ClusterMessage.request(MessageType.RELAY_STREAM_FETCH, "stream", REQUESTER, LOCAL,
                new RelayStreamFetchPayload(TOPIC, from, 16)));
        return awaitSent(transport, MessageType.RELAY_STREAM_BATCH, 5_000).payload(RelayStreamBatchPayload.class);
    }

    /** Envia um RELAY_STREAM_FETCH ao nó e devolve o leaderHighWatermark do lote de resposta. */
    private static long fetchLeaderHighWatermark(ScriptedTransport transport, long from) throws InterruptedException {
        transport.clearSent();
        transport.deliver(ClusterMessage.request(MessageType.RELAY_STREAM_FETCH, "stream", REQUESTER, LOCAL,
                new RelayStreamFetchPayload(TOPIC, from, 16)));
        ClusterMessage batch = awaitSent(transport, MessageType.RELAY_STREAM_BATCH, 5_000);
        return batch.payload(RelayStreamBatchPayload.class).leaderHighWatermark();
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

    private static long frontier(ReplicationManager manager) {
        ReplicationManager.TopicReplicationStatus status = manager.getTopicReplicationStatuses().get(TOPIC);
        return status == null ? -1L : status.nextExpectedSequence() - 1L;
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

    private static byte[] frame(long sequence) {
        return RelayEntryCodec.encode(new RelayEntry(1L, sequence, TOPIC, UUID.randomUUID(),
                Long.toString(sequence).getBytes(StandardCharsets.UTF_8)));
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

    private static void closeQuietly(ReplicationManager manager, ClusterCoordinator coordinator) {
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

    /** Ação executada antes de aplicar a sequência dada (para travar o apply num ponto). */
    @FunctionalInterface
    private interface ApplyGate {
        void beforeApply(long sequence) throws InterruptedException;
    }

    /** Handler que serve um snapshot qualquer e lê a sequência do payload (texto) para o gate. */
    private static final class SnapshotHandler implements ReplicationHandler {
        private final ApplyGate gate;

        SnapshotHandler(ApplyGate gate) {
            this.gate = gate;
        }

        @Override
        public void apply(UUID operationId, Object payload) throws Exception {
            long sequence = Long.parseLong(new String((byte[]) payload, StandardCharsets.UTF_8));
            if (gate != null) {
                gate.beforeApply(sequence);
            }
        }

        @Override
        public Object getSnapshot() {
            return new byte[] {1};
        }
    }
}
