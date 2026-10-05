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
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.RelayStreamBatchPayload;
import dev.nishisan.utils.ngrid.common.SyncResponsePayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 8.10.1 — autocura do seguidor à frente do líder, no nível do {@link ReplicationManager}.
 *
 * <p>Reproduz o lado do seguidor do incidente do CTP: o cursor local (10) fica acima do
 * {@code leaderHighWatermark} que o líder anuncia (4) e os lotes chegam vazios, sem {@code needSnapshot}.
 * Sem a autocura o seguidor ficaria assim para sempre, com lag {@code 0}, e descartaria como duplicadas
 * as operações 5..10 que o líder produzisse. Com ela, K respostas seguidas durante T armam o bootstrap,
 * o seguidor pede o snapshot, instala-o e volta a seguir a numeração do líder.
 */
class FollowerAheadSelfHealTest {

    private static final String TOPIC = "map:catalog";
    private static final NodeId LEADER = NodeId.of("zzz-leader");
    private static final NodeId FOLLOWER = NodeId.of("aaa-follower");
    private static final long LEADER_EPOCH = 7L;
    private static final long MIN_DURATION_MS = 400L;

    private Path tempDir;
    private ScheduledExecutorService scheduler;
    private int savedConfirmations;
    private long savedMinDuration;
    private long savedCooldown;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("follower-ahead-self-heal");
        scheduler = Executors.newScheduledThreadPool(2);
        savedConfirmations = ReplicationManager.FOLLOWER_AHEAD_CONFIRMATIONS;
        savedMinDuration = ReplicationManager.FOLLOWER_AHEAD_MIN_DURATION_MS;
        savedCooldown = ReplicationManager.FOLLOWER_AHEAD_COOLDOWN_MS;
        ReplicationManager.FOLLOWER_AHEAD_CONFIRMATIONS = 3;
        ReplicationManager.FOLLOWER_AHEAD_MIN_DURATION_MS = MIN_DURATION_MS;
        ReplicationManager.FOLLOWER_AHEAD_COOLDOWN_MS = 60_000L;
    }

    @AfterEach
    void tearDown() {
        ReplicationManager.FOLLOWER_AHEAD_CONFIRMATIONS = savedConfirmations;
        ReplicationManager.FOLLOWER_AHEAD_MIN_DURATION_MS = savedMinDuration;
        ReplicationManager.FOLLOWER_AHEAD_COOLDOWN_MS = savedCooldown;
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void seguidorAFrenteDoLiderReinstalaOTopicoEVoltaASeguirANumeracaoDoLider() throws Exception {
        LogCapture capture = LogCapture.attach();
        try (Follower f = new Follower()) {
            f.adoptLeader(true);
            TestEnv env = f.env();

            // Linhagem antiga: o seguidor aplicou 1..10.
            f.applyOldLineage();

            // O líder passa a anunciar HWM 4 (fronteira rebaixada por um rótulo desatualizado).
            env.transport.clearSent();
            deliverBatch(env.transport, 11L, List.of(), 4L);
            assertFalse(status(env.manager).relayPendingBootstrap(), "uma resposta isolada não arma o bootstrap");
            deliverBatch(env.transport, 11L, List.of(), 4L);
            deliverBatch(env.transport, 11L, List.of(), 4L);
            assertFalse(status(env.manager).relayPendingBootstrap(),
                    "K respostas dentro do tempo mínimo não armam o bootstrap");
            assertEquals(0L, env.manager.getReplicationLag(TOPIC), "o sintoma do incidente: lag truncado em 0");

            Thread.sleep(MIN_DURATION_MS + 100L);
            deliverBatch(env.transport, 11L, List.of(), 4L);
            assertTrue(status(env.manager).relayPendingBootstrap(),
                    "K respostas e o tempo mínimo vencido armam o bootstrap do tópico");
            assertFalse(env.manager.isLeadershipEligible(), "com o bootstrap pendente o nó fica inelegível");
            assertTrue(capture.contains(Level.SEVERE, "NGRID_FOLLOWER_AHEAD_OF_LEADER topic=" + TOPIC
                            + " cursor=10 leaderHwm=4 leader=" + LEADER + " action=bootstrap"),
                    "o disparo loga o marcador estável em SEVERE");
            f.installLeaderSnapshotAndFollow(4L);
        } finally {
            capture.detach();
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void liderSemVetorDeFronteirasAnteriorA880NaoArmaOBootstrap() throws Exception {
        try (Follower f = new Follower()) {
            // Líder ≤ 8.7.0: heartbeat sem vetor por tópico; o HWM dele pode ser o contador cru (0 num
            // tópico ocioso após a promoção, #177) e não serve como prova de divergência.
            f.adoptLeader(false);
            TestEnv env = f.env();
            f.applyOldLineage();
            for (int i = 0; i < 3; i++) {
                deliverBatch(env.transport, 11L, List.of(), 0L);
            }
            Thread.sleep(MIN_DURATION_MS + 100L);
            for (int i = 0; i < 3; i++) {
                deliverBatch(env.transport, 11L, List.of(), 0L);
            }
            assertFalse(status(env.manager).relayPendingBootstrap(),
                    "contra um líder anterior à 8.8.0 a autocura do seguidor fica inerte");
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void needSnapshotDoLiderParaCursorAcimaDoHwmArmaOBootstrapNaPrimeiraResposta() throws Exception {
        LogCapture capture = LogCapture.attach();
        try (Follower f = new Follower()) {
            f.adoptLeader(true);
            TestEnv env = f.env();
            f.applyOldLineage();
            env.transport.clearSent();

            // O líder (8.10.1, já produziu no mandato) detecta o seguidor à frente na primeira busca.
            deliverNeedSnapshot(env.transport, 11L, 4L, 1L);
            assertTrue(status(env.manager).relayPendingBootstrap(),
                    "o needSnapshot de seguidor à frente arma o bootstrap na hora, sem esperar K e T");
            assertTrue(capture.contains(Level.SEVERE, "NGRID_FOLLOWER_AHEAD_OF_LEADER topic=" + TOPIC
                    + " cursor=10 leaderHwm=4 leader=" + LEADER + " action=bootstrap detectedBy=leader"));
            // Um pedido de snapshot simples seria descartado pelo guard de snapshot antigo (rótulo 4 <
            // aplicado 10); com o bootstrap pendente o rótulo menor é instalado.
            f.installLeaderSnapshotAndFollow(4L);
        } finally {
            capture.detach();
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void needSnapshotAbaixoDaJanelaRetidaSegueOCaminhoSimples() throws Exception {
        try (Follower f = new Follower()) {
            f.adoptLeader(true);
            TestEnv env = f.env();
            f.applyOldLineage();
            env.transport.clearSent();

            // Busca abaixo da janela retida (from=11 < oldest=30 <= hwm=50): snapshot simples, sem bootstrap.
            deliverNeedSnapshot(env.transport, 11L, 50L, 30L);
            awaitSent(env.transport, MessageType.SYNC_REQUEST, 10_000);
            assertFalse(status(env.manager).relayPendingBootstrap(),
                    "o needSnapshot de janela retida não é sinal de seguidor à frente");
            assertTrue(status(env.manager).syncing(), "o caminho simples arma só o guard de sync");
        }
    }

    /** Recursos de um seguidor de teste. */
    private record TestEnv(ScriptedTransport transport, ClusterCoordinator coordinator, ReplicationManager manager,
            RecordingHandler handler) {
    }

    /** Seguidor de teste com transporte em memória, seguindo {@link #LEADER}. */
    private final class Follower implements AutoCloseable {
        private final TestEnv env;

        Follower() {
            ScriptedTransport transport = new ScriptedTransport(new NodeInfo(FOLLOWER, "127.0.0.1", 1),
                    List.of(new NodeInfo(LEADER, "127.0.0.1", 2)));
            ClusterCoordinator coordinator = new ClusterCoordinator(transport,
                    ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(60),
                            Duration.ofSeconds(60), 2, null),
                    scheduler);
            ReplicationManager manager = new ReplicationManager(transport, coordinator,
                    ReplicationConfig.builder(1)
                            .strictConsistency(false)
                            .leaderLocalApply(false)
                            .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                            .operationTimeout(Duration.ofSeconds(5))
                            .dataDirectory(tempDir)
                            .build());
            RecordingHandler handler = new RecordingHandler();
            manager.registerHandler(TOPIC, handler);
            manager.start();
            coordinator.start();
            this.env = new TestEnv(transport, coordinator, manager, handler);
        }

        TestEnv env() {
            return env;
        }

        /** Adota o líder; {@code withFrontierVector} = o líder anuncia o vetor por tópico (8.8.0+). */
        void adoptLeader(boolean withFrontierVector) throws InterruptedException {
            Map<String, Long> vector = withFrontierVector ? Map.of(TOPIC, 10L) : null;
            awaitCondition(() -> {
                env.coordinator.onMessage(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", LEADER, null,
                        HeartbeatPayload.now(10L, LEADER_EPOCH, true, vector)));
                return LEADER.equals(env.coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null));
            }, 10_000, "o seguidor deve adotar o líder");
        }

        void applyOldLineage() throws InterruptedException {
            deliverBatch(env.transport, 1L, frames(1, 10, "old-"), 10L);
            awaitCondition(() -> env.manager.getRelayStreamCursor(TOPIC) == 10L && frontier(env.manager) == 10L,
                    10_000, "o seguidor deve aplicar 1..10");
        }

        /** Atende o SYNC_REQUEST com o rótulo dado e prova que as operações novas do líder chegam. */
        void installLeaderSnapshotAndFollow(long label) throws InterruptedException {
            ClusterMessage syncRequest = awaitSent(env.transport, MessageType.SYNC_REQUEST, 10_000);
            assertEquals(LEADER, syncRequest.destination(), "o snapshot é pedido ao líder");
            env.transport.deliver(ScriptedTransport.syncResponse(env.transport.sentOfType(MessageType.SYNC_REQUEST), LEADER,
                    new SyncResponsePayload(TOPIC, label, new byte[0])));
            awaitCondition(() -> !status(env.manager).relayPendingBootstrap(), 10_000,
                    "a instalação do snapshot desarma o bootstrap");
            assertTrue(env.handler.resetCalled.get(), "o snapshot substitui o estado local");
            assertEquals(label, env.manager.getRelayStreamCursor(TOPIC), "o cursor reancora no rótulo do líder");
            assertEquals(label, frontier(env.manager), "a fronteira reancora no rótulo do líder");

            // As operações novas do líder já não são descartadas como duplicadas.
            env.handler.applied.clear();
            deliverBatch(env.transport, label + 1L, frames((int) label + 1, (int) label + 3, "new-"), label + 3L);
            awaitCondition(() -> frontier(env.manager) == label + 3L, 10_000, "o seguidor aplica as novas do líder");
            assertEquals(List.of("new-" + (label + 1), "new-" + (label + 2), "new-" + (label + 3)), env.handler.applied);
        }

        @Override
        public void close() {
            try {
                env.manager.close();
            } catch (Exception ignored) {
                // teardown best-effort
            }
            try {
                env.coordinator.close();
            } catch (Exception ignored) {
                // teardown best-effort
            }
        }
    }

    // ---- apoio ----

    private static void deliverNeedSnapshot(ScriptedTransport transport, long from, long hwm, long oldest) {
        transport.deliver(ClusterMessage.request(MessageType.RELAY_STREAM_BATCH, "stream", LEADER, FOLLOWER,
                new RelayStreamBatchPayload(TOPIC, from, List.of(), hwm, oldest, true)));
    }

    private static void deliverBatch(ScriptedTransport transport, long from, List<byte[]> frames, long hwm) {
        transport.deliver(ClusterMessage.request(MessageType.RELAY_STREAM_BATCH, "stream", LEADER, FOLLOWER,
                new RelayStreamBatchPayload(TOPIC, from, frames, hwm, 1L, false)));
    }

    private static List<byte[]> frames(int first, int last, String prefix) {
        List<byte[]> out = new ArrayList<>();
        for (int seq = first; seq <= last; seq++) {
            out.add(RelayEntryCodec.encode(new RelayEntry(LEADER_EPOCH, seq, TOPIC, UUID.randomUUID(),
                    (prefix + seq).getBytes(StandardCharsets.UTF_8))));
        }
        return out;
    }

    private static ReplicationManager.TopicReplicationStatus status(ReplicationManager manager) {
        return manager.getTopicReplicationStatuses().get(TOPIC);
    }

    private static long frontier(ReplicationManager manager) {
        return status(manager).nextExpectedSequence() - 1L;
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

    /** Handler que registra os payloads aplicados e as chamadas de reset. */
    private static final class RecordingHandler implements ReplicationHandler {
        final List<String> applied = new CopyOnWriteArrayList<>();
        final AtomicBoolean resetCalled = new AtomicBoolean();

        @Override
        public void apply(UUID operationId, Object payload) {
            applied.add(new String((byte[]) payload, StandardCharsets.UTF_8));
        }

        @Override
        public void resetState() {
            resetCalled.set(true);
        }
    }

    /** Captura os registros de log do {@link ReplicationManager}. */
    private static final class LogCapture extends Handler {
        private final Logger logger = Logger.getLogger(ReplicationManager.class.getName());
        private final List<LogRecord> records = new CopyOnWriteArrayList<>();

        static LogCapture attach() {
            LogCapture capture = new LogCapture();
            capture.logger.addHandler(capture);
            return capture;
        }

        void detach() {
            logger.removeHandler(this);
        }

        boolean contains(Level level, String text) {
            for (LogRecord record : records) {
                if (record.getLevel().equals(level) && record.getMessage() != null
                        && record.getMessage().contains(text)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void publish(LogRecord record) {
            records.add(record);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
