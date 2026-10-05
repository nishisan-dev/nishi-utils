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
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.ngrid.common.RelayStreamBatchPayload;
import dev.nishisan.utils.ngrid.common.RelayStreamFetchPayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
 * 8.10.1 — detecção do seguidor à frente no LÍDER.
 *
 * <p>A autocura do seguidor só enxerga a condição enquanto os lotes chegam vazios. Se o líder produz além
 * do cursor do seguidor antes do tempo mínimo, o seguidor volta a receber dados a partir do cursor e as
 * operações em (HWM antigo, cursor] nunca são buscadas. O líder vê a condição na primeira busca
 * ({@code from - 1 > hwm}) e responde {@code needSnapshot} — mas só depois de produzir no mandato atual:
 * antes disso o yield do líder recém-eleito (revisão #178) tem a precedência.
 */
class LeaderSideFollowerAheadTest {

    private static final String TOPIC = "map:catalog";
    private static final NodeId LOCAL = NodeId.of("aaa-leader");
    private static final NodeId FOLLOWER = NodeId.of("mmm-follower");
    private static final long FRONTIER = 100L;
    private static final long FOLLOWER_CURSOR = 150L;

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("leader-side-follower-ahead");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void liderQueJaProduziuRespondeNeedSnapshotNaPrimeiraBuscaDoSeguidorAFrente() throws Exception {
        Map<String, Long> state = new HashMap<>();
        state.put(TOPIC, FRONTIER + 1L);
        state.put("_topic:" + TOPIC, FRONTIER);
        state.put("_global", FRONTIER);
        try (ObjectOutputStream oos = new ObjectOutputStream(
                Files.newOutputStream(tempDir.resolve("sequence-state.dat")))) {
            oos.writeObject(state);
        }
        LogCapture capture = LogCapture.attach();
        ScriptedTransport transport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1), List.of());
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
                        .dataDirectory(tempDir)
                        .build());
        try {
            manager.registerHandler(TOPIC, (operationId, payload) -> {
            });
            manager.start();
            coordinator.start();
            awaitCondition(() -> coordinator.isLeader() && !manager.isLeaderSyncing(), 15_000,
                    "o nó deve liderar com o drain-gate liberado");

            // Antes de produzir: o yield do #178 tem a precedência; o líder só informa o HWM.
            RelayStreamBatchPayload beforeProducing = fetch(transport, FOLLOWER_CURSOR + 1L);
            assertFalse(beforeProducing.needSnapshot(),
                    "um líder que ainda não produziu no mandato não manda o seguidor descartar estado");
            assertEquals(FRONTIER, beforeProducing.leaderHighWatermark());

            manager.replicate(TOPIC, "x".getBytes(StandardCharsets.UTF_8)).get(5, TimeUnit.SECONDS);

            // Depois de produzir: a primeira busca do seguidor à frente já recebe needSnapshot.
            RelayStreamBatchPayload afterProducing = fetch(transport, FOLLOWER_CURSOR + 1L);
            assertTrue(afterProducing.needSnapshot(),
                    "o líder que já produziu detecta o seguidor à frente na primeira busca");
            assertEquals(FRONTIER + 1L, afterProducing.leaderHighWatermark());
            assertTrue(capture.contains(Level.WARNING, "NGRID_FOLLOWER_AHEAD_OF_LEADER topic=" + TOPIC
                    + " cursor=" + FOLLOWER_CURSOR + " leaderHwm=" + (FRONTIER + 1L) + " follower=" + FOLLOWER
                    + " action=needSnapshot detectedBy=leader"), "o líder loga o marcador em WARNING");

            // Um seguidor em dia (cursor == HWM) não é afetado.
            RelayStreamBatchPayload caughtUp = fetch(transport, FRONTIER + 2L);
            assertFalse(caughtUp.needSnapshot(), "um seguidor em dia não recebe needSnapshot");
        } finally {
            capture.detach();
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
    }

    private static RelayStreamBatchPayload fetch(ScriptedTransport transport, long from) throws InterruptedException {
        transport.clearSent();
        transport.deliver(ClusterMessage.request(MessageType.RELAY_STREAM_FETCH, "stream", FOLLOWER, LOCAL,
                new RelayStreamFetchPayload(TOPIC, from, 16)));
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            List<ClusterMessage> sent = transport.sentOfType(MessageType.RELAY_STREAM_BATCH);
            if (!sent.isEmpty()) {
                return sent.get(0).payload(RelayStreamBatchPayload.class);
            }
            Thread.sleep(20);
        }
        throw new AssertionError("o líder não respondeu o RELAY_STREAM_FETCH");
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
                String message = record.getMessage();
                if (record.getLevel().equals(level) && message != null && message.contains(text)) {
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
