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
import dev.nishisan.utils.ngrid.common.SyncRequestPayload;
import dev.nishisan.utils.ngrid.common.SyncResponsePayload;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 8.10.1 — o servidor de snapshot no {@link ReplicationManager}.
 *
 * <ul>
 *   <li>O rótulo é um limite inferior do conteúdo: com o apply local ASSÍNCRONO do líder, uma operação
 *       já numerada mas ainda não aplicada não entra no rótulo.</li>
 *   <li>Os chunks de uma transferência usam uma sessão estável (requisitante e tópico), liberada no
 *       último chunk; um chunk sem transferência em andamento não é respondido.</li>
 * </ul>
 */
class SnapshotServingSessionTest {

    private static final String TOPIC = "map:catalog";
    private static final NodeId LOCAL = NodeId.of("aaa-leader");
    private static final NodeId REQUESTER = NodeId.of("mmm-requester");
    private static final NodeId OTHER_REQUESTER = NodeId.of("nnn-requester");

    private Path tempDir;
    private ScheduledExecutorService scheduler;
    private ScriptedTransport transport;
    private ClusterCoordinator coordinator;
    private ReplicationManager manager;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("snapshot-serving-session");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        try {
            if (manager != null) {
                manager.close();
            }
        } catch (Exception ignored) {
            // teardown best-effort
        }
        try {
            if (coordinator != null) {
                coordinator.close();
            }
        } catch (Exception ignored) {
            // teardown best-effort
        }
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void rotuloNaoIncluiOperacaoNumeradaAindaNaoAplicadaPeloLider() throws Exception {
        CountDownLatch releaseApply = new CountDownLatch(1);
        RecordingHandler handler = new RecordingHandler(releaseApply);
        startLeader(handler, true);
        try {
            manager.replicate(TOPIC, "a".getBytes(StandardCharsets.UTF_8)).get(5, TimeUnit.SECONDS);
            handler.blockNext = true;
            CompletableFuture<ReplicationResult> blocked =
                    manager.replicate(TOPIC, "b".getBytes(StandardCharsets.UTF_8));
            awaitCondition(() -> handler.applying, 5_000, "o apply assíncrono da operação 2 deve começar");

            // A operação 2 já tem sequência (o contador está em 2), mas o estado ainda não a contém.
            assertEquals(1L, requestLabel(REQUESTER),
                    "o rótulo para abaixo da operação ainda pendente de apply local");

            releaseApply.countDown();
            blocked.get(5, TimeUnit.SECONDS);
            assertEquals(2L, requestLabel(OTHER_REQUESTER), "aplicada, a operação entra no rótulo");
        } finally {
            releaseApply.countDown();
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void chunksUsamUmaSessaoEstavelLiberadaNoFimEChunkSemTransferenciaNaoERespondido() throws Exception {
        RecordingHandler handler = new RecordingHandler(null);
        handler.chunks = 3;
        startLeader(handler, false);

        for (int chunk = 0; chunk < 3; chunk++) {
            transport.clearSent();
            transport.deliver(ClusterMessage.request(MessageType.SYNC_REQUEST, "sync", REQUESTER, LOCAL,
                    new SyncRequestPayload(TOPIC, chunk)));
            awaitSent(MessageType.SYNC_RESPONSE);
        }
        String session = REQUESTER + "::" + TOPIC;
        assertEquals(List.of(session + "#0", session + "#1", session + "#2"), handler.calls,
                "todos os chunks da transferência usam a mesma sessão");
        assertEquals(List.of(session), handler.released, "o último chunk libera a sessão");

        // Chunk 1 de outro requisitante, sem chunk 0: nenhuma resposta (ele recomeça do chunk 0).
        transport.clearSent();
        transport.deliver(ClusterMessage.request(MessageType.SYNC_REQUEST, "sync", OTHER_REQUESTER, LOCAL,
                new SyncRequestPayload(TOPIC, 1)));
        Thread.sleep(300);
        assertTrue(transport.sentOfType(MessageType.SYNC_RESPONSE).isEmpty(),
                "um chunk sem transferência em andamento não é respondido");
        assertEquals(3, handler.calls.size(), "o handler não serve chunk de sessão inexistente");
    }

    // ---- apoio ----

    private void startLeader(RecordingHandler handler, boolean leaderLocalApply) throws InterruptedException {
        transport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1), List.of());
        coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(5),
                        Duration.ofSeconds(60), 1, null).withPairMode(true),
                scheduler);
        manager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .strictConsistency(false)
                        .leaderLocalApply(leaderLocalApply)
                        .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                        .operationTimeout(Duration.ofSeconds(10))
                        .dataDirectory(tempDir)
                        .build());
        manager.registerHandler(TOPIC, handler);
        manager.start();
        coordinator.start();
        awaitCondition(() -> coordinator.isLeader() && !manager.isLeaderSyncing(), 15_000,
                "o nó deve liderar com o drain-gate liberado");
    }

    private long requestLabel(NodeId requester) throws InterruptedException {
        transport.clearSent();
        transport.deliver(ClusterMessage.request(MessageType.SYNC_REQUEST, "sync", requester, LOCAL,
                new SyncRequestPayload(TOPIC, 0)));
        return awaitSent(MessageType.SYNC_RESPONSE).payload(SyncResponsePayload.class).sequence();
    }

    private ClusterMessage awaitSent(MessageType type) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            List<ClusterMessage> sent = transport.sentOfType(type);
            if (!sent.isEmpty()) {
                return sent.get(0);
            }
            Thread.sleep(20);
        }
        throw new AssertionError("o nó não enviou " + type);
    }

    private static void awaitCondition(BooleanSupplier condition, long timeoutMs, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        fail(message);
    }

    /** Handler que registra as chamadas de chunk por sessão e pode travar um apply. */
    private static final class RecordingHandler implements ReplicationHandler {
        private final CountDownLatch release;
        final List<String> calls = new CopyOnWriteArrayList<>();
        final List<String> released = new CopyOnWriteArrayList<>();
        volatile int chunks = 1;
        volatile boolean blockNext;
        volatile boolean applying;

        RecordingHandler(CountDownLatch release) {
            this.release = release;
        }

        @Override
        public void apply(UUID operationId, Object payload) throws Exception {
            if (blockNext) {
                blockNext = false;
                applying = true;
                release.await();
            }
        }

        @Override
        public SnapshotChunk getSnapshotChunk(String sessionId, int chunkIndex) {
            calls.add(sessionId + "#" + chunkIndex);
            return new SnapshotChunk(new byte[] {1}, chunkIndex + 1 < chunks);
        }

        @Override
        public void releaseSnapshotSession(String sessionId) {
            released.add(sessionId);
        }
    }
}
