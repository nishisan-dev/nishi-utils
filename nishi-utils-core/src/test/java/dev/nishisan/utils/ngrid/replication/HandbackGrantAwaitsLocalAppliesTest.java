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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
 * 8.10.1 — o líder interino só concede o handback depois do apply local de todas as operações aceitas.
 *
 * <p>Com backlog de apply local, o rótulo do snapshot (limitado abaixo da pendente mais antiga) saía menor
 * que a fronteira congelada do GRANT: o candidato liderava a partir do rótulo menor, enquanto um terceiro
 * nó já tinha puxado a faixa acima dele do op-log do interino e podia depois pular essas operações em
 * silêncio. Agora o GRANT espera, com prazo, o apply terminar; no prazo, o handback é abortado.
 */
class HandbackGrantAwaitsLocalAppliesTest {

    private static final String TOPIC = "map:catalog";
    private static final NodeId LOCAL = NodeId.of("mmm-interim");
    private static final NodeId CANDIDATE = NodeId.of("aaa-candidate");
    /** Janela de pedido do candidato; o interino espera no máximo a metade (1 s). */
    private static final Duration REQUEST_WINDOW = Duration.ofSeconds(2);

    private Path tempDir;
    private ScheduledExecutorService scheduler;
    private ScriptedTransport transport;
    private ClusterCoordinator coordinator;
    private ReplicationManager manager;
    private BlockingHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("handback-grant-awaits-applies");
        scheduler = Executors.newScheduledThreadPool(2);
        handler = new BlockingHandler();
        transport = new ScriptedTransport(new NodeInfo(LOCAL, "127.0.0.1", 1, Set.of(), 50), List.of());
        coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(5),
                        Duration.ofSeconds(60), 1, null).withPairMode(true),
                scheduler);
        manager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .strictConsistency(false)
                        .leaderLocalApply(true)
                        .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                        .operationTimeout(Duration.ofSeconds(10))
                        .affinityHandbackMode(true)
                        .handoverMaxDuration(Duration.ofSeconds(30))
                        .handoverRequestTimeout(REQUEST_WINDOW)
                        .dataDirectory(tempDir)
                        .build());
        manager.registerHandler(TOPIC, handler);
        manager.start();
        coordinator.start();
        awaitCondition(() -> coordinator.isLeader() && !manager.isLeaderSyncing(), 15_000,
                "o interino deve liderar com o drain-gate liberado");
    }

    @AfterEach
    void tearDown() {
        handler.release.countDown();
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
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void grantEsperaOApplyLocalEsaiComRotuloIgualAFronteira() throws Exception {
        manager.replicate(TOPIC, bytes("a")).get(5, TimeUnit.SECONDS);
        CompletableFuture<ReplicationResult> blocked = replicateBlocked("b");
        requestHandback();

        // Com o apply da operação 2 preso, a produção congela e o GRANT não sai.
        assertTrue(manager.isHandoverFreezing(), "o pedido congela a produção");
        Thread.sleep(400);
        assertTrue(transport.sentOfType(MessageType.HANDBACK_GRANT).isEmpty(),
                "o GRANT espera o apply local das operações aceitas");

        handler.release.countDown();
        blocked.get(5, TimeUnit.SECONDS);
        ClusterMessage grant = awaitSent(MessageType.HANDBACK_GRANT, 5_000);
        assertEquals(Map.of(TOPIC, 2L), grant.payload(HandbackGrantPayload.class).frozenByTopic(),
                "a fronteira congelada inclui a operação aplicada");
        transport.clearSent();
        transport.deliver(ClusterMessage.request(MessageType.SYNC_REQUEST, "sync", CANDIDATE, LOCAL,
                new SyncRequestPayload(TOPIC, 0)));
        assertEquals(2L, awaitSent(MessageType.SYNC_RESPONSE, 5_000).payload(SyncResponsePayload.class).sequence(),
                "o rótulo do snapshot é igual à fronteira congelada do GRANT");
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void applyPresoAlemDoPrazoAbortaOHandbackERetomaAProducao() throws Exception {
        manager.replicate(TOPIC, bytes("a")).get(5, TimeUnit.SECONDS);
        CompletableFuture<ReplicationResult> blocked = replicateBlocked("b");
        requestHandback();

        ClusterMessage abort = awaitSent(MessageType.HANDBACK_ABORT, 5_000);
        assertEquals("pending local applies", abort.payload(HandbackAbortPayload.class).reason());
        assertTrue(transport.sentOfType(MessageType.HANDBACK_GRANT).isEmpty(), "nenhum GRANT é enviado");
        assertFalse(manager.isHandoverFreezing(), "o abort descongela a produção");

        handler.release.countDown();
        blocked.get(5, TimeUnit.SECONDS);
        manager.replicate(TOPIC, bytes("c")).get(5, TimeUnit.SECONDS);
        assertTrue(coordinator.isLeader(), "o interino mantém a liderança e volta a produzir");
    }

    // ---- apoio ----

    private CompletableFuture<ReplicationResult> replicateBlocked(String value) throws InterruptedException {
        handler.blockNext = true;
        CompletableFuture<ReplicationResult> future = manager.replicate(TOPIC, bytes(value));
        awaitCondition(() -> handler.applying, 5_000, "o apply assíncrono deve começar e travar");
        return future;
    }

    private void requestHandback() throws InterruptedException {
        transport.connect(new NodeInfo(CANDIDATE, "127.0.0.1", 2, Set.of(), 100));
        for (int i = 0; i < 5; i++) {
            transport.deliver(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", CANDIDATE, null,
                    HeartbeatPayload.now(0L, 0L, false)));
            Thread.sleep(50);
        }
        assertTrue(coordinator.isLeader(), "o candidato atrasado não toma a liderança");
        transport.clearSent();
        transport.deliver(ClusterMessage.request(MessageType.HANDBACK_REQUEST, "handback", CANDIDATE, LOCAL,
                new HandbackRequestPayload(CANDIDATE, 0L, 2L)));
        awaitCondition(manager::isHandoverFreezing, 5_000, "o pedido de handback deve congelar a produção");
    }

    private ClusterMessage awaitSent(MessageType type, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
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

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    /** Handler cujo próximo apply pode ser travado até a liberação. */
    private static final class BlockingHandler implements ReplicationHandler {
        final CountDownLatch release = new CountDownLatch(1);
        volatile boolean blockNext;
        volatile boolean applying;

        @Override
        public void apply(UUID operationId, Object payload) throws Exception {
            if (blockNext) {
                blockNext = false;
                applying = true;
                release.await();
            }
        }

        @Override
        public Object getSnapshot() {
            return new byte[] {1};
        }
    }
}
