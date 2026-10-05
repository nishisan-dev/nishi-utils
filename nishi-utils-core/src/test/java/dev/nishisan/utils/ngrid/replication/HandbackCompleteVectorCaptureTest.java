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
import dev.nishisan.utils.ngrid.cluster.coordination.LeadershipListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.HandbackCompletePayload;
import dev.nishisan.utils.ngrid.common.HandbackGrantPayload;
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 8.10.1 — o vetor de cutover do {@code HANDBACK_COMPLETE} é capturado ANTES de o candidato assumir a
 * liderança.
 *
 * <p>Os listeners de liderança rodam de forma síncrona dentro de {@code assumeLeadershipForHandback}, e a
 * aplicação volta a escrever no instante da promoção. Se o vetor fosse lido depois, ele incluiria uma
 * escrita que o incumbente rebaixado nunca recebeu; o incumbente faria SET da fronteira acima dela e a
 * perderia em silêncio. Aqui um listener registrado depois do {@link ReplicationManager} faz essa escrita
 * de forma determinística, no meio da promoção.
 */
class HandbackCompleteVectorCaptureTest {

    private static final String TOPIC = "map:catalog";
    private static final NodeId CANDIDATE = NodeId.of("zzz-candidate");
    private static final NodeId INTERIM = NodeId.of("aaa-interim");
    private static final long WATERMARK = 100L;
    private static final long INTERIM_EPOCH = 3L;

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("handback-complete-capture");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void escritaFeitaNaPromocaoNaoEntraNoVetorDeCutoverEnviadoAoIncumbente() throws Exception {
        ScriptedTransport transport = new ScriptedTransport(new NodeInfo(CANDIDATE, "127.0.0.1", 1, Set.of(), 100),
                List.of(new NodeInfo(INTERIM, "127.0.0.1", 2, Set.of(), 50)));
        ClusterCoordinator coordinator = new ClusterCoordinator(transport,
                ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(5),
                        Duration.ofSeconds(60), 2, null),
                scheduler);
        ReplicationManager manager = new ReplicationManager(transport, coordinator,
                ReplicationConfig.builder(1)
                        .strictConsistency(false)
                        .leaderLocalApply(false)
                        .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                        .operationTimeout(Duration.ofSeconds(5))
                        .affinityHandbackMode(true)
                        .handoverRequestTimeout(Duration.ofSeconds(20))
                        .handoverSnapshotTimeout(Duration.ofSeconds(20))
                        .dataDirectory(tempDir)
                        .build());
        AtomicReference<Throwable> writeFailure = new AtomicReference<>();
        try {
            manager.registerHandler(TOPIC, new ReplicationHandler() {
                @Override
                public void apply(java.util.UUID operationId, Object payload) {
                }
            });
            manager.start();
            coordinator.start();
            // A aplicação: escreve assim que o nó local vira líder (síncrono, dentro da promoção).
            coordinator.addLeadershipListener(new LeadershipListener() {
                @Override
                public void onLeaderChanged(NodeId newLeader) {
                    if (!CANDIDATE.equals(newLeader)) {
                        return;
                    }
                    try {
                        long deadline = System.currentTimeMillis() + 5_000;
                        while (manager.isLeaderSyncing() && System.currentTimeMillis() < deadline) {
                            Thread.sleep(10);
                        }
                        manager.replicate(TOPIC, "first-write".getBytes(StandardCharsets.UTF_8))
                                .get(5, TimeUnit.SECONDS);
                    } catch (Throwable t) {
                        writeFailure.set(t);
                    }
                }
            });

            // O interino serve a liderança, à frente do candidato (que segue como seguidor até o handback).
            awaitCondition(() -> {
                interimHeartbeat(transport);
                return INTERIM.equals(coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null))
                        && coordinator.isAgreedLeaderHealthy();
            }, 10_000, "o candidato deve seguir o interino");

            // O candidato pede o handback; o interino concede.
            awaitCondition(() -> {
                interimHeartbeat(transport);
                return !transport.sentOfType(MessageType.HANDBACK_REQUEST).isEmpty();
            }, 15_000, "o candidato deve pedir o handback");
            transport.deliver(ClusterMessage.request(MessageType.HANDBACK_GRANT, "handback", INTERIM, CANDIDATE,
                    new HandbackGrantPayload(TOPIC, WATERMARK, INTERIM_EPOCH, Map.of(TOPIC, WATERMARK))));

            // O candidato pede o snapshot e instala o rótulo W; o cutover o promove.
            awaitCondition(() -> !transport.sentOfType(MessageType.SYNC_REQUEST).isEmpty(), 10_000,
                    "o candidato deve pedir o snapshot ao interino");
            transport.deliver(ScriptedTransport.syncResponse(transport.sentOfType(MessageType.SYNC_REQUEST), INTERIM,
                    new SyncResponsePayload(TOPIC, WATERMARK, new byte[0])));

            awaitCondition(() -> !transport.sentOfType(MessageType.HANDBACK_COMPLETE).isEmpty(), 15_000,
                    "o candidato deve concluir o handback");
            HandbackCompletePayload complete = transport.sentOfType(MessageType.HANDBACK_COMPLETE).get(0)
                    .payload(HandbackCompletePayload.class);
            assertNull(writeFailure.get(), "a escrita da aplicação na promoção deve ser aceita");
            assertEquals(WATERMARK + 1L, manager.getTopicReplicationStatuses().get(TOPIC).nextExpectedSequence() - 1L,
                    "a escrita feita na promoção avançou a fronteira do novo líder");
            assertEquals(Map.of(TOPIC, WATERMARK), complete.cutoverByTopic(),
                    "o vetor de cutover é o do snapshot instalado, sem a escrita feita durante a promoção");
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
    }

    private static void interimHeartbeat(ScriptedTransport transport) {
        transport.deliver(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", INTERIM, null,
                HeartbeatPayload.now(WATERMARK, INTERIM_EPOCH, true, Map.of(TOPIC, WATERMARK))));
    }

    private static void awaitCondition(BooleanSupplier condition, long timeoutMs, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        fail(message);
    }
}
