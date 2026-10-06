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
import dev.nishisan.utils.ngrid.common.HeartbeatPayload;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Gate de prontidão (8.11.2): enquanto o dono do manager ainda registra handlers
 * ({@code deferHandlersReady} até {@code markHandlersReady}) o nó não anuncia fronteira, não é
 * elegível e não pede handback — mesmo sendo o de maior afinidade e com um incumbente servindo. No
 * incidente do CTP o REQUEST saiu com o catálogo ainda carregando do disco.
 */
class HandlersReadyGateTest {

    private static final String TOPIC = "map:_ngrid-queue-offsets";
    private static final NodeId CANDIDATE = NodeId.of("storage-217");
    private static final NodeId INTERIM = NodeId.of("storage-079");

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("handlers-ready-gate");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void candidatoNaoPedeHandbackNemFicaElegivelAntesDeTodosOsHandlersRegistrarem() throws Exception {
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
                        .dataDirectory(tempDir)
                        .build());
        try {
            manager.registerHandler(TOPIC, new ReplicationHandler() {
                @Override
                public void apply(UUID operationId, Object payload) {
                }
            });
            manager.deferHandlersReady(); // o dono ainda tem mapas por registrar
            manager.start();
            coordinator.start();

            awaitCondition(() -> {
                interimHeartbeat(transport);
                return INTERIM.equals(coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null))
                        && coordinator.isAgreedLeaderHealthy();
            }, 10_000, "o candidato deve seguir o interino");

            long deadline = System.currentTimeMillis() + 1_500;
            while (System.currentTimeMillis() < deadline) {
                interimHeartbeat(transport);
                assertTrue(transport.sentOfType(MessageType.HANDBACK_REQUEST).isEmpty(),
                        "sem todos os handlers registrados o candidato não pede handback");
                assertFalse(manager.isLeadershipEligible(), "não elegível enquanto os handlers não estão prontos");
                assertEquals(-1L, manager.getAdvertisedHighWatermark(), "não anuncia fronteira");
                assertFalse(coordinator.isLeader());
                Thread.sleep(50);
            }

            manager.markHandlersReady();
            assertTrue(manager.isLeadershipEligible(), "pronto: elegível de novo");
            awaitCondition(() -> {
                interimHeartbeat(transport);
                return !transport.sentOfType(MessageType.HANDBACK_REQUEST).isEmpty();
            }, 15_000, "com os handlers prontos o candidato pede o handback");
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

    /**
     * Guarda do GRANT: mesmo que o REQUEST tenha saído pronto, um GRANT recebido com o gate rearmado não
     * inicia instalação alguma — é abortado com a razão explícita. (Em produção o gate não volta a
     * engajar; a guarda é defesa em profundidade contra um GRANT que chegue com o conjunto parcial.)
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void grantRecebidoComHandlersNaoProntosEAbortadoSemInstalarNada() throws Exception {
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
                        .dataDirectory(tempDir)
                        .build());
        try {
            manager.registerHandler(TOPIC, new ReplicationHandler() {
                @Override
                public void apply(UUID operationId, Object payload) {
                }

                @Override
                public Object getSnapshot() {
                    return new byte[] {1};
                }
            });
            manager.start();
            coordinator.start();
            awaitCondition(() -> {
                interimHeartbeat(transport);
                return !transport.sentOfType(MessageType.HANDBACK_REQUEST).isEmpty();
            }, 15_000, "o candidato pronto pede o handback");

            manager.deferHandlersReady(); // o gate rearma antes de o GRANT chegar
            transport.deliver(ClusterMessage.request(MessageType.HANDBACK_GRANT, "handback", INTERIM, CANDIDATE,
                    new HandbackGrantPayload(TOPIC, 10L, 3L, Map.of(TOPIC, 10L))));
            awaitCondition(() -> !transport.sentOfType(MessageType.HANDBACK_ABORT).isEmpty(), 5_000,
                    "o GRANT com handlers não prontos deve ser abortado");
            HandbackAbortPayload abort = transport.sentOfType(MessageType.HANDBACK_ABORT).get(0)
                    .payload(HandbackAbortPayload.class);
            assertTrue(abort.reason().contains("not ready"), "razão: " + abort.reason());
            assertTrue(transport.sentOfType(MessageType.SYNC_REQUEST).isEmpty(), "nenhum snapshot é pedido");
            assertTrue(transport.sentOfType(MessageType.HANDBACK_COMPLETE).isEmpty(), "nenhum COMPLETE");
            assertFalse(coordinator.isLeader());
            awaitCondition(() -> !manager.isHandbackInProgress(), 5_000, "o papel é liberado no abort");
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
                HeartbeatPayload.now(10L, 3L, true, Map.of(TOPIC, 10L))));
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
