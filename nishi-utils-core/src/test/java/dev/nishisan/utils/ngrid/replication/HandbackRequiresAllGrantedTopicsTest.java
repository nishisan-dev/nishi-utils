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
import dev.nishisan.utils.ngrid.common.HandbackCompletePayload;
import dev.nishisan.utils.ngrid.common.HandbackGrantPayload;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Lado candidato do handback (8.11.2, incidente do CTP de 2026-10-05): o GRANT nomeia três tópicos,
 * mas o candidato — recém-reiniciado — só tem o handler de {@code _ngrid-queue-offsets}; os mapas do
 * catálogo ainda carregam do disco. Até a 8.11.1 o candidato instalava só o que tinha handler,
 * declarava o cutover e enviava ao incumbente, no vetor, as fronteiras de DISCO dos tópicos que não
 * instalou — e o incumbente renumerava {@code ngrrd.nodes} para baixo.
 *
 * <p>Agora o candidato espera o registro dos handlers exigidos (armados em {@code registerHandler}),
 * só conclui com todos instalados, envia só os tópicos instalados e mantém o papel "em andamento"
 * durante a promoção (um único REQUEST por tentativa). Se o prazo do snapshot estoura, aborta.
 */
class HandbackRequiresAllGrantedTopicsTest {

    private static final String OFFSETS = "map:_ngrid-queue-offsets";
    private static final String CATALOG = "map:ngrrd.catalog";
    private static final String NODES = "map:ngrrd.nodes";
    private static final long OFFSETS_W = 40L;
    private static final long CATALOG_W = 300L;
    private static final long NODES_W = 76L;
    /** Fronteira de disco do candidato para `nodes`: abaixo da congelada (os 4 ticks de status do incidente). */
    private static final long NODES_DISK = 72L;
    private static final Map<String, Long> FROZEN = Map.of(OFFSETS, OFFSETS_W, CATALOG, CATALOG_W, NODES, NODES_W);
    private static final NodeId CANDIDATE = NodeId.of("storage-217");
    private static final NodeId INTERIM = NodeId.of("storage-079");
    private static final long INTERIM_EPOCH = 26L;

    private Path tempDir;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("handback-all-topics");
        scheduler = Executors.newScheduledThreadPool(2);
    }

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void candidatoSoConcluiDepoisDeInstalarTodosOsTopicosDoGrant() throws Exception {
        Fixture f = new Fixture(Duration.ofSeconds(20));
        try {
            AtomicReference<Boolean> inProgressAtPromotion = new AtomicReference<>();
            f.coordinator.addLeadershipListener(newLeader -> {
                if (CANDIDATE.equals(newLeader)) {
                    // D: durante a promoção o handback ainda está em andamento (CANDIDATE_PROMOTING).
                    inProgressAtPromotion.set(f.manager.isHandbackInProgress());
                }
            });
            f.requestAndGrant();

            // Só o tópico com handler é pedido e instalado.
            f.awaitSyncRequest(OFFSETS);
            f.serveSnapshot(OFFSETS, OFFSETS_W);
            long deadline = System.currentTimeMillis() + 800;
            while (System.currentTimeMillis() < deadline) {
                f.interimHeartbeat();
                assertTrue(f.transport.sentOfType(MessageType.HANDBACK_COMPLETE).isEmpty(),
                        "o cutover não pode ser declarado com os tópicos do catálogo por instalar");
                assertFalse(f.coordinator.isLeader(), "o candidato não pode assumir com instalação parcial");
                assertTrue(f.manager.isHandbackInProgress(), "o handback segue em andamento, esperando os handlers");
                assertFalse(f.syncRequested(CATALOG) || f.syncRequested(NODES),
                        "sem handler não há pedido de snapshot do catálogo nem de nodes");
                Thread.sleep(50);
            }

            // Os handlers que faltavam registram (fim da carga do disco): cada um é armado e instalado.
            f.manager.registerHandler(CATALOG, new SnapshotHandler());
            f.manager.registerHandler(NODES, new SnapshotHandler());
            f.awaitSyncRequest(CATALOG);
            f.awaitSyncRequest(NODES);
            f.serveSnapshot(CATALOG, CATALOG_W);
            f.serveSnapshot(NODES, NODES_W);

            f.awaitCondition(() -> !f.transport.sentOfType(MessageType.HANDBACK_COMPLETE).isEmpty(), 15_000,
                    "o candidato deve concluir o handback depois do último tópico exigido");
            HandbackCompletePayload complete = f.transport.sentOfType(MessageType.HANDBACK_COMPLETE).get(0)
                    .payload(HandbackCompletePayload.class);
            assertEquals(FROZEN, complete.cutoverByTopic(),
                    "o vetor de cutover é exatamente o vetor congelado do GRANT: só tópicos instalados");
            assertTrue(f.coordinator.isLeader(), "com todos os tópicos instalados o candidato assume");
            assertEquals(NODES_W, frontier(f.manager, NODES),
                    "nodes foi instalado com o rótulo do incumbente, não com a fronteira de disco");
            assertEquals(1, f.transport.sentOfType(MessageType.HANDBACK_REQUEST).size(),
                    "um único HANDBACK_REQUEST por tentativa");
            assertNotNull(inProgressAtPromotion.get(), "a promoção deve ter sido observada");
            assertTrue(inProgressAtPromotion.get(),
                    "o papel do candidato não pode ser zerado antes da promoção (reenvio espúrio do REQUEST)");
            f.awaitCondition(() -> !f.manager.isHandbackInProgress(), 5_000,
                    "o papel é liberado depois da promoção e do COMPLETE");
        } finally {
            f.close();
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void prazoDoSnapshotEstouradoComTopicoExigidoSemHandlerAbortaEFicaSeguidor() throws Exception {
        Fixture f = new Fixture(Duration.ofSeconds(2));
        try {
            f.requestAndGrant();
            f.awaitSyncRequest(OFFSETS);
            f.serveSnapshot(OFFSETS, OFFSETS_W);

            // O handler do catálogo nunca registra: o prazo (handoverSnapshotTimeout) vence.
            f.awaitCondition(() -> !f.transport.sentOfType(MessageType.HANDBACK_ABORT).isEmpty(), 15_000,
                    "o candidato deve abortar ao estourar o prazo com tópicos exigidos por instalar");
            HandbackAbortPayload abort = f.transport.sentOfType(MessageType.HANDBACK_ABORT).get(0)
                    .payload(HandbackAbortPayload.class);
            assertEquals(INTERIM, f.transport.sentOfType(MessageType.HANDBACK_ABORT).get(0).destination());
            assertTrue(abort.reason().contains("timed out"), "razão do abort: " + abort.reason());
            assertTrue(f.transport.sentOfType(MessageType.HANDBACK_COMPLETE).isEmpty(),
                    "nenhum COMPLETE depois do abort");
            assertFalse(f.coordinator.isLeader(), "o candidato fica seguidor");
            f.awaitCondition(() -> !f.manager.isHandbackInProgress(), 5_000, "o papel é liberado no abort");
        } finally {
            f.close();
        }
    }

    // ---- apoio ----

    private static long frontier(ReplicationManager manager, String topic) {
        return manager.getTopicReplicationStatuses().get(topic).nextExpectedSequence() - 1L;
    }

    /** Candidato com estado de disco dos três tópicos e só o handler de offsets registrado. */
    private final class Fixture {
        final ScriptedTransport transport;
        final ClusterCoordinator coordinator;
        final ReplicationManager manager;

        Fixture(Duration snapshotTimeout) throws Exception {
            Map<String, Long> state = new HashMap<>();
            state.put(OFFSETS, OFFSETS_W - 10L + 1L);
            state.put(CATALOG, CATALOG_W + 1L);
            state.put(NODES, NODES_DISK + 1L);
            state.put("_topic:" + OFFSETS, OFFSETS_W - 10L);
            state.put("_topic:" + CATALOG, CATALOG_W);
            state.put("_topic:" + NODES, NODES_DISK);
            state.put("_global", CATALOG_W);
            writeSequenceState(state);

            transport = new ScriptedTransport(new NodeInfo(CANDIDATE, "127.0.0.1", 1, Set.of(), 100),
                    List.of(new NodeInfo(INTERIM, "127.0.0.1", 2, Set.of(), 50)));
            coordinator = new ClusterCoordinator(transport,
                    ClusterCoordinatorConfig.of(Duration.ofMillis(100), Duration.ofSeconds(5),
                            Duration.ofSeconds(60), 2, null),
                    scheduler);
            manager = new ReplicationManager(transport, coordinator,
                    ReplicationConfig.builder(1)
                            .strictConsistency(false)
                            .leaderLocalApply(false)
                            .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                            .operationTimeout(Duration.ofSeconds(5))
                            .affinityHandbackMode(true)
                            .handoverRequestTimeout(Duration.ofSeconds(20))
                            .handoverSnapshotTimeout(snapshotTimeout)
                            .handoverCooldown(Duration.ofSeconds(60))
                            .dataDirectory(tempDir)
                            .build());
            manager.registerHandler(OFFSETS, new SnapshotHandler());
            manager.start();
            coordinator.start();
        }

        /** O interino serve; o candidato (maior afinidade) pede o handback e recebe o GRANT com os três tópicos. */
        void requestAndGrant() throws InterruptedException {
            awaitCondition(() -> {
                interimHeartbeat();
                return INTERIM.equals(coordinator.leaderInfo().map(NodeInfo::nodeId).orElse(null))
                        && coordinator.isAgreedLeaderHealthy();
            }, 10_000, "o candidato deve seguir o interino");
            awaitCondition(() -> {
                interimHeartbeat();
                return !transport.sentOfType(MessageType.HANDBACK_REQUEST).isEmpty();
            }, 15_000, "o candidato deve pedir o handback");
            long frozenTotal = FROZEN.values().stream().mapToLong(Long::longValue).sum();
            transport.deliver(ClusterMessage.request(MessageType.HANDBACK_GRANT, "handback", INTERIM, CANDIDATE,
                    new HandbackGrantPayload(OFFSETS, frozenTotal, INTERIM_EPOCH, FROZEN)));
        }

        void interimHeartbeat() {
            long total = FROZEN.values().stream().mapToLong(Long::longValue).sum();
            transport.deliver(ClusterMessage.lightweight(MessageType.HEARTBEAT, "hb", INTERIM, null,
                    HeartbeatPayload.now(total, INTERIM_EPOCH, true, FROZEN)));
        }

        boolean syncRequested(String topic) {
            return transport.sentOfType(MessageType.SYNC_REQUEST).stream()
                    .anyMatch(m -> m.payload(SyncRequestPayload.class).topic().equals(topic));
        }

        void awaitSyncRequest(String topic) throws InterruptedException {
            awaitCondition(() -> {
                interimHeartbeat();
                return syncRequested(topic);
            }, 10_000, "o candidato deve pedir o snapshot de " + topic);
        }

        void serveSnapshot(String topic, long label) {
            transport.deliver(ScriptedTransport.syncResponse(transport.sentOfType(MessageType.SYNC_REQUEST), INTERIM,
                    new SyncResponsePayload(topic, label, new byte[0])));
        }

        void awaitCondition(BooleanSupplier condition, long timeoutMs, String message) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                interimHeartbeat();
                if (condition.getAsBoolean()) {
                    return;
                }
                Thread.sleep(50);
            }
            fail(message);
        }

        void close() {
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

    private void writeSequenceState(Map<String, Long> state) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(
                Files.newOutputStream(tempDir.resolve("sequence-state.dat")))) {
            oos.writeObject(new HashMap<>(state));
        }
    }

    /** Handler que aceita qualquer snapshot. */
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
