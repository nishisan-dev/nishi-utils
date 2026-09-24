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

package dev.nishisan.utils.oss.cluster.node;

import java.nio.file.Path;
import java.nio.file.Files;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.format.SeriesGeometry;
import dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridCluster;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.oss.cluster.admin.AdminService;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.metrics.BlobVolumeSummary;
import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.protocol.AdminNodeRequest;
import dev.nishisan.utils.oss.cluster.protocol.AdminNodeStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.AdminRebalanceResponse;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.MigrateControlRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateResponse;
import dev.nishisan.utils.oss.cluster.protocol.MigrateStatus;
import dev.nishisan.utils.oss.cluster.protocol.NodeStatusView;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator;
import dev.nishisan.utils.oss.cluster.rebalance.RebalanceSettings;
import dev.nishisan.utils.oss.cluster.rebalance.Rebalancer;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link AdminRequestHandler} sem cluster real: {@link LeaderViewFake} substitui
 * {@code ClusterCoordinator}/{@code Transport} (mesmo padrão de {@code PlacementRequestHandlerTest}),
 * {@link CatalogService} continua real sobre {@link NGrid#local(int)}, e {@link RecordingRpc} grava/
 * simula o encaminhamento de {@code ngrrd.admin.metrics} para outro nó.
 */
class AdminRequestHandlerTest {

    private static final NodeId SELF = NodeId.of("storage-self");
    private static final NodeId CLIENT = NodeId.of("client-1");

    private NGridCluster cluster;
    private NGridNode node;
    private CatalogService catalog;
    private String geometryId;
    private LeaderViewFake leaderView;
    private RecordingRpc rpc;
    private NodeMetricsSnapshot localSnapshot;
    private AdminRequestHandler handler;
    private MigrationCoordinator coordinator;
    private Rebalancer rebalancer;
    private AdminService adminService;

    @BeforeEach
    void setUp() throws Exception {
        cluster = NGrid.local(1)
                .map(CatalogService.CATALOG_MAP)
                .map(CatalogService.NODES_MAP)
                .map(CatalogService.GEOMETRIES_MAP)
                .start();
        node = cluster.node(0);
        catalog = CatalogService.from(node);
        var geometry = GeometryDescriptor.from(
                new SeriesGeometry(NgrrdYamlLoader.parse(
                        Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml")),
                        ignored -> null)));
        catalog.putGeometry(geometry);
        geometryId = geometry.id();
        leaderView = new LeaderViewFake();
        rpc = new RecordingRpc();
        localSnapshot = fixedSnapshot(SELF.value());
        coordinator = new MigrationCoordinator(catalog, rpc, leaderView, 2, Duration.ofMillis(10),
                Duration.ofSeconds(5), Clock.systemUTC());
        rebalancer = new Rebalancer(catalog, leaderView, coordinator, new RebalanceSettings(50L, 0.10, 50), false,
                Duration.ofSeconds(60), Duration.ofSeconds(5), Clock.systemUTC());
        adminService = new AdminService(catalog, rebalancer, Clock.systemUTC());
        handler = new AdminRequestHandler(node.transport(), SELF, leaderView, catalog, () -> localSnapshot, rpc,
                rebalancer, adminService, coordinator);
    }

    @AfterEach
    void tearDown() throws Exception {
        rebalancer.close();
        coordinator.close();
        cluster.close();
    }

    private static NodeMetricsSnapshot fixedSnapshot(String nodeId) {
        return new NodeMetricsSnapshot(nodeId, 1_000L, true, 5L, 100L, 1_000L, 2, 3L, 30L, 0L, 1L, 0L, 4L,
                LatencySnapshot.EMPTY, LatencySnapshot.EMPTY, LatencySnapshot.EMPTY, Map.of(),
                new BlobVolumeSummary(1, 100L, 1_000L, 0.1, 5, 0L), 0L, 0L, 0L, 0L, 0L, 0L, 0L);
    }

    @Test
    void statusForaDoLiderRespondeNotLeaderComOIdDoLiderConhecido() {
        leaderView.leader = false;
        leaderView.leaderId = Optional.of("storage-b");

        AdminStatusResponse response = (AdminStatusResponse) handler.handle(Commands.ADMIN_STATUS, null, CLIENT);

        assertEquals(SeriesStatus.NOT_LEADER, response.status());
        assertEquals("storage-b", response.leaderNodeId());
        assertTrue(response.nodes().isEmpty());
    }

    @Test
    void statusNoLiderMarcaNoInalcancavelEAgregaContagemDeSeries() {
        leaderView.leader = true;
        leaderView.reachable.add("storage-a");
        // "storage-b" não entra em reachable -> visto como caído.
        catalog.putNodeStatus(new StorageNodeStatus("storage-a", NodeState.ACTIVE, 2, 0, 0, 1_000L));
        catalog.putNodeStatus(new StorageNodeStatus("storage-b", NodeState.ACTIVE, 1, 0, 0, 1_000L));
        catalog.putPlacement("series-1", SeriesPlacement.active("storage-a", 1_000L));
        catalog.putPlacement("series-2", SeriesPlacement.active("storage-a", 1_000L));
        catalog.putPlacement("series-3", SeriesPlacement.active("storage-b", 1_000L));

        AdminStatusResponse response = (AdminStatusResponse) handler.handle(Commands.ADMIN_STATUS, null, CLIENT);

        assertEquals(SeriesStatus.OK, response.status());
        assertEquals(SELF.value(), response.leaderNodeId());
        Map<String, NodeStatusView> byNode = response.nodes().stream()
                .collect(Collectors.toMap(view -> view.status().nodeId(), view -> view));
        assertTrue(byNode.get("storage-a").reachable());
        assertFalse(byNode.get("storage-b").reachable());
        assertEquals(Map.of("storage-a", 2L, "storage-b", 1L), response.seriesCountByNode());
        assertEquals(3, response.geometriesPending());
        assertEquals(0, response.migrationsInFlight(), "MÉDIO-5: em repouso, nenhuma migração ativa no coordenador");
    }

    @Test
    void statusReportaMigracoesEmCursoSegundoOCoordinatorReal() throws Exception {
        // MÉDIO-5 do Refuter: migrationsInFlight vem de coordinator.activeMigrationCount(), não mais
        // hardcoded — prende 2 migrações reais em MIGRATE_START (via BlockingMigrationRpc) e confirma
        // que o status as reporta enquanto estão em curso.
        leaderView.leader = true;
        catalog.putNodeStatus(new StorageNodeStatus("storage-a", NodeState.ACTIVE, 2, 0, 0, 1_000L));
        catalog.putNodeStatus(new StorageNodeStatus("storage-b", NodeState.ACTIVE, 0, 0, 0, 1_000L));
        catalog.putPlacement("series-x", SeriesPlacement.active("storage-a", 1_000L)
                .withGeometry(geometryId, true, 1_000L));
        catalog.putPlacement("series-y", SeriesPlacement.active("storage-a", 1_000L)
                .withGeometry(geometryId, true, 1_000L));

        CountDownLatch releaseStart = new CountDownLatch(1);
        BlockingMigrationRpc migrationRpc = new BlockingMigrationRpc(releaseStart);
        MigrationCoordinator busyCoordinator = new MigrationCoordinator(catalog, migrationRpc, leaderView, 2,
                Duration.ofMillis(20), Duration.ofSeconds(10), Clock.systemUTC());
        AdminRequestHandler busyHandler = new AdminRequestHandler(node.transport(), SELF, leaderView, catalog,
                () -> localSnapshot, rpc, rebalancer, adminService, busyCoordinator);
        try {
            busyCoordinator.migrate("series-x", "storage-a", "storage-b");
            busyCoordinator.migrate("series-y", "storage-a", "storage-b");
            awaitTrue("as duas migrações deveriam ter chamado MIGRATE_START", () -> migrationRpc.startCalls.get() >= 2);

            AdminStatusResponse response = (AdminStatusResponse) busyHandler.handle(Commands.ADMIN_STATUS, null, CLIENT);
            assertEquals(2, response.migrationsInFlight());
        } finally {
            releaseStart.countDown();
            awaitTrue("as migrações deveriam resolver após liberar o latch", () -> busyCoordinator.activeMigrationCount() == 0);
            busyCoordinator.close();
        }
    }

    private static void awaitTrue(String description, BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20L);
        }
        if (!condition.getAsBoolean()) {
            fail("Condição não satisfeita a tempo: " + description);
        }
    }

    @Test
    void metricsSemNodeIdDevolveSnapshotLocal() {
        NodeMetricsSnapshot response = (NodeMetricsSnapshot) handler.handle(Commands.ADMIN_METRICS,
                new AdminNodeRequest(null, false), CLIENT);

        assertSame(localSnapshot, response);
        assertTrue(rpc.calls.isEmpty(), "não deveria encaminhar quando o pedido já é para este nó");
    }

    @Test
    void metricsComNodeIdIgualASelfDevolveSnapshotLocal() {
        NodeMetricsSnapshot response = (NodeMetricsSnapshot) handler.handle(Commands.ADMIN_METRICS,
                new AdminNodeRequest(SELF.value(), false), CLIENT);

        assertSame(localSnapshot, response);
        assertTrue(rpc.calls.isEmpty());
    }

    @Test
    void metricsDeOutroNoEncaminhaUmaVezMarcandoForwarded() {
        NodeMetricsSnapshot remote = fixedSnapshot("storage-b");
        rpc.responseFor("storage-b", remote);

        NodeMetricsSnapshot response = (NodeMetricsSnapshot) handler.handle(Commands.ADMIN_METRICS,
                new AdminNodeRequest("storage-b", false), CLIENT);

        assertSame(remote, response);
        assertEquals(1, rpc.calls.size());
        RecordingRpc.Call call = rpc.calls.get(0);
        assertEquals(NodeId.of("storage-b"), call.target());
        assertEquals(Commands.ADMIN_METRICS, call.command());
        AdminNodeRequest forwardedRequest = (AdminNodeRequest) call.body();
        assertEquals("storage-b", forwardedRequest.nodeId());
        assertTrue(forwardedRequest.forwarded(), "o encaminhamento deve marcar forwarded=true");
    }

    @Test
    void metricsJaEncaminhadoNaoEncadeiaOutroSalto() {
        assertThrows(IllegalStateException.class, () -> handler.handle(Commands.ADMIN_METRICS,
                new AdminNodeRequest("storage-c", true), CLIENT));
        assertTrue(rpc.calls.isEmpty(), "não deveria nem tentar um segundo encaminhamento");
    }

    @Test
    void rebalanceForaDoLiderRespondeNotLeaderComOIdDoLiderConhecido() {
        leaderView.leader = false;
        leaderView.leaderId = Optional.of("storage-b");

        AdminRebalanceResponse response =
                (AdminRebalanceResponse) handler.handle(Commands.ADMIN_REBALANCE, null, CLIENT);

        assertEquals(SeriesStatus.NOT_LEADER, response.status());
        assertEquals("storage-b", response.leaderNodeId());
    }

    @Test
    void rebalanceNoLiderDisparaCicloEDevolvePlanejadoEIniciado() {
        leaderView.leader = true;

        AdminRebalanceResponse response =
                (AdminRebalanceResponse) handler.handle(Commands.ADMIN_REBALANCE, null, CLIENT);

        assertEquals(SeriesStatus.OK, response.status());
        assertEquals(SELF.value(), response.leaderNodeId());
        // Catálogo vazio (nenhum StorageNodeStatus/placement registrado neste teste) -> nada a mover.
        assertEquals(0, response.planned());
        assertEquals(0, response.started());
    }

    @Test
    void drainForaDoLiderRespondeNotLeaderComOIdDoLiderConhecido() {
        leaderView.leader = false;
        leaderView.leaderId = Optional.of("storage-b");

        AdminNodeStatusResponse response =
                (AdminNodeStatusResponse) handler.handle(Commands.ADMIN_DRAIN, new AdminNodeRequest("storage-a", false), CLIENT);

        assertEquals(SeriesStatus.NOT_LEADER, response.status());
        assertEquals("storage-b", response.leaderNodeId());
        assertNull(response.nodeStatus());
    }

    @Test
    void drainNoLiderMarcaNoComoDrainingEDisparaRebalance() {
        leaderView.leader = true;
        // Nenhum outro nó ACTIVE alcançável para receber as séries: o ciclo de rebalanceamento
        // disparado pelo drain() não consegue mover nada (RebalancePlanner desiste sem destino), então
        // o nó permanece DRAINING — a promoção a DRAINED só acontece quando ele fica sem série alguma.
        catalog.putNodeStatus(new StorageNodeStatus("storage-a", NodeState.ACTIVE, 3, 0, 0, 1_000L));
        catalog.putPlacement("series-1", SeriesPlacement.active("storage-a", 1_000L));

        AdminNodeStatusResponse response =
                (AdminNodeStatusResponse) handler.handle(Commands.ADMIN_DRAIN, new AdminNodeRequest("storage-a", false), CLIENT);

        assertEquals(SeriesStatus.OK, response.status());
        assertEquals(NodeState.DRAINING, response.nodeStatus().state());
        assertEquals(NodeState.DRAINING, catalog.nodeStatusLocal("storage-a").orElseThrow().state());
    }

    @Test
    void drainDeNoDesconhecidoRespondeError() {
        leaderView.leader = true;

        AdminNodeStatusResponse response =
                (AdminNodeStatusResponse) handler.handle(Commands.ADMIN_DRAIN, new AdminNodeRequest("storage-x", false), CLIENT);

        assertEquals(SeriesStatus.ERROR, response.status());
        assertTrue(response.message().contains("storage-x"));
    }

    @Test
    void activateNoLiderMarcaNoComoActive() {
        leaderView.leader = true;
        catalog.putNodeStatus(new StorageNodeStatus("storage-a", NodeState.DRAINING, 0, 0, 0, 1_000L));

        AdminNodeStatusResponse response =
                (AdminNodeStatusResponse) handler.handle(Commands.ADMIN_ACTIVATE, new AdminNodeRequest("storage-a", false), CLIENT);

        assertEquals(SeriesStatus.OK, response.status());
        assertEquals(NodeState.ACTIVE, response.nodeStatus().state());
        assertEquals(NodeState.ACTIVE, catalog.nodeStatusLocal("storage-a").orElseThrow().state());
    }

    @Test
    void drainEIdempotenteSobreUmNoJaDraining() {
        leaderView.leader = true;
        catalog.putNodeStatus(new StorageNodeStatus("storage-a", NodeState.DRAINING, 0, 0, 0, 1_000L));

        AdminNodeStatusResponse response =
                (AdminNodeStatusResponse) handler.handle(Commands.ADMIN_DRAIN, new AdminNodeRequest("storage-a", false), CLIENT);

        assertEquals(SeriesStatus.OK, response.status());
        assertEquals(NodeState.DRAINING, response.nodeStatus().state());
    }

    /** {@link PlacementRequestHandler.LeaderView} fake, sem {@code ClusterCoordinator}/{@code Transport} reais. */
    private static final class LeaderViewFake implements PlacementRequestHandler.LeaderView {
        private boolean leader;
        private Optional<String> leaderId = Optional.empty();
        private final Set<String> reachable = ConcurrentHashMap.newKeySet();

        @Override
        public boolean isLeader() {
            return leader;
        }

        @Override
        public Optional<String> leaderId() {
            return leaderId;
        }

        @Override
        public Set<String> reachableNodeIds() {
            return Set.copyOf(reachable);
        }
    }

    /** {@link ClusterRpc} fake: grava toda chamada e devolve a resposta programada por {@link #responseFor}. */
    private static final class RecordingRpc implements ClusterRpc {
        record Call(NodeId target, String command, Object body) {
        }

        private final List<Call> calls = new CopyOnWriteArrayList<>();
        private final Map<String, Object> responsesByTargetId = new ConcurrentHashMap<>();

        void responseFor(String targetNodeId, Object response) {
            responsesByTargetId.put(targetNodeId, response);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
            calls.add(new Call(target, command, body));
            Object response = responsesByTargetId.get(target.value());
            if (response == null) {
                throw new IllegalStateException("nenhuma resposta programada para " + target);
            }
            return (R) response;
        }

        @Override
        public NodeId localId() {
            return SELF;
        }

        @Override
        public Optional<NodeId> leaderId() {
            return Optional.empty();
        }
    }

    /**
     * {@link ClusterRpc} fake: {@code MIGRATE_START} bloqueia em {@code releaseStart} até ser liberado
     * (contando cada chamada em {@code startCalls}); {@code MIGRATE_STATUS} confirma
     * {@code COMMITTED} de cara; {@code MIGRATE_FINISH}/{@code MIGRATE_ABORT} respondem OK — mesmo
     * padrão de {@code RebalancerTest.BlockingMigrationRpc}, usado aqui só para manter 2 migrações
     * reais em curso tempo suficiente para observar {@code activeMigrationCount()}.
     */
    private static final class BlockingMigrationRpc implements ClusterRpc {
        private final CountDownLatch releaseStart;
        private final AtomicInteger startCalls = new AtomicInteger();

        BlockingMigrationRpc(CountDownLatch releaseStart) {
            this.releaseStart = releaseStart;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
            return (R) switch (command) {
                case Commands.MIGRATE_START -> {
                    startCalls.incrementAndGet();
                    try {
                        if (!releaseStart.await(10, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("releaseStart nunca veio");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    yield MigrateResponse.of(MigrateStatus.OK, null);
                }
                case Commands.MIGRATE_STATUS -> new MigrateResponse(MigrateStatus.COMMITTED, null, 1L);
                case Commands.MIGRATE_FINISH, Commands.MIGRATE_ABORT -> MigrateResponse.of(MigrateStatus.OK, null);
                default -> throw new IllegalStateException("comando inesperado: " + command
                        + " (body=" + describe(body) + ")");
            };
        }

        private static String describe(Object body) {
            return body instanceof MigrateControlRequest request ? request.seriesKey() : String.valueOf(body);
        }

        @Override
        public NodeId localId() {
            return NodeId.of("test-admin-migrations");
        }

        @Override
        public Optional<NodeId> leaderId() {
            return Optional.empty();
        }
    }
}
