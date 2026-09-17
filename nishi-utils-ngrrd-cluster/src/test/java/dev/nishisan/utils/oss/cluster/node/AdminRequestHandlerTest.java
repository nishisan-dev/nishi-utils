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

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridCluster;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.metrics.BlobVolumeSummary;
import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.protocol.AdminNodeRequest;
import dev.nishisan.utils.oss.cluster.protocol.AdminRebalanceResponse;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
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
    private CatalogService catalog;
    private LeaderViewFake leaderView;
    private RecordingRpc rpc;
    private NodeMetricsSnapshot localSnapshot;
    private AdminRequestHandler handler;
    private MigrationCoordinator coordinator;
    private Rebalancer rebalancer;

    @BeforeEach
    void setUp() throws Exception {
        cluster = NGrid.local(1)
                .map(CatalogService.CATALOG_MAP)
                .map(CatalogService.NODES_MAP)
                .start();
        NGridNode node = cluster.node(0);
        catalog = CatalogService.from(node);
        leaderView = new LeaderViewFake();
        rpc = new RecordingRpc();
        localSnapshot = fixedSnapshot(SELF.value());
        coordinator = new MigrationCoordinator(catalog, rpc, leaderView, 2, Duration.ofMillis(10),
                Duration.ofSeconds(5), Clock.systemUTC());
        rebalancer = new Rebalancer(catalog, leaderView, coordinator, new RebalanceSettings(50L, 0.10, 50), false,
                Duration.ofSeconds(60), Duration.ofSeconds(5), Clock.systemUTC());
        handler = new AdminRequestHandler(node.transport(), SELF, leaderView, catalog, () -> localSnapshot, rpc,
                rebalancer);
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
                new BlobVolumeSummary(1, 100L, 1_000L, 0.1, 5, 0L), 0L, 0L);
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
}
