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

package dev.nishisan.utils.oss.cluster.admin;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridCluster;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.node.PlacementRequestHandler;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.MigrateControlRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateResponse;
import dev.nishisan.utils.oss.cluster.protocol.MigrateStatus;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator;
import dev.nishisan.utils.oss.cluster.rebalance.RebalanceSettings;
import dev.nishisan.utils.oss.cluster.rebalance.Rebalancer;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Cobre {@link AdminService} sem cluster real: {@link CatalogService} continua real sobre
 * {@link NGrid#local(int)} (mesmo padrão de {@code AdminRequestHandlerTest}/{@code RebalancerTest}), e
 * um {@link ClusterRpc} fake resolve o protocolo de migração instantaneamente — o suficiente para
 * observar que {@code drain()} de fato dispara um ciclo do {@link Rebalancer} que move a série para
 * outro nó {@code ACTIVE}.
 */
@Timeout(30)
class AdminServiceTest {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10);

    private NGridCluster cluster;
    private CatalogService catalog;
    private LeaderViewFake leaderView;
    private MigrationCoordinator coordinator;
    private Rebalancer rebalancer;
    private AdminService adminService;

    @BeforeEach
    void setUp() throws Exception {
        cluster = NGrid.local(1)
                .map(CatalogService.CATALOG_MAP)
                .map(CatalogService.NODES_MAP)
                .start();
        NGridNode node = cluster.node(0);
        catalog = CatalogService.from(node);
        leaderView = new LeaderViewFake();
        leaderView.leader = true;
        leaderView.reachable.add("storage-a");
        leaderView.reachable.add("storage-b");
        coordinator = new MigrationCoordinator(catalog, new AutoResolvingMigrationRpc(), leaderView, 2,
                Duration.ofMillis(10), Duration.ofSeconds(5), Clock.systemUTC());
        rebalancer = new Rebalancer(catalog, leaderView, coordinator, new RebalanceSettings(1L, 0.0, 50), false,
                Duration.ofSeconds(60), Duration.ofSeconds(5), Clock.systemUTC());
        adminService = new AdminService(catalog, rebalancer, Clock.systemUTC());
    }

    @AfterEach
    void tearDown() throws Exception {
        rebalancer.close();
        coordinator.close();
        cluster.close();
    }

    @Test
    void drainDeNoDesconhecidoLancaIllegalArgumentException() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> adminService.drain("storage-fantasma"));
        assertTrue(e.getMessage().contains("storage-fantasma"));
    }

    @Test
    void activateDeNoDesconhecidoLancaIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> adminService.activate("storage-fantasma"));
    }

    @Test
    void drainMarcaNoComoDraining() {
        // Sem série alguma no nó, o ciclo de rebalanceamento disparado pelo drain() já o promoveria
        // direto a DRAINED (nada a esvaziar) — este teste cobre especificamente a transição para
        // DRAINING em si, então mantém ao menos uma série ACTIVE nele.
        catalog.putNodeStatus(new StorageNodeStatus("storage-a", NodeState.ACTIVE, 1, 0, 0, 1_000L));
        catalog.putPlacement("series-0", SeriesPlacement.active("storage-a", 1_000L));

        StorageNodeStatus result = adminService.drain("storage-a");

        assertEquals(NodeState.DRAINING, result.state());
        assertEquals(NodeState.DRAINING, catalog.nodeStatusLocal("storage-a").orElseThrow().state());
    }

    @Test
    void activateMarcaNoComoActive() {
        catalog.putNodeStatus(new StorageNodeStatus("storage-a", NodeState.DRAINING, 0, 0, 0, 1_000L));

        StorageNodeStatus result = adminService.activate("storage-a");

        assertEquals(NodeState.ACTIVE, result.state());
        assertEquals(NodeState.ACTIVE, catalog.nodeStatusLocal("storage-a").orElseThrow().state());
    }

    @Test
    void drainEIdempotenteSobreUmNoJaDraining() {
        catalog.putNodeStatus(new StorageNodeStatus("storage-a", NodeState.DRAINING, 0, 0, 0, 1_000L));

        StorageNodeStatus first = adminService.drain("storage-a");
        StorageNodeStatus second = adminService.drain("storage-a");

        assertEquals(NodeState.DRAINING, first.state());
        assertEquals(NodeState.DRAINING, second.state());
    }

    @Test
    void activateEIdempotenteSobreUmNoJaActive() {
        catalog.putNodeStatus(new StorageNodeStatus("storage-a", NodeState.ACTIVE, 0, 0, 0, 1_000L));

        StorageNodeStatus first = adminService.activate("storage-a");
        StorageNodeStatus second = adminService.activate("storage-a");

        assertEquals(NodeState.ACTIVE, first.state());
        assertEquals(NodeState.ACTIVE, second.state());
    }

    @Test
    void drainDisparaUmCicloDeRebalanceamentoQueEsvaziaONo() throws InterruptedException {
        catalog.putNodeStatus(new StorageNodeStatus("storage-a", NodeState.ACTIVE, 1, 0, 0, 1_000L));
        catalog.putNodeStatus(new StorageNodeStatus("storage-b", NodeState.ACTIVE, 0, 0, 0, 1_000L));
        catalog.putPlacement("series-0", SeriesPlacement.active("storage-a", 1_000L));

        adminService.drain("storage-a");

        awaitTrue("série migrada para storage-b e storage-a promovido a DRAINED", () -> {
            Optional<SeriesPlacement> placement = catalog.placementLocal("series-0");
            Optional<StorageNodeStatus> statusA = catalog.nodeStatusLocal("storage-a");
            return placement.isPresent() && "storage-b".equals(placement.get().ownerNodeId())
                    && statusA.isPresent() && statusA.get().state() == NodeState.DRAINED;
        });
    }

    private static void awaitTrue(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + AWAIT_TIMEOUT.toMillis();
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

    /** {@link PlacementRequestHandler.LeaderView} fake, sem {@code ClusterCoordinator}/{@code Transport} reais. */
    private static final class LeaderViewFake implements PlacementRequestHandler.LeaderView {
        private boolean leader;
        private final Set<String> reachable = ConcurrentHashMap.newKeySet();

        @Override
        public boolean isLeader() {
            return leader;
        }

        @Override
        public Optional<String> leaderId() {
            return Optional.empty();
        }

        @Override
        public Set<String> reachableNodeIds() {
            return Set.copyOf(reachable);
        }
    }

    /**
     * {@link ClusterRpc} fake: resolve {@code MIGRATE_START}/{@code MIGRATE_STATUS}/{@code MIGRATE_FINISH}/
     * {@code MIGRATE_ABORT} imediatamente com sucesso — não move bytes de verdade (não há storage nodes
     * reais neste teste), só permite observar que {@link MigrationCoordinator} completa o fluxo e flipa o
     * catálogo, que é o que {@link AdminServiceTest#drainDisparaUmCicloDeRebalanceamentoQueEsvaziaONo}
     * observa.
     */
    private static final class AutoResolvingMigrationRpc implements ClusterRpc {

        @Override
        @SuppressWarnings("unchecked")
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
            return (R) switch (command) {
                case Commands.MIGRATE_START -> MigrateResponse.of(MigrateStatus.OK, null);
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
            return NodeId.of("test-admin-service");
        }

        @Override
        public Optional<NodeId> leaderId() {
            return Optional.empty();
        }
    }
}
