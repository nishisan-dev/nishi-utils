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

package dev.nishisan.utils.oss.cluster.rebalance;

import java.nio.file.Path;
import java.nio.file.Files;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.format.SeriesGeometry;
import dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridCluster;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogReplicaStatus;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.node.PlacementRequestHandler;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import dev.nishisan.utils.oss.cluster.placement.PlacementRule;
import dev.nishisan.utils.oss.cluster.placement.PlacementRules;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.MigrateControlRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateResponse;
import dev.nishisan.utils.oss.cluster.protocol.MigrateStartRequest;
import dev.nishisan.utils.oss.cluster.protocol.MigrateStatus;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Cobre {@link Rebalancer#triggerNow()} sem cluster real de storage nodes: {@link CatalogService}
 * continua real sobre {@link NGrid#local(int)} (mesmo padrão de {@code AdminRequestHandlerTest}), e um
 * {@link BlockingMigrationRpc} prende o {@code MIGRATE_START} num {@link CountDownLatch} controlado
 * pelo teste — é o que permite observar o estado de {@code running} enquanto uma migração disparada por
 * {@code triggerNow()} ainda está em curso.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RebalancerTest {

    private static final Duration AWAIT_TIMEOUT = Duration.ofSeconds(10);

    private NGridCluster cluster;
    private CatalogService catalog;
    private String geometryId;
    private LeaderViewFake leaderView;

    @BeforeEach
    void setUp() throws Exception {
        cluster = NGrid.local(1)
                .map(CatalogService.CATALOG_MAP)
                .map(CatalogService.NODES_MAP)
                .map(CatalogService.GEOMETRIES_MAP)
                .start();
        NGridNode node = cluster.node(0);
        catalog = CatalogService.from(node);
        var geometry = GeometryDescriptor.from(
                new SeriesGeometry(NgrrdYamlLoader.parse(
                        Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml")),
                        ignored -> null)));
        catalog.putGeometry(geometry);
        geometryId = geometry.id();
        leaderView = new LeaderViewFake();
        leaderView.leader = true;
        leaderView.reachable.add("storage-a");
        leaderView.reachable.add("storage-b");
    }

    @AfterEach
    void tearDown() throws Exception {
        cluster.close();
    }

    /**
     * Achado dos MÉDIOS do Refuter: {@code triggerNow()} liberava {@code running} assim que só
     * submetia as migrações — um segundo {@code ngrrd.admin.rebalance} podia montar um plano NOVO em
     * cima de migrações ainda em curso. Prende a única migração planejada em {@code MIGRATE_START}
     * (via {@link BlockingMigrationRpc}), confirma que um segundo {@code triggerNow()} nesse meio-tempo
     * não planeja nada (0/0, porque {@code running} continua {@code true}), libera a migração e
     * confirma que {@code running} volta a {@code false} (via reflexão) sem que a chamada de
     * {@code triggerNow()} em si tenha bloqueado esperando isso.
     */
    @Test
    void triggerNowNaoPlanejaNovoCicloEnquantoOAnteriorAindaNaoResolveu() throws Exception {
        catalog.putNodeStatus(new StorageNodeStatus("storage-a", NodeState.ACTIVE, 2, 0, 0, 1_000L));
        catalog.putNodeStatus(new StorageNodeStatus("storage-b", NodeState.ACTIVE, 0, 0, 0, 1_000L));
        catalog.putPlacement("series-0", SeriesPlacement.active("storage-a", 1_000L)
                .withGeometry(geometryId, true, 1_000L));
        catalog.putPlacement("series-1", SeriesPlacement.active("storage-a", 1_000L)
                .withGeometry(geometryId, true, 1_000L));

        CountDownLatch releaseStart = new CountDownLatch(1);
        AtomicInteger startCalls = new AtomicInteger();
        BlockingMigrationRpc rpc = new BlockingMigrationRpc(releaseStart, startCalls);
        MigrationCoordinator coordinator = new MigrationCoordinator(catalog, rpc, leaderView, 2,
                Duration.ofMillis(20), Duration.ofSeconds(5), Clock.systemUTC());
        Rebalancer rebalancer = new Rebalancer(catalog, leaderView, coordinator,
                new RebalanceSettings(1L, 0.0, 50), false, Duration.ofSeconds(60), Duration.ofSeconds(5),
                Clock.systemUTC());
        try {
            Rebalancer.TriggerResult first = rebalancer.triggerNow();
            assertEquals(1, first.planned(), "2 séries, 1 vazio, minDelta=1 -> exatamente 1 movimento");
            assertEquals(1, first.started());
            assertTrue(runningField(rebalancer), "running deveria continuar true com a migração em curso");

            awaitTrue("MIGRATE_START chamado (preso no latch)", () -> startCalls.get() >= 1);

            Rebalancer.TriggerResult second = rebalancer.triggerNow();
            assertEquals(0, second.planned(), "não deveria planejar um ciclo novo com o anterior em curso");
            assertEquals(0, second.started());

            releaseStart.countDown();

            awaitTrue("running libera depois que a migração resolve", () -> !runningField(rebalancer));
            assertTrue(rpc.callsTo(Commands.MIGRATE_FINISH) >= 1, "a migração destravada deveria ter completado");
        } finally {
            rebalancer.close();
            coordinator.close();
        }
    }

    /**
     * Achado do Refuter (BAIXO): {@code running} só era liberado no caminho feliz — uma exceção
     * síncrona de {@code coordinator.migrate()} (aqui, {@link RejectedExecutionException} do pool de
     * migração já fechado, simulando um {@code node.close()} correndo ao mesmo tempo de um {@code
     * ngrrd.admin.rebalance}) escapava {@code triggerNow()} sem passar pelo {@code running.set(false)},
     * travando todo ciclo futuro.
     */
    @Test
    void triggerNowLiberaRunningMesmoSeMigrateLancarAoSubmeter() {
        catalog.putNodeStatus(new StorageNodeStatus("storage-a", NodeState.ACTIVE, 2, 0, 0, 1_000L));
        catalog.putNodeStatus(new StorageNodeStatus("storage-b", NodeState.ACTIVE, 0, 0, 0, 1_000L));
        catalog.putPlacement("series-0", SeriesPlacement.active("storage-a", 1_000L)
                .withGeometry(geometryId, true, 1_000L));
        catalog.putPlacement("series-1", SeriesPlacement.active("storage-a", 1_000L)
                .withGeometry(geometryId, true, 1_000L));

        MigrationCoordinator coordinator = new MigrationCoordinator(catalog,
                new BlockingMigrationRpc(new CountDownLatch(0), new AtomicInteger()), leaderView, 2,
                Duration.ofMillis(20), Duration.ofSeconds(5), Clock.systemUTC());
        Rebalancer rebalancer = new Rebalancer(catalog, leaderView, coordinator,
                new RebalanceSettings(1L, 0.0, 50), false, Duration.ofSeconds(60), Duration.ofSeconds(5),
                Clock.systemUTC());
        // Fecha o pool de migração ANTES de disparar — coordinator.migrate() lança
        // RejectedExecutionException ao tentar submeter, de dentro do laço de triggerNow().
        coordinator.close();

        assertThrows(RejectedExecutionException.class, rebalancer::triggerNow);
        assertFalse(runningField(rebalancer), "running deveria ter voltado a false mesmo com a exceção");

        rebalancer.close();
    }

    /**
     * Issue #177: nó com a réplica do catálogo atrasada, sincronizando ou com lag desconhecido nunca é
     * escolhido como destino; nó sem o campo (8.6.0, em rolling upgrade) continua elegível. As exclusões
     * voltam no {@link Rebalancer.TriggerResult}.
     */
    @Test
    void triggerNowNaoEscolheDestinoComReplicaDoCatalogoAtrasada() throws Exception {
        leaderView.reachable.addAll(Set.of("storage-lag", "storage-sync", "storage-unknown", "storage-legacy"));
        catalog.putNodeStatus(withReplica("storage-a", 3, CatalogReplicaStatus.ofLeader()));
        catalog.putNodeStatus(withReplica("storage-lag", 0,
                new CatalogReplicaStatus(false, 12_345L, 20_000L, 7_656L, false, false, true)));
        catalog.putNodeStatus(withReplica("storage-sync", 0,
                new CatalogReplicaStatus(false, 0L, 20_000L, 1L, true, false, false)));
        catalog.putNodeStatus(withReplica("storage-unknown", 0, CatalogReplicaStatus.from(false, null)));
        catalog.putNodeStatus(new StorageNodeStatus("storage-legacy", NodeState.ACTIVE, 0, 0, 0, 1_000L));
        for (int i = 0; i < 3; i++) {
            catalog.putPlacement("series-" + i, SeriesPlacement.active("storage-a", 1_000L)
                    .withGeometry(geometryId, true, 1_000L));
        }

        BlockingMigrationRpc rpc = new BlockingMigrationRpc(new CountDownLatch(0), new AtomicInteger());
        MigrationCoordinator coordinator = new MigrationCoordinator(catalog, rpc, leaderView, 2,
                Duration.ofMillis(20), Duration.ofSeconds(5), Clock.systemUTC());
        Rebalancer rebalancer = new Rebalancer(catalog, leaderView, coordinator,
                new RebalanceSettings(1L, 0.0, 50), false, Duration.ofSeconds(60), Duration.ofSeconds(5),
                Clock.systemUTC());
        try {
            Rebalancer.TriggerResult result = rebalancer.triggerNow();

            assertEquals(Map.of(
                    "storage-lag", "lag=12345>1000",
                    "storage-sync", "sincronizando",
                    "storage-unknown", "lag desconhecido"), result.excludedDestinations());
            assertEquals(1, result.planned(), "só storage-legacy é destino elegível");
            awaitTrue("migração planejada iniciada", () -> !rpc.startTargets().isEmpty());
            assertEquals(List.of("storage-legacy"), rpc.startTargets());
        } finally {
            rebalancer.close();
            coordinator.close();
        }
    }

    @Test
    void triggerNowComPortaDesligadaIgnoraOLagDaReplica() {
        catalog.putNodeStatus(withReplica("storage-a", 2, CatalogReplicaStatus.ofLeader()));
        catalog.putNodeStatus(withReplica("storage-b", 0,
                new CatalogReplicaStatus(false, 99_999L, 100_000L, 2L, false, false, true)));
        for (int i = 0; i < 2; i++) {
            catalog.putPlacement("series-" + i, SeriesPlacement.active("storage-a", 1_000L)
                    .withGeometry(geometryId, true, 1_000L));
        }
        MigrationCoordinator coordinator = new MigrationCoordinator(catalog,
                new BlockingMigrationRpc(new CountDownLatch(0), new AtomicInteger()), leaderView, 2,
                Duration.ofMillis(20), Duration.ofSeconds(5), Clock.systemUTC());
        Rebalancer rebalancer = new Rebalancer(catalog, leaderView, coordinator,
                new RebalanceSettings(1L, 0.0, 50, -1L), false, Duration.ofSeconds(60), Duration.ofSeconds(5),
                Clock.systemUTC());
        try {
            Rebalancer.TriggerResult result = rebalancer.triggerNow();

            assertEquals(Map.of(), result.excludedDestinations());
            assertEquals(1, result.planned());
        } finally {
            rebalancer.close();
            coordinator.close();
        }
    }

    /**
     * O conjunto de exclusões costuma se repetir a cada ciclo enquanto um nó segue atrasado: o marcador
     * {@code NGRRD_REBALANCE_DEST_EXCLUDED} sai em INFO só quando o conjunto (nós e motivos) muda, e em FINE
     * nos ciclos em que se repete.
     */
    @Test
    void exclusaoDeDestinoSoLogaEmInfoQuandoOConjuntoMuda() {
        leaderView.reachable.add("storage-lag");
        catalog.putNodeStatus(withReplica("storage-a", 0, CatalogReplicaStatus.ofLeader()));
        catalog.putNodeStatus(withReplica("storage-lag", 0,
                new CatalogReplicaStatus(false, 5_000L, 9_000L, 4_001L, false, false, true)));
        MigrationCoordinator coordinator = new MigrationCoordinator(catalog,
                new BlockingMigrationRpc(new CountDownLatch(0), new AtomicInteger()), leaderView, 2,
                Duration.ofMillis(20), Duration.ofSeconds(5), Clock.systemUTC());
        Rebalancer rebalancer = new Rebalancer(catalog, leaderView, coordinator,
                new RebalanceSettings(1L, 0.0, 50), false, Duration.ofSeconds(60), Duration.ofSeconds(5),
                Clock.systemUTC());
        Logger logger = Logger.getLogger(Rebalancer.class.getName());
        List<Level> levels = new CopyOnWriteArrayList<>();
        Handler capture = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage() != null && record.getMessage().startsWith("NGRRD_REBALANCE_DEST_EXCLUDED")) {
                    levels.add(record.getLevel());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        Level previousLevel = logger.getLevel();
        logger.setLevel(Level.ALL);
        logger.addHandler(capture);
        try {
            rebalancer.triggerNow();
            rebalancer.triggerNow();
            catalog.putNodeStatus(withReplica("storage-lag", 0,
                    new CatalogReplicaStatus(false, 7_000L, 11_000L, 4_001L, false, false, true)));
            rebalancer.triggerNow();
            rebalancer.triggerNow();
        } finally {
            logger.removeHandler(capture);
            logger.setLevel(previousLevel);
            rebalancer.close();
            coordinator.close();
        }

        assertEquals(List.of(Level.INFO, Level.FINE, Level.INFO, Level.FINE), levels);
    }

    /** Issue #167 (item 3): um destino na cota de séries aparece em {@code excludedDestinations} com {@code quota_series}. */
    @Test
    void triggerNowNaoEscolheDestinoNaCotaDeSeries() throws Exception {
        leaderView.reachable.add("storage-quota");
        catalog.putNodeStatus(withReplica("storage-a", 3, CatalogReplicaStatus.ofLeader()));
        catalog.putNodeStatus(new StorageNodeStatus("storage-quota", NodeState.ACTIVE, 2, 0, 0, 1_000L,
                DistributionMode.COUNT, 1, 0, StorageCapabilities.ALL, CatalogReplicaStatus.ofLeader(), 2, 0, null));
        catalog.putNodeStatus(withReplica("storage-b", 0, CatalogReplicaStatus.ofLeader()));
        for (int i = 0; i < 3; i++) {
            catalog.putPlacement("series-" + i, SeriesPlacement.active("storage-a", 1_000L)
                    .withGeometry(geometryId, true, 1_000L));
        }

        BlockingMigrationRpc rpc = new BlockingMigrationRpc(new CountDownLatch(0), new AtomicInteger());
        MigrationCoordinator coordinator = new MigrationCoordinator(catalog, rpc, leaderView, 2,
                Duration.ofMillis(20), Duration.ofSeconds(5), Clock.systemUTC());
        Rebalancer rebalancer = new Rebalancer(catalog, leaderView, coordinator,
                new RebalanceSettings(1L, 0.0, 50), false, Duration.ofSeconds(60), Duration.ofSeconds(5),
                Clock.systemUTC());
        try {
            Rebalancer.TriggerResult result = rebalancer.triggerNow();

            assertEquals(Map.of("storage-quota", "quota_series(3/2)"), result.excludedDestinations());
            assertTrue(result.planned() >= 1, "storage-b é destino elegível");
            awaitTrue("migração planejada iniciada", () -> !rpc.startTargets().isEmpty());
            assertTrue(rpc.startTargets().stream().allMatch("storage-b"::equals), rpc.startTargets().toString());
        } finally {
            rebalancer.close();
            coordinator.close();
        }
    }

    /** Issue #167 (item 3): drain preso por regra loga {@code NGRRD_DRAIN_PENDING} com o novo motivo e {@code rulesSkipped}. */
    @Test
    void drainPresoPorRegraLogaOMotivoComRulesSkipped() {
        PlacementRules rules = PlacementRules.of(List.of(
                new PlacementRule("only-a", null, "series-", Set.of("storage-a"), null)));
        catalog.putNodeStatus(withReplica("storage-a", 2, CatalogReplicaStatus.ofLeader())
                .withState(NodeState.DRAINING, 1_000L));
        catalog.putNodeStatus(withReplica("storage-b", 0, CatalogReplicaStatus.ofLeader()));
        for (int i = 0; i < 2; i++) {
            catalog.putPlacement("series-" + i, SeriesPlacement.active("storage-a", 1_000L)
                    .withGeometry(geometryId, true, 1_000L));
        }
        MigrationCoordinator coordinator = new MigrationCoordinator(catalog,
                new BlockingMigrationRpc(new CountDownLatch(0), new AtomicInteger()), leaderView, 2,
                Duration.ofMillis(20), Duration.ofSeconds(5), Clock.systemUTC());
        Rebalancer rebalancer = new Rebalancer(catalog, leaderView, coordinator,
                new RebalanceSettings(1L, 0.0, 50), false, Duration.ofSeconds(60), Duration.ofSeconds(5),
                Clock.systemUTC(), rules);
        Logger logger = Logger.getLogger(Rebalancer.class.getName());
        List<String> messages = new CopyOnWriteArrayList<>();
        Handler capture = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage() != null && record.getMessage().startsWith("NGRRD_DRAIN_PENDING")) {
                    messages.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        logger.addHandler(capture);
        try {
            Rebalancer.TriggerResult result = rebalancer.triggerNow();
            assertEquals(0, result.planned());
        } finally {
            logger.removeHandler(capture);
            rebalancer.close();
            coordinator.close();
        }

        assertEquals(List.of("NGRRD_DRAIN_PENDING reason=no_admissible_destination_or_confirmed_geometry_or_quota_or_rules"
                + " rulesSkipped=2"), messages);
        assertEquals(NodeState.DRAINING, catalog.nodeStatusLocal("storage-a").orElseThrow().state());
    }

    private static StorageNodeStatus withReplica(String nodeId, long seriesCount, CatalogReplicaStatus replica) {
        return new StorageNodeStatus(nodeId, NodeState.ACTIVE, seriesCount, 0, 0, 1_000L, DistributionMode.COUNT, 1,
                0, StorageCapabilities.ALL, replica);
    }

    private static boolean runningField(Rebalancer rebalancer) {
        try {
            Field field = Rebalancer.class.getDeclaredField("running");
            field.setAccessible(true);
            return ((AtomicBoolean) field.get(rebalancer)).get();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void awaitTrue(String description, java.util.function.BooleanSupplier condition)
            throws InterruptedException {
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
     * {@link ClusterRpc} fake: {@code MIGRATE_START} bloqueia em {@code releaseStart} (contando cada
     * chamada em {@code startCalls}) antes de responder OK; {@code MIGRATE_STATUS} já confirma
     * {@code COMMITTED} de cara (não há transferência de bytes de verdade neste teste — o que se quer
     * observar é só o ciclo de vida de {@code running}); {@code MIGRATE_FINISH}/{@code MIGRATE_ABORT}
     * respondem OK.
     */
    private static final class BlockingMigrationRpc implements ClusterRpc {
        private final CountDownLatch releaseStart;
        private final AtomicInteger startCalls;
        private final java.util.List<String> commands = new java.util.concurrent.CopyOnWriteArrayList<>();
        private final java.util.List<String> startTargets = new java.util.concurrent.CopyOnWriteArrayList<>();

        BlockingMigrationRpc(CountDownLatch releaseStart, AtomicInteger startCalls) {
            this.releaseStart = releaseStart;
            this.startCalls = startCalls;
        }

        /** Destinos de cada {@code MIGRATE_START} recebido, na ordem. */
        List<String> startTargets() {
            return List.copyOf(startTargets);
        }

        long callsTo(String command) {
            return commands.stream().filter(command::equals).count();
        }

        @Override
        @SuppressWarnings("unchecked")
        public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
            commands.add(command);
            return (R) switch (command) {
                case Commands.MIGRATE_START -> {
                    startCalls.incrementAndGet();
                    if (body instanceof MigrateStartRequest start) {
                        startTargets.add(start.targetNodeId());
                    }
                    try {
                        if (!releaseStart.await(AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                            throw new NgrrdClusterException(dev.nishisan.utils.oss.cluster.api.ErrorCode.TIMEOUT,
                                    "releaseStart nunca veio");
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
            return NodeId.of("test-rebalancer");
        }

        @Override
        public Optional<NodeId> leaderId() {
            return Optional.empty();
        }
    }
}
