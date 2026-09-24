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
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.ngrid.structures.NGridNodeBuilder;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.cluster.admin.AdminService;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.placement.LeastLoadedPlacementPolicy;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationExecutor;
import dev.nishisan.utils.oss.cluster.rebalance.RebalanceSettings;
import dev.nishisan.utils.oss.cluster.rebalance.Rebalancer;
import dev.nishisan.utils.oss.cluster.rpc.TransportClusterRpc;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Um storage node do cluster ngrrd: um {@link NGridNode} com role
 * {@code storage} + um {@link BlobVolume} local, com os handlers do protocolo
 * ({@link StorageRequestHandler}, {@link PlacementRequestHandler}) e o
 * {@link NodeStatusReporter} já registrados e em execução.
 */
public final class NgrrdStorageNode implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(NgrrdStorageNode.class.getName());

    private static final String STORAGE_ROLE = "storage";
    /** Hooks no-op — usado por {@link #start(StorageNodeConfig)} (produção; sem testes de queda do líder). */
    private static final MigrationCoordinator.MigrationHooks DEFAULT_MIGRATION_HOOKS =
            new MigrationCoordinator.MigrationHooks() {
            };

    private final StorageNodeConfig config;
    private final BlobVolumeRegistry volumeRegistry;
    private final BlobVolume volume;
    private final NGridNode node;
    private final CatalogService catalog;
    private final TransportClusterRpc rpc;
    private final SeriesHandleRegistry registry;
    private final StorageRequestHandler storageHandler;
    private final PlacementRequestHandler placementHandler;
    private final NodeStatusReporter statusReporter;
    private final AdminRequestHandler adminHandler;
    private final MigrationExecutor migrationExecutor;
    private final MigrationCoordinator migrationCoordinator;
    private final Rebalancer rebalancer;
    private final LocalReconciler localReconciler;
    private final GeometryService geometryService;

    private NgrrdStorageNode(StorageNodeConfig config, BlobVolumeRegistry volumeRegistry, BlobVolume volume,
            NGridNode node, CatalogService catalog, TransportClusterRpc rpc, SeriesHandleRegistry registry,
            StorageRequestHandler storageHandler, PlacementRequestHandler placementHandler,
            NodeStatusReporter statusReporter, AdminRequestHandler adminHandler,
            MigrationExecutor migrationExecutor, MigrationCoordinator migrationCoordinator, Rebalancer rebalancer,
            LocalReconciler localReconciler, GeometryService geometryService) {
        this.config = config;
        this.volumeRegistry = volumeRegistry;
        this.volume = volume;
        this.node = node;
        this.catalog = catalog;
        this.rpc = rpc;
        this.registry = registry;
        this.storageHandler = storageHandler;
        this.placementHandler = placementHandler;
        this.statusReporter = statusReporter;
        this.adminHandler = adminHandler;
        this.migrationExecutor = migrationExecutor;
        this.migrationCoordinator = migrationCoordinator;
        this.rebalancer = rebalancer;
        this.localReconciler = localReconciler;
        this.geometryService = geometryService;
    }

    /**
     * Sobe um storage node completo a partir de {@code cfg}: volume, NGridNode,
     * handlers e reporter.
     *
     * <p>m1: qualquer falha depois de abrir o volume ou o {@code NGridNode}
     * fecha o que já foi aberto antes de propagar — o {@code try/catch} cobre
     * toda a sequência até o {@code return}, não só a abertura do
     * {@code NGridNode}.</p>
     */
    public static NgrrdStorageNode start(StorageNodeConfig cfg) throws IOException {
        return start(cfg, DEFAULT_MIGRATION_HOOKS);
    }

    /**
     * Como {@link #start(StorageNodeConfig)}, mas injetando {@code migrationHooks} no
     * {@link MigrationCoordinator} deste nó — usado exclusivamente pelos testes de queda do líder
     * durante uma migração (ver Javadoc de {@link MigrationCoordinator.MigrationHooks}).
     */
    public static NgrrdStorageNode start(StorageNodeConfig cfg, MigrationCoordinator.MigrationHooks migrationHooks)
            throws IOException {
        Objects.requireNonNull(cfg, "cfg");
        Objects.requireNonNull(migrationHooks, "migrationHooks");

        BlobVolumeRegistry volumeRegistry = NgrrdBlob.registry()
                .basePath(cfg.volumeDir())
                .shardCount(cfg.shardCount())
                .segmentBytes(cfg.segmentBytes())
                .initialShardCapacityBytes(cfg.initialShardCapacityBytes())
                .volume(cfg.volumeName())
                .build();
        try {
            BlobVolume volume = volumeRegistry.require(cfg.volumeName());
            volume.storage().configureCapacity(cfg.capacityBytes());

            NGridNodeBuilder builder = NGrid.node(cfg.host(), cfg.port())
                    .id(cfg.nodeId())
                    .priority(cfg.priority())
                    .roles(STORAGE_ROLE)
                    .dataDir(cfg.dataDir())
                    // Mitiga a deferência mútua a três (D9/D10c) que trava a sincronização do 3º nó
                    // (achado desta sessão em RebalanceClusterTest/PlacementUnderLeaderChurnClusterTest):
                    // dá tempo do nó recém-subido descobrir peers e watermarks antes de se autoeleger.
                    // O cliente não passa por aqui (é inelegível para liderança, não precisa da janela).
                    .bootDiscoveryWindow(cfg.bootDiscoveryWindow())
                    // Handback orquestrado (D11): sem ele, o nó de maior afinidade que volta reassume
                    // por watermark enquanto o incumbente ainda produz — dois líderes, e o D10c descarta
                    // a cauda do perdedor (flips do catálogo já confirmados sumiam e a série migrada era
                    // recriada vazia na origem — achado do RebalanceClusterTest com log FINE).
                    .affinityHandbackMode(cfg.affinityHandbackMode());
            CatalogService.declareMaps(builder);
            if (cfg.seed() != null) {
                builder.seed(cfg.seed());
            }
            if (!cfg.peers().isEmpty()) {
                builder.peers(cfg.peers().toArray(new String[0]));
            }
            NGridNode node = builder.start();
            try {
                CatalogService catalog = CatalogService.from(node);
                TransportClusterRpc rpc = new TransportClusterRpc(node.transport(), node.coordinator(),
                        cfg.requestTimeout());
                SeriesHandleRegistry registry = new SeriesHandleRegistry(volume, cfg.volumeName(),
                        cfg.handleIdleTtl(), cfg.maxOpenHandles(), Clock.systemUTC());

                NodeId self = node.transport().local().nodeId();
                // Adaptador em vez de método de referência: StorageRequestHandler.PlacementLookup agora
                // também exige placementStrong (round-trip ao líder), usado quando a réplica local do
                // catálogo ainda está vazia (ex.: logo após um restart) — ver F1.2.
                StorageRequestHandler.PlacementLookup placementLookup = new StorageRequestHandler.PlacementLookup() {
                    @Override
                    public Optional<SeriesPlacement> placementLocal(String seriesKey) {
                        return catalog.placementLocal(seriesKey);
                    }

                    @Override
                    public Optional<SeriesPlacement> placementStrong(String seriesKey) {
                        return catalog.placementStrong(seriesKey);
                    }
                };
                StorageRequestHandler storageHandler = new StorageRequestHandler(node.transport(),
                        placementLookup, registry, volume, cfg.seriesObjectPrefix(), self, cfg.defaultDurability(),
                        cfg.defaultOnGeometryChange(), Clock.systemUTC());
                PlacementRequestHandler.LeaderView leaderView =
                        PlacementRequestHandler.fromCoordinator(node.coordinator(), node.transport());
                PlacementRequestHandler placementHandler = new PlacementRequestHandler(node.transport(), catalog,
                        leaderView, new LeastLoadedPlacementPolicy(), cfg.nodeStatusStaleAfter(),
                        cfg.placementGraceAfterLeadership(), Clock.systemUTC());

                GeometryService geometryService = new GeometryService(node.transport(), catalog, volume, rpc,
                        leaderView, registry, cfg.seriesObjectPrefix(), Clock.systemUTC());
                storageHandler.geometryService(geometryService);
                MigrationExecutor migrationExecutor = new MigrationExecutor(node.transport(), registry, volume, rpc,
                        catalog, self, cfg.seriesObjectPrefix(), cfg.migrationChunkBytes(), cfg.maxSeriesBytes(),
                        Clock.systemUTC());
                MigrationCoordinator migrationCoordinator = new MigrationCoordinator(catalog, rpc, leaderView,
                        cfg.maxConcurrentMigrations(), cfg.migrationStatusPollInterval(), cfg.migrationTimeout(),
                        Clock.systemUTC(), migrationHooks);
                RebalanceSettings rebalanceSettings = new RebalanceSettings(cfg.rebalanceMinDelta(),
                        cfg.rebalanceTolerance(), cfg.maxMovesPerCycle());
                Rebalancer rebalancer = new Rebalancer(catalog, leaderView, migrationCoordinator, rebalanceSettings,
                        cfg.rebalanceEnabled(), cfg.rebalanceInterval(), cfg.migrationTimeout(), Clock.systemUTC());
                AdminService adminService = new AdminService(catalog, rebalancer, Clock.systemUTC());
                LocalReconciler localReconciler = new LocalReconciler(volume, catalog, rpc, registry, cfg.nodeId(),
                        cfg.seriesObjectPrefix(), cfg.orphanGrace(), cfg.reconcileInterval(),
                        node.coordinator()::isLeader, Clock.systemUTC());

                NodeStatusReporter statusReporter = new NodeStatusReporter(catalog, volume, registry, cfg.nodeId(),
                        cfg.capacityBytes(), cfg.statusReportInterval(), Clock.systemUTC(),
                        storageHandler::metricsSnapshot, node.coordinator()::isLeader, cfg.metricsListener(),
                        migrationExecutor, cfg.migrationTimeout(), localReconciler);
                AdminRequestHandler adminHandler = new AdminRequestHandler(node.transport(), self, leaderView,
                        catalog, statusReporter::metricsSnapshot, rpc, rebalancer, adminService, migrationCoordinator);

                statusReporter.distribution(cfg.distributionMode(), cfg.weight());
                node.transport().addListener(geometryService);
                rpc.registerLocalHandler(geometryService);
                node.transport().addListener(storageHandler);
                node.transport().addListener(placementHandler);
                node.transport().addListener(adminHandler);
                node.transport().addListener(migrationExecutor);
                node.coordinator().addLeadershipListener(placementHandler);
                node.coordinator().addLeadershipListener(migrationCoordinator);
                node.coordinator().addLeadershipListener(rebalancer);
                node.coordinator().addMembershipListener(rebalancer);
                node.coordinator().addLeadershipListener(localReconciler);
                rpc.registerLocalHandler(storageHandler);
                rpc.registerLocalHandler(placementHandler);
                rpc.registerLocalHandler(adminHandler);
                rpc.registerLocalHandler(migrationExecutor);

                node.coordinator().addLeadershipListener(statusReporter);
                statusReporter.start();
                localReconciler.start();
                geometryService.start();

                // Seed: addLeadershipListener não dispara um callback sintético para quem já registra o
                // listener com o nó JÁ líder — ex.: o primeiro líder eleito de um cluster recém-formado,
                // decidido durante builder.start() acima, ANTES deste registro. Sem isto,
                // migrationCoordinator/rebalancer deste nó nunca saberiam que já são líder até a PRÓXIMA
                // troca de liderança (se houver alguma) — nenhuma migração nem rebalanceamento automático
                // rodaria nele enquanto ele seguisse líder ininterruptamente desde o início.
                migrationCoordinator.onLeaderChanged(self);
                rebalancer.onLeaderChanged(self);

                return new NgrrdStorageNode(cfg, volumeRegistry, volume, node, catalog, rpc, registry,
                        storageHandler, placementHandler, statusReporter, adminHandler, migrationExecutor,
                        migrationCoordinator, rebalancer, localReconciler, geometryService);
            } catch (RuntimeException e) {
                try {
                    node.close();
                } catch (IOException | RuntimeException closeError) {
                    e.addSuppressed(closeError);
                }
                throw e;
            }
        } catch (IOException | RuntimeException e) {
            try {
                volumeRegistry.close();
            } catch (RuntimeException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    public String nodeId() {
        return config.nodeId();
    }

    public NGridNode node() {
        return node;
    }

    public CatalogService catalog() {
        return catalog;
    }

    public BlobVolume volume() {
        return volume;
    }

    public SeriesHandleRegistry registry() {
        return registry;
    }

    public TransportClusterRpc rpc() {
        return rpc;
    }

    public boolean isLeader() {
        return node.coordinator().isLeader();
    }

    public StorageNodeConfig config() {
        return config;
    }

    public MigrationExecutor migrationExecutor() {
        return migrationExecutor;
    }

    public MigrationCoordinator migrationCoordinator() {
        return migrationCoordinator;
    }

    public Rebalancer rebalancer() {
        return rebalancer;
    }

    public LocalReconciler localReconciler() {
        return localReconciler;
    }

    /** Snapshot completo das métricas operacionais deste nó (ver {@link NodeMetricsSnapshot}). */
    public NodeMetricsSnapshot metricsSnapshot() {
        return statusReporter.metricsSnapshot();
    }

    /**
     * Encerra na ordem: <b>statusReporter → localReconciler</b> (cada um espera, com timeout de 5 s,
     * o(s) próprio(s) executor(es) pararem antes de devolver — {@code NodeStatusReporter} tem DOIS:
     * o scheduler de manutenção e o executor de publicação dedicado, ver seu Javadoc — de propósito
     * ANTES de qualquer coisa tocar o volume/registry, para que nenhum tick tardio de
     * {@code publishMetrics}/reconciliação ainda em voo encontre o volume já fechado) → rebalancer →
     * migrationCoordinator → migrationExecutor → handlers (removidos do transporte/coordenador, o que
     * impede qualquer NOVA requisição de entrar — uma já em trânsito ainda pode concluir de forma
     * concorrente com os passos seguintes: best-effort, não uma garantia dura, por isso
     * {@code metricsSnapshot()}/{@code NodeStatusReporter} toleram um volume já fechado sem logar
     * SEVERE) → registry (checkpoint+close de tudo que estiver aberto) → nó NGrid → registro de
     * volumes. Erros de cada etapa são logados, não propagados — um recurso não fechado não deve
     * impedir o fechamento dos demais.
     */
    @Override
    public void close() {
        safely("geometry service", geometryService::close);
        safely("status reporter", statusReporter::close);
        safely("local reconciler", localReconciler::close);
        safely("rebalancer", rebalancer::close);
        safely("migration coordinator", migrationCoordinator::close);
        safely("migration executor", migrationExecutor::close);
        safely("handlers", () -> {
            node.transport().removeListener(geometryService);
            rpc.unregisterLocalHandler(geometryService);
            node.transport().removeListener(storageHandler);
            node.transport().removeListener(placementHandler);
            node.transport().removeListener(adminHandler);
            node.transport().removeListener(migrationExecutor);
            node.coordinator().removeLeadershipListener(placementHandler);
            node.coordinator().removeLeadershipListener(migrationCoordinator);
            node.coordinator().removeLeadershipListener(rebalancer);
            node.coordinator().removeMembershipListener(rebalancer);
            node.coordinator().removeLeadershipListener(localReconciler);
            node.coordinator().removeLeadershipListener(statusReporter);
            rpc.unregisterLocalHandler(storageHandler);
            rpc.unregisterLocalHandler(placementHandler);
            rpc.unregisterLocalHandler(adminHandler);
            rpc.unregisterLocalHandler(migrationExecutor);
        });
        safely("series handle registry", registry::close);
        safely("NGrid node", () -> {
            try {
                node.close();
            } catch (IOException e) {
                throw new UncheckedIOExceptionOnClose(e);
            }
        });
        safely("blob volume registry", volumeRegistry::close);
    }

    private void safely(String what, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Falha ao fechar " + what + " do storage node " + config.nodeId(), e);
        }
    }

    /** Envelope interno só para carregar {@link IOException} através de {@link Runnable#run()}. */
    private static final class UncheckedIOExceptionOnClose extends RuntimeException {
        UncheckedIOExceptionOnClose(IOException cause) {
            super(cause);
        }
    }
}
