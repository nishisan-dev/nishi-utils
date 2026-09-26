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

import dev.nishisan.utils.ngrid.cluster.coordination.LeadershipListener;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.map.MapClusterService;
import dev.nishisan.utils.ngrid.structures.NGrid;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.ngrid.structures.NGridNodeBuilder;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.cluster.admin.AdminService;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogReplicaStatus;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.client.CatalogLookupClient;
import dev.nishisan.utils.oss.cluster.client.NodeCapabilities;
import dev.nishisan.utils.oss.cluster.client.RetryPolicy;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.placement.LeastLoadedPlacementPolicy;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationExecutor;
import dev.nishisan.utils.oss.cluster.rebalance.RebalanceSettings;
import dev.nishisan.utils.oss.cluster.rebalance.Rebalancer;
import dev.nishisan.utils.oss.cluster.rpc.LocalRequestHandler;
import dev.nishisan.utils.oss.cluster.rpc.TransportClusterRpc;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
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
    /** Backoff das retentativas de {@code ngrrd.catalog.lookup} na confirmação de redirecionamentos. */
    private static final Duration LEADER_LOOKUP_BACKOFF_MIN = Duration.ofMillis(50);
    private static final Duration LEADER_LOOKUP_BACKOFF_MAX = Duration.ofMillis(500);
    /** Chaves por página de {@code ngrrd.catalog.lookup} na confirmação de redirecionamentos. */
    private static final int LEADER_LOOKUP_BATCH_SIZE = 2_000;
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
        return start(cfg, migrationHooks, UnaryOperator.identity());
    }

    /**
     * Gancho de teste: como {@link #start(StorageNodeConfig, MigrationCoordinator.MigrationHooks)}, mas
     * aplicando {@code lookupDecorator} ao {@link StorageRequestHandler.PlacementLookup} de produção antes
     * de entregá-lo ao {@link StorageRequestHandler} — permite simular, num cluster real, a réplica local do
     * catálogo atrasada neste nó (issue #177). Não é API estável.
     *
     * <p>Limitação: só o {@link StorageRequestHandler} enxerga a visão decorada. {@link MigrationExecutor},
     * {@link LocalReconciler}, {@link GeometryService}, {@link PlacementRequestHandler} e o reporter de status
     * continuam lendo a réplica real via {@link CatalogService}.</p>
     *
     * @param lookupDecorator recebe o adaptador de produção e devolve o que o handler vai usar (nunca
     *                        {@code null}); {@link UnaryOperator#identity()} equivale ao comportamento normal
     */
    public static NgrrdStorageNode start(StorageNodeConfig cfg, MigrationCoordinator.MigrationHooks migrationHooks,
            UnaryOperator<StorageRequestHandler.PlacementLookup> lookupDecorator) throws IOException {
        Objects.requireNonNull(cfg, "cfg");
        Objects.requireNonNull(migrationHooks, "migrationHooks");
        Objects.requireNonNull(lookupDecorator, "lookupDecorator");

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
                StorageRequestHandler.PlacementLookup productionLookup = new StorageRequestHandler.PlacementLookup() {
                    @Override
                    public Optional<SeriesPlacement> placementLocal(String seriesKey) {
                        return catalog.placementLocal(seriesKey);
                    }

                    @Override
                    public Optional<SeriesPlacement> placementStrong(String seriesKey) {
                        return catalog.placementStrong(seriesKey);
                    }

                    @Override
                    public Map<String, SeriesPlacement> placementsAtLeader(Collection<String> seriesKeys,
                            Duration maxWait) {
                        return placementsAtLeaderOf(node, catalog, rpc, seriesKeys, maxWait);
                    }

                    @Override
                    public boolean localIsAuthoritative() {
                        return node.coordinator().isLeader();
                    }

                    @Override
                    public boolean leaderKnown() {
                        return rpc.leaderId().isPresent();
                    }
                };
                StorageRequestHandler.PlacementLookup placementLookup = Objects.requireNonNull(
                        lookupDecorator.apply(productionLookup), "lookupDecorator devolveu null");
                StorageRequestHandler storageHandler = new StorageRequestHandler(node.transport(),
                        placementLookup, registry, volume, cfg.seriesObjectPrefix(), self, cfg.defaultDurability(),
                        cfg.defaultOnGeometryChange(), Clock.systemUTC());
                PlacementRequestHandler.LeaderView leaderView =
                        PlacementRequestHandler.fromCoordinator(node.coordinator(), node.transport());
                PlacementRequestHandler placementHandler = new PlacementRequestHandler(node.transport(), catalog,
                        leaderView, node.replicationManager()::isLeaderSyncing, new LeastLoadedPlacementPolicy(),
                        cfg.nodeStatusStaleAfter(), cfg.placementGraceAfterLeadership(), Clock.systemUTC(),
                        cfg.placementRules());
                wirePlacementHandler(placementHandler, self, node.coordinator()::addLeadershipListener,
                        node.transport()::addListener, rpc::registerLocalHandler);

                GeometryService geometryService = new GeometryService(node.transport(), catalog, volume, rpc,
                        leaderView, registry, cfg.seriesObjectPrefix(), Clock.systemUTC());
                storageHandler.geometryService(geometryService);
                MigrationExecutor migrationExecutor = new MigrationExecutor(node.transport(), registry, volume, rpc,
                        catalog, self, cfg.seriesObjectPrefix(), cfg.migrationChunkBytes(), cfg.maxSeriesBytes(),
                        cfg.migrationBytesPerSecond(), Clock.systemUTC(), cfg.quotaMaxSeries(), cfg.quotaMaxBytes());
                MigrationCoordinator migrationCoordinator = new MigrationCoordinator(catalog, rpc, leaderView,
                        cfg.maxConcurrentMigrations(), cfg.migrationStatusPollInterval(), cfg.migrationTimeout(),
                        Clock.systemUTC(), migrationHooks, cfg.maxDestinationCatalogLag(), cfg.placementRules());
                RebalanceSettings rebalanceSettings = new RebalanceSettings(cfg.rebalanceMinDelta(),
                        cfg.rebalanceTolerance(), cfg.maxMovesPerCycle(), cfg.maxDestinationCatalogLag());
                Rebalancer rebalancer = new Rebalancer(catalog, leaderView, migrationCoordinator, rebalanceSettings,
                        cfg.rebalanceEnabled(), cfg.rebalanceInterval(), cfg.migrationTimeout(), Clock.systemUTC(),
                        cfg.placementRules());
                AdminService adminService = new AdminService(catalog, rebalancer, Clock.systemUTC());
                LocalReconciler localReconciler = new LocalReconciler(volume, catalog, rpc, registry, cfg.nodeId(),
                        cfg.seriesObjectPrefix(), cfg.orphanGrace(), cfg.reconcileInterval(),
                        node.coordinator()::isLeader, Clock.systemUTC());

                NodeStatusReporter statusReporter = new NodeStatusReporter(catalog, volume, registry, cfg.nodeId(),
                        cfg.capacityBytes(), cfg.statusReportInterval(), Clock.systemUTC(),
                        storageHandler::metricsSnapshot, node.coordinator()::isLeader, cfg.metricsListener(),
                        migrationExecutor, cfg.migrationTimeout(), localReconciler);
                AdminRequestHandler adminHandler = new AdminRequestHandler(node.transport(), self, leaderView,
                        catalog, statusReporter::metricsSnapshot, rpc, rebalancer, adminService, migrationCoordinator,
                        cfg.placementRules());

                statusReporter.distribution(cfg.distributionMode(), cfg.weight());
                // Issue #167 (item 3): cota dura e fingerprint das regras de placement em todo status.
                statusReporter.quota(cfg.quotaMaxSeries(), cfg.quotaMaxBytes());
                statusReporter.placementRulesHash(cfg.placementRules().fingerprint());
                LOGGER.info("NGRRD_PLACEMENT_RULES loaded count=" + cfg.placementRules().size() + " hash="
                        + (cfg.placementRules().fingerprint() == null ? "-" : cfg.placementRules().fingerprint())
                        + " node=" + cfg.nodeId());
                // Issue #177: lag POR TÓPICO do catálogo (o lag global do snapshot operacional não serve).
                String catalogTopic = MapClusterService.topicFor(CatalogService.CATALOG_MAP);
                statusReporter.catalogReplication(() -> CatalogReplicaStatus.from(node.coordinator().isLeader(),
                        node.replicationManager().getTopicReplicationStatuses().get(catalogTopic)));
                node.transport().addListener(geometryService);
                rpc.registerLocalHandler(geometryService);
                node.transport().addListener(storageHandler);
                node.transport().addListener(adminHandler);
                node.transport().addListener(migrationExecutor);
                node.coordinator().addLeadershipListener(migrationCoordinator);
                node.coordinator().addLeadershipListener(rebalancer);
                node.coordinator().addMembershipListener(rebalancer);
                node.coordinator().addLeadershipListener(localReconciler);
                rpc.registerLocalHandler(storageHandler);
                rpc.registerLocalHandler(adminHandler);
                rpc.registerLocalHandler(migrationExecutor);

                node.coordinator().addLeadershipListener(statusReporter);
                statusReporter.start();
                localReconciler.start();
                geometryService.start();

                // Seed: addLeadershipListener não dispara um callback sintético para quem já registra o
                // listener com o nó JÁ líder — ex.: o primeiro líder eleito, decidido durante
                // builder.start() acima, ANTES deste registro. Sem isto, migrationCoordinator/rebalancer
                // deste nó nunca saberiam que já são líder até a PRÓXIMA troca de liderança (se houver
                // alguma) — nenhuma migração nem rebalanceamento automático rodaria nele enquanto ele
                // seguisse líder ininterruptamente desde o início. O placementHandler já foi semeado em
                // wirePlacementHandler, antes de atender requisições.
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

    /**
     * {@link StorageRequestHandler.PlacementLookup#placementsAtLeader} de produção (issue #177): no líder,
     * a réplica local é a fonte; num seguidor, {@code ngrrd.catalog.lookup} via {@link CatalogLookupClient}
     * com prazo total {@code maxWait}. Nunca usa {@code placementStrong}/{@code DistributedMap} num
     * seguidor — o {@code invokeLeader} do core pode bloquear por muito mais que o prazo (várias tentativas
     * de {@code requestTimeout} com espera entre elas), e esta consulta roda segurando o lock de
     * coordenação de um {@code OPEN}.
     *
     * <p>A capacidade {@code catalog.lookup} do líder é conferida só na réplica local de
     * {@code ngrrd.nodes} (sem leitura forte, pelo mesmo motivo — ver {@link #leaderCapabilitiesFromLocal}):
     * um líder que não a anuncie falha a consulta na hora, e o handler responde pela réplica local durante o
     * cooldown.</p>
     */
    private static Map<String, SeriesPlacement> placementsAtLeaderOf(NGridNode node, CatalogService catalog,
            TransportClusterRpc rpc, Collection<String> seriesKeys, Duration maxWait) {
        if (node.coordinator().isLeader()) {
            Map<String, SeriesPlacement> found = new HashMap<>();
            for (String seriesKey : seriesKeys) {
                catalog.placementLocal(seriesKey).ifPresent(placement -> found.put(seriesKey, placement));
            }
            return found;
        }
        CatalogLookupClient lookup = new CatalogLookupClient(rpc,
                new RetryPolicy(maxWait, LEADER_LOOKUP_BACKOFF_MIN, LEADER_LOOKUP_BACKOFF_MAX), Clock.systemUTC(),
                LEADER_LOOKUP_BATCH_SIZE, leaderCapabilitiesFromLocal(catalog::nodeStatusLocal));
        return lookup.lookup(seriesKeys, maxWait);
    }

    /**
     * Conferência de capacidades do líder usada por {@link #placementsAtLeaderOf}: só a réplica local de
     * {@code ngrrd.nodes}, e resposta negativa imediata — status presente sem a capacidade (líder anterior à
     * 8.6.0) ou status ainda ausente na réplica. Sem isso, um status ausente faria o {@link NodeCapabilities}
     * reler a réplica até o prazo inteiro da confirmação, a cada fim de cooldown; aqui a falha é imediata e o
     * handler responde pela réplica local.
     */
    static NodeCapabilities leaderCapabilitiesFromLocal(Function<String, Optional<StorageNodeStatus>> localStatus) {
        Function<String, Optional<StorageNodeStatus>> presentOrFail = nodeId -> {
            Optional<StorageNodeStatus> status = localStatus.apply(nodeId);
            if (status.isEmpty()) {
                throw new NgrrdClusterException(ErrorCode.UNSUPPORTED_BY_NODE, "status do líder " + nodeId
                        + " ausente na réplica local; não foi possível conferir as capacidades dele");
            }
            return status;
        };
        return new NodeCapabilities(localStatus, presentOrFail);
    }

    /**
     * Liga o {@link PlacementRequestHandler} ao nó numa ordem que fecha a janela do "não existe" falso
     * logo após o boot: (1) listener de liderança, para nenhuma troca de líder posterior se perder;
     * (2) seed ({@code onLeaderChanged(self)}), que marca o início da janela de graça se o nó já for o
     * líder — {@code addLeadershipListener} não dispara callback sintético para quem registra com o nó
     * JÁ líder, o que vale para qualquer primeiro líder eleito durante {@code builder.start()}; (3) só
     * então os caminhos de requisição (rede e local). Com a ordem invertida, um {@code CATALOG_LOOKUP}
     * que chegasse entre o registro e o seed veria a janela como expirada e responderia um miss como
     * "não existe" definitivo.
     */
    static void wirePlacementHandler(PlacementRequestHandler handler, NodeId self,
            Consumer<? super LeadershipListener> leadershipRegistrar,
            Consumer<? super TransportListener> transportRegistrar,
            Consumer<? super LocalRequestHandler> localRegistrar) {
        leadershipRegistrar.accept(handler);
        handler.onLeaderChanged(self);
        transportRegistrar.accept(handler);
        localRegistrar.accept(handler);
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
