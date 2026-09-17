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
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.placement.LeastLoadedPlacementPolicy;
import dev.nishisan.utils.oss.cluster.rpc.TransportClusterRpc;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.util.Objects;
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

    private NgrrdStorageNode(StorageNodeConfig config, BlobVolumeRegistry volumeRegistry, BlobVolume volume,
            NGridNode node, CatalogService catalog, TransportClusterRpc rpc, SeriesHandleRegistry registry,
            StorageRequestHandler storageHandler, PlacementRequestHandler placementHandler,
            NodeStatusReporter statusReporter) {
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
        Objects.requireNonNull(cfg, "cfg");

        BlobVolumeRegistry volumeRegistry = NgrrdBlob.registry()
                .basePath(cfg.volumeDir())
                .shardCount(cfg.shardCount())
                .segmentBytes(cfg.segmentBytes())
                .initialShardCapacityBytes(cfg.initialShardCapacityBytes())
                .volume(cfg.volumeName())
                .build();
        try {
            BlobVolume volume = volumeRegistry.require(cfg.volumeName());

            NGridNodeBuilder builder = NGrid.node(cfg.host(), cfg.port())
                    .id(cfg.nodeId())
                    .priority(cfg.priority())
                    .roles(STORAGE_ROLE)
                    .dataDir(cfg.dataDir());
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
                StorageRequestHandler storageHandler = new StorageRequestHandler(node.transport(),
                        catalog::placementLocal, registry, self, cfg.defaultDurability(),
                        cfg.defaultOnGeometryChange());
                PlacementRequestHandler.LeaderView leaderView =
                        PlacementRequestHandler.fromCoordinator(node.coordinator(), node.transport());
                PlacementRequestHandler placementHandler = new PlacementRequestHandler(node.transport(), catalog,
                        leaderView, new LeastLoadedPlacementPolicy(), cfg.statusReportInterval(), Clock.systemUTC());

                node.transport().addListener(storageHandler);
                node.transport().addListener(placementHandler);
                node.coordinator().addLeadershipListener(placementHandler);
                rpc.registerLocalHandler(storageHandler);
                rpc.registerLocalHandler(placementHandler);

                NodeStatusReporter statusReporter = new NodeStatusReporter(catalog, volume, registry, cfg.nodeId(),
                        cfg.capacityBytes(), cfg.statusReportInterval(), Clock.systemUTC());
                statusReporter.start();

                return new NgrrdStorageNode(cfg, volumeRegistry, volume, node, catalog, rpc, registry,
                        storageHandler, placementHandler, statusReporter);
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

    /** Métricas mínimas do {@link StorageRequestHandler} deste nó. */
    public StorageRequestHandler.StorageHandlerMetrics metricsSnapshot() {
        return storageHandler.metricsSnapshot();
    }

    /**
     * Encerra na ordem: reporter (pede cancelamento e desligamento do scheduler
     * — {@code shutdownNow()} + espera de até 5 s pela tarefa em andamento, mas
     * um tick que já estava publicando no catálogo pode terminar de forma
     * concorrente com as etapas seguintes, best-effort, não uma garantia dura)
     * → handlers (removidos do transporte/coordenador) → registry
     * (checkpoint+close de tudo que estiver aberto) → nó NGrid → registro de
     * volumes. Erros de cada etapa são logados, não propagados — um recurso não
     * fechado não deve impedir o fechamento dos demais.
     */
    @Override
    public void close() {
        safely("status reporter", statusReporter::close);
        safely("handlers", () -> {
            node.transport().removeListener(storageHandler);
            node.transport().removeListener(placementHandler);
            node.coordinator().removeLeadershipListener(placementHandler);
            rpc.unregisterLocalHandler(storageHandler);
            rpc.unregisterLocalHandler(placementHandler);
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
