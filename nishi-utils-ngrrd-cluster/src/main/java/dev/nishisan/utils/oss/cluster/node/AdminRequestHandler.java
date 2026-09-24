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

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.admin.AdminService;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.protocol.AdminNodeRequest;
import dev.nishisan.utils.oss.cluster.protocol.AdminNodeStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.AdminRebalanceResponse;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.NodeStatusView;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.rebalance.MigrationCoordinator;
import dev.nishisan.utils.oss.cluster.rebalance.Rebalancer;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import dev.nishisan.utils.oss.cluster.rpc.RequestHandlerSupport;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Atende {@link Commands#ADMIN_STATUS} (só líder) e {@link Commands#ADMIN_METRICS} (qualquer nó).
 *
 * <p>{@code ADMIN_STATUS}: fora do líder, responde {@link SeriesStatus#NOT_LEADER} com o
 * {@code leaderNodeId} conhecido (mesmo padrão de {@code PlacementRequestHandler}/{@code PlaceResponse}).
 * No líder, monta {@link AdminStatusResponse} a partir da visão local (eventual) do catálogo —
 * {@link PlacementRequestHandler.LeaderView#reachableNodeIds()} decide quem entra marcado
 * {@code reachable=true}.</p>
 *
 * <p>{@code ADMIN_METRICS}: {@code nodeId} nulo ou igual a este nó devolve o
 * {@link NodeMetricsSnapshot} local; um {@code nodeId} de outro nó é encaminhado por
 * {@link ClusterRpc#call} — no máximo um salto: {@link AdminNodeRequest#forwarded()} marcado
 * impede um segundo encaminhamento em cadeia, mesmo que o catálogo local deste nó intermediário
 * esteja desatualizado sobre quem é o dono verdadeiro do {@code nodeId} pedido.</p>
 *
 * <p>{@code ADMIN_DRAIN}/{@code ADMIN_ACTIVATE}: só o líder responde, mesmo padrão
 * {@code NOT_LEADER}/{@code leaderNodeId} de {@code ADMIN_STATUS}/{@code ADMIN_REBALANCE}; delega a
 * transição em si a {@link AdminService}, que já dispara um ciclo do {@link Rebalancer}. Um
 * {@code nodeId} desconhecido do catálogo responde {@link SeriesStatus#ERROR}.</p>
 */
public final class AdminRequestHandler extends RequestHandlerSupport {

    private final NodeId self;
    private final PlacementRequestHandler.LeaderView leaderView;
    private final CatalogService catalog;
    private final Supplier<NodeMetricsSnapshot> localMetricsSupplier;
    private final ClusterRpc rpc;
    private final Rebalancer rebalancer;
    private final AdminService adminService;
    private final MigrationCoordinator migrationCoordinator;

    public AdminRequestHandler(Transport transport, NodeId self, PlacementRequestHandler.LeaderView leaderView,
            CatalogService catalog, Supplier<NodeMetricsSnapshot> localMetricsSupplier, ClusterRpc rpc,
            Rebalancer rebalancer, AdminService adminService, MigrationCoordinator migrationCoordinator) {
        super(transport, Set.of(Commands.ADMIN_STATUS, Commands.ADMIN_METRICS, Commands.ADMIN_REBALANCE,
                Commands.ADMIN_DRAIN, Commands.ADMIN_ACTIVATE));
        this.self = Objects.requireNonNull(self, "self");
        this.leaderView = Objects.requireNonNull(leaderView, "leaderView");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.localMetricsSupplier = Objects.requireNonNull(localMetricsSupplier, "localMetricsSupplier");
        this.rpc = Objects.requireNonNull(rpc, "rpc");
        this.rebalancer = Objects.requireNonNull(rebalancer, "rebalancer");
        this.adminService = Objects.requireNonNull(adminService, "adminService");
        this.migrationCoordinator = Objects.requireNonNull(migrationCoordinator, "migrationCoordinator");
    }

    @Override
    protected Object handle(String command, Object body, NodeId source) {
        return switch (command) {
            case Commands.ADMIN_STATUS -> handleStatus();
            case Commands.ADMIN_METRICS -> handleMetrics((AdminNodeRequest) body);
            case Commands.ADMIN_REBALANCE -> handleRebalance();
            case Commands.ADMIN_DRAIN -> handleDrain((AdminNodeRequest) body);
            case Commands.ADMIN_ACTIVATE -> handleActivate((AdminNodeRequest) body);
            default -> throw new IllegalArgumentException("Comando não suportado por AdminRequestHandler: " + command);
        };
    }

    private AdminNodeStatusResponse handleDrain(AdminNodeRequest request) {
        return handleTransition(request, adminService::drain);
    }

    private AdminNodeStatusResponse handleActivate(AdminNodeRequest request) {
        return handleTransition(request, adminService::activate);
    }

    private AdminNodeStatusResponse handleTransition(AdminNodeRequest request,
            Function<String, StorageNodeStatus> transition) {
        if (!leaderView.isLeader()) {
            return new AdminNodeStatusResponse(SeriesStatus.NOT_LEADER, leaderView.leaderId().orElse(null), null, null);
        }
        try {
            StorageNodeStatus updated = transition.apply(request.nodeId());
            return new AdminNodeStatusResponse(SeriesStatus.OK, self.value(), updated, null);
        } catch (RuntimeException e) {
            return new AdminNodeStatusResponse(SeriesStatus.ERROR, self.value(), null, e.getMessage());
        }
    }

    private AdminRebalanceResponse handleRebalance() {
        if (!leaderView.isLeader()) {
            return new AdminRebalanceResponse(SeriesStatus.NOT_LEADER, leaderView.leaderId().orElse(null), 0, 0);
        }
        Rebalancer.TriggerResult result = rebalancer.triggerNow();
        return new AdminRebalanceResponse(SeriesStatus.OK, self.value(), result.planned(), result.started());
    }

    private AdminStatusResponse handleStatus() {
        if (!leaderView.isLeader()) {
            return new AdminStatusResponse(SeriesStatus.NOT_LEADER, leaderView.leaderId().orElse(null),
                    List.of(), 0, Map.of());
        }
        Collection<StorageNodeStatus> nodes = catalog.nodesLocal();
        Set<String> reachable = leaderView.reachableNodeIds();
        List<NodeStatusView> views = nodes.stream()
                .map(status -> new NodeStatusView(status, reachable.contains(status.nodeId())))
                .toList();
        Map<String, Long> seriesCountByNode = catalog.seriesByOwnerLocal().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> (long) entry.getValue().size()));
        return new AdminStatusResponse(SeriesStatus.OK, self.value(), views,
                migrationCoordinator.activeMigrationCount(), seriesCountByNode,
                catalog.placementsLocal().values().stream().filter(p -> !p.geometryConfirmed()).count());
    }

    private NodeMetricsSnapshot handleMetrics(AdminNodeRequest request) {
        String targetNodeId = request != null ? request.nodeId() : null;
        if (targetNodeId == null || targetNodeId.equals(self.value())) {
            return localMetricsSupplier.get();
        }
        boolean alreadyForwarded = request.forwarded();
        if (alreadyForwarded) {
            throw new IllegalStateException("ngrrd.admin.metrics já encaminhado uma vez; "
                    + self.value() + " não encadeia para " + targetNodeId);
        }
        return rpc.call(NodeId.of(targetNodeId), Commands.ADMIN_METRICS,
                new AdminNodeRequest(targetNodeId, true), NodeMetricsSnapshot.class);
    }
}
