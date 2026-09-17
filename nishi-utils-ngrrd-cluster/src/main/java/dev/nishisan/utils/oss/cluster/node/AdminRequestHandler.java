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
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.protocol.AdminNodeRequest;
import dev.nishisan.utils.oss.cluster.protocol.AdminRebalanceResponse;
import dev.nishisan.utils.oss.cluster.protocol.AdminStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.NodeStatusView;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.rebalance.Rebalancer;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;
import dev.nishisan.utils.oss.cluster.rpc.RequestHandlerSupport;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
 */
public final class AdminRequestHandler extends RequestHandlerSupport {

    private final NodeId self;
    private final PlacementRequestHandler.LeaderView leaderView;
    private final CatalogService catalog;
    private final Supplier<NodeMetricsSnapshot> localMetricsSupplier;
    private final ClusterRpc rpc;
    private final Rebalancer rebalancer;

    public AdminRequestHandler(Transport transport, NodeId self, PlacementRequestHandler.LeaderView leaderView,
            CatalogService catalog, Supplier<NodeMetricsSnapshot> localMetricsSupplier, ClusterRpc rpc,
            Rebalancer rebalancer) {
        super(transport, Set.of(Commands.ADMIN_STATUS, Commands.ADMIN_METRICS, Commands.ADMIN_REBALANCE));
        this.self = Objects.requireNonNull(self, "self");
        this.leaderView = Objects.requireNonNull(leaderView, "leaderView");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.localMetricsSupplier = Objects.requireNonNull(localMetricsSupplier, "localMetricsSupplier");
        this.rpc = Objects.requireNonNull(rpc, "rpc");
        this.rebalancer = Objects.requireNonNull(rebalancer, "rebalancer");
    }

    @Override
    protected Object handle(String command, Object body, NodeId source) {
        return switch (command) {
            case Commands.ADMIN_STATUS -> handleStatus();
            case Commands.ADMIN_METRICS -> handleMetrics((AdminNodeRequest) body);
            case Commands.ADMIN_REBALANCE -> handleRebalance();
            default -> throw new IllegalArgumentException("Comando não suportado por AdminRequestHandler: " + command);
        };
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
        return new AdminStatusResponse(SeriesStatus.OK, self.value(), views, 0, seriesCountByNode);
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
