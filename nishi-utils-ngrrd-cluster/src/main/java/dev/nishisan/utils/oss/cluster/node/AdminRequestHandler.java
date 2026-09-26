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
import dev.nishisan.utils.oss.cluster.placement.PlacementRules;
import dev.nishisan.utils.oss.cluster.protocol.AdminForgetResponse;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
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
 *
 * <p>{@code ADMIN_FORGET} (revisão #178, B9; desde a 8.8.0): sem {@link AdminNodeRequest#forwarded()},
 * só o líder responde — recusa ({@code ERROR}) se o nó ainda é o próprio líder, ainda está alcançável ou
 * ainda tem séries/migrações de entrada no catálogo (drene antes); senão propaga a ordem, com
 * {@code forwarded=true}, a cada storage alcançável do catálogo, esquece o peer localmente
 * ({@code NGridNode.decommissionPeer}) e remove o nó do catálogo. Com {@code forwarded=true}, qualquer nó
 * apenas esquece o peer no seu {@code Transport}. Sem um {@code peerDecommissioner} configurado, responde
 * {@code ERROR}.</p>
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
    /** Regras de placement deste nó (issue #167, item 3), reportadas em {@code ngrrd.admin.status} quando líder. */
    private final PlacementRules placementRules;
    /** Esquece um peer votante no transporte deste nó ({@code ngrrd.admin.forget}); {@code null} = não suportado. */
    private final Predicate<String> peerDecommissioner;

    public AdminRequestHandler(Transport transport, NodeId self, PlacementRequestHandler.LeaderView leaderView,
            CatalogService catalog, Supplier<NodeMetricsSnapshot> localMetricsSupplier, ClusterRpc rpc,
            Rebalancer rebalancer, AdminService adminService, MigrationCoordinator migrationCoordinator) {
        this(transport, self, leaderView, catalog, localMetricsSupplier, rpc, rebalancer, adminService,
                migrationCoordinator, PlacementRules.NONE);
    }

    /**
     * @param placementRules regras de placement deste nó ({@code ngrrd.placement.rules}, issue #167 item 3),
     *                       cujo fingerprint e contagem saem em {@code ngrrd.admin.status}; {@code null} = nenhuma
     */
    public AdminRequestHandler(Transport transport, NodeId self, PlacementRequestHandler.LeaderView leaderView,
            CatalogService catalog, Supplier<NodeMetricsSnapshot> localMetricsSupplier, ClusterRpc rpc,
            Rebalancer rebalancer, AdminService adminService, MigrationCoordinator migrationCoordinator,
            PlacementRules placementRules) {
        this(transport, self, leaderView, catalog, localMetricsSupplier, rpc, rebalancer, adminService,
                migrationCoordinator, placementRules, null);
    }

    /**
     * @param peerDecommissioner esquece o peer votante {@code nodeId} no transporte deste nó
     *                           ({@code NGridNode.decommissionPeer}, revisão #178 B9), devolvendo se ele era
     *                           conhecido; {@code null} = {@code ngrrd.admin.forget} responde {@code ERROR}
     * @since 8.8.0
     */
    public AdminRequestHandler(Transport transport, NodeId self, PlacementRequestHandler.LeaderView leaderView,
            CatalogService catalog, Supplier<NodeMetricsSnapshot> localMetricsSupplier, ClusterRpc rpc,
            Rebalancer rebalancer, AdminService adminService, MigrationCoordinator migrationCoordinator,
            PlacementRules placementRules, Predicate<String> peerDecommissioner) {
        super(transport, Set.of(Commands.ADMIN_STATUS, Commands.ADMIN_METRICS, Commands.ADMIN_REBALANCE,
                Commands.ADMIN_DRAIN, Commands.ADMIN_ACTIVATE, Commands.ADMIN_FORGET));
        this.placementRules = Objects.requireNonNullElse(placementRules, PlacementRules.NONE);
        this.peerDecommissioner = peerDecommissioner;
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
            case Commands.ADMIN_FORGET -> handleForget((AdminNodeRequest) body);
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

    private AdminForgetResponse handleForget(AdminNodeRequest request) {
        String target = request != null ? request.nodeId() : null;
        if (target == null || target.isBlank()) {
            return AdminForgetResponse.of(SeriesStatus.ERROR, null, target, "nodeId é obrigatório");
        }
        if (peerDecommissioner == null) {
            return AdminForgetResponse.of(SeriesStatus.ERROR, null, target,
                    "ngrrd.admin.forget não suportado por " + self.value());
        }
        if (request.forwarded()) {
            // Ordem propagada pelo líder: só o transporte local. "Já desconhecido" também é sucesso.
            peerDecommissioner.test(target);
            return new AdminForgetResponse(SeriesStatus.OK, null, target, List.of(self.value()), List.of(), null);
        }
        if (!leaderView.isLeader()) {
            return AdminForgetResponse.of(SeriesStatus.NOT_LEADER, leaderView.leaderId().orElse(null), target, null);
        }
        if (target.equals(self.value())) {
            return AdminForgetResponse.of(SeriesStatus.ERROR, self.value(), target,
                    "o líder não esquece a si mesmo; pare este nó e repita o comando a partir de outro");
        }
        if (leaderView.reachableNodeIds().contains(target)) {
            return AdminForgetResponse.of(SeriesStatus.ERROR, self.value(), target,
                    "nó ainda alcançável; pare o processo (drenado) antes de esquecê-lo");
        }
        long owned = catalog.seriesByOwnerLocal().getOrDefault(target, List.of()).size();
        long inbound = catalog.placementsLocal().values().stream()
                .filter(placement -> target.equals(placement.targetNodeId())).count();
        if (owned > 0 || inbound > 0) {
            return AdminForgetResponse.of(SeriesStatus.ERROR, self.value(), target,
                    "nó ainda tem " + owned + " série(s) e " + inbound + " migração(ões) de entrada no catálogo;"
                            + " drene-o (ngrrd.admin.drain) e aguarde DRAINED antes de esquecê-lo");
        }
        List<String> forgottenOn = new ArrayList<>();
        List<String> failedOn = new ArrayList<>();
        Set<String> reachable = leaderView.reachableNodeIds();
        for (StorageNodeStatus status : catalog.nodesLocal()) {
            String peer = status.nodeId();
            if (peer.equals(target) || peer.equals(self.value())) {
                continue;
            }
            if (!reachable.contains(peer)) {
                failedOn.add(peer);
                continue;
            }
            try {
                AdminForgetResponse response = rpc.call(NodeId.of(peer), Commands.ADMIN_FORGET,
                        new AdminNodeRequest(target, true), AdminForgetResponse.class);
                if (response != null && response.status() == SeriesStatus.OK) {
                    forgottenOn.add(peer);
                } else {
                    failedOn.add(peer);
                }
            } catch (RuntimeException e) {
                failedOn.add(peer);
            }
        }
        peerDecommissioner.test(target);
        forgottenOn.add(self.value());
        catalog.removeNodeStatus(target);
        return new AdminForgetResponse(SeriesStatus.OK, self.value(), target, forgottenOn, failedOn,
                failedOn.isEmpty() ? null : "repita o comando quando voltarem: " + String.join(", ", failedOn));
    }

    private AdminRebalanceResponse handleRebalance() {
        if (!leaderView.isLeader()) {
            return new AdminRebalanceResponse(SeriesStatus.NOT_LEADER, leaderView.leaderId().orElse(null), 0, 0);
        }
        Rebalancer.TriggerResult result = rebalancer.triggerNow();
        return new AdminRebalanceResponse(SeriesStatus.OK, self.value(), result.planned(), result.started(),
                result.excludedDestinations());
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
                catalog.placementsLocal().values().stream().filter(p -> !p.geometryConfirmed()).count(),
                placementRules.fingerprint(), placementRules.size());
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
