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

package dev.nishisan.utils.oss.cluster.placement;

import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Contexto usado por {@link PlacementPolicy#choose} para decidir o storage node
 * alvo de uma nova série.
 *
 * @param nodes                status conhecido (visão local do líder) de todos os storage nodes
 * @param reachableNodeIds     nós atualmente membros ativos do cluster ({@code ClusterCoordinator})
 * @param pendingSeriesByNode  séries já colocadas pelo líder em cada nó desde o último
 *                             {@code reportedAtEpochMs} daquele nó; nunca {@code null}. O status de
 *                             carga é reportado a cada ~10 s — sem esse ajuste, todas as séries
 *                             novas de uma rajada de placements cairiam no mesmo nó antes do
 *                             próximo relatório. A carga efetiva de um nó é
 *                             {@code seriesCount + pendingSeriesByNode.get(nodeId)}.
 * @param nowEpochMs           instante da decisão
 * @param statusReportInterval intervalo esperado entre relatórios de status dos nós
 * @param preferredOwnerNodeId dono preferido (ex.: adoção pelo {@code LocalReconciler}), ou {@code null}
 */
public record PlacementContext(
        Collection<StorageNodeStatus> nodes,
        Set<String> reachableNodeIds,
        Map<String, Long> pendingSeriesByNode,
        long nowEpochMs,
        Duration statusReportInterval,
        String preferredOwnerNodeId) {

    public PlacementContext {
        Objects.requireNonNull(nodes, "nodes");
        Objects.requireNonNull(reachableNodeIds, "reachableNodeIds");
        Objects.requireNonNull(statusReportInterval, "statusReportInterval");
        // Cópia defensiva: o chamador (líder) não deve conseguir mutar o contexto depois de
        // construí-lo — a decisão de placement precisa ver sempre o mesmo snapshot.
        nodes = List.copyOf(nodes);
        reachableNodeIds = Set.copyOf(reachableNodeIds);
        pendingSeriesByNode = Map.copyOf(Objects.requireNonNullElse(pendingSeriesByNode, Map.of()));
    }
}
