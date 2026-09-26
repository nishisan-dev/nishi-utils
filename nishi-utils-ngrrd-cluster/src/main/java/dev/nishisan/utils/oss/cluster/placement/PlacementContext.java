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
 * @param nowEpochMs            instante da decisão
 * @param nodeStatusStaleAfter  prazo completo além do qual o último status reportado de um nó deixa
 *                              de ser considerado fresco (ex.: {@code StorageNodeConfig.nodeStatusStaleAfter()},
 *                              tipicamente {@code max(5 × statusReportInterval, 15s)}) — não é mais o
 *                              intervalo "cru" entre relatórios: um handoff de liderança faz o status
 *                              mais recente visível ao NOVO líder já nascer "velho" se o prazo for
 *                              curto demais (só {@code 2 × interval}), descartando um nó legítimo.
 * @param preferredOwnerNodeId dono preferido (ex.: adoção pelo {@code LocalReconciler}), ou {@code null}
 * @param requestedBytes aligned allocation size of the new series
 * @param pendingBytesByNode incoming bytes not yet reflected in node reports
 * @param seriesKey            chave da série sendo colocada (issue #167, item 3) — usada pelas regras de
 *                             placement; {@code null} num contexto legado (sem regras aplicáveis)
 * @param definitionName       {@code metadata.name} da definição da série, ou {@code null} quando o cliente
 *                             não informou (série legada: só casa regras sem critério de definição)
 * @param placementRules       regras de placement do líder; nunca {@code null} ({@link PlacementRules#NONE})
 */
public record PlacementContext(
        Collection<StorageNodeStatus> nodes,
        Set<String> reachableNodeIds,
        Map<String, Long> pendingSeriesByNode,
        long nowEpochMs,
        Duration nodeStatusStaleAfter,
        String preferredOwnerNodeId, long requestedBytes, Map<String, Long> pendingBytesByNode,
        String seriesKey, String definitionName, PlacementRules placementRules) {

    public PlacementContext(Collection<StorageNodeStatus> nodes, Set<String> reachableNodeIds,
            Map<String, Long> pendingSeriesByNode, long nowEpochMs, Duration nodeStatusStaleAfter,
            String preferredOwnerNodeId) {
        this(nodes, reachableNodeIds, pendingSeriesByNode, nowEpochMs, nodeStatusStaleAfter, preferredOwnerNodeId, 0, Map.of());
    }

    /** Forma da 8.7.0, sem chave/definição/regras (nenhuma regra é aplicada). */
    public PlacementContext(Collection<StorageNodeStatus> nodes, Set<String> reachableNodeIds,
            Map<String, Long> pendingSeriesByNode, long nowEpochMs, Duration nodeStatusStaleAfter,
            String preferredOwnerNodeId, long requestedBytes, Map<String, Long> pendingBytesByNode) {
        this(nodes, reachableNodeIds, pendingSeriesByNode, nowEpochMs, nodeStatusStaleAfter, preferredOwnerNodeId,
                requestedBytes, pendingBytesByNode, null, null, PlacementRules.NONE);
    }

    public PlacementContext {
        if (requestedBytes < 0) { throw new IllegalArgumentException("negative requested bytes"); }
        placementRules = Objects.requireNonNullElse(placementRules, PlacementRules.NONE);
        pendingBytesByNode = Map.copyOf(pendingBytesByNode);
        Objects.requireNonNull(nodes, "nodes");
        Objects.requireNonNull(reachableNodeIds, "reachableNodeIds");
        Objects.requireNonNull(nodeStatusStaleAfter, "nodeStatusStaleAfter");
        // Cópia defensiva: o chamador (líder) não deve conseguir mutar o contexto depois de
        // construí-lo — a decisão de placement precisa ver sempre o mesmo snapshot.
        nodes = List.copyOf(nodes);
        reachableNodeIds = Set.copyOf(reachableNodeIds);
        pendingSeriesByNode = Map.copyOf(Objects.requireNonNullElse(pendingSeriesByNode, Map.of()));
    }
}
