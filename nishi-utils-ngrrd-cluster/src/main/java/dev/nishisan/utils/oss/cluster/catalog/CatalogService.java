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

package dev.nishisan.utils.oss.cluster.catalog;

import dev.nishisan.utils.ngrid.structures.Consistency;
import dev.nishisan.utils.ngrid.structures.DistributedMap;
import dev.nishisan.utils.ngrid.structures.NGridNode;
import dev.nishisan.utils.ngrid.structures.NGridNodeBuilder;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Fachada sobre os dois {@link DistributedMap} do catálogo do cluster ngrrd:
 * {@value #CATALOG_MAP} (placement por série) e {@value #NODES_MAP} (status dos
 * storage nodes).
 *
 * <p>Escrita sempre passa pelo líder — comportamento herdado de
 * {@link DistributedMap#put}/{@code remove}, que já encaminha automaticamente ao
 * líder quando o nó local é follower. Leitura local aqui é sempre eventual
 * ({@link Consistency#EVENTUAL}): reflete a cópia replicada no nó local, que pode
 * estar levemente atrasada em relação ao líder. {@link #placementStrong} força
 * leitura no líder para as decisões que exigem a visão mais recente (ex.: antes
 * de decidir um novo placement).
 */
public final class CatalogService {

    /** Nome do {@link DistributedMap} de placement por série. */
    public static final String CATALOG_MAP = "ngrrd.catalog";

    /** Nome do {@link DistributedMap} de status dos storage nodes. */
    public static final String NODES_MAP = "ngrrd.nodes";

    private final DistributedMap<String, SeriesPlacement> catalog;
    private final DistributedMap<String, StorageNodeStatus> nodes;

    public CatalogService(DistributedMap<String, SeriesPlacement> catalog,
            DistributedMap<String, StorageNodeStatus> nodes) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.nodes = Objects.requireNonNull(nodes, "nodes");
    }

    /**
     * Registra os dois mapas do catálogo no builder do nó. Necessário porque o
     * NGrid exige que todo mapa usado por um nó esteja declarado em todos os
     * participantes do cluster, sob pena de {@code UnknownMapRequestHandler}.
     */
    public static void declareMaps(NGridNodeBuilder builder) {
        builder.map(CATALOG_MAP).map(NODES_MAP);
    }

    /** Constrói o serviço a partir dos mapas já registrados e iniciados em {@code node}. */
    public static CatalogService from(NGridNode node) {
        DistributedMap<String, SeriesPlacement> catalogMap =
                node.getMap(CATALOG_MAP, String.class, SeriesPlacement.class);
        DistributedMap<String, StorageNodeStatus> nodesMap =
                node.getMap(NODES_MAP, String.class, StorageNodeStatus.class);
        return new CatalogService(catalogMap, nodesMap);
    }

    /** Leitura eventual do placement da série, a partir da cópia replicada local. */
    public Optional<SeriesPlacement> placementLocal(String seriesKey) {
        return catalog.getOptional(seriesKey, Consistency.EVENTUAL);
    }

    /** Leitura forte do placement da série, roteada ao líder quando o nó local não é líder. */
    public Optional<SeriesPlacement> placementStrong(String seriesKey) {
        return catalog.getOptional(seriesKey, Consistency.STRONG);
    }

    /** Grava o placement da série; roteado ao líder pelo próprio {@link DistributedMap}. */
    public void putPlacement(String seriesKey, SeriesPlacement placement) {
        catalog.put(seriesKey, placement);
    }

    /** Remove o placement da série; roteado ao líder pelo próprio {@link DistributedMap}. */
    public void removePlacement(String seriesKey) {
        catalog.remove(seriesKey);
    }

    /** Cópia imutável do catálogo na visão local (eventual) do nó. */
    public Map<String, SeriesPlacement> placementsLocal() {
        return catalog.entrySet().stream()
                .collect(Collectors.collectingAndThen(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue),
                        Map::copyOf));
    }

    /**
     * Séries {@link PlacementState#ACTIVE} agrupadas pelo respectivo dono, a
     * partir da visão local do catálogo — usado pelo Rebalancer para decidir
     * movimentações entre nós.
     */
    public Map<String, List<String>> seriesByOwnerLocal() {
        return catalog.entrySet().stream()
                .filter(entry -> entry.getValue().state() == PlacementState.ACTIVE)
                .collect(Collectors.collectingAndThen(
                        Collectors.groupingBy(
                                entry -> entry.getValue().ownerNodeId(),
                                Collectors.mapping(Map.Entry::getKey, Collectors.toUnmodifiableList())),
                        Map::copyOf));
    }

    /** Leitura eventual do status de um storage node específico. */
    public Optional<StorageNodeStatus> nodeStatusLocal(String nodeId) {
        return nodes.getOptional(nodeId, Consistency.EVENTUAL);
    }

    /** Snapshot local (eventual) do status de todos os storage nodes conhecidos. */
    public Collection<StorageNodeStatus> nodesLocal() {
        return nodes.values();
    }

    /** Publica/atualiza o status de um storage node; roteado ao líder pelo próprio {@link DistributedMap}. */
    public void putNodeStatus(StorageNodeStatus status) {
        nodes.put(status.nodeId(), status);
    }
}
