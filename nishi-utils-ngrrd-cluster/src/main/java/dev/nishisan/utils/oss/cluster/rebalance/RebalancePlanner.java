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

import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pure deterministic planner shared by drain and regular balancing. Exact-size planning accounts
 * for incoming allocations without crediting outgoing bytes before FINISH. COUNT keeps the legacy
 * count-difference threshold; CAPACITY/WEIGHT use proportional targets. Unknown geometry is skipped.
 */
public final class RebalancePlanner {

    private RebalancePlanner() {
    }

    /**
     * @param nodes          status conhecido (visão local do líder) de todos os storage nodes
     * @param seriesByOwner  séries {@code ACTIVE}, agrupadas pelo dono atual (ver
     *                       {@code CatalogService#seriesByOwnerLocal})
     * @param reachable      nós atualmente alcançáveis segundo o {@code ClusterCoordinator}/{@code Transport}
     * @param migratingKeys  chaves de série já em migração — nunca replanejadas
     * @param settings       limites do ciclo
     * @return movimentos planejados, na ordem em que devem ser submetidos
     */
    public static List<Move> plan(Collection<StorageNodeStatus> nodes, Map<String, List<String>> seriesByOwner,
            Set<String> reachable, Set<String> migratingKeys, RebalanceSettings settings) {
        return plan(nodes, seriesByOwner, reachable, migratingKeys, settings, Set.of());
    }

    /**
     * Como {@link #plan(Collection, Map, Set, Set, RebalanceSettings)}, sem nunca escolher como destino um nó
     * de {@code excludedDestinations} (issue #177, ver {@link CatalogLagGate}). O nó excluído continua na
     * distribuição alvo e pode ser origem; sem nenhum destino elegível, o plano sai vazio.
     */
    public static List<Move> plan(Collection<StorageNodeStatus> nodes, Map<String, List<String>> seriesByOwner,
            Set<String> reachable, Set<String> migratingKeys, RebalanceSettings settings,
            Set<String> excludedDestinations) {
        Map<String, Long> unknownSizes = new HashMap<>();
        seriesByOwner.values().forEach(keys -> keys.forEach(key -> unknownSizes.put(key, 0L)));
        return plan(nodes, seriesByOwner, reachable, migratingKeys, settings, unknownSizes, Map.of(), Map.of(),
                excludedDestinations);
    }

    /** Plans with exact confirmed allocation sizes and outstanding incoming budgets. */
    public static List<Move> plan(Collection<StorageNodeStatus> nodes, Map<String, List<String>> seriesByOwner,
            Set<String> reachable, Set<String> migratingKeys, RebalanceSettings settings,
            Map<String, Long> regionBytesBySeries, Map<String, Long> pendingBytesByNode,
            Map<String, Long> pendingSeriesByNode) {
        return plan(nodes, seriesByOwner, reachable, migratingKeys, settings, regionBytesBySeries,
                pendingBytesByNode, pendingSeriesByNode, Set.of());
    }

    /**
     * Como o planejamento com tamanhos exatos, sem nunca escolher como destino um nó de
     * {@code excludedDestinations} (issue #177).
     */
    public static List<Move> plan(Collection<StorageNodeStatus> nodes, Map<String, List<String>> seriesByOwner,
            Set<String> reachable, Set<String> migratingKeys, RebalanceSettings settings,
            Map<String, Long> regionBytesBySeries, Map<String, Long> pendingBytesByNode,
            Map<String, Long> pendingSeriesByNode, Set<String> excludedDestinations) {
        return new CapacityAwarePlanner(nodes, seriesByOwner, reachable, migratingKeys, settings,
                regionBytesBySeries, pendingBytesByNode, pendingSeriesByNode, excludedDestinations).plan();
    }
}
