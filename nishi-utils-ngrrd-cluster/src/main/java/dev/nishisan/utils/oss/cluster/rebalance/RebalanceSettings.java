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

/**
 * Parâmetros puros consumidos por {@link RebalancePlanner#plan}, extraídos de
 * {@code StorageNodeConfig} para manter o planejador livre de dependência do nó.
 *
 * @param rebalanceMinDelta   diferença mínima absoluta de séries entre o nó mais e o menos carregado
 *                            para disparar um movimento
 * @param rebalanceTolerance  diferença mínima relativa (fração da média) entre o nó mais e o menos
 *                            carregado para disparar um movimento
 * @param maxMovesPerCycle    teto de movimentos planejados neste ciclo
 * @param maxDestinationCatalogLag lag máximo da réplica do catálogo de um destino de migração (issue #177,
 *                            ver {@link CatalogLagGate}); {@code -1} desliga a porta, {@code 0} exige a
 *                            réplica em dia
 */
public record RebalanceSettings(long rebalanceMinDelta, double rebalanceTolerance, int maxMovesPerCycle,
        long maxDestinationCatalogLag) {

    /** Padrão de {@code ngrrd.rebalance.maxDestinationCatalogLag}. */
    public static final long DEFAULT_MAX_DESTINATION_CATALOG_LAG = 1_000L;

    /** Parâmetros sem a porta de lag explícita — usa {@link #DEFAULT_MAX_DESTINATION_CATALOG_LAG}. */
    public RebalanceSettings(long rebalanceMinDelta, double rebalanceTolerance, int maxMovesPerCycle) {
        this(rebalanceMinDelta, rebalanceTolerance, maxMovesPerCycle, DEFAULT_MAX_DESTINATION_CATALOG_LAG);
    }

    public RebalanceSettings {
        if (rebalanceMinDelta < 0) {
            throw new IllegalArgumentException("rebalanceMinDelta deve ser >= 0: " + rebalanceMinDelta);
        }
        if (!Double.isFinite(rebalanceTolerance) || rebalanceTolerance < 0) {
            throw new IllegalArgumentException("rebalanceTolerance deve ser >= 0: " + rebalanceTolerance);
        }
        if (maxMovesPerCycle <= 0) {
            throw new IllegalArgumentException("maxMovesPerCycle deve ser > 0: " + maxMovesPerCycle);
        }
        if (maxDestinationCatalogLag < -1) {
            throw new IllegalArgumentException("maxDestinationCatalogLag deve ser >= -1: " + maxDestinationCatalogLag);
        }
    }
}
