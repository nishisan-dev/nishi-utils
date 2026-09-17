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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Visão do catálogo consumida pelos colaboradores do líder ({@code PlacementRequestHandler},
 * {@code MigrationCoordinator}, {@code Rebalancer}) — isola a dependência de {@link CatalogService}
 * (que por sua vez envolve {@code DistributedMap}, classe final do core) para permitir fakes nos
 * testes unitários desses colaboradores. {@link CatalogService} implementa esta interface
 * diretamente: nenhum adaptador é necessário em produção.
 */
public interface CatalogView {

    /** Leitura forte (round-trip ao líder) do placement da série. */
    Optional<SeriesPlacement> placementStrong(String seriesKey);

    /** Snapshot local (eventual) do status de todos os storage nodes conhecidos. */
    Collection<StorageNodeStatus> nodesLocal();

    /** Cópia imutável do catálogo na visão local (eventual) do nó. */
    Map<String, SeriesPlacement> placementsLocal();

    /** Grava o placement da série; roteado ao líder pelo próprio {@code DistributedMap}. */
    void putPlacement(String seriesKey, SeriesPlacement placement);
}
