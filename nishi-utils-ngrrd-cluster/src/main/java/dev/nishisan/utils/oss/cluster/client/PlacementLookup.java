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

package dev.nishisan.utils.oss.cluster.client;

import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;

/**
 * Resolução e correção de placement consumida por {@link RemoteSeriesHandle} e
 * {@link WriteDispatcher} — isola a dependência de {@link PlacementResolver}
 * (que por sua vez depende de {@code CatalogService}, classe concreta sobre um
 * {@code NGridNode} real) para permitir fakes nos testes unitários desses dois
 * colaboradores.
 */
public interface PlacementLookup {

    /**
     * Placement atual da série, resolvendo com o líder ({@code ngrrd.place})
     * quando ainda não conhecido.
     */
    SeriesPlacement resolve(String seriesKey, String definitionHashHex);

    /** Descarta o override local conhecido para {@code seriesKey}, se houver. */
    void invalidate(String seriesKey);

    /**
     * Atualiza o override local para {@code ACTIVE(ownerNodeId)} sem consultar
     * o líder — usado quando uma resposta {@code WRONG_OWNER} já traz o dono
     * correto.
     */
    void noteOwner(String seriesKey, String ownerNodeId);
}
