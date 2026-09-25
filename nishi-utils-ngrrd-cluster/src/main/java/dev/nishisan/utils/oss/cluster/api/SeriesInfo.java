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

package dev.nishisan.utils.oss.cluster.api;

import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;

/**
 * Placement de uma série existente, devolvido por {@link NgrrdClusterClient#find}: espelha o
 * {@link SeriesPlacement} do catálogo sem expor o tipo interno do pacote {@code catalog}.
 *
 * @param seriesKey        chave lógica da série
 * @param ownerNodeId      nó dono atual da série
 * @param state            estado da entrada no catálogo ({@code MIGRATING} também conta como
 *                         existente)
 * @param targetNodeId     nó de destino da migração em curso; {@code null} fora de migração
 * @param updatedAtEpochMs instante da última transição desta entrada no catálogo
 */
public record SeriesInfo(String seriesKey, String ownerNodeId, PlacementState state, String targetNodeId,
        long updatedAtEpochMs) {

    /** Constrói a partir do placement resolvido pelo cliente para {@code seriesKey}. */
    public static SeriesInfo of(String seriesKey, SeriesPlacement placement) {
        return new SeriesInfo(seriesKey, placement.ownerNodeId(), placement.state(), placement.targetNodeId(),
                placement.updatedAtEpochMs());
    }
}
