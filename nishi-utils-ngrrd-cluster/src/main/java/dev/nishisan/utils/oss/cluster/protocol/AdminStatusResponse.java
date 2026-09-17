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

package dev.nishisan.utils.oss.cluster.protocol;

import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Resposta de {@code ngrrd.admin.status}: visão geral do cluster segundo o líder.
 *
 * @param leaderNodeId        líder atual do cluster
 * @param nodes               status de cada storage node conhecido; nunca {@code null}
 * @param migrationsInFlight  quantidade de migrações em curso
 * @param seriesCountByNode   quantidade de séries por nó, segundo o catálogo; nunca {@code null}
 */
public record AdminStatusResponse(
        String leaderNodeId,
        List<StorageNodeStatus> nodes,
        int migrationsInFlight,
        Map<String, Long> seriesCountByNode) {

    public AdminStatusResponse {
        nodes = List.copyOf(Objects.requireNonNullElse(nodes, List.of()));
        seriesCountByNode = Map.copyOf(Objects.requireNonNullElse(seriesCountByNode, Map.of()));
    }
}
