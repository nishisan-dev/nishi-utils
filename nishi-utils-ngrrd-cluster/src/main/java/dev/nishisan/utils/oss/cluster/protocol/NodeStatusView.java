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

import java.util.Objects;

/**
 * Um {@link StorageNodeStatus} do catálogo, anotado com se o líder que montou
 * a resposta de {@code ngrrd.admin.status} enxerga o nó como alcançável agora
 * (via {@code ClusterCoordinator}/{@code Transport}) — {@code state} sozinho
 * (do catálogo replicado) não distingue "caiu agora mesmo" de "ainda não
 * reportou de novo".
 *
 * @param status    último status publicado pelo nó
 * @param reachable se o líder considera este nó alcançável no instante da resposta
 */
public record NodeStatusView(StorageNodeStatus status, boolean reachable) {

    public NodeStatusView {
        Objects.requireNonNull(status, "status é obrigatório");
    }
}
