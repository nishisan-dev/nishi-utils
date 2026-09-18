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

/**
 * Estado operacional de um storage node, reportado em {@link StorageNodeStatus}.
 */
public enum NodeState {

    /** Recebe novos placements e atende requisições normalmente. */
    ACTIVE,

    /** Não recebe novos placements; suas séries estão sendo migradas para fora. */
    DRAINING,

    /** Todas as séries já foram migradas para fora; pronto para remoção do cluster. */
    DRAINED
}
