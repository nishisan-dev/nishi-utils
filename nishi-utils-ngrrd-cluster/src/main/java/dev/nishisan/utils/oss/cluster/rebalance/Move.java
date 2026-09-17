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

import java.util.Objects;

/**
 * Um movimento planejado por {@link RebalancePlanner}: mover {@code seriesKey}
 * de {@code src} para {@code dst}.
 */
public record Move(String seriesKey, String src, String dst) {

    public Move {
        Objects.requireNonNull(seriesKey, "seriesKey é obrigatório");
        Objects.requireNonNull(src, "src é obrigatório");
        Objects.requireNonNull(dst, "dst é obrigatório");
        if (src.equals(dst)) {
            throw new IllegalArgumentException("src e dst não podem ser o mesmo nó: " + src);
        }
    }
}
