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

package dev.nishisan.utils.map;

import java.io.Serializable;

/**
 * A single WAL entry representing a mutating operation (PUT/REMOVE).
 *
 * @param timestamp the time the operation was recorded
 * @param type      the operation type (PUT or REMOVE)
 * @param key       the entry key
 * @param value     the entry value, or {@code null} for REMOVE operations
 */
public record NMapWALEntry(
        long timestamp,
        NMapOperationType type,
        Serializable key,
        Serializable value) {
}
