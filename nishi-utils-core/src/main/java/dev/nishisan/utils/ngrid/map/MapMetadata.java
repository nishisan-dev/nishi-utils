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

package dev.nishisan.utils.ngrid.map;

/**
 * Metadata persisted alongside the snapshot/WAL files.
 *
 * @param lastSnapshotOffset    the offset of the last snapshot
 * @param lastSnapshotTimestamp the timestamp of the last snapshot
 * @param version               the metadata format version
 */
public record MapMetadata(
                long lastSnapshotOffset,
                long lastSnapshotTimestamp,
                int version) {
}
