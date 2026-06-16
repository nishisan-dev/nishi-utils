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

/**
 * Declarative selector for the {@link NMap} offloading backend, configured
 * through {@link NMapConfig}. It lets callers pick a strategy and tune its
 * memory/disk knobs without having to assemble an
 * {@link NMapOffloadStrategyFactory} by hand.
 * <p>
 * A custom {@link NMapConfig#offloadStrategyFactory()} always takes precedence
 * over this mode, remaining the extension point for bespoke strategies.
 */
public enum OffloadMode {

    /**
     * Heap-only storage ({@link InMemoryStrategy}). WAL + snapshot persistence
     * applies when the persistence mode is not
     * {@link NMapPersistenceMode#DISABLED}.
     */
    IN_MEMORY,

    /**
     * One file per entry on disk ({@link DiskOffloadStrategy}) with a bounded
     * hot cache and configurable shard fan-out. Inherently persistent.
     */
    DISK,

    /**
     * Hot entries in memory, cold entries spilled to disk
     * ({@link HybridOffloadStrategy}) by a configurable eviction policy.
     * Inherently persistent.
     */
    HYBRID,

    /**
     * Log-structured store ({@link SegmentOffloadStrategy}): entries are grouped
     * into a small, fixed number of append-only segment files, collapsing the
     * inode/file count at scale. Supports optional LZ4 value compression and
     * background compaction. Inherently persistent.
     */
    SEGMENT
}
