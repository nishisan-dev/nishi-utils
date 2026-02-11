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
 * Callback interface invoked when a persistence operation fails.
 * <p>
 * By default, {@link NMap} uses a no-op listener. External components
 * (metrics, alerting, circuit breakers) can register a custom listener via
 * {@link NMapConfig.Builder#healthListener(NMapHealthListener)}.
 */
@FunctionalInterface
public interface NMapHealthListener {

    /**
     * Called when a persistence operation fails.
     *
     * @param mapName the name of the map whose persistence failed
     * @param type    the type of operation that failed
     * @param cause   the underlying exception
     */
    void onPersistenceFailure(String mapName, PersistenceFailureType type, Throwable cause);

    /**
     * Types of persistence operations that can fail.
     */
    enum PersistenceFailureType {
        /** Write-ahead log entry write failure */
        WAL_WRITE,
        /** WAL file open/creation failure */
        WAL_OPEN,
        /** Full snapshot write failure */
        SNAPSHOT_WRITE,
        /** Snapshot or WAL load failure */
        SNAPSHOT_LOAD,
        /** Metadata file write failure */
        META_WRITE
    }
}
