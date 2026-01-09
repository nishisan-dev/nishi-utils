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

package dev.nishisan.utils.ngrid.replication;

import java.io.Serializable;
import java.util.UUID;

/**
 * Callback invoked to apply a replicated operation locally or manage state snapshots.
 */
public interface ReplicationHandler {
    /**
     * Applies a single replicated operation.
     */
    void apply(UUID operationId, Serializable payload) throws Exception;

    /**
     * Returns a snapshot chunk.
     * @param chunkIndex Index of the chunk requested.
     * @return A SnapshotChunk or null if no handler.
     */
    default SnapshotChunk getSnapshotChunk(int chunkIndex) {
        Serializable data = (chunkIndex == 0) ? getSnapshot() : null;
        return (data != null) ? new SnapshotChunk(data, false) : null;
    }

    /**
     * Returns a full snapshot of the current state.
     * @deprecated Use {@link #getSnapshotChunk(int)} for better scalability.
     */
    @Deprecated
    default Serializable getSnapshot() {
        return null;
    }

    /**
     * Resets the local state, typically before installing a multi-chunk snapshot.
     */
    default void resetState() throws Exception {
        // no-op by default
    }

    /**
     * Installs a full snapshot or a chunk of it.
     */
    default void installSnapshot(Serializable snapshot) throws Exception {
        // no-op by default
    }

    record SnapshotChunk(Serializable data, boolean hasMore) implements Serializable {}
}
