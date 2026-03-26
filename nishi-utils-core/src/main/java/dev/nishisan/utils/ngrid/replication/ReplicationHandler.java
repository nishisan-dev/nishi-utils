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

import java.util.UUID;

/**
 * Callback invoked to apply a replicated operation locally or manage state
 * snapshots.
 */
public interface ReplicationHandler {
    /**
     * Applies a single replicated operation.
     *
     * @param operationId the operation identifier
     * @param payload     the operation payload
     * @throws Exception if the operation cannot be applied
     */
    void apply(UUID operationId, Object payload) throws Exception;

    /**
     * Returns a snapshot chunk.
     * 
     * @param chunkIndex Index of the chunk requested.
     * @return A SnapshotChunk or null if no handler.
     */
    default SnapshotChunk getSnapshotChunk(int chunkIndex) {
        Object data = (chunkIndex == 0) ? getSnapshot() : null;
        return (data != null) ? new SnapshotChunk(data, false) : null;
    }

    /**
     * Returns a full snapshot of the current state.
     *
     * @return the snapshot, or {@code null} if unavailable
     * @deprecated Use {@link #getSnapshotChunk(int)} for better scalability.
     */
    @Deprecated
    default Object getSnapshot() {
        return null;
    }

    /**
     * Resets the local state, typically before installing a multi-chunk snapshot.
     *
     * @throws Exception if the state cannot be reset
     */
    default void resetState() throws Exception {
        // no-op by default
    }

    /**
     * Installs a full snapshot or a chunk of it.
     *
     * @param snapshot the snapshot data to install
     * @throws Exception if the snapshot cannot be installed
     */
    default void installSnapshot(Object snapshot) throws Exception {
        // no-op by default
    }

    record SnapshotChunk(Object data, boolean hasMore) {
    }

    /**
     * Called when the local node is promoted to leader.
     * Handlers can use this callback to perform any necessary state cleanup
     * (e.g., truncating stale persistent data) or initialization required
     * exclusively by the leader.
     *
     * @throws Exception if an error occurs during the callback
     */
    default void onBecameLeader() throws Exception {
    }
}
