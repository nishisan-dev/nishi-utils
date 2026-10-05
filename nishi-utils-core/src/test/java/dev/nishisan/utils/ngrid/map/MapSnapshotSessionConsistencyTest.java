/*
 *  Copyright (C) 2020-2026 Lucas Nishimura <lucas.nishimura at gmail.com>
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

import dev.nishisan.utils.ngrid.replication.ReplicationHandler.SnapshotChunk;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 8.10.1: a multi-chunk map snapshot is served from ONE capture taken on chunk 0. Up to 8.10.0 every
 * chunk re-copied the live {@code ConcurrentHashMap} and sliced it by position, so removals and resizes
 * between two chunks shifted positions and keys that existed during the whole transfer were never sent.
 */
class MapSnapshotSessionConsistencyTest {

    private static final int INITIAL_KEYS = 3_500;

    @Test
    void keysShiftedByRemovalsOfServedKeysAreStillServed() {
        MapClusterService<String, String> service = newService();
        Map<String, String> initial = new HashMap<>();
        for (int i = 0; i < INITIAL_KEYS; i++) {
            initial.put("k-" + i, "v-" + i);
        }
        service.installSnapshot(MapReplicationCodec.encodeSnapshot(initial));

        String session = "follower::map:catalog";
        SnapshotChunk chunk = service.getSnapshotChunk(session, 0);
        Set<Object> served = new HashSet<>(MapReplicationCodec.decodeSnapshot((byte[]) chunk.data()).keySet());

        // Remove 600 keys that chunk 0 already served: without a resize the iteration order of the remaining
        // keys is unchanged, so every later position shifts left by 600 — with live slicing the 600 keys right
        // after the first chunk move into the range already served and are never sent.
        List<String> iterationOrder = new ArrayList<>(service.keySet());
        for (String key : iterationOrder.subList(0, 600)) {
            service.apply(UUID.randomUUID(), MapReplicationCodec.encode(MapReplicationCommand.remove(key)));
        }
        int index = 1;
        while (chunk.hasMore()) {
            chunk = service.getSnapshotChunk(session, index++);
            assertNotNull(chunk, "a live session serves every chunk");
            served.addAll(MapReplicationCodec.decodeSnapshot((byte[]) chunk.data()).keySet());
        }
        Set<Object> missing = new HashSet<>(initial.keySet());
        missing.removeAll(served);
        assertTrue(missing.isEmpty(), missing.size() + " keys still present during the transfer were never served");
    }

    @Test
    void everyKeyPresentOnChunkZeroIsServedDespiteRemovalsAndResizesDuringTheTransfer() {
        MapClusterService<String, String> service = newService();
        Map<String, String> initial = new HashMap<>();
        for (int i = 0; i < INITIAL_KEYS; i++) {
            initial.put("k-" + i, "v-" + i);
        }
        service.installSnapshot(MapReplicationCodec.encodeSnapshot(initial));

        String session = "follower::map:catalog";
        Set<Object> served = new HashSet<>();
        SnapshotChunk chunk = service.getSnapshotChunk(session, 0);
        served.addAll(MapReplicationCodec.decodeSnapshot((byte[]) chunk.data()).keySet());
        assertTrue(chunk.hasMore(), "the snapshot must span several chunks");

        // Concurrent writes between chunks: remove the first 600 keys in iteration order (positions shift
        // left) and insert enough new keys to force resizes (the iteration order is reshuffled).
        List<String> iterationOrder = new ArrayList<>(service.keySet());
        for (String key : iterationOrder.subList(0, 600)) {
            service.apply(UUID.randomUUID(), MapReplicationCodec.encode(MapReplicationCommand.remove(key)));
        }
        for (int i = 0; i < 20_000; i++) {
            service.apply(UUID.randomUUID(),
                    MapReplicationCodec.encode(MapReplicationCommand.put("new-" + i, "x")));
        }

        int index = 1;
        while (chunk.hasMore()) {
            chunk = service.getSnapshotChunk(session, index++);
            assertNotNull(chunk, "a live session serves every chunk");
            served.addAll(MapReplicationCodec.decodeSnapshot((byte[]) chunk.data()).keySet());
        }

        Set<Object> missing = new HashSet<>(initial.keySet());
        missing.removeAll(served);
        assertTrue(missing.isEmpty(), missing.size() + " keys present on chunk 0 were never served");
        assertEquals(INITIAL_KEYS, served.size(), "the transfer serves exactly the chunk-0 capture");
        assertNull(service.getSnapshotChunk(session, index), "the last chunk releases the session");
    }

    private static MapClusterService<String, String> newService() {
        // Bare manager (test constructor): no transport, no persistence; registerHandler only records it.
        ReplicationManager manager = new ReplicationManager() {
        };
        return new MapClusterService<>(manager, "map:catalog", null);
    }
}
