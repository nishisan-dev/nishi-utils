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
package dev.nishisan.utils.ngrid.queue;

import dev.nishisan.utils.ngrid.replication.ReplicationHandler.SnapshotChunk;
import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import dev.nishisan.utils.queue.NQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 8.10.1: a multi-chunk queue snapshot pages by LOGICAL index from a cursor opened on chunk 0. Up to
 * 8.10.0 each chunk re-read from the current head by position, so records consumed between two chunks
 * shifted every position and the records right after the served range were never sent.
 */
class QueueSnapshotSessionConsistencyTest {

    private static final int ITEMS = 2_500;

    @TempDir
    Path tempDir;

    @Test
    void recordsConsumedBetweenChunksDoNotShiftTheRemainingOnesOutOfTheTransfer() throws Exception {
        QueueClusterService<String> service = new QueueClusterService<>(tempDir, "snap-q", new ReplicationManager() {
        });
        try {
            List<String> items = new ArrayList<>();
            for (int i = 0; i < ITEMS; i++) {
                items.add("item-" + i);
            }
            service.installSnapshot(new ArrayList<Object>(items));

            String session = "follower::queue:snap-q";
            Set<Object> served = new LinkedHashSet<>();
            SnapshotChunk chunk = service.getSnapshotChunk(session, 0);
            served.addAll((List<?>) chunk.data());
            assertTrue(chunk.hasMore(), "the snapshot must span several chunks");

            // Concurrent consumption between chunks: the head advances by 300 records.
            for (int i = 0; i < 300; i++) {
                assertTrue(service.queue().poll().isPresent());
            }
            // And new records arrive after the capture (outside the cursor bound).
            service.queue().offer("late-0");

            int index = 1;
            while (chunk.hasMore()) {
                chunk = service.getSnapshotChunk(session, index++);
                assertNotNull(chunk, "a live session serves every chunk");
                served.addAll((List<?>) chunk.data());
            }

            List<String> missing = new ArrayList<>(items);
            missing.removeAll(served);
            assertTrue(missing.isEmpty(), missing.size() + " records present on chunk 0 were never served: "
                    + missing.subList(0, Math.min(5, missing.size())));
            assertEquals(ITEMS, served.size(), "the transfer serves exactly the records durable on chunk 0");
        } finally {
            service.close();
        }
    }

    @Test
    void aDuplicateOrOutOfOrderChunkRequestIsNotServed() throws Exception {
        QueueClusterService<String> service = new QueueClusterService<>(tempDir, "dup-q", new ReplicationManager() {
        });
        try {
            List<Object> items = new ArrayList<>();
            for (int i = 0; i < ITEMS; i++) {
                items.add("item-" + i);
            }
            service.installSnapshot(items);
            String session = "follower::queue:dup-q";
            assertTrue(service.getSnapshotChunk(session, 0).hasMore());
            assertNotNull(service.getSnapshotChunk(session, 1), "the expected next chunk is served");
            // A duplicate of chunk 1 would be served the page AFTER it (the cursor only moves forward) and
            // the requester would skip a page: it is refused, and the session is dropped.
            assertNull(service.getSnapshotChunk(session, 1), "a duplicate chunk request is not served");
            assertNull(service.getSnapshotChunk(session, 2), "the dropped session serves nothing more");
        } finally {
            service.close();
        }
    }

    @Test
    void theTransferStaysCorrectAcrossACompactionBetweenChunks() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withCompactionWasteThreshold(0.3d)
                .withCompactionInterval(Duration.ofMillis(1))
                .withCompactionBufferSize(4096);
        QueueClusterService<String> service = new QueueClusterService<>(tempDir, "compact-q", new ReplicationManager() {
        }, options);
        try {
            List<Object> items = new ArrayList<>();
            for (int i = 0; i < ITEMS; i++) {
                items.add(String.format("item-%05d", i)); // fixed-size records: offsets stay record-aligned
            }
            service.installSnapshot(items);

            String session = "follower::queue:compact-q";
            List<Object> served = new ArrayList<>();
            SnapshotChunk chunk = service.getSnapshotChunk(session, 0);
            served.addAll((List<?>) chunk.data());
            Path dataLog = tempDir.resolve("compact-q").resolve("data.log");
            long sizeBefore = Files.size(dataLog);

            // Consume past the served page and let the compaction rewrite the log: every byte offset moves.
            int consumed = 1_500;
            for (int i = 0; i < consumed; i++) {
                assertTrue(service.queue().poll().isPresent());
            }
            long deadline = System.currentTimeMillis() + 10_000;
            while (Files.size(dataLog) >= sizeBefore && System.currentTimeMillis() < deadline) {
                service.queue().peek();
                Thread.sleep(20);
            }
            assertTrue(Files.size(dataLog) < sizeBefore, "the log must have been compacted between chunks");

            int index = 1;
            while (chunk.hasMore()) {
                chunk = service.getSnapshotChunk(session, index++);
                assertNotNull(chunk, "a live session serves every chunk");
                served.addAll((List<?>) chunk.data());
            }
            // Served: the first page, then every record that stayed in the queue, in order, exactly once.
            List<Object> expected = new ArrayList<>(items.subList(0, 1_000));
            expected.addAll(items.subList(consumed, ITEMS));
            assertEquals(expected, served, "after the compaction the cursor must resume by logical index");
        } finally {
            service.close();
        }
    }
}
