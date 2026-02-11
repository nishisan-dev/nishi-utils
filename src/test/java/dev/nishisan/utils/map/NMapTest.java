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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NMapTest {

    @TempDir
    Path tempDir;

    @Test
    void putAndGetShouldWorkWithPersistence() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(0)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(10)
                .batchTimeout(Duration.ofMillis(5))
                .build();

        try (NMap<String, String> map = NMap.open(tempDir, "test-put", cfg)) {
            map.put("a", "1");
            map.put("b", "2");
            assertEquals(Optional.of("1"), map.get("a"));
            assertEquals(Optional.of("2"), map.get("b"));
            assertEquals(2, map.size());
            assertTrue(map.containsKey("a"));
            assertFalse(map.containsKey("c"));
        }
    }

    @Test
    void removeShouldReturnPreviousValue() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(0)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(10)
                .batchTimeout(Duration.ofMillis(5))
                .build();

        try (NMap<String, String> map = NMap.open(tempDir, "test-remove", cfg)) {
            map.put("x", "10");
            Optional<String> removed = map.remove("x");
            assertTrue(removed.isPresent());
            assertEquals("10", removed.get());
            assertFalse(map.containsKey("x"));
            assertTrue(map.isEmpty());
        }
    }

    @Test
    void walShouldReplayOnRestart() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(0)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(10)
                .batchTimeout(Duration.ofMillis(5))
                .build();

        try (NMap<String, String> map = NMap.open(tempDir, "test-replay", cfg)) {
            map.put("a", "1");
            map.put("b", "2");
            map.remove("a");
        }

        // Reopen and verify state survives
        try (NMap<String, String> map = NMap.open(tempDir, "test-replay", cfg)) {
            assertFalse(map.containsKey("a"));
            assertEquals(Optional.of("2"), map.get("b"));
            assertEquals(1, map.size());
        }
    }

    @Test
    void snapshotShouldBeCreatedAndWalRotated() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(2)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(1)
                .batchTimeout(Duration.ofMillis(2))
                .build();

        try (NMap<String, String> map = NMap.open(tempDir, "test-snapshot", cfg)) {
            map.put("k1", "v1");
            map.put("k2", "v2");
            map.remove("k1");
            // Give the background writer time to trigger snapshot
            Thread.sleep(100);
        }

        // Reopen and verify
        try (NMap<String, String> map = NMap.open(tempDir, "test-snapshot", cfg)) {
            assertFalse(map.containsKey("k1"));
            assertEquals(Optional.of("v2"), map.get("k2"));
        }
    }

    @Test
    void inMemoryModeShouldNotPersist() throws Exception {
        NMapConfig cfg = NMapConfig.inMemory();

        try (NMap<String, String> map = NMap.open(tempDir, "test-inmemory", cfg)) {
            map.put("key", "value");
            assertEquals(Optional.of("value"), map.get("key"));
        }

        // Reopen — should be empty since persistence was disabled
        try (NMap<String, String> map = NMap.open(tempDir, "test-inmemory", cfg)) {
            assertTrue(map.isEmpty());
        }
    }

    @Test
    void snapshotShouldReturnImmutableCopy() throws Exception {
        NMapConfig cfg = NMapConfig.inMemory();

        try (NMap<String, String> map = NMap.open(tempDir, "test-snap-copy", cfg)) {
            map.put("a", "1");
            map.put("b", "2");
            var snap = map.snapshot();
            assertEquals(2, snap.size());
            assertEquals("1", snap.get("a"));

            // Mutate original — snapshot should not change
            map.put("c", "3");
            assertFalse(snap.containsKey("c"));
        }
    }

    @Test
    void forEachShouldIterateAllEntries() throws Exception {
        NMapConfig cfg = NMapConfig.inMemory();

        try (NMap<String, Integer> map = NMap.open(tempDir, "test-foreach", cfg)) {
            map.put("a", 1);
            map.put("b", 2);
            map.put("c", 3);

            int[] sum = { 0 };
            map.forEach((k, v) -> sum[0] += v);
            assertEquals(6, sum[0]);
        }
    }
}
