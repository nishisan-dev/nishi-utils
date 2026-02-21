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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NMapOffloadTest {

    @TempDir
    Path tempDir;

    private NMapConfig diskConfig() {
        return NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadStrategyFactory(DiskOffloadStrategy::new)
                .batchSize(10)
                .batchTimeout(Duration.ofMillis(5))
                .build();
    }

    @Test
    void diskOffloadPutAndGet() throws Exception {
        try (NMap<String, String> map = NMap.open(tempDir, "offload-put", diskConfig())) {
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
    void diskOffloadRemove() throws Exception {
        try (NMap<String, String> map = NMap.open(tempDir, "offload-remove", diskConfig())) {
            map.put("x", "10");
            Optional<String> removed = map.remove("x");
            assertTrue(removed.isPresent());
            assertEquals("10", removed.get());
            assertFalse(map.containsKey("x"));
            assertTrue(map.isEmpty());
        }
    }

    @Test
    void diskOffloadSurvivesRestart() throws Exception {
        NMapConfig cfg = diskConfig();

        try (NMap<String, String> map = NMap.open(tempDir, "offload-restart", cfg)) {
            map.put("persistent-key", "persistent-value");
            map.put("another-key", "another-value");
        }

        // Reopen — data should survive because DiskOffloadStrategy is inherently
        // persistent
        try (NMap<String, String> map = NMap.open(tempDir, "offload-restart", cfg)) {
            assertEquals(Optional.of("persistent-value"), map.get("persistent-key"));
            assertEquals(Optional.of("another-value"), map.get("another-key"));
            assertEquals(2, map.size());
        }
    }

    @Test
    void diskOffloadClear() throws Exception {
        try (NMap<String, String> map = NMap.open(tempDir, "offload-clear", diskConfig())) {
            map.put("a", "1");
            map.put("b", "2");
            map.put("c", "3");
            assertEquals(3, map.size());

            map.clear();
            assertTrue(map.isEmpty());
            assertEquals(0, map.size());
            assertFalse(map.containsKey("a"));
        }
    }

    @Test
    void diskOffloadLargeDataset() throws Exception {
        try (NMap<String, String> map = NMap.open(tempDir, "offload-large", diskConfig())) {
            for (int i = 0; i < 1000; i++) {
                map.put("key-" + i, "value-" + i);
            }
            assertEquals(1000, map.size());

            // Verify random access works
            assertEquals(Optional.of("value-500"), map.get("key-500"));
            assertEquals(Optional.of("value-0"), map.get("key-0"));
            assertEquals(Optional.of("value-999"), map.get("key-999"));
        }
    }

    @Test
    void defaultStrategyMatchesExistingBehavior() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(0)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(10)
                .batchTimeout(Duration.ofMillis(5))
                .build();

        try (NMap<String, String> map = NMap.open(tempDir, "default-strategy", cfg)) {
            map.put("a", "1");
            map.put("b", "2");
            assertEquals(Optional.of("1"), map.get("a"));
            assertEquals(2, map.size());
            assertTrue(map.strategy() instanceof InMemoryStrategy);
        }
    }

    @Test
    void diskOffloadForEachAndKeySet() throws Exception {
        try (NMap<String, Integer> map = NMap.open(tempDir, "offload-iter", diskConfig())) {
            map.put("a", 1);
            map.put("b", 2);
            map.put("c", 3);

            // Test keySet
            assertEquals(3, map.keySet().size());
            assertTrue(map.keySet().contains("a"));
            assertTrue(map.keySet().contains("b"));
            assertTrue(map.keySet().contains("c"));

            // Test forEach
            AtomicInteger sum = new AtomicInteger(0);
            map.forEach((k, v) -> sum.addAndGet(v));
            assertEquals(6, sum.get());
        }
    }

    @Test
    void diskOffloadUpdateExistingEntry() throws Exception {
        try (NMap<String, String> map = NMap.open(tempDir, "offload-update", diskConfig())) {
            map.put("key", "v1");
            assertEquals(Optional.of("v1"), map.get("key"));

            // Update same key
            Optional<String> prev = map.put("key", "v2");
            assertTrue(prev.isPresent());
            assertEquals("v1", prev.get());
            assertEquals(Optional.of("v2"), map.get("key"));
            assertEquals(1, map.size());
        }
    }

    @Test
    void diskOffloadSnapshot() throws Exception {
        try (NMap<String, String> map = NMap.open(tempDir, "offload-snap", diskConfig())) {
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
}
