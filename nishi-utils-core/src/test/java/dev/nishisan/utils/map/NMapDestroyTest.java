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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NMapDestroyTest {

    @TempDir
    Path tempDir;

    // ── InMemory + WAL persistence ──────────────────────────────────────

    @Test
    void destroyShouldRemoveWalAndSnapshotFiles() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(2)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(1)
                .batchTimeout(Duration.ofMillis(2))
                .build();

        Path mapDir = tempDir.resolve("destroy-wal");

        NMap<String, String> map = NMap.open(tempDir, "destroy-wal", cfg);
        map.put("a", "1");
        map.put("b", "2");
        map.put("c", "3");
        // Give the background writer time to flush + snapshot
        Thread.sleep(200);

        // Sanity: persistence directory must exist before destroy
        assertTrue(Files.exists(mapDir), "Map directory should exist before destroy");

        map.destroy();

        // All persistence files and directory should be gone
        assertFalse(Files.exists(mapDir.resolve("wal.log")), "WAL file should be deleted");
        assertFalse(Files.exists(mapDir.resolve("snapshot.dat")), "Snapshot file should be deleted");
        assertFalse(Files.exists(mapDir.resolve("map.meta")), "Meta file should be deleted");
        assertFalse(Files.exists(mapDir), "Map directory should be deleted");
    }

    @Test
    void destroyShouldWorkWhenPersistenceIsDisabled() throws Exception {
        NMapConfig cfg = NMapConfig.inMemory();

        NMap<String, String> map = NMap.open(tempDir, "destroy-inmem", cfg);
        map.put("a", "1");
        map.destroy();
        // No files to check — just verifying no exception is thrown
    }

    @Test
    void destroyShouldPreventDataRecoveryOnReopen() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(0)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(1)
                .batchTimeout(Duration.ofMillis(2))
                .build();

        NMap<String, String> map = NMap.open(tempDir, "destroy-reopen", cfg);
        map.put("key", "value");
        Thread.sleep(100);
        map.destroy();

        // Reopen — should be empty because persistence files were destroyed
        try (NMap<String, String> reopened = NMap.open(tempDir, "destroy-reopen", cfg)) {
            assertTrue(reopened.isEmpty(), "Reopened map should be empty after destroy");
        }
    }

    // ── DiskOffloadStrategy ─────────────────────────────────────────────

    @Test
    void destroyShouldRemoveDiskOffloadDirectory() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadStrategyFactory(DiskOffloadStrategy::new)
                .build();

        Path mapDir = tempDir.resolve("destroy-disk");
        Path offloadDir = mapDir.resolve("offload");

        NMap<String, String> map = NMap.open(tempDir, "destroy-disk", cfg);
        map.put("a", "1");
        map.put("b", "2");

        assertTrue(Files.exists(offloadDir), "Offload directory should exist before destroy");

        map.destroy();

        assertFalse(Files.exists(offloadDir), "Offload directory should be deleted after destroy");
    }

    @Test
    void destroyShouldPreventDiskOffloadRecoveryOnReopen() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadStrategyFactory(DiskOffloadStrategy::new)
                .build();

        NMap<String, String> map = NMap.open(tempDir, "destroy-disk-reopen", cfg);
        map.put("key", "value");
        map.destroy();

        try (NMap<String, String> reopened = NMap.open(tempDir, "destroy-disk-reopen", cfg)) {
            assertTrue(reopened.isEmpty(), "Reopened disk-offload map should be empty after destroy");
        }
    }

    // ── HybridOffloadStrategy ───────────────────────────────────────────

    @Test
    void destroyShouldRemoveHybridOffloadDirectory() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadStrategyFactory(new NMapOffloadStrategyFactory() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <K, V> NMapOffloadStrategy<K, V> create(Path baseDir, String name) {
                        return (NMapOffloadStrategy<K, V>) HybridOffloadStrategy.<String, String>builder(baseDir, name)
                                .evictionPolicy(EvictionPolicy.LRU)
                                .maxInMemoryEntries(2)
                                .build();
                    }
                })
                .build();

        Path mapDir = tempDir.resolve("destroy-hybrid");
        Path offloadDir = mapDir.resolve("hybrid-offload");

        NMap<String, String> map = NMap.open(tempDir, "destroy-hybrid", cfg);
        // Insert enough to trigger spill to disk
        map.put("a", "1");
        map.put("b", "2");
        map.put("c", "3");
        map.put("d", "4");

        assertTrue(Files.exists(offloadDir), "Hybrid offload directory should exist before destroy");

        map.destroy();

        assertFalse(Files.exists(offloadDir), "Hybrid offload directory should be deleted after destroy");
    }

    // ── DiskOffload + WAL combined ──────────────────────────────────────

    @Test
    void destroyShouldRemoveBothWalAndOffloadFiles() throws Exception {
        // InMemory strategy + WAL persistence (default scenario)
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(0)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(1)
                .batchTimeout(Duration.ofMillis(2))
                .build();

        Path mapDir = tempDir.resolve("destroy-combined");

        NMap<String, String> map = NMap.open(tempDir, "destroy-combined", cfg);
        map.put("a", "1");
        Thread.sleep(100);

        assertTrue(Files.exists(mapDir), "Map directory should exist");

        map.destroy();

        assertFalse(Files.exists(mapDir.resolve("wal.log")), "WAL should be deleted");
        assertFalse(Files.exists(mapDir), "Map directory should be deleted");
    }
}

