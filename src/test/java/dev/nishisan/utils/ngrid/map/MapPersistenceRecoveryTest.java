package dev.nishisan.utils.ngrid.map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MapPersistenceRecoveryTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldRecoverFromIncompleteRotationWithOldWal() throws Exception {
        Path baseDir = tempDir.resolve("maps");
        String mapName = "recovery-test";
        Path mapDir = baseDir.resolve(mapName);
        
        // 1. Create initial state and persist it to WAL
        MapPersistenceConfig cfg = MapPersistenceConfig.builder(baseDir, mapName)
                .mode(MapPersistenceMode.ASYNC_WITH_FSYNC)
                .batchSize(1)
                .batchTimeout(Duration.ofMillis(1))
                .build();

        Map<String, String> data1 = new ConcurrentHashMap<>();
        try (MapPersistence<String, String> p1 = new MapPersistence<>(cfg, data1)) {
            p1.start();
            data1.put("key1", "value1");
            p1.appendAsync(MapReplicationCommandType.PUT, "key1", "value1");
            // Wait for persistence
            Thread.sleep(100);
        }

        // Verify wal.log exists
        Path walPath = mapDir.resolve("wal.log");
        assertTrue(Files.exists(walPath), "wal.log must exist");

        // 2. Simulate Crash during rotation:
        //    - Rename wal.log -> wal.log.old
        //    - Create empty/new wal.log
        //    - NO snapshot created
        Path oldWalPath = mapDir.resolve("wal.log.old");
        Files.move(walPath, oldWalPath);
        Files.createFile(walPath);

        assertTrue(Files.exists(walPath), "New empty wal.log must exist");
        assertTrue(Files.exists(oldWalPath), "Old wal.log must exist");
        assertEquals(0, Files.size(walPath), "New wal.log must be empty");
        assertTrue(Files.size(oldWalPath) > 0, "Old wal.log must have data");

        // 3. Load from disk and verify recovery
        Map<String, String> recoveredData = new ConcurrentHashMap<>();
        try (MapPersistence<String, String> p2 = new MapPersistence<>(cfg, recoveredData)) {
            p2.load();
        }

        // 4. Assertions: key1 must be present, recovered from wal.log.old
        assertTrue(recoveredData.containsKey("key1"), "Should recover key1 from wal.log.old");
        assertEquals("value1", recoveredData.get("key1"));
    }
}
