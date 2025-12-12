package dev.nishisan.utils.ngrid.map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MapPersistenceTest {

    @TempDir
    Path tempDir;

    @Test
    void walShouldReplayPutAndRemoveOnStartup() throws Exception {
        Path baseDir = tempDir.resolve("maps");
        Map<String, String> data = new ConcurrentHashMap<>();

        MapPersistenceConfig cfg = MapPersistenceConfig.builder(baseDir, "m1")
                .mode(MapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(0)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(10)
                .batchTimeout(Duration.ofMillis(5))
                .build();

        try (MapPersistence<String, String> p = new MapPersistence<>(cfg, data)) {
            p.start();
            data.put("a", "1");
            p.appendAsync(MapReplicationCommandType.PUT, "a", "1");
            data.put("b", "2");
            p.appendAsync(MapReplicationCommandType.PUT, "b", "2");
            data.remove("a");
            p.appendAsync(MapReplicationCommandType.REMOVE, "a", null);
        }

        Map<String, String> recovered = new ConcurrentHashMap<>();
        try (MapPersistence<String, String> p2 = new MapPersistence<>(cfg, recovered)) {
            p2.load();
        }

        assertFalse(recovered.containsKey("a"));
        assertEquals(Optional.of("2"), Optional.ofNullable(recovered.get("b")));
    }

    @Test
    void snapshotShouldBeCreatedAndWalRotated() throws Exception {
        Path baseDir = tempDir.resolve("maps");
        Map<String, String> data = new ConcurrentHashMap<>();

        MapPersistenceConfig cfg = MapPersistenceConfig.builder(baseDir, "m2")
                .mode(MapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(2)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(1)
                .batchTimeout(Duration.ofMillis(2))
                .build();

        try (MapPersistence<String, String> p = new MapPersistence<>(cfg, data)) {
            p.start();
            data.put("k1", "v1");
            p.appendAsync(MapReplicationCommandType.PUT, "k1", "v1");
            data.put("k2", "v2");
            p.appendAsync(MapReplicationCommandType.PUT, "k2", "v2");
            data.remove("k1");
            p.appendAsync(MapReplicationCommandType.REMOVE, "k1", null);
            // give the background writer some time to trigger snapshot by ops
            Thread.sleep(50);
        }

        Path mapDir = baseDir.resolve("m2");
        assertTrue(Files.exists(mapDir.resolve("snapshot.dat")), "Expected snapshot.dat to exist");
        assertTrue(Files.exists(mapDir.resolve("wal.log")), "Expected wal.log to exist");

        Map<String, String> recovered = new ConcurrentHashMap<>();
        try (MapPersistence<String, String> p2 = new MapPersistence<>(cfg, recovered)) {
            p2.load();
        }
        assertFalse(recovered.containsKey("k1"));
        assertEquals("v2", recovered.get("k2"));
    }

    @Test
    void recoveryShouldToleratePartialWalTailAfterCrash() throws Exception {
        Path baseDir = tempDir.resolve("maps");
        Map<String, String> data = new ConcurrentHashMap<>();

        MapPersistenceConfig cfg = MapPersistenceConfig.builder(baseDir, "m3")
                .mode(MapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(0)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(10)
                .batchTimeout(Duration.ofMillis(5))
                .build();

        try (MapPersistence<String, String> p = new MapPersistence<>(cfg, data)) {
            p.start();
            data.put("x", "1");
            p.appendAsync(MapReplicationCommandType.PUT, "x", "1");
            data.put("y", "2");
            p.appendAsync(MapReplicationCommandType.PUT, "y", "2");
        }

        // Corrupt the WAL by truncating a few bytes from the end (simulate crash mid-write).
        Path wal = baseDir.resolve("m3").resolve("wal.log");
        long size = Files.size(wal);
        assertTrue(size > 8, "WAL should have some content to truncate");
        try (FileChannel ch = FileChannel.open(wal, StandardOpenOption.WRITE)) {
            ch.truncate(size - 3);
        }

        Map<String, String> recovered = new ConcurrentHashMap<>();
        try (MapPersistence<String, String> p2 = new MapPersistence<>(cfg, recovered)) {
            p2.load();
        }
        // At least the first entry should survive; depending on where truncation hits, the second may be lost.
        assertEquals("1", recovered.get("x"));
    }
}


