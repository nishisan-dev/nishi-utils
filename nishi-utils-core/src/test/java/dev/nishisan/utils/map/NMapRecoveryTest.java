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

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NMapRecoveryTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldRecoverFromPartialWalTailAfterCrash() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(0)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(10)
                .batchTimeout(Duration.ofMillis(5))
                .build();

        try (NMap<String, String> map = NMap.open(tempDir, "crash-test", cfg)) {
            map.put("x", "1");
            map.put("y", "2");
        }

        // Corrupt the WAL by truncating a few bytes from the end
        Path wal = tempDir.resolve("crash-test").resolve("wal.log");
        long size = Files.size(wal);
        assertTrue(size > 8, "WAL should have content to truncate");
        try (FileChannel ch = FileChannel.open(wal, StandardOpenOption.WRITE)) {
            ch.truncate(size - 3);
        }

        try (NMap<String, String> map = NMap.open(tempDir, "crash-test", cfg)) {
            // At least the first entry should survive
            assertEquals(Optional.of("1"), map.get("x"));
        }
    }

    @Test
    void shouldRecoverFromIncompleteRotationWithOldWal() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .snapshotIntervalOperations(0)
                .snapshotIntervalTime(Duration.ZERO)
                .batchSize(1)
                .batchTimeout(Duration.ofMillis(1))
                .build();

        try (NMap<String, String> map = NMap.open(tempDir, "rotation-crash", cfg)) {
            map.put("key1", "value1");
            Thread.sleep(100); // ensure persisted
        }

        // Simulate incomplete rotation: rename wal.log → wal.log.old, create empty
        // wal.log
        Path mapDir = tempDir.resolve("rotation-crash");
        Path walPath = mapDir.resolve("wal.log");
        Path oldWalPath = mapDir.resolve("wal.log.old");
        Files.move(walPath, oldWalPath, StandardCopyOption.REPLACE_EXISTING);
        Files.createFile(walPath);

        assertTrue(Files.exists(walPath));
        assertTrue(Files.exists(oldWalPath));
        assertEquals(0, Files.size(walPath));

        try (NMap<String, String> map = NMap.open(tempDir, "rotation-crash", cfg)) {
            assertTrue(map.containsKey("key1"));
            assertEquals(Optional.of("value1"), map.get("key1"));
        }
    }
}
