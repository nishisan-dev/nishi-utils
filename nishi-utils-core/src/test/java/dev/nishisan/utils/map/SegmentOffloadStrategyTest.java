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
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre a {@link SegmentOffloadStrategy}: CRUD, update, persistência/recovery,
 * tolerância a cauda corrompida, colisão de hashCode, concorrência, compressão
 * LZ4, limites de {@code numSegments}, hot-cache limitado e o colapso de
 * arquivos (poucos segmentos em vez de um arquivo por entrada).
 */
class SegmentOffloadStrategyTest {

    @TempDir
    Path tempDir;

    private SegmentOffloadStrategy<String, String> open(String name, int numSegments) {
        return open(name, numSegments, false, 1_000);
    }

    private SegmentOffloadStrategy<String, String> open(String name, int numSegments,
            boolean compression, int hotCacheMaxEntries) {
        return new SegmentOffloadStrategy<>(tempDir, name, numSegments, compression,
                hotCacheMaxEntries, 0.5, false);
    }

    private Path segmentDir(String name) {
        return tempDir.resolve(name).resolve("segment-offload");
    }

    private long logFileCount(String name) throws Exception {
        try (Stream<Path> s = Files.list(segmentDir(name))) {
            return s.filter(p -> p.getFileName().toString().endsWith(".log")).count();
        }
    }

    @Test
    void putGetRemove() {
        try (SegmentOffloadStrategy<String, String> s = open("crud", 8)) {
            assertNull(s.put("a", "1"));
            assertNull(s.put("b", "2"));
            assertEquals("1", s.get("a"));
            assertEquals("2", s.get("b"));
            assertEquals(2, s.size());
            assertTrue(s.containsKey("a"));
            assertFalse(s.containsKey("z"));

            assertEquals("1", s.remove("a"));
            assertNull(s.get("a"));
            assertFalse(s.containsKey("a"));
            assertEquals(1, s.size());
            assertNull(s.remove("missing"));
        }
    }

    @Test
    void updateReturnsPreviousAndKeepsSize() {
        try (SegmentOffloadStrategy<String, String> s = open("update", 4)) {
            assertNull(s.put("k", "v1"));
            assertEquals("v1", s.put("k", "v2"));
            assertEquals("v2", s.get("k"));
            assertEquals(1, s.size());
        }
    }

    @Test
    void dataSurvivesRestartViaReplay() {
        String name = "restart";
        try (SegmentOffloadStrategy<String, String> s = open(name, 8)) {
            for (int i = 0; i < 200; i++) {
                s.put("key-" + i, "val-" + i);
            }
            s.remove("key-5");
            s.put("key-7", "updated-7");
        }
        try (SegmentOffloadStrategy<String, String> s = open(name, 8)) {
            assertEquals(199, s.size());
            assertNull(s.get("key-5"));
            assertEquals("updated-7", s.get("key-7"));
            assertEquals("val-100", s.get("key-100"));
        }
    }

    @Test
    void collapsesFileCountToNumSegments() throws Exception {
        String name = "collapse";
        try (SegmentOffloadStrategy<String, String> s = open(name, 8)) {
            for (int i = 0; i < 1_000; i++) {
                s.put("key-" + i, "value-" + i);
            }
            assertEquals(1_000, s.size());
        }
        // 1000 entries → still only 8 segment files (vs 1000 files in file-per-entry)
        assertEquals(8, logFileCount(name));
    }

    @Test
    void toleratesCorruptTailOnRecovery() throws Exception {
        String name = "corrupt-tail";
        try (SegmentOffloadStrategy<String, String> s = open(name, 4)) {
            for (int i = 0; i < 100; i++) {
                s.put("key-" + i, "val-" + i);
            }
        }
        // Append garbage to the tail of every segment log
        try (Stream<Path> logs = Files.list(segmentDir(name))) {
            for (Path log : logs.filter(p -> p.getFileName().toString().endsWith(".log")).toList()) {
                try (var ch = Files.newByteChannel(log, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                    ch.write(java.nio.ByteBuffer.wrap(new byte[]{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
                }
            }
        }
        // Recovery must discard the garbage tail and keep every valid record
        try (SegmentOffloadStrategy<String, String> s = open(name, 4)) {
            assertEquals(100, s.size());
            for (int i = 0; i < 100; i++) {
                assertEquals("val-" + i, s.get("key-" + i));
            }
        }
    }

    @Test
    void hashCollisionKeysCoexist() {
        String name = "collision";
        String k1 = "Aa"; // hashCode 2112
        String k2 = "BB"; // hashCode 2112
        assertEquals(k1.hashCode(), k2.hashCode());
        try (SegmentOffloadStrategy<String, String> s = open(name, 8)) {
            s.put(k1, "value-Aa");
            s.put(k2, "value-BB");
            assertEquals("value-Aa", s.get(k1));
            assertEquals("value-BB", s.get(k2));
            assertEquals(2, s.size());
        }
        try (SegmentOffloadStrategy<String, String> s = open(name, 8)) {
            assertEquals("value-Aa", s.get(k1));
            assertEquals("value-BB", s.get(k2));
        }
    }

    @Test
    void concurrentPutAndGet() throws Exception {
        int threads = 8;
        int opsPerThread = 250;
        try (SegmentOffloadStrategy<String, String> s = open("concurrent", 16)) {
            ExecutorService exec = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int tid = t;
                futures.add(exec.submit(() -> {
                    for (int i = 0; i < opsPerThread; i++) {
                        String key = "t" + tid + "-k" + i;
                        s.put(key, "v" + i);
                        assertEquals("v" + i, s.get(key));
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get();
            }
            exec.shutdown();
            assertEquals(threads * opsPerThread, s.size());
        }
    }

    @Test
    void compressionRoundTripsAndShrinksDisk() throws Exception {
        // A highly compressible value
        String value = "A".repeat(8_000);

        String plainName = "compress-off";
        try (SegmentOffloadStrategy<String, String> s = open(plainName, 1, false, 1_000)) {
            for (int i = 0; i < 50; i++) {
                s.put("k" + i, value);
            }
            assertEquals(value, s.get("k0"));
        }

        String compName = "compress-on";
        try (SegmentOffloadStrategy<String, String> s = open(compName, 1, true, 1_000)) {
            for (int i = 0; i < 50; i++) {
                s.put("k" + i, value);
            }
            assertEquals(value, s.get("k0"));
        }
        // Compressed log must be meaningfully smaller for compressible data
        long plainSize = Files.size(segmentDir(plainName).resolve("seg-000.log"));
        long compSize = Files.size(segmentDir(compName).resolve("seg-000.log"));
        assertTrue(compSize < plainSize / 2,
                "Compressed=" + compSize + " should be < half of plain=" + plainSize);

        // Compressed data must survive a restart too
        try (SegmentOffloadStrategy<String, String> s = open(compName, 1, true, 1_000)) {
            assertEquals(value, s.get("k0"));
            assertEquals(50, s.size());
        }
    }

    @Test
    void worksWithSingleSegment() throws Exception {
        String name = "one-segment";
        try (SegmentOffloadStrategy<String, String> s = open(name, 1)) {
            for (int i = 0; i < 100; i++) {
                s.put("k" + i, "v" + i);
            }
            assertEquals(100, s.size());
            assertEquals("v50", s.get("k50"));
        }
        assertEquals(1, logFileCount(name));
    }

    @Test
    void worksWithMaxSegments() throws Exception {
        String name = "max-segments";
        try (SegmentOffloadStrategy<String, String> s = open(name, 128)) {
            for (int i = 0; i < 500; i++) {
                s.put("k" + i, "v" + i);
            }
            assertEquals(500, s.size());
            assertEquals("v499", s.get("k499"));
        }
        assertEquals(128, logFileCount(name));
    }

    @Test
    void rejectsInvalidNumSegments() {
        assertThrows(IllegalArgumentException.class, () -> open("bad-low", 0));
        assertThrows(IllegalArgumentException.class, () -> open("bad-high", 129));
    }

    @Test
    void boundedHotCacheStillServesAllEntries() {
        try (SegmentOffloadStrategy<String, String> s = open("bounded", 4, false, 16)) {
            for (int i = 0; i < 400; i++) {
                s.put("k" + i, "v" + i);
            }
            for (int i = 0; i < 400; i++) {
                assertEquals("v" + i, s.get("k" + i));
            }
        }
    }

    @Test
    void cacheDisabledStillWorks() {
        try (SegmentOffloadStrategy<String, String> s = open("no-cache", 4, false, 0)) {
            s.put("a", "1");
            assertEquals("1", s.get("a"));
            assertEquals("1", s.get("a"));
        }
    }

    @Test
    void clearEmptiesEverything() {
        String name = "clear";
        try (SegmentOffloadStrategy<String, String> s = open(name, 4)) {
            for (int i = 0; i < 50; i++) {
                s.put("k" + i, "v" + i);
            }
            s.clear();
            assertTrue(s.isEmpty());
            assertEquals(0, s.size());
            assertNull(s.get("k0"));
            // After clear, restart yields an empty map
        }
        try (SegmentOffloadStrategy<String, String> s = open(name, 4)) {
            assertTrue(s.isEmpty());
        }
    }

    @Test
    void destroyRemovesDirectory() throws Exception {
        String name = "destroy";
        SegmentOffloadStrategy<String, String> s = open(name, 4);
        s.put("a", "1");
        s.destroy();
        assertFalse(Files.exists(segmentDir(name)));
    }

    @Test
    void forEachAndKeySet() {
        try (SegmentOffloadStrategy<String, Integer> s =
                new SegmentOffloadStrategy<>(tempDir, "iter", 4, false, 1_000, 0.5, false)) {
            s.put("a", 1);
            s.put("b", 2);
            s.put("c", 3);
            assertEquals(3, s.keySet().size());
            AtomicInteger sum = new AtomicInteger();
            s.forEach((k, v) -> sum.addAndGet(v));
            assertEquals(6, sum.get());
            assertEquals(3, s.entrySet().size());
        }
    }

    @Test
    void hintFileWrittenOnCloseAndUsedOnReopen() throws Exception {
        String name = "hint";
        try (SegmentOffloadStrategy<String, String> s = open(name, 4)) {
            for (int i = 0; i < 100; i++) {
                s.put("k" + i, "v" + i);
            }
        }
        // A hint file must exist after a clean close
        try (Stream<Path> st = Files.list(segmentDir(name))) {
            assertTrue(st.anyMatch(p -> p.getFileName().toString().endsWith(".hint")),
                    "expected a .hint file after close");
        }
        // Reopen uses the hint
        try (SegmentOffloadStrategy<String, String> s = open(name, 4)) {
            assertEquals(100, s.size());
            assertEquals("v50", s.get("k50"));
        }
        // Delete hints → must fall back to full log replay and stay correct
        try (Stream<Path> st = Files.list(segmentDir(name))) {
            for (Path p : st.filter(x -> x.getFileName().toString().endsWith(".hint")).toList()) {
                Files.delete(p);
            }
        }
        try (SegmentOffloadStrategy<String, String> s = open(name, 4)) {
            assertEquals(100, s.size());
            assertEquals("v99", s.get("k99"));
        }
    }

    @Test
    void incrementalRecoveryPicksUpDeltaAfterStaleHint() throws Exception {
        String name = "incremental";
        try (SegmentOffloadStrategy<String, String> s = open(name, 4)) {
            for (int i = 0; i < 100; i++) {
                s.put("k" + i, "v" + i);
            }
        }
        // Reopen (loads hint), append more WITHOUT closing — simulates a crash
        SegmentOffloadStrategy<String, String> crashed = open(name, 4);
        crashed.put("late-1", "L1");
        crashed.put("late-2", "L2");
        crashed.remove("k0");
        // Do NOT close 'crashed' (no fresh hint). A new instance must replay the delta.
        try (SegmentOffloadStrategy<String, String> s = open(name, 4)) {
            assertEquals("L1", s.get("late-1"));
            assertEquals("L2", s.get("late-2"));
            assertNull(s.get("k0"));
            assertEquals(101, s.size()); // 100 - k0 + late-1 + late-2
        }
        crashed.close();
    }

    @Test
    void manualCompactionReclaimsSpace() throws Exception {
        String name = "compact";
        String big = "X".repeat(2_000);
        Path log = tempDir.resolve(name).resolve("segment-offload").resolve("seg-000.log");
        // threshold 1.0 disables auto-compaction; cache disabled to force disk reads
        try (SegmentOffloadStrategy<String, String> s =
                new SegmentOffloadStrategy<>(tempDir, name, 1, false, 0, 1.0, false)) {
            for (int i = 0; i < 100; i++) {
                s.put("k", big + i); // 99 dead versions of the same key
            }
            for (int i = 0; i < 50; i++) {
                s.put("live" + i, big);
            }
            long before = Files.size(log);
            s.compactNow(0);
            long after = Files.size(log);
            assertTrue(after < before, "after=" + after + " should be < before=" + before);
            assertEquals(big + 99, s.get("k"));
            assertEquals(big, s.get("live0"));
            assertEquals(51, s.size());
        }
        // Post-compaction data must survive a restart (compaction rewrote the hint)
        try (SegmentOffloadStrategy<String, String> s =
                new SegmentOffloadStrategy<>(tempDir, name, 1, false, 0, 1.0, false)) {
            assertEquals(big + 99, s.get("k"));
            assertEquals(51, s.size());
        }
    }

    @Test
    void automaticCompactionKeepsLogBounded() throws Exception {
        String name = "auto-compact";
        String big = "Y".repeat(4_000);
        Path log = tempDir.resolve(name).resolve("segment-offload").resolve("seg-000.log");
        try (SegmentOffloadStrategy<String, String> s =
                new SegmentOffloadStrategy<>(tempDir, name, 1, false, 0, 0.5, false)) {
            // ~4 MB of writes over only 10 live keys → mostly garbage
            for (int round = 0; round < 100; round++) {
                for (int i = 0; i < 10; i++) {
                    s.put("k" + i, big + round);
                }
            }
            // Background compaction must keep the log far below the total written
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline && Files.size(log) > 1_000_000) {
                Thread.sleep(25);
            }
            assertTrue(Files.size(log) < 1_000_000,
                    "expected auto-compaction to bound the log, size=" + Files.size(log));
            for (int i = 0; i < 10; i++) {
                assertEquals(big + 99, s.get("k" + i));
            }
            assertEquals(10, s.size());
        }
    }

    @Test
    void worksThroughNMapApi() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadMode(OffloadMode.SEGMENT)
                .numSegments(8)
                .compressionEnabled(true)
                .hotCacheMaxEntries(1_000)
                .build();
        String name = "nmap-segment";
        try (NMap<String, String> map = NMap.open(tempDir, name, cfg)) {
            org.junit.jupiter.api.Assertions.assertInstanceOf(SegmentOffloadStrategy.class, map.strategy());
            for (int i = 0; i < 300; i++) {
                map.put("k" + i, "v" + i);
            }
            assertEquals(java.util.Optional.of("v150"), map.get("k150"));
        }
        // Reopen through the API — data persists
        try (NMap<String, String> map = NMap.open(tempDir, name, cfg)) {
            assertEquals(300, map.size());
            assertEquals(java.util.Optional.of("v299"), map.get("k299"));
        }
    }
}
