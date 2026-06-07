package dev.nishisan.utils.ngrid.replication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ResendLogTest {

    @TempDir
    Path tempDir;

    private static byte[] frame(long seq) {
        return RelayEntryCodec.encode(new RelayEntry(1L, seq, "queue:orders", UUID.randomUUID(),
                ("payload-" + seq).getBytes(StandardCharsets.UTF_8)));
    }

    private static ResendLog open(Path dir, int segMaxEntries, Duration retention, long maxEntries) {
        return new ResendLog(dir, segMaxEntries, Duration.ZERO, retention, maxEntries, false);
    }

    @Test
    void appendAndRandomGetById() {
        try (ResendLog log = open(tempDir, 4, Duration.ZERO, 1_000)) {
            long now = System.currentTimeMillis();
            for (long seq = 1; seq <= 10; seq++) {
                log.append(seq, now, frame(seq));
            }
            assertEquals(10L, log.size());
            RelayEntry e = log.get(7L);
            assertNotNull(e);
            assertEquals(7L, e.sequence());
            assertEquals("payload-7", new String(e.payloadBytes(), StandardCharsets.UTF_8));
            assertNull(log.get(999L), "absent sequence returns null");
        }
    }

    @Test
    void toleratesSequenceGaps() {
        try (ResendLog log = open(tempDir, 4, Duration.ZERO, 1_000)) {
            long now = System.currentTimeMillis();
            // Sequences 3 and 6 never committed (aborted ops) -> gaps.
            for (long seq : new long[] { 1, 2, 4, 5, 7, 8 }) {
                log.append(seq, now, frame(seq));
            }
            assertNull(log.get(3L), "gap sequence must be absent");
            assertNotNull(log.get(4L));
            List<RelayEntry> range = log.read(2L, 7L);
            assertEquals(List.of(2L, 4L, 5L, 7L),
                    range.stream().map(RelayEntry::sequence).toList());
        }
    }

    @Test
    void rangeReadSpansSegments() {
        try (ResendLog log = open(tempDir, 3, Duration.ZERO, 1_000)) {
            long now = System.currentTimeMillis();
            for (long seq = 1; seq <= 12; seq++) {
                log.append(seq, now, frame(seq));
            }
            List<Long> seqs = log.read(4L, 9L).stream().map(RelayEntry::sequence).toList();
            assertEquals(List.of(4L, 5L, 6L, 7L, 8L, 9L), seqs);
        }
    }

    @Test
    void countCapDropsOldestWholeSegments() {
        // segmentMaxEntries=3, maxEntries=6 -> at most ~2-3 segments retained; oldest sequences evicted.
        try (ResendLog log = open(tempDir, 3, Duration.ZERO, 6)) {
            long now = System.currentTimeMillis();
            for (long seq = 1; seq <= 12; seq++) {
                log.append(seq, now, frame(seq));
            }
            assertTrue(log.size() <= 9, "count cap must bound retained entries, was " + log.size());
            assertNull(log.get(1L), "oldest sequences must be evicted by the count cap");
            assertNotNull(log.get(12L), "newest sequence must be retained");
            assertTrue(log.oldestSequence() > 1L);
        }
    }

    @Test
    void temporalRetentionDropsExpiredSegments() throws Exception {
        try (ResendLog log = new ResendLog(tempDir, 3, Duration.ZERO, Duration.ofMillis(150), 1_000_000, false)) {
            long old = System.currentTimeMillis() - 10_000; // well beyond the window
            for (long seq = 1; seq <= 6; seq++) {
                log.append(seq, old, frame(seq));
            }
            // A fresh append in a new segment triggers retention of the expired sealed segments.
            Thread.sleep(50);
            long now = System.currentTimeMillis();
            for (long seq = 7; seq <= 9; seq++) {
                log.append(seq, now, frame(seq));
            }
            assertNull(log.get(1L), "expired entries must be evicted by the temporal window");
            assertNotNull(log.get(9L), "fresh entries must be retained");
        }
    }

    @Test
    void survivesCleanReopen() {
        long now = System.currentTimeMillis();
        try (ResendLog log = open(tempDir, 4, Duration.ZERO, 1_000)) {
            for (long seq = 1; seq <= 10; seq++) {
                log.append(seq, now, frame(seq));
            }
        }
        try (ResendLog reopened = open(tempDir, 4, Duration.ZERO, 1_000)) {
            assertEquals(10L, reopened.size(), "all entries must survive a reopen");
            assertEquals(5L, reopened.get(5L).sequence());
            assertEquals(List.of(8L, 9L, 10L),
                    reopened.read(8L, 10L).stream().map(RelayEntry::sequence).toList());
        }
    }

    @Test
    void recoversFromTornTrailingRecord() throws Exception {
        long now = System.currentTimeMillis();
        try (ResendLog log = open(tempDir, 100, Duration.ZERO, 1_000)) {
            for (long seq = 1; seq <= 5; seq++) {
                log.append(seq, now, frame(seq));
            }
        }
        // Simulate a crash mid-append: append garbage bytes to the (single) segment file.
        Path segment;
        try (Stream<Path> files = Files.list(tempDir)) {
            segment = files.filter(p -> p.getFileName().toString().endsWith(".dat")).findFirst().orElseThrow();
        }
        try (RandomAccessFile raf = new RandomAccessFile(segment.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.writeInt(9999); // a length prefix that overruns the file -> torn tail
            raf.write(new byte[] { 1, 2, 3 });
        }
        try (ResendLog reopened = open(tempDir, 100, Duration.ZERO, 1_000)) {
            assertEquals(5L, reopened.size(), "torn trailing record must be truncated, good records kept");
            assertNotNull(reopened.get(5L));
            // The recovered log must accept new appends after truncation.
            reopened.append(6L, now, frame(6L));
            assertEquals(6L, reopened.get(6L).sequence());
        }
    }

    @Test
    void storeIsolatesTopics() {
        try (ResendLogStore store = new ResendLogStore(tempDir, 8, Duration.ZERO, Duration.ZERO, 1_000, false)) {
            long now = System.currentTimeMillis();
            store.append("queue:orders", 1L, now,
                    RelayEntryCodec.encode(new RelayEntry(1L, 1L, "queue:orders", UUID.randomUUID(), new byte[] { 1 })));
            store.append("map:profiles", 1L, now,
                    RelayEntryCodec.encode(new RelayEntry(1L, 1L, "map:profiles", UUID.randomUUID(), new byte[] { 2 })));

            assertEquals(1L, store.size("queue:orders"));
            assertEquals(1L, store.size("map:profiles"));
            assertEquals("queue:orders", store.read("queue:orders", 1L, 1L).get(0).topic());
        }
    }
}
