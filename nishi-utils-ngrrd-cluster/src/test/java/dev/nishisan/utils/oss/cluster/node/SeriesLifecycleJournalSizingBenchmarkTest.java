package dev.nishisan.utils.oss.cluster.node;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opt-in local sizing/compaction experiment, not a production latency assertion.
 * Run with -Dngrrd.journal.benchmark=true and a 1 GiB test heap.
 */
@EnabledIfSystemProperty(named = "ngrrd.journal.benchmark", matches = "true")
class SeriesLifecycleJournalSizingBenchmarkTest {
    private static final int SERIES = 500_000;
    private static final long HOUR = 3_600_000L;
    private static final long RECEIPT = 1_790_002_800_000L;
    @TempDir Path base;

    @Test
    void measureFullPlacementEstimateAndTwoSlimReceiptCompactions() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, SeriesLifecycleJournal.Entry> updates = new HashMap<>(SERIES * 4 / 3 + 1);
        long legacyBytes = 0;
        long slimBytes = 0;
        for (int i = 0; i < SERIES; i++) {
            String key = key(i);
            String generation = String.format(Locale.ROOT, "9a41d271-f3c0-4000-8000-%012d", i);
            var placement = new SeriesPlacement("storage-209", null, PlacementState.ACTIVE, null,
                    1_790_000_000_000L + i, 1_790_000_000_000L + i,
                    "geometry-optical-lane-0-v1", true, "optical-lane-0-v1", generation, null);
            var full = new SeriesLifecycleJournal.Entry(generation, RECEIPT,
                    SeriesLifecycleJournal.Phase.ACTIVE, placement);
            var slim = new SeriesLifecycleJournal.Entry(generation, RECEIPT,
                    SeriesLifecycleJournal.Phase.ACTIVE, null);
            // 8.10.2 uses this JSON record shape and an eight-byte length/CRC header.
            legacyBytes += mapper.writeValueAsBytes(new SeriesLifecycleJournal.Update(key, full)).length + 8;
            slimBytes += mapper.writeValueAsBytes(new SeriesLifecycleJournal.Update(key, slim)).length + 8;
            updates.put(key, slim);
        }

        Logger logger = Logger.getLogger(SeriesLifecycleJournal.class.getName());
        Level previousLevel = logger.getLevel();
        CompactionCapture capture = new CompactionCapture();
        logger.setLevel(Level.INFO);
        logger.addHandler(capture);
        try (var journal = new SeriesLifecycleJournal(base)) {
            journal.putAll(updates);
            assertEquals(slimBytes, Files.size(base.resolve("series-lifecycle.wal")));
            int initialCompactions = capture.records.size();
            assertEquals(1, initialCompactions, "initial bulk should establish the live-size threshold");
            for (int round = 1; round <= 2; round++) {
                final long receipt = RECEIPT + HOUR * round;
                updates.replaceAll((key, entry) -> new SeriesLifecycleJournal.Entry(entry.generationId(),
                        receipt, SeriesLifecycleJournal.Phase.ACTIVE, null));
                journal.putAll(updates);
                assertEquals(initialCompactions + round, capture.records.size(),
                        "each equal-sized hourly receipt round must cross the doubling threshold");
                assertEquals(slimBytes, Files.size(base.resolve("series-lifecycle.wal")));
                assertEquals(receipt, journal.get(key(SERIES - 1)).receivedThrough());
            }
            System.out.printf(Locale.ROOT,
                    "JOURNAL_BENCHMARK java=%s filesystem=%s series=%d full8102EstimatedBytes=%d slimActualBytes=%d ratio=%.3f initialCompaction=%s receiptCycle1=%s receiptCycle2=%s%n",
                    System.getProperty("java.version"), Files.getFileStore(base).type(), SERIES,
                    legacyBytes, slimBytes, (double) legacyBytes / slimBytes,
                    capture.records.get(0), capture.records.get(1), capture.records.get(2));
            assertEquals(3, journal.fsyncCount(), "one bulk fsync per round; production batch counts differ");
            assertTrue(capture.records.stream().allMatch(record -> record.contains("lockHeldNanos=")));
        } finally {
            logger.removeHandler(capture);
            logger.setLevel(previousLevel);
        }
    }

    private static String key(int i) {
        return "device:dev-" + i + "/iface:int-TenGigE0_0_0_" + (i % 48) + "/group:optical-lane-0-v1";
    }

    private static final class CompactionCapture extends Handler {
        private final List<String> records = new ArrayList<>();
        @Override public void publish(LogRecord record) {
            if (record.getMessage().startsWith("NGRRD_LIFECYCLE_COMPACT ")) records.add(record.getMessage());
        }
        @Override public void flush() { }
        @Override public void close() { }
    }
}
