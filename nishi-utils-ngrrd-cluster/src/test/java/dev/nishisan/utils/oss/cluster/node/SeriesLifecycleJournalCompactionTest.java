package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeriesLifecycleJournalCompactionTest {

    private static final long MIN_COMPACT_BYTES = 64 * 1024;

    @TempDir
    Path base;

    @Test
    void compactacaoAmortizadaNaoReescreveOJournalACadaMutacaoQuandoOVivoPassaDoLimiar() throws Exception {
        try (var journal = new SeriesLifecycleJournal(base, MIN_COMPACT_BYTES)) {
            // Live state larger than the minimum threshold: with a fixed threshold, every later
            // mutation would rewrite the whole journal.
            Map<String, SeriesLifecycleJournal.Entry> initial = new HashMap<>();
            for (int i = 0; i < 2_000; i++) initial.put(key(i), entry(i, 1));
            journal.putAll(initial);
            long afterBulk = journal.compactionCount();

            for (int i = 0; i < 500; i++) journal.put(key(i), entry(i, 2));

            long duringSinglePuts = journal.compactionCount() - afterBulk;
            assertTrue(duringSinglePuts <= 1,
                    "500 mutations must not trigger one compaction each; compactions=" + duringSinglePuts);
            assertEquals(500 + 1, journal.fsyncCount());
        }
    }

    @Test
    void estadoSobreviveACompactacoesEAoReplay() throws Exception {
        Map<String, SeriesLifecycleJournal.Entry> expected = new HashMap<>();
        try (var journal = new SeriesLifecycleJournal(base, MIN_COMPACT_BYTES)) {
            for (int round = 1; round <= 6; round++) {
                for (int i = 0; i < 800; i++) {
                    var value = entry(i, round);
                    journal.put(key(i), value);
                    expected.put(key(i), value);
                }
            }
            assertTrue(journal.compactionCount() >= 1, "the scenario must exercise at least one compaction");
            assertEquals(expected, journal.snapshot());
        }
        try (var reopened = new SeriesLifecycleJournal(base, MIN_COMPACT_BYTES)) {
            assertEquals(expected, reopened.snapshot());
        }
    }

    private static String key(int i) {
        return "device:dev-" + i + "/iface:int-TenGigE0_0_0_" + (i % 48) + "/group:optical-lane-0-v1";
    }

    private static SeriesLifecycleJournal.Entry entry(int i, int round) {
        long createdAt = 1_790_000_000_000L + i;
        return new SeriesLifecycleJournal.Entry("legacy:" + createdAt, 3_600_000L * round,
                SeriesLifecycleJournal.Phase.ACTIVE, null);
    }
}
