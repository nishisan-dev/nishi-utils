package dev.nishisan.utils.oss.cluster.node;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;

import static dev.nishisan.utils.oss.cluster.node.SeriesLifecycleJournal.Phase.ACTIVE;
import static org.junit.jupiter.api.Assertions.*;

class SeriesLifecycleJournalTest {
    @TempDir Path base;

    @Test
    void leEntradaComCampoDesconhecido() throws Exception {
        // Payload as a newer release would write it: unknown fields at the top level and inside the entry.
        String payload = "{\"key\":\"series-a\",\"futureTop\":\"ignored\","
                + "\"entry\":{\"generationId\":\"generation-1\",\"receivedThrough\":3600000,"
                + "\"phase\":\"ACTIVE\",\"placement\":null,\"writerEpoch\":7}}";
        writeRecord(base.resolve("series-lifecycle.wal"), payload);

        try (var journal = new SeriesLifecycleJournal(base, 1)) {
            var entry = journal.get("series-a");
            assertNotNull(entry, "the record must be replayed despite the unknown fields");
            assertEquals("generation-1", entry.generationId());
            assertEquals(3_600_000L, entry.receivedThrough());
            assertEquals(ACTIVE, entry.phase());
            assertNull(entry.placement());
        }
    }

    // Same framing as the journal: [length][crc32][json].
    private static void writeRecord(Path file, String json) throws Exception {
        byte[] payload = json.getBytes(StandardCharsets.UTF_8);
        CRC32 crc = new CRC32();
        crc.update(payload);
        try (var out = new DataOutputStream(Files.newOutputStream(file))) {
            out.writeInt(payload.length);
            out.writeInt((int) crc.getValue());
            out.write(payload);
        }
    }
}
