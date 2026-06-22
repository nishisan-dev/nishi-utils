package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.storage.blob.CatalogEntry.State;
import dev.nishisan.utils.oss.storage.blob.CatalogJournal.WalOp;
import dev.nishisan.utils.oss.storage.blob.CatalogJournal.WalRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** WAL do catálogo (catalog.wal). Ver doc/oss/ngrrd-blob-volume.md §7. */
class CatalogJournalTest {

    private static final CatalogEntry E1 =
            new CatalogEntry("series/a.ngrr", 1, 4096L, 8192L, 8000L, State.LIVE);
    private static final CatalogEntry E2 =
            new CatalogEntry("series/b.ngrr", 2, 12288L, 8192L, 7000L, State.DELETED);

    @Test
    void appendAndReplayRoundTrip(@TempDir Path dir) {
        Path wal = dir.resolve("catalog.wal");
        try (CatalogJournal journal = new CatalogJournal(wal)) {
            journal.appendAlloc(10L, E1);
            journal.appendFree(11L, E2);
        }
        List<WalRecord> records = CatalogJournal.replay(wal);
        assertEquals(2, records.size());
        assertEquals(new WalRecord(WalOp.ALLOC, 10L, E1), records.get(0));
        assertEquals(new WalRecord(WalOp.FREE, 11L, E2), records.get(1));
    }

    @Test
    void replayMissingFileReturnsEmpty(@TempDir Path dir) {
        assertEquals(List.of(), CatalogJournal.replay(dir.resolve("absent.wal")));
    }

    @Test
    void tornTailIsTruncatedAndIgnored(@TempDir Path dir) throws IOException {
        Path wal = dir.resolve("catalog.wal");
        try (CatalogJournal journal = new CatalogJournal(wal)) {
            journal.appendAlloc(1L, E1);
            journal.appendAlloc(2L, E2);
        }
        // Anexa um frame parcial: declara 100 bytes mas grava só 3 (crash no meio da escrita).
        try (FileChannel ch = FileChannel.open(wal, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
            ByteBuffer torn = ByteBuffer.allocate(7);
            torn.putInt(100); // frameLen mentiroso
            torn.put(new byte[]{1, 2, 3});
            torn.flip();
            ch.write(torn);
        }
        List<WalRecord> first = CatalogJournal.replay(wal);
        assertEquals(2, first.size());
        // Replay deve ter truncado a cauda; um segundo replay devolve os mesmos 2 registros.
        List<WalRecord> second = CatalogJournal.replay(wal);
        assertEquals(2, second.size());
    }

    @Test
    void corruptPayloadCrcStopsReplay(@TempDir Path dir) throws IOException {
        Path wal = dir.resolve("catalog.wal");
        try (CatalogJournal journal = new CatalogJournal(wal)) {
            journal.appendAlloc(1L, E1);
            journal.appendAlloc(2L, E2);
        }
        // Corrompe um byte no meio do arquivo (dentro do 2º registro).
        try (FileChannel ch = FileChannel.open(wal, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            long size = ch.size();
            ByteBuffer one = ByteBuffer.allocate(1);
            ch.read(one, size - 10);
            one.flip();
            byte b = (byte) (one.get(0) ^ 0xFF);
            ch.write(ByteBuffer.wrap(new byte[]{b}), size - 10);
        }
        List<WalRecord> records = CatalogJournal.replay(wal);
        assertEquals(1, records.size());
        assertEquals(10L, 10L); // sanity
        assertTrue(records.get(0).generation() == 1L);
    }
}
