package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.storage.SeriesChannel;
import dev.nishisan.utils.oss.storage.VerifyResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Backend de storage sharded blob (ver doc/oss/ngrrd-blob-volume.md). */
class BlobStorageTest {

    private static final int SHARDS = 4;
    private static final long SEG = 1L << 20;       // 1 MiB
    private static final long INITIAL_CAP = 1L << 20;

    private static BlobStorage open(Path dir) {
        return BlobStorage.openOrCreate(dir, SHARDS, SEG, INITIAL_CAP);
    }

    private static byte[] pattern(int len, int seed) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = (byte) (seed * 31 + i);
        }
        return b;
    }

    @Test
    void freshVolumeHasNoSeries(@TempDir Path dir) {
        try (BlobStorage bs = open(dir)) {
            assertFalse(bs.seriesExists("series/x.ngrr"));
            assertFalse(bs.exists("series/x.ngrr"));
            assertTrue(bs.list("series").isEmpty());
        }
    }

    @Test
    void seriesRoundTripViaChannel(@TempDir Path dir) {
        byte[] data = pattern(8192, 7);
        try (BlobStorage bs = open(dir)) {
            try (SeriesChannel ch = bs.openSeries("series/dev/iface.ngrr")) {
                assertEquals(0, ch.size()); // unbound (não existe ainda)
                ch.allocate(8192);
                ch.writeRegion(0, data);
                ch.force();
            }
            try (SeriesChannel ch = bs.openSeries("series/dev/iface.ngrr")) {
                assertEquals(8192, ch.size());
                assertArrayEquals(data, ch.readRegion(0, 8192));
            }
            assertTrue(bs.seriesExists("series/dev/iface.ngrr"));
        }
    }

    @Test
    void getReturnsExactObjectBytesNotPaddedRegion(@TempDir Path dir) {
        byte[] data = pattern(5000, 3); // align(5000) = 8192, mas get deve devolver 5000
        try (BlobStorage bs = open(dir)) {
            try (SeriesChannel ch = bs.openSeries("series/a.ngrr")) {
                ch.allocate(5000);
                assertEquals(8192, ch.size()); // slot alinhado
                ch.writeRegion(0, data);
                ch.force();
            }
            assertArrayEquals(data, bs.get("series/a.ngrr").orElseThrow());
        }
    }

    @Test
    void putAndGetArbitraryObject(@TempDir Path dir) {
        byte[] data = pattern(1234, 9);
        try (BlobStorage bs = open(dir)) {
            bs.put("series/k.ngrr", data);
            assertTrue(bs.exists("series/k.ngrr"));
            assertArrayEquals(data, bs.get("series/k.ngrr").orElseThrow());
        }
    }

    @Test
    void verifyOrReplaceIfIdenticalSkipsIdenticalContent(@TempDir Path dir) {
        byte[] data = pattern(2048, 4);
        try (BlobStorage bs = open(dir)) {
            assertEquals(VerifyResult.WRITTEN, bs.verifyOrReplaceIfIdentical("series/v.ngrr", data));
            assertEquals(VerifyResult.IDENTICAL_SKIPPED, bs.verifyOrReplaceIfIdentical("series/v.ngrr", data));
            assertEquals(VerifyResult.REPLACED, bs.verifyOrReplaceIfIdentical("series/v.ngrr", pattern(2048, 5)));
        }
    }

    @Test
    void atomicReplaceWithDifferentSizeReallocates(@TempDir Path dir) {
        try (BlobStorage bs = open(dir)) {
            bs.atomicReplace("series/r.ngrr", pattern(3000, 1));
            byte[] larger = pattern(20000, 2);
            bs.atomicReplace("series/r.ngrr", larger);
            assertArrayEquals(larger, bs.get("series/r.ngrr").orElseThrow());
        }
    }

    @Test
    void deleteRemovesSeries(@TempDir Path dir) {
        try (BlobStorage bs = open(dir)) {
            bs.put("series/d.ngrr", pattern(1000, 1));
            bs.delete("series/d.ngrr");
            assertFalse(bs.exists("series/d.ngrr"));
            assertTrue(bs.get("series/d.ngrr").isEmpty());
        }
    }

    @Test
    void listReturnsKeysUnderPrefix(@TempDir Path dir) {
        try (BlobStorage bs = open(dir)) {
            bs.put("series/a.ngrr", pattern(100, 1));
            bs.put("series/b.ngrr", pattern(100, 2));
            bs.put("schema/x.yaml", pattern(100, 3));
            List<String> series = bs.list("series");
            assertEquals(2, series.size());
            assertTrue(series.contains("series/a.ngrr") && series.contains("series/b.ngrr"));
        }
    }

    @Test
    void cleanReopenReadsFromSnapshot(@TempDir Path dir) {
        byte[] data = pattern(4096, 6);
        try (BlobStorage bs = open(dir)) {
            bs.put("series/persist.ngrr", data);
        } // close() faz snapshot e esvazia o WAL
        try (BlobStorage bs = open(dir)) {
            assertArrayEquals(data, bs.get("series/persist.ngrr").orElseThrow());
        }
    }

    @Test
    void crashRecoveryReplaysWalWithoutSnapshot(@TempDir Path dir) {
        byte[] d1 = pattern(2000, 1);
        byte[] d2 = pattern(3000, 2);
        BlobStorage crashed = open(dir);
        crashed.put("series/c1.ngrr", d1);
        crashed.put("series/c2.ngrr", d2);
        // Simula crash: NÃO chama close()/checkpoint() — apenas o WAL (fsync por registro) persiste.
        try (BlobStorage recovered = open(dir)) {
            assertArrayEquals(d1, recovered.get("series/c1.ngrr").orElseThrow());
            assertArrayEquals(d2, recovered.get("series/c2.ngrr").orElseThrow());
        }
    }

    @Test
    void growsShardWhenCapacityExceeded(@TempDir Path tmp) {
        // 1 shard, segmento de 64 KiB: escrever > 64 KiB força crescimento.
        Path dir = tmp.resolve("vol");
        try (BlobStorage bs = BlobStorage.openOrCreate(dir, 1, 64 * 1024, 64 * 1024)) {
            for (int i = 0; i < 12; i++) {
                bs.put("series/s" + i + ".ngrr", pattern(8192, i));
            }
            for (int i = 0; i < 12; i++) {
                assertArrayEquals(pattern(8192, i), bs.get("series/s" + i + ".ngrr").orElseThrow());
            }
        }
        // reabre e confere persistência pós-crescimento
        try (BlobStorage bs = BlobStorage.openOrCreate(dir, 1, 64 * 1024, 64 * 1024)) {
            assertArrayEquals(pattern(8192, 11), bs.get("series/s11.ngrr").orElseThrow());
        }
    }

    @Test
    void concurrentSeriesInSameShardWriteSafely(@TempDir Path dir) throws InterruptedException {
        int series = 40; // distribuídas em 4 shards -> várias por shard (mmap compartilhado)
        try (BlobStorage bs = open(dir)) {
            ExecutorService pool = Executors.newFixedThreadPool(8);
            CountDownLatch start = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            for (int i = 0; i < series; i++) {
                final int idx = i;
                pool.submit(() -> {
                    try {
                        start.await();
                        byte[] data = pattern(8192, idx);
                        try (SeriesChannel ch = bs.openSeries("series/conc-" + idx + ".ngrr")) {
                            ch.allocate(8192);
                            ch.writeRegion(0, data);
                            ch.force();
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "timeout nas escritas concorrentes");
            assertNull(failure.get(), () -> "falha concorrente: " + failure.get());
            for (int i = 0; i < series; i++) {
                assertArrayEquals(pattern(8192, i), bs.get("series/conc-" + i + ".ngrr").orElseThrow(),
                        "série conc-" + i + " corrompida");
            }
        }
    }
}
