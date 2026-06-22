package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.metrics.BlobVolumeMetrics;
import dev.nishisan.utils.oss.metrics.BlobVolumeMetricsListener;
import dev.nishisan.utils.oss.metrics.BlobVolumeStats;
import dev.nishisan.utils.oss.storage.SeriesChannel;
import dev.nishisan.utils.oss.storage.VerifyResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
    void emiteOnShardGrowAoCrescer(@TempDir Path tmp) {
        Path dir = tmp.resolve("vol");
        List<long[]> grows = new CopyOnWriteArrayList<>(); // [shardId, oldCap, newCap]
        BlobVolumeMetricsListener listener = new BlobVolumeMetricsListener() {
            @Override
            public void onShardGrow(int shardId, long oldCapacityBytes, long newCapacityBytes) {
                grows.add(new long[]{shardId, oldCapacityBytes, newCapacityBytes});
            }
        };
        try (BlobStorage bs = BlobStorage.openOrCreate(dir, 1, 64 * 1024, 64 * 1024, listener)) {
            for (int i = 0; i < 12; i++) {
                bs.put("series/s" + i + ".ngrr", pattern(8192, i));
            }
        }
        assertFalse(grows.isEmpty(), "esperado ao menos um onShardGrow");
        for (long[] g : grows) {
            assertEquals(0L, g[0], "único shard");
            assertTrue(g[2] > g[1], "newCap deve ser > oldCap");
            assertEquals(0L, g[2] % (64 * 1024), "newCap múltiplo do segmento");
        }
    }

    @Test
    void contabilizaAllocEFreeSemDuplicarEmResize(@TempDir Path dir) {
        BlobVolumeMetrics m = new BlobVolumeMetrics();
        try (BlobStorage bs = BlobStorage.openOrCreate(dir, SHARDS, SEG, INITIAL_CAP, m)) {
            // put novo -> 1 alloc, 0 free
            bs.put("series/k.ngrr", pattern(4096, 1));
            assertEquals(1, m.regionAllocateCount());
            assertEquals(0, m.regionFreeCount());

            // put same-size (align(4090)==align(4096)) com objectBytes diferente -> 0 eventos
            bs.put("series/k.ngrr", pattern(4090, 2));
            assertEquals(1, m.regionAllocateCount());
            assertEquals(0, m.regionFreeCount());

            // put de tamanho diferente -> 1 free (região antiga) + 1 alloc (nova)
            bs.put("series/k.ngrr", pattern(20000, 3));
            assertEquals(2, m.regionAllocateCount());
            assertEquals(1, m.regionFreeCount());

            // delete -> 1 free
            bs.delete("series/k.ngrr");
            assertEquals(2, m.regionAllocateCount());
            assertEquals(2, m.regionFreeCount());
        }
    }

    @Test
    void emiteOnCheckpointComContagemEDuracao(@TempDir Path dir) {
        BlobVolumeMetrics m = new BlobVolumeMetrics();
        int n = 5;
        try (BlobStorage bs = BlobStorage.openOrCreate(dir, SHARDS, SEG, INITIAL_CAP, m)) {
            for (int i = 0; i < n; i++) {
                bs.put("series/s" + i + ".ngrr", pattern(2048, i));
            }
            bs.checkpoint();
            assertEquals(1, m.checkpointCount());
            assertEquals(n, m.lastCheckpointEntryCount());
            assertTrue(m.lastCheckpointDurationMs() >= 0);
        }
        // close() dispara um checkpoint adicional no shutdown
        assertEquals(2, m.checkpointCount());
    }

    @Test
    void statsRefleteUsoLiquidoPorShard(@TempDir Path dir) {
        int n = 20;
        try (BlobStorage bs = open(dir)) {
            for (int i = 0; i < n; i++) {
                bs.put("series/s" + i + ".ngrr", pattern(4096, i));
            }

            BlobVolumeStats stats = bs.stats();
            assertEquals(SHARDS, stats.shardCount());
            assertEquals(n, stats.catalogEntryCount());

            // seriesPerShard bate com o group-by esperado via roteamento
            long[] expectedSeries = new long[SHARDS];
            for (int i = 0; i < n; i++) {
                expectedSeries[BlobRouting.shardFor("series/s" + i + ".ngrr", SHARDS)]++;
            }
            assertArrayEquals(expectedSeries, stats.seriesPerShard());

            // fill ratio em [0,1] e uso <= capacidade
            for (int s = 0; s < SHARDS; s++) {
                assertTrue(stats.fillRatioPerShard()[s] >= 0.0 && stats.fillRatioPerShard()[s] <= 1.0,
                        "fillRatio fora de [0,1] no shard " + s);
                assertTrue(stats.shardUsedBytes()[s] <= stats.shardCapacityBytes()[s]);
            }

            // uso LÍQUIDO: deletar uma série faz o used do seu shard cair
            String victim = "series/s0.ngrr";
            int vs = BlobRouting.shardFor(victim, SHARDS);
            long usedBefore = bs.stats().shardUsedBytes()[vs];
            bs.delete(victim);
            long usedAfter = bs.stats().shardUsedBytes()[vs];
            assertTrue(usedAfter < usedBefore, "used líquido deve cair após delete");
            assertEquals(n - 1, bs.stats().catalogEntryCount());

            // WAL tem bytes antes do checkpoint; cai (rotação) e catalog.bin > 0 depois
            assertTrue(bs.stats().walBytes() > 0, "WAL deve ter bytes antes do checkpoint");
            bs.checkpoint();
            assertEquals(0L, bs.stats().walBytes(), "WAL deve zerar após rotação no checkpoint");
            assertTrue(bs.stats().catalogImageBytes() > 0, "catalog.bin deve ter bytes após checkpoint");
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
