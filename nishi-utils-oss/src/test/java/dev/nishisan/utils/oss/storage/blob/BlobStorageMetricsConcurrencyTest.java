package dev.nishisan.utils.oss.storage.blob;

import dev.nishisan.utils.oss.metrics.BlobVolumeMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contagem determinística dos eventos operacionais sob concorrência: como os
 * callbacks são emitidos sob o {@code structuralLock} do volume (serializados) e o
 * coletor usa {@code AtomicLong}, os totais independem do interleaving — cada put
 * de chave nova = 1 alloc; cada delete de chave viva = 1 free.
 */
class BlobStorageMetricsConcurrencyTest {

    private static final int THREADS = 8;
    private static final int PER_THREAD = 50;
    private static final long SEG = 1L << 20;

    private static byte[] small(int seed) {
        byte[] b = new byte[512];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) (seed * 7 + i);
        }
        return b;
    }

    private static String key(int t, int k) {
        return "series/t" + t + "-k" + k + ".ngrr";
    }

    private static void runConcurrent(BiConsumer<Integer, Integer> action) throws InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int t = 0; t < THREADS; t++) {
            final int thread = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int k = 0; k < PER_THREAD; k++) {
                        action.accept(thread, k);
                    }
                } catch (Throwable th) {
                    failure.compareAndSet(null, th);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "timeout nas operações concorrentes");
        assertNull(failure.get(), () -> "falha concorrente: " + failure.get());
    }

    @Test
    void allocCountIgualAoTotalDePutsConcorrentes(@TempDir Path dir) throws InterruptedException {
        BlobVolumeMetrics m = new BlobVolumeMetrics();
        try (BlobStorage bs = BlobStorage.openOrCreate(dir, 4, SEG, SEG, m)) {
            runConcurrent((t, k) -> bs.put(key(t, k), small(k)));
            assertEquals((long) THREADS * PER_THREAD, m.regionAllocateCount());
        }
    }

    @Test
    void freeCountIgualAoTotalDeDeletesConcorrentes(@TempDir Path dir) throws InterruptedException {
        BlobVolumeMetrics m = new BlobVolumeMetrics();
        try (BlobStorage bs = BlobStorage.openOrCreate(dir, 4, SEG, SEG, m)) {
            for (int t = 0; t < THREADS; t++) {
                for (int k = 0; k < PER_THREAD; k++) {
                    bs.put(key(t, k), small(k));
                }
            }
            runConcurrent((t, k) -> bs.delete(key(t, k)));
            assertEquals((long) THREADS * PER_THREAD, m.regionFreeCount());
        }
    }
}
