package dev.nishisan.utils.oss.cluster.rebalance;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

class MigrationBandwidthTest {
    @Test void concurrentTransfersShareOneBudget() throws Exception {
        var limiter = new MigrationBandwidth(8 * 1024);
        var start = new CountDownLatch(1);
        var times = new CopyOnWriteArrayList<Long>();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var jobs = new java.util.ArrayList<Future<?>>();
            for (int i = 0; i < 3; i++) {
                jobs.add(pool.submit(() -> {
                    start.await();
                    assertTrue(limiter.acquire(1024, () -> true));
                    times.add(System.nanoTime());
                    return null;
                }));
            }
            start.countDown();
            for (var job : jobs) { job.get(3, TimeUnit.SECONDS); }
        }
        times.sort(Long::compare);
        assertTrue(times.getLast() - times.getFirst() >= TimeUnit.MILLISECONDS.toNanos(200),
                "three transfers must not each receive the full per-node bandwidth");
    }

    @Test void abortCancelsBandwidthWaitPromptly() throws Exception {
        var limiter = new MigrationBandwidth(1);
        assertTrue(limiter.acquire(1000, () -> true));
        var active = new AtomicBoolean(true);
        var result = CompletableFuture.supplyAsync(() -> limiter.acquire(1000, active::get));
        active.set(false);
        assertFalse(result.get(1, TimeUnit.SECONDS));
        assertThrows(IllegalArgumentException.class, () -> new MigrationBandwidth(0));
    }
}
