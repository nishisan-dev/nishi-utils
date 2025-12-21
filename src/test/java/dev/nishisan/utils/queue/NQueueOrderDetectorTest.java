package dev.nishisan.utils.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NQueueOrderDetectorTest {

    @TempDir
    Path tempDir;

    @Test
    void handoff_shouldNotTriggerOutOfOrder_andShouldDeliverInOrder() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withShortCircuit(true)
                .withOrderDetection(true);

        try (NQueue<String> queue = NQueue.open(tempDir, "handoff-detector", options)) {
            ExecutorService exec = Executors.newSingleThreadExecutor();
            try {
                CountDownLatch started = new CountDownLatch(1);
                Future<Optional<String>> f = exec.submit(() -> {
                    started.countDown();
                    return queue.poll(2, TimeUnit.SECONDS);
                });

                assertTrue(started.await(1, TimeUnit.SECONDS), "Consumer thread didn't start in time");

                // Give poll() a tiny window to block so offer can short-circuit.
                Thread.sleep(50);

                long off = queue.offer("x");
                assertEquals(NQueue.OFFSET_HANDOFF, off, "Expected handoff path");

                Optional<String> v = f.get(2, TimeUnit.SECONDS);
                assertTrue(v.isPresent());
                assertEquals("x", v.get());

                assertEquals(0L, queue.getOutOfOrderCount());
            } finally {
                exec.shutdownNow();
            }
        }
    }

    @Test
    void memoryBufferAndCompaction_withOrderDetection_shouldNotTriggerOutOfOrder() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withOrderDetection(true)
                .withMemoryBuffer(true)
                .withMemoryBufferSize(512)
                // Make compaction very likely under load.
                .withCompactionWasteThreshold(0.1)
                .withCompactionInterval(Duration.ofMillis(50))
                .withRevalidationInterval(Duration.ofMillis(10))
                .withLockTryTimeout(Duration.ofMillis(1));

        try (NQueue<Integer> queue = NQueue.open(tempDir, "mem-compact-detector", options)) {
            int producers = 4;
            int perProducer = 500;
            int total = producers * perProducer;

            ExecutorService exec = Executors.newFixedThreadPool(producers + 2);
            try {
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();

                // Producers
                for (int p = 0; p < producers; p++) {
                    final int id = p;
                    futures.add(exec.submit(() -> {
                        start.await();
                        for (int i = 0; i < perProducer; i++) {
                            queue.offer(id * perProducer + i);
                        }
                        return null;
                    }));
                }

                // Consumers
                futures.add(exec.submit(() -> {
                    start.await();
                    int consumed = 0;
                    while (consumed < total) {
                        Optional<Integer> v = queue.poll(2, TimeUnit.SECONDS);
                        if (v.isPresent()) consumed++;
                    }
                    return null;
                }));

                start.countDown();

                for (Future<?> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }

                assertEquals(0L, queue.getOutOfOrderCount());

                Long violationsMetric = queue.getStats().getCounterValueOrNull("nqueue.out_of_order");
                if (violationsMetric != null) {
                    assertEquals(0L, violationsMetric);
                }
            } finally {
                exec.shutdownNow();
            }
        }
    }
}

