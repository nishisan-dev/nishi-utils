package dev.nishisan.utils.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NQueueCompactionTest {

    @TempDir
    Path tempDir;

    @Test
    void compactsWhenWasteThresholdExceeded() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withCompactionWasteThreshold(0.25d)
                .withCompactionInterval(Duration.ofHours(1))
                .withCompactionBufferSize(1024);

        Path queueDir = tempDir.resolve("threshold-queue");
        try (NQueue<String> queue = NQueue.open(tempDir, "threshold-queue", options)) {
            queue.offer("first");
            queue.offer("second");
            queue.offer("third");
            queue.offer("fourth");

            assertEquals("first", queue.poll().orElseThrow());
            assertEquals("second", queue.poll().orElseThrow());
            assertEquals("third", queue.poll().orElseThrow());

            Optional<String> peeked = queue.peek();
            assertTrue(peeked.isPresent());
            assertEquals("fourth", peeked.get());
        }

        Path metaPath = queueDir.resolve("queue.meta");
        Path dataPath = queueDir.resolve("data.log");
        NQueueQueueMeta meta = awaitMeta(metaPath, Duration.ofSeconds(2), m -> m.getConsumerOffset() == 0L);

        assertEquals(1L, meta.getRecordCount());
        assertEquals(0L, meta.getConsumerOffset());
        assertEquals(Files.size(dataPath), meta.getProducerOffset());
    }

    @Test
    void compactsWhenIntervalElapsed() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withCompactionWasteThreshold(0.95d)
                .withCompactionInterval(Duration.ofMillis(10))
                .withCompactionBufferSize(1024);

        Path queueDir = tempDir.resolve("interval-queue");
        try (NQueue<String> queue = NQueue.open(tempDir, "interval-queue", options)) {
            queue.offer("first");
            queue.offer("second");

            assertEquals("first", queue.poll().orElseThrow());

            Thread.sleep(20);

            queue.offer("third");

            Optional<String> peeked = queue.peek();
            assertTrue(peeked.isPresent());
            assertEquals("second", peeked.get());
        }

        Path metaPath = queueDir.resolve("queue.meta");
        NQueueQueueMeta meta = awaitMeta(metaPath, Duration.ofSeconds(2), m -> m.getConsumerOffset() == 0L);

        assertEquals(2L, meta.getRecordCount());
        assertEquals(0L, meta.getConsumerOffset());
    }

    @Test
    void preservesFifoOrderWhileCompacting() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withCompactionWasteThreshold(0.2d)
                .withCompactionInterval(Duration.ofMillis(10))
                .withCompactionBufferSize(512)
                .withFsync(false);

        Path queueDir = tempDir.resolve("fifo-compaction");
        List<String> expected = new ArrayList<>();

        try (NQueue<String> queue = NQueue.open(tempDir, "fifo-compaction", options)) {
            for (int i = 0; i < 20; i++) {
                String value = "msg-" + i;
                expected.add(value);
                queue.offer(value);
                if (i < 10) {
                    queue.poll().orElseThrow();
                }
            }

            // Give the background compaction time to trigger while the queue still has records.
            TimeUnit.MILLISECONDS.sleep(50);

            List<String> consumed = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                consumed.add(queue.poll().orElseThrow());
            }

            assertEquals(expected.subList(10, expected.size()), consumed);
        }

        NQueueQueueMeta meta = awaitMeta(queueDir.resolve("queue.meta"), Duration.ofSeconds(2), m -> m.getConsumerOffset() == m.getProducerOffset());
        assertEquals(meta.getProducerOffset(), meta.getConsumerOffset(), "Offsets should converge after draining");
        assertEquals(0L, meta.getRecordCount(), "Queue should be empty after draining records");
    }

    @Test
    void handlesConcurrentProducersConsumersDuringCompaction() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withCompactionWasteThreshold(0.15d)
                .withCompactionInterval(Duration.ofMillis(5))
                .withCompactionBufferSize(256)
                .withFsync(false);

        int totalRecords = 60;
        int producerThreads = 3;
        int consumerThreads = 3;
        ExecutorService executor = Executors.newFixedThreadPool(6);
        AtomicInteger sequence = new AtomicInteger();
        AtomicInteger consumedCount = new AtomicInteger();
        List<Integer> consumed = Collections.synchronizedList(new ArrayList<>());
        List<Integer> offeredOrder = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch producersDone = new CountDownLatch(producerThreads);

        try (NQueue<TestMessage> queue = NQueue.open(tempDir, "concurrent", options)) {
            List<Future<?>> producers = new ArrayList<>();
            for (int i = 0; i < producerThreads; i++) {
                producers.add(executor.submit(() -> {
                    start.await();
                    while (true) {
                        int seq = sequence.getAndIncrement();
                        if (seq >= totalRecords) {
                            producersDone.countDown();
                            return null;
                        }
                        queue.offer(new TestMessage(seq));
                        offeredOrder.add(seq);
                    }
                }));
            }

            List<Future<?>> consumers = new ArrayList<>();
            for (int i = 0; i < consumerThreads; i++) {
                consumers.add(executor.submit(() -> {
                    start.await();
                    AtomicInteger emptyPolls = new AtomicInteger();
                    while (consumedCount.get() < totalRecords) {
                        Optional<TestMessage> next = queue.poll(50, TimeUnit.MILLISECONDS);
                        next.ifPresent(msg -> {
                            consumed.add(msg.value());
                            consumedCount.incrementAndGet();
                            emptyPolls.set(0);
                        });
                        if (next.isEmpty()) {
                            int attempts = emptyPolls.incrementAndGet();
                            if (producersDone.getCount() == 0 && attempts > 100) {
                                fail("Timed out waiting for remaining records after producers finished");
                            }
                        }
                    }
                    return null;
                }));
            }

            start.countDown();
            for (Future<?> f : producers) {
                f.get(30, TimeUnit.SECONDS);
            }
            for (Future<?> f : consumers) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertEquals(totalRecords, consumed.size(), "All produced records must be consumed");
        assertEquals(offeredOrder, consumed, "FIFO order must be preserved");
    }

    @Test
    void handlesSustainedHighLoadWhileCompacting() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withCompactionWasteThreshold(0.10d)
                .withCompactionInterval(Duration.ofMillis(5))
                .withCompactionBufferSize(256)
                .withFsync(false);

        int producers = 4;
        int consumers = 2;
        int total = 500;

        ExecutorService executor = Executors.newFixedThreadPool(producers + consumers);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger nextSeq = new AtomicInteger();
        AtomicInteger consumed = new AtomicInteger();

        try (NQueue<TestMessage> queue = NQueue.open(tempDir, "high-load", options)) {
            List<Future<?>> tasks = new ArrayList<>();

            for (int i = 0; i < producers; i++) {
                tasks.add(executor.submit(() -> {
                    start.await();
                    while (true) {
                        int seq = nextSeq.getAndIncrement();
                        if (seq >= total) {
                            return null;
                        }
                        queue.offer(new TestMessage(seq));
                    }
                }));
            }

            for (int i = 0; i < consumers; i++) {
                tasks.add(executor.submit(() -> {
                    start.await();
                    while (consumed.get() < total) {
                        queue.poll(50, TimeUnit.MILLISECONDS).ifPresent(msg -> consumed.incrementAndGet());
                    }
                    return null;
                }));
            }

            start.countDown();
            for (Future<?> f : tasks) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }

        assertEquals(total, consumed.get(), "All records should be consumed under sustained load");
    }

    @Test
    void resumesCompactionAfterStateChangesDuringCopy() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withCompactionWasteThreshold(0.1d)
                .withCompactionInterval(Duration.ofMillis(5))
                .withCompactionBufferSize(256)
                .withFsync(false);

        Path queueDir = tempDir.resolve("contention");
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch workerStarted = new CountDownLatch(1);

        try (NQueue<Integer> queue = NQueue.open(tempDir, "contention", options)) {
            for (int i = 0; i < 50; i++) {
                queue.offer(i);
            }

            for (int i = 0; i < 35; i++) {
                queue.poll().orElseThrow();
            }

            Thread worker = new Thread(() -> {
                workerStarted.countDown();
                int value = 1_000;
                while (running.get()) {
                    try {
                        queue.offer(value++);
                        queue.poll(10, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        fail("Background worker encountered error", e);
                    }
                }
            });
            worker.start();

            workerStarted.await();
            TimeUnit.MILLISECONDS.sleep(200); // Allow background compaction attempts while the worker mutates state
            running.set(false);
            worker.join();

            // Drain remaining items to make compaction converge and avoid masking stale files when closing.
            while (queue.poll(10, TimeUnit.MILLISECONDS).isPresent()) {
                // keep draining
            }
        }

        NQueueQueueMeta meta = awaitMeta(queueDir.resolve("queue.meta"), Duration.ofSeconds(3), m -> m.getConsumerOffset() == m.getProducerOffset());
        assertEquals(0L, meta.getConsumerOffset(), "Compaction should rewrite file even if a prior attempt was aborted due to state changes");
    }

    private record TestMessage(int value) implements java.io.Serializable {
        private static final long serialVersionUID = 1L;
    }

    private static NQueueQueueMeta awaitMeta(Path metaPath, Duration timeout, java.util.function.Predicate<NQueueQueueMeta> condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        NQueueQueueMeta meta = NQueueQueueMeta.read(metaPath);
        while (!condition.test(meta) && System.nanoTime() < deadline) {
            Thread.sleep(25);
            meta = NQueueQueueMeta.read(metaPath);
        }
        return meta;
    }
}
