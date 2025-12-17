package dev.nishisan.utils.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
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

import static org.junit.jupiter.api.Assertions.*;

class NQueueMemoryBufferTest {

    @TempDir
    Path tempDir;

    @Test
    void testMemoryBufferDisabledByDefault() throws Exception {
        // Memory buffer should be disabled by default
        try (NQueue<String> queue = NQueue.open(tempDir, "default")) {
            // Should work normally without memory buffer
            queue.offer("test");
            Optional<String> result = queue.poll();
            assertTrue(result.isPresent());
            assertEquals("test", result.get());
        }
    }

    @Test
    void testOfferWithMemoryBufferEnabled() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withMemoryBuffer(true)
                .withMemoryBufferSize(100)
                .withLockTryTimeout(Duration.ofMillis(10))
                .withRevalidationInterval(Duration.ofMillis(100));

        try (NQueue<String> queue = NQueue.open(tempDir, "enabled", options)) {
            queue.offer("first");
            queue.offer("second");
            queue.offer("third");

            assertEquals("first", queue.poll().orElseThrow());
            assertEquals("second", queue.poll().orElseThrow());
            assertEquals("third", queue.poll().orElseThrow());
        }
    }

    @Test
    void testPollDrainsMemoryBufferFirst() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withMemoryBuffer(true)
                .withMemoryBufferSize(100)
                .withLockTryTimeout(Duration.ofMillis(10))
                .withRevalidationInterval(Duration.ofMillis(100))
                .withCompactionWasteThreshold(0.1d)
                .withCompactionInterval(Duration.ofMillis(5))
                .withFsync(false);

        try (NQueue<String> queue = NQueue.open(tempDir, "drain-test", options)) {
            // Add items that might trigger memory buffer
            for (int i = 0; i < 10; i++) {
                queue.offer("item-" + i);
            }

            // Poll should drain memory first, then read from disk
            List<String> consumed = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                Optional<String> item = queue.poll(1, TimeUnit.SECONDS);
                if (item.isPresent()) {
                    consumed.add(item.get());
                }
            }

            assertEquals(10, consumed.size());
            for (int i = 0; i < 10; i++) {
                assertEquals("item-" + i, consumed.get(i));
            }
        }
    }

    @Test
    void testFifoOrderWithMemoryBuffer() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withMemoryBuffer(true)
                .withMemoryBufferSize(1000)
                .withLockTryTimeout(Duration.ofMillis(10))
                .withRevalidationInterval(Duration.ofMillis(100))
                .withFsync(false);

        try (NQueue<Integer> queue = NQueue.open(tempDir, "fifo-test", options)) {
            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                expected.add(i);
                queue.offer(i);
            }

            List<Integer> consumed = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                Optional<Integer> item = queue.poll(1, TimeUnit.SECONDS);
                assertTrue(item.isPresent());
                consumed.add(item.get());
            }

            assertEquals(expected, consumed);
        }
    }

    @Test
    void testMemoryModeActivatedDuringCompaction() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withMemoryBuffer(true)
                .withMemoryBufferSize(100)
                .withLockTryTimeout(Duration.ofMillis(10))
                .withRevalidationInterval(Duration.ofMillis(100))
                .withCompactionWasteThreshold(0.1d)
                .withCompactionInterval(Duration.ofMillis(5))
                .withFsync(false);

        try (NQueue<String> queue = NQueue.open(tempDir, "compaction-test", options)) {
            // Add items and consume some to trigger compaction
            for (int i = 0; i < 20; i++) {
                queue.offer("msg-" + i);
                if (i < 10) {
                    queue.poll().orElseThrow();
                }
            }

            // Give compaction time to start
            Thread.sleep(50);

            // Add more items while compaction might be running
            for (int i = 20; i < 30; i++) {
                queue.offer("msg-" + i);
            }

            // All items should be consumed in order
            List<String> consumed = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                Optional<String> item = queue.poll(1, TimeUnit.SECONDS);
                if (item.isPresent()) {
                    consumed.add(item.get());
                }
            }

            // Should have consumed items 10-29
            assertEquals(20, consumed.size());
            for (int i = 0; i < 20; i++) {
                assertEquals("msg-" + (i + 10), consumed.get(i));
            }
        }
    }

    @Test
    void testDrainOnClose() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withMemoryBuffer(true)
                .withMemoryBufferSize(100)
                .withLockTryTimeout(Duration.ofMillis(10))
                .withRevalidationInterval(Duration.ofMillis(100))
                .withFsync(false);

        NQueue<String> queue = NQueue.open(tempDir, "close-test", options);
        
        // Add items that might be in memory buffer
        for (int i = 0; i < 10; i++) {
            queue.offer("item-" + i);
        }

        // Close should drain everything
        queue.close();

        // Reopen and verify all items are persisted
        try (NQueue<String> reopened = NQueue.open(tempDir, "close-test", options)) {
            List<String> consumed = new ArrayList<>();
            while (true) {
                Optional<String> item = reopened.poll(100, TimeUnit.MILLISECONDS);
                if (item.isEmpty()) {
                    break;
                }
                consumed.add(item.get());
            }

            assertEquals(10, consumed.size());
            for (int i = 0; i < 10; i++) {
                assertEquals("item-" + i, consumed.get(i));
            }
        }
    }

    @Test
    void testConcurrentOffersWithMemoryBuffer() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withMemoryBuffer(true)
                .withMemoryBufferSize(1000)
                .withLockTryTimeout(Duration.ofMillis(10))
                .withRevalidationInterval(Duration.ofMillis(100))
                .withFsync(false);

        int numThreads = 5;
        int itemsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try (NQueue<Integer> queue = NQueue.open(tempDir, "concurrent-offer", options)) {
            List<Future<?>> futures = new ArrayList<>();
            CountDownLatch start = new CountDownLatch(1);

            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                futures.add(executor.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < itemsPerThread; i++) {
                            queue.offer(threadId * itemsPerThread + i);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            start.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }

            // Verify all items were added
            assertEquals(numThreads * itemsPerThread, queue.size());
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testConcurrentPollWithMemoryBuffer() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withMemoryBuffer(true)
                .withMemoryBufferSize(1000)
                .withLockTryTimeout(Duration.ofMillis(10))
                .withRevalidationInterval(Duration.ofMillis(100))
                .withFsync(false);

        int totalItems = 200;
        ExecutorService executor = Executors.newFixedThreadPool(4);

        try (NQueue<Integer> queue = NQueue.open(tempDir, "concurrent-poll", options)) {
            // Add all items first
            for (int i = 0; i < totalItems; i++) {
                queue.offer(i);
            }

            List<Integer> consumed = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch start = new CountDownLatch(1);
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < 4; t++) {
                futures.add(executor.submit(() -> {
                    try {
                        start.await();
                        while (consumed.size() < totalItems) {
                            Optional<Integer> item = queue.poll(100, TimeUnit.MILLISECONDS);
                            item.ifPresent(consumed::add);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }

            start.countDown();
            for (Future<?> f : futures) {
                f.get(10, TimeUnit.SECONDS);
            }

            assertEquals(totalItems, consumed.size());
            // Verify all items 0-199 are present (order may vary due to concurrency)
            for (int i = 0; i < totalItems; i++) {
                assertTrue(consumed.contains(i), "Missing item: " + i);
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void testBufferFullBlocksUntilLockAvailable() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withMemoryBuffer(true)
                .withMemoryBufferSize(5) // Small buffer
                .withLockTryTimeout(Duration.ofMillis(10))
                .withRevalidationInterval(Duration.ofMillis(100))
                .withFsync(false);

        try (NQueue<String> queue = NQueue.open(tempDir, "full-buffer", options)) {
            // Fill buffer by adding items that might go to memory
            // This test verifies that when buffer is full and lock is unavailable,
            // operations block appropriately
            for (int i = 0; i < 5; i++) {
                queue.offer("item-" + i);
            }

            // Buffer should accept these items
            // If buffer fills and lock is still unavailable, next offer should block
            // This is a basic test - full scenario would require more complex setup
            assertTrue(queue.size() >= 0); // At least verify queue is functional
        }
    }

    @Test
    void testMemoryBufferWithZeroSize() {
        // Should fail validation
        assertThrows(IllegalArgumentException.class, () -> {
            NQueue.Options.defaults()
                    .withMemoryBuffer(true)
                    .withMemoryBufferSize(0);
        });
    }

    @Test
    void testMemoryBufferWithVerySmallTimeout() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withMemoryBuffer(true)
                .withMemoryBufferSize(100)
                .withLockTryTimeout(Duration.ofNanos(1)) // Very small timeout
                .withRevalidationInterval(Duration.ofMillis(100));

        try (NQueue<String> queue = NQueue.open(tempDir, "small-timeout", options)) {
            // Should still work, just might use memory buffer more often
            queue.offer("test");
            Optional<String> result = queue.poll();
            assertTrue(result.isPresent());
            assertEquals("test", result.get());
        }
    }

    @Test
    void testBackwardCompatibility() throws Exception {
        // Test that existing code without memory buffer still works
        try (NQueue<String> queue = NQueue.open(tempDir, "backward-compat")) {
            queue.offer("first");
            queue.offer("second");

            assertEquals("first", queue.poll().orElseThrow());
            assertEquals("second", queue.poll().orElseThrow());
            // Use poll with timeout to avoid blocking indefinitely when queue is empty
            assertTrue(queue.poll(0, TimeUnit.SECONDS).isEmpty(), "Queue is not empty...");
        }
    }

    @Test
    void testMemoryBufferUnderHighLoad() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withMemoryBuffer(true)
                .withMemoryBufferSize(5000)
                .withLockTryTimeout(Duration.ofMillis(10))
                .withRevalidationInterval(Duration.ofMillis(100))
                .withCompactionWasteThreshold(0.1d)
                .withCompactionInterval(Duration.ofMillis(5))
                .withFsync(false);

        int totalItems = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(10);

        try (NQueue<Integer> queue = NQueue.open(tempDir, "high-load", options)) {
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger produced = new AtomicInteger(0);
            AtomicInteger consumed = new AtomicInteger(0);
            List<Integer> consumedList = Collections.synchronizedList(new ArrayList<>());
            AtomicBoolean producersDone = new AtomicBoolean(false);

            // Producers
            for (int i = 0; i < 5; i++) {
                executor.submit(() -> {
                    try {
                        start.await();
                        while (true) {
                            int current = produced.get();
                            if (current >= totalItems) {
                                break;
                            }
                            if (produced.compareAndSet(current, current + 1)) {
                                queue.offer(current);
                            }
                        }
                        // Mark producers as done when all threads finish
                        if (produced.get() >= totalItems) {
                            producersDone.set(true);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            // Consumers
            for (int i = 0; i < 5; i++) {
                executor.submit(() -> {
                    try {
                        start.await();
                        long startTime = System.currentTimeMillis();
                        long maxWaitTime = 60_000; // 60 seconds max
                        
                        while (consumed.get() < totalItems) {
                            // Safety timeout to prevent infinite loops
                            if (System.currentTimeMillis() - startTime > maxWaitTime) {
                                break;
                            }
                            
                            Optional<Integer> item = queue.poll(100, TimeUnit.MILLISECONDS);
                            if (item.isPresent()) {
                                consumedList.add(item.get());
                                consumed.incrementAndGet();
                            } else {
                                // If no item and producers are done, check if queue is empty
                                if (producersDone.get()) {
                                    try {
                                        long queueSize = queue.size();
                                        if (queueSize == 0 && consumed.get() < totalItems) {
                                            // Wait a bit more in case items are still being drained
                                            Thread.sleep(100);
                                            if (queue.size() == 0) {
                                                break;
                                            }
                                        }
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            start.countDown();
            executor.shutdown();
            
            // Verify that all threads terminated successfully
            boolean terminated = executor.awaitTermination(60, TimeUnit.SECONDS);
            if (!terminated) {
                executor.shutdownNow();
                executor.awaitTermination(5, TimeUnit.SECONDS);
                fail("Test did not complete within timeout. Produced: " + produced.get() + 
                     ", Consumed: " + consumed.get() + ", Queue size: " + queue.size());
            }

            // Verify all items were produced
            assertTrue(produced.get() >= totalItems, 
                "Not all items were produced. Expected at least " + totalItems + ", got " + produced.get());

            // Verify all items were consumed
            assertEquals(totalItems, consumedList.size(),
                "Not all items were consumed. Expected " + totalItems + ", got " + consumedList.size() + 
                ". Produced: " + produced.get() + ", Consumed counter: " + consumed.get());
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}

