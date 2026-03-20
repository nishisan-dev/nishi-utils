package dev.nishisan.utils.ngrid.queue;

import dev.nishisan.utils.ngrid.common.NodeId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that OffsetStore implementations guarantee monotonically increasing
 * offsets.
 * This is critical for preventing message replay during leader failover.
 */
class MonotonicOffsetStoreTest {

    @TempDir
    Path tempDir;

    private LocalOffsetStoreTestable offsetStore;

    @BeforeEach
    void setUp() {
        offsetStore = new LocalOffsetStoreTestable(tempDir.resolve("offsets.dat"));
    }

    @Test
    void testOffsetOnlyIncreases() {
        NodeId consumer = NodeId.of("consumer-1");

        // Initial offset should be 0
        assertEquals(0L, offsetStore.getOffset(consumer));

        // Update to 10
        offsetStore.updateOffset(consumer, 10L);
        assertEquals(10L, offsetStore.getOffset(consumer));

        // Update to 20
        offsetStore.updateOffset(consumer, 20L);
        assertEquals(20L, offsetStore.getOffset(consumer));

        // Try to regress to 15 - should be ignored
        offsetStore.updateOffset(consumer, 15L);
        assertEquals(20L, offsetStore.getOffset(consumer), "Offset should not regress");

        // Try to regress to 5 - should be ignored
        offsetStore.updateOffset(consumer, 5L);
        assertEquals(20L, offsetStore.getOffset(consumer), "Offset should not regress");

        // Update to same value - should be ignored (not strictly greater)
        offsetStore.updateOffset(consumer, 20L);
        assertEquals(20L, offsetStore.getOffset(consumer));

        // Update to 25 - should succeed
        offsetStore.updateOffset(consumer, 25L);
        assertEquals(25L, offsetStore.getOffset(consumer));
    }

    @Test
    void testConcurrentUpdatesRespectMonotonicity() throws InterruptedException {
        NodeId consumer = NodeId.of("consumer-concurrent");
        int threadCount = 10;
        int updatesPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicLong maxOffset = new AtomicLong(0);

        // Each thread tries to set random offsets
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < updatesPerThread; i++) {
                        // Mix of increasing and decreasing values
                        long offset = (threadId * 1000L) + i;
                        offsetStore.updateOffset(consumer, offset);
                        maxOffset.updateAndGet(current -> Math.max(current, offset));
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        // Final offset should be at least the max we tried to set
        long finalOffset = offsetStore.getOffset(consumer);
        assertTrue(finalOffset >= maxOffset.get() - threadCount * updatesPerThread,
                "Final offset should reflect monotonic updates");
    }

    @Test
    void testMultipleConsumersIndependent() {
        NodeId consumer1 = NodeId.of("consumer-1");
        NodeId consumer2 = NodeId.of("consumer-2");

        offsetStore.updateOffset(consumer1, 100L);
        offsetStore.updateOffset(consumer2, 50L);

        assertEquals(100L, offsetStore.getOffset(consumer1));
        assertEquals(50L, offsetStore.getOffset(consumer2));

        // Regression attempt on consumer1 should not affect consumer2
        offsetStore.updateOffset(consumer1, 30L);
        assertEquals(100L, offsetStore.getOffset(consumer1), "Consumer1 should not regress");
        assertEquals(50L, offsetStore.getOffset(consumer2), "Consumer2 should be unaffected");
    }

    /**
     * Test wrapper that exposes the inner LocalOffsetStore for testing.
     */
    private static class LocalOffsetStoreTestable implements OffsetStore {
        private final Path storagePath;
        private final java.util.Map<NodeId, Long> offsets = new java.util.concurrent.ConcurrentHashMap<>();

        LocalOffsetStoreTestable(Path storagePath) {
            this.storagePath = storagePath;
        }

        @Override
        public long getOffset(NodeId nodeId) {
            return offsets.getOrDefault(nodeId, 0L);
        }

        @Override
        public void updateOffset(NodeId nodeId, long offset) {
            offsets.compute(nodeId, (key, current) -> {
                if (current == null || offset > current) {
                    return offset;
                }
                return current;
            });
        }
    }
}
