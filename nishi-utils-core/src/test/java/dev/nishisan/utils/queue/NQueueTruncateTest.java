package dev.nishisan.utils.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link NQueue#truncateAndReopen()} used during snapshot install.
 */
class NQueueTruncateTest {

    @TempDir
    Path tempDir;

    @Test
    void truncateAndReopenClearsAllDataAndAllowsNewOffers() throws Exception {
        NQueue.Options opts = NQueue.Options.defaults()
                .withMemoryBuffer(true).withMemoryBufferSize(100)
                .withShortCircuit(false);
        NQueue<String> queue = NQueue.open(tempDir, "trunc-test", opts);

        // Offer items
        for (int i = 0; i < 50; i++) {
            queue.offer("item-" + i);
        }
        assertTrue(queue.size() > 0, "Queue should have items before truncate");

        // Close + truncate + reopen
        queue.close();
        queue.truncateAndReopen();

        // Queue should be empty
        assertEquals(0, queue.size(), "Queue should be empty after truncateAndReopen");

        // Should accept new offers with fresh indices
        queue.offer("new-item-0");
        queue.offer("new-item-1");
        assertEquals(2, queue.size(), "Queue should have 2 items after re-offer");

        // Poll and verify items
        Optional<String> first = queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
        assertTrue(first.isPresent(), "Should be able to poll after truncateAndReopen");
        assertEquals("new-item-0", first.get());

        Optional<String> second = queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
        assertTrue(second.isPresent());
        assertEquals("new-item-1", second.get());

        // Queue should be empty now
        assertEquals(0, queue.size());

        queue.close();
    }

    @Test
    void truncateRequiresClosedQueue() throws Exception {
        NQueue<String> queue = NQueue.open(tempDir, "trunc-require-close", NQueue.Options.defaults());

        assertThrows(IllegalStateException.class, queue::truncateAndReopen,
                "Should throw if queue is not closed");

        queue.close();
    }

    @Test
    void truncateAndReopenWithoutMemoryBuffer() throws Exception {
        NQueue.Options opts = NQueue.Options.defaults()
                .withMemoryBuffer(false)
                .withShortCircuit(false);
        NQueue<String> queue = NQueue.open(tempDir, "trunc-no-buf", opts);

        for (int i = 0; i < 20; i++) {
            queue.offer("item-" + i);
        }

        queue.close();
        queue.truncateAndReopen();

        assertEquals(0, queue.size(), "Queue should be empty after truncate");

        queue.offer("after-trunc");
        assertEquals(1, queue.size());

        Optional<String> val = queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
        assertTrue(val.isPresent());
        assertEquals("after-trunc", val.get());

        queue.close();
    }

    @Test
    void multipleTruncateCycles() throws Exception {
        NQueue.Options opts = NQueue.Options.defaults()
                .withMemoryBuffer(true).withMemoryBufferSize(50)
                .withShortCircuit(false);
        NQueue<String> queue = NQueue.open(tempDir, "trunc-multi", opts);

        for (int cycle = 0; cycle < 3; cycle++) {
            // Fill
            for (int i = 0; i < 10; i++) {
                queue.offer("cycle-" + cycle + "-item-" + i);
            }
            assertEquals(10, queue.size(), "Cycle " + cycle + ": should have 10 items");

            // Truncate
            queue.close();
            queue.truncateAndReopen();
            assertEquals(0, queue.size(), "Cycle " + cycle + ": should be empty after truncate");
        }

        // Final verification: queue still functional
        queue.offer("final-item");
        Optional<String> val = queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
        assertTrue(val.isPresent());
        assertEquals("final-item", val.get());

        queue.close();
    }

    @Test
    void truncatePreservesKeyAndHeadersOnSubsequentOffers() throws Exception {
        NQueue.Options opts = NQueue.Options.defaults()
                .withMemoryBuffer(true).withMemoryBufferSize(100)
                .withShortCircuit(false);
        NQueue<String> queue = NQueue.open(tempDir, "trunc-kh", opts);

        queue.offer("before-trunc".getBytes(), "data-before");
        queue.close();
        queue.truncateAndReopen();

        // Offer with key + headers after truncate
        NQueueHeaders headers = NQueueHeaders.of("h1", "v1".getBytes());
        queue.offer("my-key".getBytes(), headers, "data-after");

        // Read via readRecordAtIndex to verify metadata is preserved
        Optional<NQueueReadResult> result = queue.readRecordAtIndex(1);
        assertTrue(result.isPresent(), "Should find record at index 1");

        NQueueRecord record = result.get().getRecord();
        assertNotNull(record.meta().getKey(), "Key should be preserved");
        assertArrayEquals("my-key".getBytes(), record.meta().getKey());

        NQueueHeaders readHeaders = record.meta().getHeaders();
        assertNotNull(readHeaders, "Headers should be preserved");
        assertTrue(readHeaders.get("h1").isPresent(), "Header h1 should be present");
        assertArrayEquals("v1".getBytes(), readHeaders.get("h1").get());

        queue.close();
    }
}
