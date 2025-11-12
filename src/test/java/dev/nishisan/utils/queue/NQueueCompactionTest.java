package dev.nishisan.utils.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

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
        NQueueQueueMeta meta = NQueueQueueMeta.read(metaPath);

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
        NQueueQueueMeta meta = NQueueQueueMeta.read(metaPath);

        assertEquals(2L, meta.getRecordCount());
        assertEquals(0L, meta.getConsumerOffset());
    }
}
