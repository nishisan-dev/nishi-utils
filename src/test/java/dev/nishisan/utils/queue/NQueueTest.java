package dev.nishisan.utils.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NQueueTest {

    @TempDir
    Path tempDir;

    @Test
    void offerAndPollShouldReturnRecordsInInsertionOrder() throws Exception {
        System.out.println("TMP DIR IS:"+ tempDir);
        try (NQueue<String> queue = NQueue.open(tempDir, "basic")) {
            queue.offer("foo");
            queue.offer("bar");

            Optional<NQueueRecord> first = queue.poll();
            assertTrue(first.isPresent(), "Expected first record to be present");
            assertEquals("foo", deserialize(first.get().payload()));
            assertEquals(0L, first.get().meta().getIndex());
            assertEquals(String.class.getCanonicalName(), first.get().meta().getClassName());

            Optional<NQueueRecord> second = queue.poll();
            assertTrue(second.isPresent(), "Expected second record to be present");
            assertEquals("bar", deserialize(second.get().payload()));
            assertEquals(1L, second.get().meta().getIndex());

            assertTrue(queue.isEmpty());
            assertEquals(0L, queue.getRecordCount());
        }
    }

    @Test
    void pollShouldBlockUntilDataBecomesAvailable() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (NQueue<String> queue = NQueue.open(tempDir, "blocking")) {
            Future<Optional<NQueueRecord>> future = executor.submit(() -> {
                try {
                    return queue.poll();
                } catch (IOException e) {
                    throw new IllegalStateException("Unexpected failure while polling", e);
                }
            });

            Thread.sleep(200);
            assertFalse(future.isDone(), "Poll should block while queue is empty");

            queue.offer("delayed");

            Optional<NQueueRecord> record = future.get(2, TimeUnit.SECONDS);
            assertTrue(record.isPresent());
            assertEquals("delayed", deserialize(record.get().payload()));
            assertEquals(0L, record.get().meta().getIndex());
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void openShouldRecoverStateWhenMetadataIsCorrupted() throws Exception {
        Path queueName = Path.of("recovery");
        Path queueDir = tempDir.resolve(queueName);

        try (NQueue<String> queue = NQueue.open(tempDir, queueName.toString())) {
            queue.offer("first");
            queue.offer("second");
        }

        Path metaPath = queueDir.resolve("queue.meta");
        Path dataPath = queueDir.resolve("data.log");
        long corruptedProducerOffset = Files.size(dataPath) + 128;
        NQueueQueueMeta.write(metaPath, 9999L, corruptedProducerOffset, 999L, 999L);

        try (NQueue<String> queue = NQueue.open(tempDir, queueName.toString())) {
            assertEquals(2L, queue.size());

            Optional<NQueueRecord> first = queue.poll();
            assertTrue(first.isPresent());
            assertEquals("first", deserialize(first.get().payload()));
            assertEquals(0L, first.get().meta().getIndex());

            Optional<NQueueRecord> second = queue.poll();
            assertTrue(second.isPresent());
            assertEquals("second", deserialize(second.get().payload()));
            assertEquals(1L, second.get().meta().getIndex());

            assertTrue(queue.poll(10, TimeUnit.MILLISECONDS).isEmpty());
        }
    }

    @Test
    void metadataOffsetsShouldRemainConsistentAcrossOperations() throws Exception {
        Path queueName = Path.of("offsets");
        Path queueDir = tempDir.resolve(queueName);

        try (NQueue<String> queue = NQueue.open(tempDir, queueName.toString())) {
            long firstOffset = queue.offer("first");
            NQueueReadResult firstRead = queue.readAt(firstOffset).orElseThrow();
            NQueueQueueMeta metaAfterFirst = NQueueQueueMeta.read(queueDir.resolve("queue.meta"));

            assertEquals(firstOffset, metaAfterFirst.getConsumerOffset());
            assertEquals(firstRead.getNextOffset(), metaAfterFirst.getProducerOffset());
            assertEquals(1L, metaAfterFirst.getRecordCount());
            assertEquals(firstRead.getRecord().meta().getIndex(), metaAfterFirst.getLastIndex());

            long secondOffset = queue.offer("second");
            NQueueReadResult secondRead = queue.readAt(secondOffset).orElseThrow();
            NQueueQueueMeta metaAfterSecond = NQueueQueueMeta.read(queueDir.resolve("queue.meta"));

            assertEquals(firstOffset, metaAfterSecond.getConsumerOffset());
            assertEquals(secondRead.getNextOffset(), metaAfterSecond.getProducerOffset());
            assertEquals(2L, metaAfterSecond.getRecordCount());
            assertEquals(secondRead.getRecord().meta().getIndex(), metaAfterSecond.getLastIndex());

            Optional<NQueueRecord> consumedFirst = queue.poll();
            assertTrue(consumedFirst.isPresent());
            assertEquals("first", deserialize(consumedFirst.get().payload()));
            NQueueQueueMeta metaAfterPoll = NQueueQueueMeta.read(queueDir.resolve("queue.meta"));

            assertEquals(secondOffset, metaAfterPoll.getConsumerOffset());
            assertEquals(metaAfterSecond.getProducerOffset(), metaAfterPoll.getProducerOffset());
            assertEquals(1L, metaAfterPoll.getRecordCount());
            assertEquals(secondRead.getRecord().meta().getIndex(), metaAfterPoll.getLastIndex());

            Optional<NQueueRecord> consumedSecond = queue.poll();
            assertTrue(consumedSecond.isPresent());
            assertEquals("second", deserialize(consumedSecond.get().payload()));
            NQueueQueueMeta metaAfterEmpty = NQueueQueueMeta.read(queueDir.resolve("queue.meta"));

            assertEquals(metaAfterEmpty.getProducerOffset(), metaAfterEmpty.getConsumerOffset());
            assertEquals(0L, metaAfterEmpty.getRecordCount());
        }
    }

    private static String deserialize(byte[] payload) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(payload);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (String) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("Failed to deserialize payload", e);
        }
    }
}
