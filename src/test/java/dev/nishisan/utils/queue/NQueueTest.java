package dev.nishisan.utils.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class NQueueTest {

    @TempDir
    Path tempDir;

    @Test
    void offerAndPollShouldReturnRecordsInInsertionOrder() throws Exception {
        try (NQueue<String> queue = NQueue.open(tempDir, "basic")) {
            queue.offer("foo");
            queue.offer("bar");

            Optional<NQueueRecord> peeked = queue.peek();
            assertTrue(peeked.isPresent(), "Expected peeked record to be present");
            assertEquals("foo", deserialize(peeked.get().payload()));
            assertEquals(0L, peeked.get().meta().getIndex());
            assertEquals(String.class.getCanonicalName(), peeked.get().meta().getClassName());

            Optional<String> first = queue.poll();
            assertTrue(first.isPresent(), "Expected first record to be present");
            assertEquals("foo", first.get());

            Optional<NQueueRecord> peekedSecond = queue.peek();
            assertTrue(peekedSecond.isPresent(), "Expected peeked second record to be present");
            assertEquals(1L, peekedSecond.get().meta().getIndex());

            Optional<String> second = queue.poll();
            assertTrue(second.isPresent(), "Expected second record to be present");
            assertEquals("bar", second.get());

            assertTrue(queue.isEmpty());
            assertEquals(0L, queue.getRecordCount());
        }
    }

    @Test
    void pollShouldBlockUntilDataBecomesAvailable() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (NQueue<String> queue = NQueue.open(tempDir, "blocking")) {
            Future<Optional<String>> future = executor.submit(() -> {
                try {
                    return queue.poll();
                } catch (IOException e) {
                    throw new IllegalStateException("Unexpected failure while polling", e);
                }
            });

            Thread.sleep(200);
            assertFalse(future.isDone(), "Poll should block while queue is empty");

            long offset = queue.offer("delayed");

            Optional<String> record = future.get(2, TimeUnit.SECONDS);
            assertTrue(record.isPresent());
            assertEquals("delayed", record.get());

            NQueueReadResult readResult = queue.readAt(offset).orElseThrow();
            assertEquals(0L, readResult.getRecord().meta().getIndex());
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

            Optional<NQueueRecord> peekedFirst = queue.peek();
            assertTrue(peekedFirst.isPresent());
            assertEquals("first", deserialize(peekedFirst.get().payload()));
            assertEquals(0L, peekedFirst.get().meta().getIndex());

            Optional<String> first = queue.poll();
            assertTrue(first.isPresent());
            assertEquals("first", first.get());

            Optional<NQueueRecord> peekedSecond = queue.peek();
            assertTrue(peekedSecond.isPresent());
            assertEquals("second", deserialize(peekedSecond.get().payload()));
            assertEquals(1L, peekedSecond.get().meta().getIndex());

            Optional<String> second = queue.poll();
            assertTrue(second.isPresent());
            assertEquals("second", second.get());

            assertTrue(queue.poll(10, TimeUnit.MILLISECONDS).isEmpty());
        }
    }

    @Test
    void metadataOffsetsShouldRemainConsistentAcrossOperations() throws Exception {
        Path queueName = Path.of("offsets");
        Path queueDir = tempDir.resolve(queueName);

        try (NQueue<String> queue = NQueue.open(tempDir, queueName.toString())) {
            long firstOffset = queue.offer("first");
            String firstValue = queue.readAt(firstOffset).orElseThrow();
            assertEquals("first", firstValue);

            NQueueReadResult firstRead = queue.readRecordAt(firstOffset).orElseThrow();
            NQueueQueueMeta metaAfterFirst = NQueueQueueMeta.read(queueDir.resolve("queue.meta"));

            assertEquals(firstOffset, metaAfterFirst.getConsumerOffset());
            assertEquals(firstRead.getNextOffset(), metaAfterFirst.getProducerOffset());
            assertEquals(1L, metaAfterFirst.getRecordCount());
            assertEquals(firstRead.getRecord().meta().getIndex(), metaAfterFirst.getLastIndex());

            long secondOffset = queue.offer("second");
            String secondValue = queue.readAt(secondOffset).orElseThrow();
            assertEquals("second", secondValue);

            NQueueReadResult secondRead = queue.readRecordAt(secondOffset).orElseThrow();
            NQueueQueueMeta metaAfterSecond = NQueueQueueMeta.read(queueDir.resolve("queue.meta"));

            assertEquals(firstOffset, metaAfterSecond.getConsumerOffset());
            assertEquals(secondRead.getNextOffset(), metaAfterSecond.getProducerOffset());
            assertEquals(2L, metaAfterSecond.getRecordCount());
            assertEquals(secondRead.getRecord().meta().getIndex(), metaAfterSecond.getLastIndex());

            Optional<String> consumedFirst = queue.poll();
            assertTrue(consumedFirst.isPresent());
            assertEquals("first", consumedFirst.get());
            NQueueQueueMeta metaAfterPoll = NQueueQueueMeta.read(queueDir.resolve("queue.meta"));

            assertEquals(secondOffset, metaAfterPoll.getConsumerOffset());
            assertEquals(metaAfterSecond.getProducerOffset(), metaAfterPoll.getProducerOffset());
            assertEquals(1L, metaAfterPoll.getRecordCount());
            assertEquals(secondRead.getRecord().meta().getIndex(), metaAfterPoll.getLastIndex());

            Optional<String> consumedSecond = queue.poll();
            assertTrue(consumedSecond.isPresent());
            assertEquals("second", consumedSecond.get());
            NQueueQueueMeta metaAfterEmpty = NQueueQueueMeta.read(queueDir.resolve("queue.meta"));

            assertEquals(metaAfterEmpty.getProducerOffset(), metaAfterEmpty.getConsumerOffset());
            assertEquals(0L, metaAfterEmpty.getRecordCount());
        }
    }

    @Test
    void parallelOfferAndPollShouldHandleComplexPayloads() throws Exception {
        List<ComplexPayload> expected = buildComplexPayloads(1000);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (NQueue<ComplexPayload> queue = NQueue.open(tempDir, "parallel-complex")) {
            Future<Void> producer = executor.submit(() -> {
                ThreadLocalRandom rng = ThreadLocalRandom.current();
                for (ComplexPayload payload : expected) {
                    queue.offer(payload);
                    randomLatency(rng);
                }
                return null;
            });

            Future<List<ComplexPayload>> consumer = executor.submit(() -> {
                ThreadLocalRandom rng = ThreadLocalRandom.current();
                List<ComplexPayload> consumed = new ArrayList<>();
                while (consumed.size() < expected.size()) {
                    Optional<ComplexPayload> record = queue.poll();
                    if (record.isEmpty()) {
                        throw new IllegalStateException("Queue returned empty optional while blocking");
                    }
                    consumed.add(record.get());
                    randomLatency(rng);
                }
                return consumed;
            });

            producer.get(60, TimeUnit.SECONDS);
            List<ComplexPayload> consumed = consumer.get(60, TimeUnit.SECONDS);
            assertEquals(expected, consumed, "Consumed payloads should match the enqueued ones");
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void pollWithTimeoutShouldReturnEmptyWhenQueueRemainsEmpty() throws Exception {
        try (NQueue<String> queue = NQueue.open(tempDir, "timeout")) {
            long start = System.nanoTime();
            Optional<String> record = queue.poll(150, TimeUnit.MILLISECONDS);
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertTrue(record.isEmpty(), "Poll with timeout should return empty when no data is available");
            assertTrue(elapsed >= 100, "Poll should block for at least most of the timeout interval");
        }
    }

    private static void randomLatency(ThreadLocalRandom random) throws InterruptedException {
        long millis = random.nextLong(0, 3);
        if (millis > 0) {
            TimeUnit.MILLISECONDS.sleep(millis);
        }
    }

    private static List<ComplexPayload> buildComplexPayloads(int total) {
        Random random = new Random(1729L);
        return IntStream.range(0, total)
                .mapToObj(index -> ComplexPayload.random(index, random))
                .collect(Collectors.toList());
    }

    private static final class ComplexPayload implements Serializable {
        private static final long serialVersionUID = 1L;

        private final int id;
        private final String name;
        private final List<Integer> values;
        private final Map<String, Double> attributes;
        private final Nested nested;

        private ComplexPayload(int id, String name, List<Integer> values, Map<String, Double> attributes, Nested nested) {
            this.id = id;
            this.name = name;
            this.values = List.copyOf(values);
            this.attributes = Map.copyOf(attributes);
            this.nested = nested;
        }

        static ComplexPayload random(int id, Random random) {
            String name = "payload-" + id;

            int valueCount = 3 + random.nextInt(3);
            List<Integer> values = new ArrayList<>(valueCount);
            for (int i = 0; i < valueCount; i++) {
                values.add(random.nextInt(10_000));
            }

            int attributeCount = 2 + random.nextInt(3);
            Map<String, Double> attributes = new java.util.LinkedHashMap<>();
            for (int i = 0; i < attributeCount; i++) {
                attributes.put("attr-" + i, random.nextDouble());
            }

            Nested nested = new Nested("nested-" + id, random.nextBoolean(), random.nextLong());
            return new ComplexPayload(id, name, values, attributes, nested);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ComplexPayload that = (ComplexPayload) o;
            return id == that.id
                    && Objects.equals(name, that.name)
                    && Objects.equals(values, that.values)
                    && Objects.equals(attributes, that.attributes)
                    && Objects.equals(nested, that.nested);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, values, attributes, nested);
        }
    }

    private static final class Nested implements Serializable {
        private static final long serialVersionUID = 1L;

        private final String description;
        private final boolean active;
        private final long timestamp;

        Nested(String description, boolean active, long timestamp) {
            this.description = description;
            this.active = active;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Nested nested = (Nested) o;
            return active == nested.active
                    && timestamp == nested.timestamp
                    && Objects.equals(description, nested.description);
        }

        @Override
        public int hashCode() {
            return Objects.hash(description, active, timestamp);
        }
    }
}
