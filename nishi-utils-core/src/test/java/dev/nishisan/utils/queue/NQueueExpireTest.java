package dev.nishisan.utils.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the write-time expiration feature
 * ({@link NQueue.Options#withExpireAfterWrite(Duration)} and
 * {@link NQueue#flushExpired()}). Expired records form a contiguous prefix at the
 * head and are skipped opportunistically by {@code poll}/{@code peek}.
 */
class NQueueExpireTest {

    // Generous margins keep the time-sensitive cases stable across slow CI hosts.
    private static final Duration EXPIRE = Duration.ofMillis(50);
    private static final long SLEEP_PAST_EXPIRE_MS = 150;

    @TempDir
    Path tempDir;

    private final List<NQueue<?>> trackedQueues = Collections.synchronizedList(new ArrayList<>());

    private <T extends java.io.Serializable> NQueue<T> track(NQueue<T> queue) {
        trackedQueues.add(queue);
        return queue;
    }

    @AfterEach
    void validateConsistency() {
        for (NQueue<?> queue : trackedQueues) {
            Long violations = queue.getStats().getCounterValueOrNull("nqueue.out_of_order");
            if (violations != null && violations > 0) {
                fail("Queue detected internal FIFO violations: " + violations);
            }
        }
        trackedQueues.clear();
    }

    private static NQueue.Options expiringOptions() {
        return NQueue.Options.defaults()
                .withExpireAfterWrite(EXPIRE)
                .withFsync(false);
    }

    @Test
    void pollSkipsExpiredPrefixAndReturnsFirstValid() throws Exception {
        try (NQueue<String> queue = track(NQueue.open(tempDir, "poll-skip", expiringOptions()))) {
            queue.offer("a");
            queue.offer("b");
            Thread.sleep(SLEEP_PAST_EXPIRE_MS);
            queue.offer("c");

            Optional<String> polled = queue.poll();
            assertTrue(polled.isPresent(), "expected the first valid element");
            assertEquals("c", polled.get());
            assertTrue(queue.isEmpty(), "queue should be empty after consuming the only valid element");
        }
    }

    @Test
    void pollTimeoutReturnsEmptyWhenAllExpired() throws Exception {
        try (NQueue<String> queue = track(NQueue.open(tempDir, "poll-timeout", expiringOptions()))) {
            queue.offer("a");
            queue.offer("b");
            Thread.sleep(SLEEP_PAST_EXPIRE_MS);

            Optional<String> polled = queue.poll(50, TimeUnit.MILLISECONDS);
            assertFalse(polled.isPresent(), "all elements expired; poll should time out empty");
            assertEquals(0L, queue.getRecordCount());
        }
    }

    @Test
    void peekSkipsExpiredAndShowsFirstValid() throws Exception {
        try (NQueue<String> queue = track(NQueue.open(tempDir, "peek-skip", expiringOptions()))) {
            queue.offer("a");
            Thread.sleep(SLEEP_PAST_EXPIRE_MS);
            queue.offer("b");

            Optional<String> peeked = queue.peek();
            assertTrue(peeked.isPresent(), "expected the first valid element on peek");
            assertEquals("b", peeked.get());

            // peek discarded the expired head, so poll must observe the same element.
            assertEquals("b", queue.poll().orElseThrow());
            assertTrue(queue.isEmpty());
        }
    }

    @Test
    void flushExpiredReturnsDiscardedCount() throws Exception {
        try (NQueue<String> queue = track(NQueue.open(tempDir, "flush-count", expiringOptions()))) {
            queue.offer("a");
            queue.offer("b");
            queue.offer("c");
            Thread.sleep(SLEEP_PAST_EXPIRE_MS);
            queue.offer("d");

            long discarded = queue.flushExpired();
            assertEquals(3L, discarded, "three expired records should be discarded");
            assertEquals(1L, queue.getRecordCount(), "only the valid element should remain");
            assertEquals("d", queue.poll().orElseThrow());
        }
    }

    @Test
    void flushExpiredReturnsZeroWhenDisabled() throws Exception {
        // Default options: no expiration configured.
        try (NQueue<String> queue = track(NQueue.open(tempDir, "flush-disabled"))) {
            queue.offer("a");
            queue.offer("b");
            Thread.sleep(SLEEP_PAST_EXPIRE_MS);

            assertEquals(0L, queue.flushExpired(), "expiration disabled must discard nothing");
            assertEquals("a", queue.poll().orElseThrow());
            assertEquals("b", queue.poll().orElseThrow());
        }
    }

    @Test
    void expirationDisabledByDefaultDoesNotAffectPoll() throws Exception {
        try (NQueue<String> queue = track(NQueue.open(tempDir, "disabled-default"))) {
            queue.offer("a");
            Thread.sleep(SLEEP_PAST_EXPIRE_MS);
            assertEquals("a", queue.poll().orElseThrow(), "without expiration the element must survive");
        }
    }

    @Test
    void mixedExpiredAndValidPreservesFifoOfValid() throws Exception {
        try (NQueue<String> queue = track(NQueue.open(tempDir, "mixed", expiringOptions()))) {
            queue.offer("expired-1");
            queue.offer("expired-2");
            queue.offer("expired-3");
            Thread.sleep(SLEEP_PAST_EXPIRE_MS);
            queue.offer("valid-1");
            queue.offer("valid-2");

            assertEquals("valid-1", queue.poll().orElseThrow());
            assertEquals("valid-2", queue.poll().orElseThrow());
            assertFalse(queue.poll(50, TimeUnit.MILLISECONDS).isPresent());
        }
    }

    @Test
    void expiredMetricReflectsDiscardedCount() throws Exception {
        try (NQueue<String> queue = track(NQueue.open(tempDir, "metric", expiringOptions()))) {
            queue.offer("a");
            queue.offer("b");
            Thread.sleep(SLEEP_PAST_EXPIRE_MS);

            queue.flushExpired();

            // StatsUtils keys are case-sensitive verbatim: use the exact constant value.
            Long expired = queue.getStats().getCounterValueOrNull(NQueueMetrics.EXPIRED_EVENT);
            assertEquals(2L, expired, "the expired counter must match the discarded records");
        }
    }

    @Test
    void expireAfterWriteWorksWithTimeBasedRetention() throws Exception {
        NQueue.Options options = NQueue.Options.defaults()
                .withRetentionPolicy(NQueue.Options.RetentionPolicy.TIME_BASED)
                .withRetentionTime(Duration.ofHours(1))
                .withExpireAfterWrite(EXPIRE)
                .withFsync(false);

        try (NQueue<String> queue = track(NQueue.open(tempDir, "time-based-orthogonal", options))) {
            queue.offer("old");
            Thread.sleep(SLEEP_PAST_EXPIRE_MS);
            queue.offer("fresh");

            assertEquals("fresh", queue.poll().orElseThrow(),
                    "expireAfterWrite must skip the expired head even under TIME_BASED retention");
        }
    }

    @Test
    void withExpireAfterWriteRejectsNegativeDuration() {
        assertThrows(IllegalArgumentException.class,
                () -> NQueue.Options.defaults().withExpireAfterWrite(Duration.ofMillis(-1)));
    }
}
