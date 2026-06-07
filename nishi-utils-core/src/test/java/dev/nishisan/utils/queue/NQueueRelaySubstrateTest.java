package dev.nishisan.utils.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression coverage for the NQueue guarantees the ngrid relay-log (#124) relies
 * on when using it as a replayable log substrate:
 * <ul>
 * <li><b>Non-destructive head read</b>: {@code peek()} returns the head without
 * advancing the cursor; a following {@code poll()} returns that same head, in
 * strict FIFO order.</li>
 * <li><b>At-least-once across crash</b>: a crash (close) after {@code peek}
 * ("apply") but before {@code poll} leaves the durable cursor untouched, so on
 * reopen the same head is re-readable (re-apply); only {@code poll} makes the
 * advance durable.</li>
 * </ul>
 * These are the substrate invariants behind the relay's {@code peek → apply →
 * poll} loop (the apply must be idempotent / sequence-fenced downstream).
 */
class NQueueRelaySubstrateTest {

    @TempDir
    Path tempDir;

    @Test
    void peekIsNonDestructiveAndPollReturnsSameHeadInFifoOrder() throws Exception {
        final int n = 5_000;
        NQueue.Options opts = NQueue.Options.defaults()
                .withFsync(false)
                .withOrderDetection(true);

        try (NQueue<Integer> q = NQueue.open(tempDir, "relay-fifo", opts)) {
            for (int i = 0; i < n; i++) {
                q.offer(i);
            }
            assertEquals(n, q.size(), "size after production must equal n");

            for (int i = 0; i < n; i++) {
                Optional<Integer> peeked = q.peek();
                assertTrue(peeked.isPresent(), "peek must return the head at i=" + i);
                Optional<Integer> polled = q.poll();
                assertTrue(polled.isPresent(), "poll must return the head at i=" + i);
                assertEquals(peeked.get(), polled.get(),
                        "peek then poll must observe the SAME head at i=" + i);
                assertEquals(i, polled.get().intValue(), "strict FIFO == production order at i=" + i);
            }

            assertEquals(0L, q.size(), "size must be 0 once drained");
            assertEquals(0L, q.getRecordCount(), "recordCount must be 0 once drained");
            assertEquals(0L, q.getOutOfOrderCount(), "no out-of-order delivery expected under strict FIFO");
        }
    }

    @Test
    void crashBetweenPeekApplyAndPollAllowsReapplyThenPersistsAdvance() throws Exception {
        final int n = 10;
        NQueue.Options opts = NQueue.Options.defaults()
                .withFsync(true) // real durability of the consumer cursor
                .withOrderDetection(true);
        final String name = "relay-crash";

        // Phase 1: produce + "apply" the head via peek (NO poll), then close (crash).
        try (NQueue<Integer> q = NQueue.open(tempDir, name, opts)) {
            for (int i = 0; i < n; i++) {
                q.offer(i);
            }
            Optional<Integer> head = q.peek();
            assertTrue(head.isPresent(), "head must exist before the crash");
            assertEquals(0, head.get(), "expected head = 0");
        }

        // Phase 2: reopen. peek must return the SAME head -> re-apply possible.
        try (NQueue<Integer> q = NQueue.open(tempDir, name, opts)) {
            assertEquals(n, q.getRecordCount(), "all n records must survive (nothing consumed)");
            Optional<Integer> head = q.peek();
            assertTrue(head.isPresent(), "head must survive the crash");
            assertEquals(0, head.get(),
                    "consumerOffset must NOT have advanced: head still == 0 (at-least-once)");

            Optional<Integer> polled = q.poll(); // now consume for real -> advance + persist
            assertEquals(0, polled.get(), "poll must consume head 0");
        }

        // Phase 3: reopen again -> the poll's advance must be durable.
        try (NQueue<Integer> q = NQueue.open(tempDir, name, opts)) {
            Optional<Integer> head = q.peek();
            assertTrue(head.isPresent());
            assertEquals(1, head.get(), "after poll(0)+close+reopen the new head must be 1 (advance persisted)");
            assertEquals(n - 1, q.getRecordCount(), "n-1 records must remain after one persisted poll");
        }
    }
}
