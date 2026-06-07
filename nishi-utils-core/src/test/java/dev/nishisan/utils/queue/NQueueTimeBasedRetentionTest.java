package dev.nishisan.utils.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression coverage for the TIME_BASED retention extension required by the
 * ngrid relay-log (#124): the {@code withRetentionClampToConsumer} option, the
 * {@code recordCount} recount after compaction, and the clamp that forbids
 * discarding unconsumed records.
 */
class NQueueTimeBasedRetentionTest {

    @TempDir
    Path tempDir;

    @Test
    void clampFlagDefaultsFalseAndIsPreservedByCopyAndSnapshot() {
        NQueue.Options base = NQueue.Options.defaults();
        assertFalse(base.retentionClampToConsumer, "default must be false (preserves existing semantics)");
        assertFalse(base.snapshot().retentionClampToConsumer, "snapshot must mirror the default");

        NQueue.Options on = NQueue.Options.defaults().withRetentionClampToConsumer(true);
        assertTrue(on.retentionClampToConsumer, "builder must set the flag");
        assertTrue(on.copy().retentionClampToConsumer, "copy() must preserve the flag (used by grid enforceGridOptions)");
        assertTrue(on.snapshot().retentionClampToConsumer, "snapshot() must preserve the flag");
    }

    /**
     * After a TIME_BASED compaction physically discards expired head records,
     * {@code recordCount} must reflect the survivors — not the pre-compaction
     * (stale, overcounted) value. Deterministic: {@code close()} runs a shutdown
     * compaction and awaits it, persisting the recounted value to the meta; the
     * reopen reads it back (with DELETE_ON_CONSUME read options so no further
     * time-based discard races the drain).
     */
    @Test
    void recordCountExactAfterTimeBasedCompaction() throws Exception {
        final Duration retention = Duration.ofMillis(200);
        final String name = "rc-timebased";
        final NQueue.Options writeOpts = NQueue.Options.defaults()
                .withFsync(false)
                .withRetentionPolicy(NQueue.Options.RetentionPolicy.TIME_BASED)
                .withRetentionTime(retention)
                .withCompactionInterval(Duration.ofMillis(50));

        try (NQueue<Integer> q = NQueue.open(tempDir, name, writeOpts)) {
            for (int i = 0; i < 500; i++) {
                q.offer(i);
            }
            for (int i = 0; i < 5; i++) {
                q.poll(); // advance consumerOffset > 0
            }
            Thread.sleep(retention.toMillis() + 250); // head (idx 5..499) now older than retention
            for (int i = 0; i < 3; i++) {
                q.offer(1000 + i); // young survivors, within retention window
            }
        } // close() -> shutdown compaction (TIME_BASED) discards the expired backlog + persists recounted recordCount

        NQueue.Options readOpts = NQueue.Options.defaults().withFsync(false); // DELETE_ON_CONSUME: no time discard on read
        try (NQueue<Integer> q = NQueue.open(tempDir, name, readOpts)) {
            long reported = q.getRecordCount();
            int drained = 0;
            while (q.poll(50, TimeUnit.MILLISECONDS).isPresent()) {
                drained++;
            }
            assertEquals(drained, reported,
                    "recordCount apos compaction TIME_BASED deve refletir os registros vivos (sem stale)");
            assertTrue(drained > 0 && drained <= 10,
                    "a retencao TIME_BASED deve ter descartado o backlog expirado, restando so os jovens (drained=" + drained + ")");
        }
    }

    /**
     * With {@code withRetentionClampToConsumer(true)}, a TIME_BASED compaction must
     * NOT discard records the consumer has not read yet, even when they are older
     * than the retention window. The clamp anchors the cutoff at the consumer
     * offset, so every unconsumed record survives — the guarantee the relay-log
     * depends on (vs. the silent loss proven, without the clamp, by the spike).
     */
    @Test
    void clampNeverDiscardsUnconsumed() throws Exception {
        final Duration retention = Duration.ofMillis(200);
        final String name = "clamp-timebased";
        final int total = 200;
        final int consumed = 5;
        final NQueue.Options writeOpts = NQueue.Options.defaults()
                .withFsync(false)
                .withRetentionPolicy(NQueue.Options.RetentionPolicy.TIME_BASED)
                .withRetentionTime(retention)
                .withRetentionClampToConsumer(true) // the extension under test
                .withCompactionInterval(Duration.ofMillis(50));

        try (NQueue<Integer> q = NQueue.open(tempDir, name, writeOpts)) {
            for (int i = 0; i < total; i++) {
                q.offer(i);
            }
            for (int i = 0; i < consumed; i++) {
                q.poll();
            }
            // All remaining records are now older than the retention window. Without
            // the clamp they would be discarded; with it they must all survive.
            Thread.sleep(retention.toMillis() + 250);
        } // close() -> shutdown compaction (TIME_BASED + clamp): reclaims only the consumed prefix

        NQueue.Options readOpts = NQueue.Options.defaults().withFsync(false);
        try (NQueue<Integer> q = NQueue.open(tempDir, name, readOpts)) {
            long reported = q.getRecordCount();
            assertEquals(total - consumed, reported,
                    "recordCount deve refletir o segmento vivo preservado pelo clamp");

            int drained = 0;
            Integer first = null;
            java.util.Optional<Integer> v;
            while ((v = q.poll(50, TimeUnit.MILLISECONDS)).isPresent()) {
                if (first == null) {
                    first = v.get();
                }
                drained++;
            }
            assertEquals(total - consumed, drained,
                    "o clamp deve preservar TODOS os nao-consumidos expirados (sem perda silenciosa)");
            assertEquals(Integer.valueOf(consumed), first,
                    "a cabeca apos o clamp deve ser o primeiro nao-consumido (idx=" + consumed + ")");
        }
    }
}
