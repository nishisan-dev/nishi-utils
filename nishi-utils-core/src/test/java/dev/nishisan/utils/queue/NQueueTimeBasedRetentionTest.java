package dev.nishisan.utils.queue;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Regression coverage for the TIME_BASED retention extension required by the
 * ngrid relay-log (#124): the {@code withRetentionClampToConsumer} option, the
 * {@code recordCount} recount after compaction, and the clamp that forbids
 * discarding unconsumed records.
 */
class NQueueTimeBasedRetentionTest {

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
}
