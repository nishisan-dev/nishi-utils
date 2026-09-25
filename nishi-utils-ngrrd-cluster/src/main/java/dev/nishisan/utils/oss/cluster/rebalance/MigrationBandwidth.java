package dev.nishisan.utils.oss.cluster.rebalance;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/** Aggregate source-node pacing, shared by all outgoing migrations; at most one chunk can burst. */
final class MigrationBandwidth {
    static final long DEFAULT_BYTES_PER_SECOND = 16L * 1024 * 1024;
    private final long bytesPerSecond;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final java.util.concurrent.locks.Condition changed = lock.newCondition();
    private long nextNanos = System.nanoTime();

    MigrationBandwidth(long bytesPerSecond) {
        if (bytesPerSecond <= 0) { throw new IllegalArgumentException("migrationBytesPerSecond must be > 0"); }
        this.bytesPerSecond = bytesPerSecond;
    }

    boolean acquire(int bytes, BooleanSupplier active) {
        lock.lock();
        try {
            while (active.getAsBoolean()) {
                long now = System.nanoTime();
                long remaining = nextNanos - now;
                if (remaining <= 0) {
                    nextNanos = now + Math.max(1L, (long) Math.ceil(bytes * 1_000_000_000.0 / bytesPerSecond));
                    return true;
                }
                changed.awaitNanos(Math.min(remaining, 100_000_000L));
            }
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } finally { lock.unlock(); }
    }
}
