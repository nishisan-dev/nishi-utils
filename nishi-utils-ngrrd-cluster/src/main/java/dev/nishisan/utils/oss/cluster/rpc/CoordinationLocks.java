package dev.nishisan.utils.oss.cluster.rpc;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Reentrant guards for existing catalog/registry lock identities. Waiting for an RPC under
 * these guards releases a virtual thread's carrier on Java 21. The identity map is weak;
 * its short monitor only performs a lookup, never I/O or lock acquisition.
 */
public final class CoordinationLocks {
    private static final Map<Object, ReentrantLock> LOCKS = new WeakHashMap<>();
    private CoordinationLocks() { }

    /** Acquires a guard; close it on the acquiring thread, normally with try-with-resources. */
    public static Guard acquire(Object identity) {
        java.util.Objects.requireNonNull(identity, "identity");
        ReentrantLock lock;
        synchronized (LOCKS) {
            lock = LOCKS.computeIfAbsent(identity, ignored -> new ReentrantLock());
        }
        lock.lock();
        return new Guard(lock);
    }

    /** A held reentrant guard. */
    public static final class Guard implements AutoCloseable {
        private final ReentrantLock lock;
        private Guard(ReentrantLock lock) { this.lock = lock; }
        @Override public void close() { lock.unlock(); }
    }
}
