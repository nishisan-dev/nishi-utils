package dev.nishisan.utils.oss.cluster.rpc;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Reentrant guards for existing catalog/registry lock identities. Waiting for an RPC under
 * these guards releases a virtual thread's carrier on Java 21. The identity map is weak;
 * its short monitor only performs a lookup, never I/O or lock acquisition.
 *
 * <p>Identidades criadas por {@link #newStripe()} carregam o próprio lock: {@link #acquire} trava
 * direto, sem entrar no monitor global nem no mapa fraco. Útil para stripes adquiridos em rajada
 * (o {@code WRITE_BATCH} do storage node entra em até 256 deles por lote). A semântica é a mesma:
 * toda aquisição da mesma identidade usa o mesmo {@link ReentrantLock}.</p>
 */
public final class CoordinationLocks {
    private static final Map<Object, ReentrantLock> LOCKS = new WeakHashMap<>();
    private CoordinationLocks() { }

    /**
     * Cria uma identidade de lock com o {@link ReentrantLock} embutido, adquirida por {@link #acquire}
     * sem o mapa global. Não faça {@code synchronized} sobre ela: o único acesso é por {@link #acquire}.
     */
    public static Object newStripe() {
        return new Stripe();
    }

    /** Acquires a guard; close it on the acquiring thread, normally with try-with-resources. */
    public static Guard acquire(Object identity) {
        java.util.Objects.requireNonNull(identity, "identity");
        ReentrantLock lock;
        if (identity instanceof Stripe stripe) {
            lock = stripe.lock;
        } else {
            synchronized (LOCKS) {
                lock = LOCKS.computeIfAbsent(identity, ignored -> new ReentrantLock());
            }
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

    /** Identidade com lock próprio (ver {@link #newStripe()}). */
    private static final class Stripe {
        private final ReentrantLock lock = new ReentrantLock();
    }
}
