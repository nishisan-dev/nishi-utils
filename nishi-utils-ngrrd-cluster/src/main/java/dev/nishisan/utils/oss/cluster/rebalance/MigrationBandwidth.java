package dev.nishisan.utils.oss.cluster.rebalance;

import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * Compassa a banda agregada de saída de uma origem, compartilhada por todas as migrações em curso.
 *
 * <p>A rajada não é de "até um chunk": o delta final de cada cutover em curso (≤ 256 KiB cada, ver
 * {@code MigrationExecutor#transfer}) também pode furar a fila via {@link #acquireUrgent} e se somar à
 * rajada — um chunk normal em trânsito mais os deltas finais de cutovers simultâneos.</p>
 */
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

    /**
     * Debita {@code bytes} do orçamento imediatamente, sem esperar a vez — para o patch final do
     * cutover (série já congelada, clientes recebendo {@code MIGRATING}), que não pode ficar preso na
     * mesma fila dos chunks de 256 KiB de outras cópias. O delta final continua contando no orçamento
     * (a média de bytes/s por origem é preservada): só empurra o próximo slot, nunca "pula" o custo.
     * Os chunks concorrentes é que absorvem o atraso, acordados por {@link #changed}.
     */
    void acquireUrgent(int bytes) {
        lock.lock();
        try {
            long now = System.nanoTime();
            long cost = Math.max(1L, (long) Math.ceil(bytes * 1_000_000_000.0 / bytesPerSecond));
            nextNanos = Math.max(nextNanos, now) + cost;
            changed.signalAll();
        } finally { lock.unlock(); }
    }
}
