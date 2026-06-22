package dev.nishisan.utils.oss.writer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pool de threads <strong>compartilhado</strong> por todos os {@link NgrrdWriter}s
 * do processo, com contagem de referências. Substitui o modelo de uma thread
 * daemon por série: com milhares de séries abertas, mantém ~{@code nCPU} threads
 * em vez de milhares.
 *
 * <p>Cada writer faz {@link #acquire()} ao abrir e {@link #release()} ao fechar; o
 * pool é criado na primeira referência e encerrado (com {@code awaitTermination})
 * quando a última é liberada — garantindo que nenhuma thread {@code ngrrd-writer-*}
 * sobreviva após o fechamento de todos os handles.</p>
 *
 * <p>A serialização por série (ordem total + single-writer) é responsabilidade do
 * próprio {@link NgrrdWriter} (flag {@code scheduled} + tarefa de drain), não do
 * pool — que apenas executa drains de writers distintos em paralelo.</p>
 */
final class WriterScheduler {

    private static final Object LOCK = new Object();
    private static WriterScheduler instance;
    private static int refs;

    private final ExecutorService pool;

    private WriterScheduler(int threads) {
        AtomicInteger seq = new AtomicInteger();
        ThreadFactory factory = r -> {
            Thread t = new Thread(r, "ngrrd-writer-pool-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        this.pool = Executors.newFixedThreadPool(threads, factory);
    }

    /** Obtém o pool compartilhado (criando-o na primeira referência). */
    static WriterScheduler acquire() {
        synchronized (LOCK) {
            if (refs == 0) {
                instance = new WriterScheduler(Math.max(2, Runtime.getRuntime().availableProcessors()));
            }
            refs++;
            return instance;
        }
    }

    /** Libera uma referência; encerra o pool quando a última é liberada. */
    static void release() {
        ExecutorService toClose = null;
        synchronized (LOCK) {
            if (refs > 0 && --refs == 0) {
                toClose = instance.pool;
                instance = null;
            }
        }
        if (toClose != null) {
            toClose.shutdown();
            try {
                if (!toClose.awaitTermination(5, TimeUnit.SECONDS)) {
                    toClose.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                toClose.shutdownNow();
            }
        }
    }

    void submit(Runnable task) {
        pool.execute(task);
    }
}
