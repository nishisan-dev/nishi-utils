package dev.nishisan.utils.oss.cluster.rebalance;

import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

class MigrationBandwidthTest {
    @Test void concurrentTransfersShareOneBudget() throws Exception {
        var limiter = new MigrationBandwidth(8 * 1024);
        var start = new CountDownLatch(1);
        var times = new CopyOnWriteArrayList<Long>();
        try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
            var jobs = new java.util.ArrayList<Future<?>>();
            for (int i = 0; i < 3; i++) {
                jobs.add(pool.submit(() -> {
                    start.await();
                    assertTrue(limiter.acquire(1024, () -> true));
                    times.add(System.nanoTime());
                    return null;
                }));
            }
            start.countDown();
            for (var job : jobs) { job.get(3, TimeUnit.SECONDS); }
        }
        times.sort(Long::compare);
        assertTrue(times.getLast() - times.getFirst() >= TimeUnit.MILLISECONDS.toNanos(200),
                "three transfers must not each receive the full per-node bandwidth");
    }

    @Test void abortCancelsBandwidthWaitPromptly() throws Exception {
        var limiter = new MigrationBandwidth(1);
        assertTrue(limiter.acquire(1000, () -> true));
        var active = new AtomicBoolean(true);
        var result = CompletableFuture.supplyAsync(() -> limiter.acquire(1000, active::get));
        active.set(false);
        assertFalse(result.get(1, TimeUnit.SECONDS));
        assertThrows(IllegalArgumentException.class, () -> new MigrationBandwidth(0));
    }

    /**
     * {@code acquireUrgent} não pode esperar a vez, mesmo com a fila de chunks concorrentes já
     * saturada — quem chama é o patch final do cutover, com a série já congelada (clientes recebendo
     * {@code MIGRATING}).
     *
     * <p>A fila é saturada por uma SEGUNDA THREAD de fato parada dentro de {@code acquire} (não só por
     * uma reserva síncrona feita pela própria thread de teste) — mais fiel ao cenário real de um chunk
     * concorrente em voo — e o limite de tempo é folgado (200 ms) para reduzir sensibilidade a jitter
     * de CI.</p>
     */
    @Test void acquireUrgentNaoEsperaComFilaCheia() throws Exception {
        var limiter = new MigrationBandwidth(1024); // 1 KiB/s: um chunk normal já satura por ~4 s.
        assertTrue(limiter.acquire(4096, () -> true), "primeira reserva é imediata (fila ainda vazia)");

        var contenderStarted = new CountDownLatch(1);
        Thread contender = new Thread(() -> {
            contenderStarted.countDown();
            limiter.acquire(4096, () -> true); // fica de fato parado aqui, disputando a mesma fila.
        }, "bandwidth-contender");
        contender.setDaemon(true);
        contender.start();
        try {
            assertTrue(contenderStarted.await(1, TimeUnit.SECONDS), "thread concorrente não chegou a iniciar");
            Thread.sleep(50L); // dá tempo da thread concorrente entrar de fato no await() da fila.

            long start = System.nanoTime();
            limiter.acquireUrgent(256);
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            assertTrue(elapsedMs < 200L, "acquireUrgent não deveria esperar a fila, levou " + elapsedMs + " ms");
        } finally {
            contender.join(8_000L); // libera a thread concorrente; não é o foco da asserção acima.
        }
    }

    /**
     * {@code acquireUrgent} continua contando no orçamento — não é grátis, só prioritário. Depois de
     * um {@code acquireUrgent} de N bytes, o próximo {@code acquire} espera pelo custo desses N bytes
     * (tolerância generosa para evitar flakiness de CI).
     */
    @Test void acquireUrgentDebitaOrcamento() throws Exception {
        long bytesPerSecond = 10L * 1024; // 10 KiB/s
        var limiter = new MigrationBandwidth(bytesPerSecond);
        int urgentBytes = 5 * 1024; // custo esperado: ~500 ms
        long expectedWaitMs = (urgentBytes * 1000L) / bytesPerSecond;

        limiter.acquireUrgent(urgentBytes);
        long start = System.nanoTime();
        assertTrue(limiter.acquire(1, () -> true));
        long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue(waitedMs >= expectedWaitMs - 150L,
                "esperou " + waitedMs + " ms, esperava-se pelo menos ~" + expectedWaitMs + " ms");
        assertTrue(waitedMs <= expectedWaitMs + 500L,
                "esperou " + waitedMs + " ms, bem mais que o custo esperado de ~" + expectedWaitMs + " ms");
    }
}
