/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.oss.cluster.metrics;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link LatencyHistogram}: distribuição conhecida (p50/p99 pelo método do rank mais
 * próximo), histograma vazio, gravação concorrente sem perda de amostras e a janela circular
 * (últimas 1024 amostras) descrita no Javadoc da classe.
 */
class LatencyHistogramTest {

    @Test
    void semAmostrasDevolveSnapshotVazio() {
        LatencyHistogram histogram = new LatencyHistogram();

        LatencySnapshot snapshot = histogram.snapshot();

        assertEquals(LatencySnapshot.EMPTY, snapshot);
        assertEquals(0L, snapshot.count());
    }

    @Test
    void distribuicaoConhecidaCalculaP50EP99PeloRankMaisProximo() {
        LatencyHistogram histogram = new LatencyHistogram();
        // 1..100 microssegundos (em nanos): rank mais próximo de p50 sobre 100 amostras
        // ordenadas -> ceil(0.50*100)=50 -> índice 49 (0-based) -> valor 50; p99 -> ceil(0.99*100)=99
        // -> índice 98 -> valor 99.
        for (int micros = 1; micros <= 100; micros++) {
            histogram.record(micros * 1_000L);
        }

        LatencySnapshot snapshot = histogram.snapshot();

        assertEquals(100L, snapshot.count());
        assertEquals(50L, snapshot.p50Micros());
        assertEquals(99L, snapshot.p99Micros());
        assertEquals(100L, snapshot.maxMicros());
    }

    @Test
    void maximoEContagemSaoAcumuladosMesmoAlemDaJanelaCircular() {
        LatencyHistogram histogram = new LatencyHistogram();
        // Primeira amostra é a maior de todas; depois mais de 1024 amostras pequenas sobrescrevem a
        // janela circular por completo -- count/max continuam refletindo TODO o histórico (contadores
        // acumulados), mesmo que a amostra que gerou o máximo já tenha sido sobrescrita na janela.
        histogram.record(50_000_000L); // 50 000 us
        for (int i = 0; i < 2_000; i++) {
            histogram.record(1_000L); // 1 us
        }

        LatencySnapshot snapshot = histogram.snapshot();

        assertEquals(2_001L, snapshot.count());
        assertEquals(50_000L, snapshot.maxMicros());
        // A janela recente (últimas 1024 amostras) só tem amostras de 1us -> percentis também 1us.
        assertEquals(1L, snapshot.p50Micros());
        assertEquals(1L, snapshot.p99Micros());
    }

    @Test
    void gravacaoConcorrenteNaoPerdeAmostrasNaContagem() throws InterruptedException {
        LatencyHistogram histogram = new LatencyHistogram();
        int threads = 8;
        int perThread = 500;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger errors = new AtomicInteger();
        List<Runnable> tasks = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            tasks.add(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    errors.incrementAndGet();
                    return;
                }
                for (int i = 1; i <= perThread; i++) {
                    histogram.record(i * 1_000L);
                }
            });
        }
        try {
            tasks.forEach(pool::execute);
            assertTrue(ready.await(5, TimeUnit.SECONDS), "threads não ficaram prontas a tempo");
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "gravação concorrente não terminou a tempo");
        } finally {
            pool.shutdownNow();
        }

        assertEquals(0, errors.get());
        assertEquals((long) threads * perThread, histogram.snapshot().count());
    }

    @Test
    void recordESnapshotConcorrentesNaoLancamExcecao() throws InterruptedException {
        // M5 (achado do Refuter): snapshot() lê totalCount (para decidir min(totalCount, CAPACITY))
        // ANTES de copiar os slots — este teste não afirma um resultado exato (a janela pode incluir
        // um slot ainda zerado por um record() concorrente em andamento, ver Javadoc da classe), só que
        // a corrida de record()+snapshot() simultâneos nunca lança exceção nem trava.
        LatencyHistogram histogram = new LatencyHistogram();
        int writerThreads = 6;
        int readerThreads = 2;
        int perWriter = 400;
        ExecutorService pool = Executors.newFixedThreadPool(writerThreads + readerThreads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch writersFinished = new CountDownLatch(writerThreads);
        AtomicInteger errors = new AtomicInteger();

        for (int t = 0; t < writerThreads; t++) {
            pool.execute(() -> {
                try {
                    start.await();
                    for (int i = 1; i <= perWriter; i++) {
                        histogram.record(i * 1_000L);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    errors.incrementAndGet();
                } finally {
                    writersFinished.countDown();
                }
            });
        }
        for (int t = 0; t < readerThreads; t++) {
            pool.execute(() -> {
                try {
                    start.await();
                    while (writersFinished.getCount() > 0) {
                        histogram.snapshot();
                    }
                    histogram.snapshot();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (RuntimeException e) {
                    errors.incrementAndGet();
                }
            });
        }

        start.countDown();
        assertTrue(writersFinished.await(10, TimeUnit.SECONDS), "escritores concorrentes não terminaram a tempo");
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS), "record()/snapshot() concorrentes não terminaram a tempo");

        assertEquals(0, errors.get());
        assertEquals((long) writerThreads * perWriter, histogram.snapshot().count());
    }

    private static final long MINUTE = 60_000_000_000L;

    @Test
    void picoNoComandoQuenteSobreviveAoReservatorio() {
        AtomicLong clock = new AtomicLong(10 * MINUTE);
        LatencyHistogram histogram = new LatencyHistogram(clock::get);

        histogram.record(10_000_000_000L);
        for (int i = 0; i < 5_000; i++) {
            histogram.record(1_000L);
        }

        LatencySnapshot snapshot = histogram.snapshot();
        assertEquals(10_000_000L, snapshot.recentMaxMicros());
        assertEquals(1L, snapshot.p99Micros());
    }

    @Test
    void picoSaiDepoisDeDoisMinutos() {
        AtomicLong clock = new AtomicLong(10 * MINUTE);
        LatencyHistogram histogram = new LatencyHistogram(clock::get);
        histogram.record(5_000_000L);

        clock.set(11 * MINUTE + 1);
        assertEquals(5_000L, histogram.snapshot().recentMaxMicros());

        clock.set(12 * MINUTE);
        LatencySnapshot snapshot = histogram.snapshot();
        assertEquals(0L, snapshot.recentMaxMicros());
        assertEquals(5_000L, snapshot.maxMicros());
    }

    @Test
    void viradaDeMinutoPreservaAnterior() {
        AtomicLong clock = new AtomicLong(10 * MINUTE);
        LatencyHistogram histogram = new LatencyHistogram(clock::get);
        histogram.record(9_000_000L);

        clock.set(11 * MINUTE);
        histogram.record(2_000_000L);

        assertEquals(9_000L, histogram.snapshot().recentMaxMicros());

        clock.set(12 * MINUTE);
        histogram.record(1_000_000L);
        assertEquals(2_000L, histogram.snapshot().recentMaxMicros());
    }

    @Test
    void amostraAtrasadaVaiAoBaldeAnteriorMesmoVazio() {
        AtomicLong clock = new AtomicLong(11 * MINUTE);
        LatencyHistogram histogram = new LatencyHistogram(clock::get);
        // A virada para o minuto 11 acontece antes de qualquer amostra do minuto 10 chegar.
        histogram.record(1_000_000L);

        clock.set(10 * MINUTE + MINUTE - 1);
        histogram.record(9_000_000L);

        clock.set(11 * MINUTE + 1);
        assertEquals(9_000L, histogram.snapshot().recentMaxMicros(), "a amostra atrasada entra no balde anterior");
        clock.set(12 * MINUTE + 1);
        assertEquals(1_000L, histogram.snapshot().recentMaxMicros(), "e sai com ele, depois de dois minutos");
    }

    @Test
    void relogioRecuandoEIgnoradoNoMaximoRecente() {
        AtomicLong clock = new AtomicLong(10 * MINUTE);
        LatencyHistogram histogram = new LatencyHistogram(clock::get);
        histogram.record(2_000_000L);
        clock.set(12 * MINUTE);
        histogram.record(1_000_000L);

        clock.set(9 * MINUTE);
        histogram.record(50_000_000L);

        clock.set(12 * MINUTE);
        LatencySnapshot snapshot = histogram.snapshot();
        assertEquals(1_000L, snapshot.recentMaxMicros(), "amostra de um minuto já fora da janela não entra");
        assertEquals(50_000L, snapshot.maxMicros(), "o máximo desde a subida continua contando tudo");
    }

    @Test
    void viradaDeMinutoConcorrenteNaoPerdeMaximo() throws InterruptedException {
        int threads = 8;
        int perThread = 2_000;
        long step = 1_000L;
        // Cada leitura do relógio avança 1 µs; a virada do minuto 10 para o 11 cai no meio das gravações.
        long base = 11 * MINUTE - (long) threads * perThread / 2 * step;
        AtomicLong ticks = new AtomicLong();
        LatencyHistogram histogram = new LatencyHistogram(() -> base + ticks.getAndIncrement() * step);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            pool.execute(() -> {
                try {
                    start.await();
                    // Decrescente: o maior valor de cada thread tende a cair no minuto 10, antes da virada.
                    for (int i = perThread; i >= 1; i--) {
                        histogram.record(i * 1_000L);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "gravações concorrentes não terminaram a tempo");
        pool.shutdown();

        LatencySnapshot snapshot = histogram.snapshot();
        assertEquals((long) perThread, snapshot.maxMicros());
        assertEquals(snapshot.maxMicros(), snapshot.recentMaxMicros(),
                "o máximo do minuto anterior sobrevive à virada concorrente");
    }

    @Test
    void maximoDesdeSubidaPreservado() {
        AtomicLong clock = new AtomicLong(10 * MINUTE);
        LatencyHistogram histogram = new LatencyHistogram(clock::get);
        histogram.record(7_000_000L);

        clock.set(30 * MINUTE);
        histogram.record(1_000_000L);

        LatencySnapshot snapshot = histogram.snapshot();
        assertEquals(7_000L, snapshot.maxMicros());
        assertEquals(1_000L, snapshot.recentMaxMicros());
    }

    @Test
    void recordConcorrenteNaoPerdeMaximo() throws InterruptedException {
        AtomicLong clock = new AtomicLong(10 * MINUTE);
        LatencyHistogram histogram = new LatencyHistogram(clock::get);
        int threads = 8;
        int perThread = 2_000;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            int base = t;
            pool.execute(() -> {
                try {
                    start.await();
                    for (int i = 1; i <= perThread; i++) {
                        histogram.record((i * threads + base) * 1_000L);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "escritores concorrentes não terminaram a tempo");
        pool.shutdownNow();

        long expectedMax = (long) (perThread * threads + threads - 1);
        LatencySnapshot snapshot = histogram.snapshot();
        assertEquals(expectedMax, snapshot.recentMaxMicros());
        assertEquals(expectedMax, snapshot.maxMicros());
    }

    @Test
    void construtorDeQuatroArgumentosUsaMaximoComoRecente() {
        LatencySnapshot snapshot = new LatencySnapshot(1L, 2L, 3L, 4L);
        assertEquals(4L, snapshot.recentMaxMicros());
    }
}
