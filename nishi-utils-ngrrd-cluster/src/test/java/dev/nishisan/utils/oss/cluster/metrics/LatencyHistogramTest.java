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
}
