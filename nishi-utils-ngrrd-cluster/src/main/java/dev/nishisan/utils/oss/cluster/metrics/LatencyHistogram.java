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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Histograma de latência thread-safe, sem dependência externa (nada de
 * HdrHistogram/Micrometer neste módulo — só o necessário para os campos de
 * {@code writeBatchLatency}/{@code checkpointLatency}/{@code readLatency} de
 * {@link NodeMetricsSnapshot}).
 *
 * <h2>Aproximação deliberada</h2>
 *
 * <p>Não é um histograma exato: mantém um <strong>reservatório circular</strong>
 * das últimas {@value #CAPACITY} amostras (nanos) — quando cheio, cada
 * {@link #record(long)} novo sobrescreve a amostra mais antiga. {@code p50}/
 * {@code p99} em {@link #snapshot()} são calculados (método do "rank mais
 * próximo": {@code ceil(p × n) - 1}) sobre essa janela recente, então refletem
 * o comportamento "agora", não a distribuição completa desde o início do
 * processo — de propósito, para não deixar picos antigos mascarados por
 * milhões de amostras acumuladas depois. {@code count} e {@code max}, ao
 * contrário, são contadores acumulados desde a criação (não sofrem com a
 * janela).</p>
 *
 * <p>{@link #record} é seguro para chamadas concorrentes (cada thread grava
 * num índice próprio via {@link AtomicLong#getAndIncrement()}); {@link #snapshot()}
 * pode, em teoria, ler uma amostra sendo sobrescrita nesse exato instante por
 * uma gravação concorrente — aceitável para um resumo aproximado, nunca usado
 * para decisões de correção, só de observabilidade.</p>
 *
 * <p>M5 (achado do Refuter): dentro de {@link #record}, a amostra é gravada em {@code samples}
 * ANTES de {@code totalCount} ser incrementado — {@link #snapshot()} lê {@code totalCount} (para
 * decidir {@code min(totalCount, CAPACITY)}) ANTES de copiar os slots, nessa ordem, de propósito.
 * Mesmo assim, enquanto {@code totalCount < CAPACITY} (histograma ainda não deu a volta completa),
 * uma janela {@code [0, min(totalCount, CAPACITY))} pode incluir um índice cujo escritor já reservou
 * a posição (via {@code writeCursor.getAndIncrement()}) mas ainda não terminou {@code samples.set(...)}
 * — porque threads concorrentes não necessariamente completam {@link #record} na mesma ordem em que
 * reservaram seus índices. Nesse caso {@code snapshot()} pode ler um slot ainda com o valor default
 * (zero) para aquela amostra específica, distorcendo levemente {@code p50}/{@code p99} POR UM
 * INSTANTE — o problema se autocorrige no próximo {@link #snapshot()}, já com o slot preenchido.
 * Aceitável pela mesma razão do parágrafo acima: aproximação de observabilidade, não garantia de
 * correção.</p>
 */
public final class LatencyHistogram {

    private static final int CAPACITY = 1024;

    private final AtomicLongArray samples = new AtomicLongArray(CAPACITY);
    private final AtomicLong writeCursor = new AtomicLong();
    private final AtomicLong totalCount = new AtomicLong();
    private final AtomicLong maxNanos = new AtomicLong();

    /** Registra uma latência observada, em nanossegundos. */
    public void record(long nanos) {
        if (nanos < 0) {
            throw new IllegalArgumentException("nanos deve ser >= 0: " + nanos);
        }
        long index = writeCursor.getAndIncrement();
        samples.set((int) (index % CAPACITY), nanos);
        totalCount.incrementAndGet();
        maxNanos.accumulateAndGet(nanos, Math::max);
    }

    /** Snapshot atual — {@link LatencySnapshot#EMPTY} se nenhuma amostra foi registrada ainda. */
    public LatencySnapshot snapshot() {
        long count = totalCount.get();
        if (count == 0L) {
            return LatencySnapshot.EMPTY;
        }
        int windowSize = (int) Math.min(count, CAPACITY);
        long[] window = new long[windowSize];
        for (int i = 0; i < windowSize; i++) {
            window[i] = samples.get(i);
        }
        Arrays.sort(window);
        long p50Nanos = percentile(window, 0.50);
        long p99Nanos = percentile(window, 0.99);
        return new LatencySnapshot(count, toMicros(p50Nanos), toMicros(p99Nanos), toMicros(maxNanos.get()));
    }

    /** Percentil pelo método do "rank mais próximo" sobre um array já ordenado. */
    private static long percentile(long[] sortedNanos, double p) {
        int n = sortedNanos.length;
        int rank = (int) Math.ceil(p * n);
        int index = Math.max(0, Math.min(n - 1, rank - 1));
        return sortedNanos[index];
    }

    private static long toMicros(long nanos) {
        return nanos / 1_000L;
    }
}
