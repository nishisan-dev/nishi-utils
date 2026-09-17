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

/**
 * Resumo aproximado da distribuição de latências observada por um
 * {@link LatencyHistogram} num instante.
 *
 * <p>{@code p50Micros}/{@code p99Micros} são calculados sobre a janela mais
 * recente de amostras mantida pelo histograma (ver Javadoc de
 * {@link LatencyHistogram} para a aproximação exata) — não são percentis
 * exatos sobre todo o histórico. {@code count} e {@code maxMicros}, ao
 * contrário, são acumulados desde a criação do histograma (não sofrem com a
 * janela).</p>
 *
 * @param count      total de amostras registradas desde a criação do histograma
 * @param p50Micros  mediana aproximada, em microssegundos, sobre a janela recente
 * @param p99Micros  percentil 99 aproximado, em microssegundos, sobre a janela recente
 * @param maxMicros  maior amostra já registrada, em microssegundos, desde a criação do histograma
 */
public record LatencySnapshot(long count, long p50Micros, long p99Micros, long maxMicros) {

    /** Snapshot de um histograma sem nenhuma amostra registrada ainda. */
    public static final LatencySnapshot EMPTY = new LatencySnapshot(0L, 0L, 0L, 0L);
}
