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

package dev.nishisan.utils.oss.cluster.api;

import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;

import java.util.Map;
import java.util.Objects;

/**
 * Snapshot das métricas mínimas do {@link NgrrdClusterClient} num instante:
 * contadores acumulados do {@code WriteDispatcher} e o tamanho atual dos
 * buffers de escrita por nó.
 *
 * @param samplesEnqueued total de amostras enfileiradas desde a conexão
 * @param samplesSent     total de amostras confirmadas ({@code OK}) por um dono
 * @param samplesFailed   total de amostras descartadas ({@code ERROR} do dono, ou
 *                        pendências que não couberam no prazo de {@code close()})
 * @param batchesSent     total de lotes {@code WRITE_BATCH} efetivamente enviados
 * @param retriesByStatus retentativas observadas, agrupadas pelo {@link SeriesStatus} que as motivou
 * @param bufferedSamples amostras atualmente no buffer de cada nó de destino, por {@code nodeId}
 * @param openHandles     quantidade de {@code NgrrdHandle} abertos neste cliente
 */
public record ClientMetricsSnapshot(
        long samplesEnqueued,
        long samplesSent,
        long samplesFailed,
        long batchesSent,
        Map<SeriesStatus, Long> retriesByStatus,
        Map<String, Long> bufferedSamples,
        int openHandles) {

    public ClientMetricsSnapshot {
        retriesByStatus = Map.copyOf(Objects.requireNonNullElse(retriesByStatus, Map.of()));
        bufferedSamples = Map.copyOf(Objects.requireNonNullElse(bufferedSamples, Map.of()));
    }
}
