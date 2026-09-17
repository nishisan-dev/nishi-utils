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

import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;
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
 * @param rpcLatency      latência de toda chamada RPC síncrona feita por este cliente
 *                        ({@code place}, {@code writeBatch}, {@code checkpoint}, {@code read}, ...),
 *                        agregada num único histograma (não quebrada por comando)
 * @param placeCount      total de chamadas {@code ngrrd.place} feitas por este cliente
 */
public record ClientMetricsSnapshot(
        long samplesEnqueued,
        long samplesSent,
        long samplesFailed,
        long batchesSent,
        Map<SeriesStatus, Long> retriesByStatus,
        Map<String, Long> bufferedSamples,
        int openHandles,
        LatencySnapshot rpcLatency,
        long placeCount) {

    public ClientMetricsSnapshot {
        retriesByStatus = Map.copyOf(Objects.requireNonNullElse(retriesByStatus, Map.of()));
        bufferedSamples = Map.copyOf(Objects.requireNonNullElse(bufferedSamples, Map.of()));
        rpcLatency = Objects.requireNonNullElse(rpcLatency, LatencySnapshot.EMPTY);
    }
}
