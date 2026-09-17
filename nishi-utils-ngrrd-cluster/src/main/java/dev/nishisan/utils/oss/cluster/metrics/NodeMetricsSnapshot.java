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

import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;

import java.util.Map;
import java.util.Objects;

/**
 * Fotografia das métricas operacionais de um storage node num instante —
 * resposta de {@code ngrrd.admin.metrics} e corpo entregue a
 * {@link NgrrdClusterMetricsListener#onNodeMetrics}. Sem {@link java.time.Duration}
 * nem tipos não serializáveis pelo codec do NGrid (ver {@code JacksonMessageCodec}).
 *
 * <p>{@code migrationsIn}/{@code migrationsOut} são sempre {@code 0} neste
 * marco (M2) — a migração de séries entre nós é escopo do M3.</p>
 *
 * @param nodeId             identificador do storage node
 * @param capturedAtEpochMs  instante em que este snapshot foi montado
 * @param leader             se este nó é o líder do cluster no instante da captura
 * @param seriesCount        quantidade de séries no catálogo do volume local
 * @param usedBytes          bytes ocupados no volume local
 * @param capacityBytes      capacidade configurada do nó; {@code <= 0} = desconhecida/sem limite
 * @param openHandles        quantidade de séries com handle aberto agora
 * @param writeBatches       total de requisições {@code writeBatch} atendidas
 * @param samplesWritten     total de amostras efetivamente gravadas
 * @param samplesFailed      total de amostras descartadas por {@code ERROR}
 * @param checkpoints        total de {@code checkpoint} concluídos com sucesso
 * @param flushes            total de requisições {@code flush} atendidas
 * @param reads              total de leituras ({@code read}/{@code readPreset}) atendidas
 * @param writeBatchLatency  latência de {@code writeBatch}, por handle afetado
 * @param checkpointLatency  latência de {@code checkpoint}
 * @param readLatency        latência de leitura ({@code read}/{@code readPreset})
 * @param errorsByStatus     respostas de erro emitidas por este nó, agrupadas por {@link SeriesStatus}
 * @param blobStats          resumo dos gauges do volume local (ver {@link BlobVolumeSummary})
 * @param migrationsIn       quantidade de migrações recebidas (sempre {@code 0} no M2)
 * @param migrationsOut      quantidade de migrações enviadas (sempre {@code 0} no M2)
 */
public record NodeMetricsSnapshot(
        String nodeId,
        long capturedAtEpochMs,
        boolean leader,
        long seriesCount,
        long usedBytes,
        long capacityBytes,
        int openHandles,
        long writeBatches,
        long samplesWritten,
        long samplesFailed,
        long checkpoints,
        long flushes,
        long reads,
        LatencySnapshot writeBatchLatency,
        LatencySnapshot checkpointLatency,
        LatencySnapshot readLatency,
        Map<SeriesStatus, Long> errorsByStatus,
        BlobVolumeSummary blobStats,
        long migrationsIn,
        long migrationsOut) {

    public NodeMetricsSnapshot {
        Objects.requireNonNull(nodeId, "nodeId é obrigatório");
        errorsByStatus = Map.copyOf(Objects.requireNonNullElse(errorsByStatus, Map.of()));
        writeBatchLatency = Objects.requireNonNullElse(writeBatchLatency, LatencySnapshot.EMPTY);
        checkpointLatency = Objects.requireNonNullElse(checkpointLatency, LatencySnapshot.EMPTY);
        readLatency = Objects.requireNonNullElse(readLatency, LatencySnapshot.EMPTY);
    }
}
