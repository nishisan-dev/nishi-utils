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
 * <p>{@code migrationsIn}/{@code migrationsOut} são alimentados por
 * {@code MigrationExecutor.ExecutorMetrics} (migrações de série entre nós, M3);
 * ficam em {@code 0} apenas enquanto o nó não participou de nenhuma como origem
 * ou destino.</p>
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
 * @param migrationsIn       quantidade de migrações recebidas neste nó como destino
 * @param migrationsOut      quantidade de migrações enviadas por este nó como origem
 * @param reconcileAdopted           M4: séries adotadas (presentes no volume, ausentes do catálogo) no
 *                                   último ciclo do {@code LocalReconciler} (sempre {@code 0} se o nó
 *                                   ainda não rodou nenhum ciclo)
 * @param reconcileOrphansDeleted    M4: cópias órfãs apagadas no último ciclo
 * @param reconcileUnplaced          M4: séries adotadas pelo líder noutro dono (este nó não era
 *                                   candidato) no último ciclo
 * @param reconcileMissing           M4: séries {@code ACTIVE} no catálogo local mas ausentes do volume,
 *                                   detectadas no último ciclo
 * @param reconcileLastDurationMs    M4: duração do último ciclo do {@code LocalReconciler}, em ms
 * @param leaderConfirmations        leituras fortes de placement feitas no líder para confirmar o dono
 *                                   de uma série sem objeto no volume antes de criá-la ou de responder
 *                                   {@code NOT_FOUND} a um {@code OPEN} (issue #174)
 * @param leaderConfirmationLatency  latência dessas leituras fortes
 * @param redirectConfirmations        séries cujo redirecionamento derivado da réplica local do catálogo foi
 *                                     enviado ao líder para confirmação (issue #177)
 * @param redirectOverrides            redirecionamentos em que o líder divergiu da réplica local
 * @param redirectConfirmationFailures redirecionamentos respondidos pela réplica local porque a confirmação
 *                                     falhou ou estava em cooldown
 * @param redirectCacheHits            redirecionamentos respondidos por uma confirmação recente em cache
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
        long migrationsOut,
        long reconcileAdopted,
        long reconcileOrphansDeleted,
        long reconcileUnplaced,
        long reconcileMissing,
        long reconcileLastDurationMs,
        long leaderConfirmations,
        LatencySnapshot leaderConfirmationLatency,
        long redirectConfirmations,
        long redirectOverrides,
        long redirectConfirmationFailures,
        long redirectCacheHits) {

    public NodeMetricsSnapshot {
        Objects.requireNonNull(nodeId, "nodeId é obrigatório");
        errorsByStatus = Map.copyOf(Objects.requireNonNullElse(errorsByStatus, Map.of()));
        writeBatchLatency = Objects.requireNonNullElse(writeBatchLatency, LatencySnapshot.EMPTY);
        checkpointLatency = Objects.requireNonNullElse(checkpointLatency, LatencySnapshot.EMPTY);
        readLatency = Objects.requireNonNullElse(readLatency, LatencySnapshot.EMPTY);
        leaderConfirmationLatency = Objects.requireNonNullElse(leaderConfirmationLatency, LatencySnapshot.EMPTY);
    }

    /** Assinatura anterior à 8.6.0, sem as métricas de confirmação no líder (zeradas). */
    public NodeMetricsSnapshot(String nodeId, long capturedAtEpochMs, boolean leader, long seriesCount,
            long usedBytes, long capacityBytes, int openHandles, long writeBatches, long samplesWritten,
            long samplesFailed, long checkpoints, long flushes, long reads, LatencySnapshot writeBatchLatency,
            LatencySnapshot checkpointLatency, LatencySnapshot readLatency, Map<SeriesStatus, Long> errorsByStatus,
            BlobVolumeSummary blobStats, long migrationsIn, long migrationsOut, long reconcileAdopted,
            long reconcileOrphansDeleted, long reconcileUnplaced, long reconcileMissing,
            long reconcileLastDurationMs) {
        this(nodeId, capturedAtEpochMs, leader, seriesCount, usedBytes, capacityBytes, openHandles, writeBatches,
                samplesWritten, samplesFailed, checkpoints, flushes, reads, writeBatchLatency, checkpointLatency,
                readLatency, errorsByStatus, blobStats, migrationsIn, migrationsOut, reconcileAdopted,
                reconcileOrphansDeleted, reconcileUnplaced, reconcileMissing, reconcileLastDurationMs, 0L,
                LatencySnapshot.EMPTY);
    }

    /** Assinatura da 8.6.0, sem as métricas de confirmação de redirecionamento (zeradas). */
    public NodeMetricsSnapshot(String nodeId, long capturedAtEpochMs, boolean leader, long seriesCount,
            long usedBytes, long capacityBytes, int openHandles, long writeBatches, long samplesWritten,
            long samplesFailed, long checkpoints, long flushes, long reads, LatencySnapshot writeBatchLatency,
            LatencySnapshot checkpointLatency, LatencySnapshot readLatency, Map<SeriesStatus, Long> errorsByStatus,
            BlobVolumeSummary blobStats, long migrationsIn, long migrationsOut, long reconcileAdopted,
            long reconcileOrphansDeleted, long reconcileUnplaced, long reconcileMissing,
            long reconcileLastDurationMs, long leaderConfirmations, LatencySnapshot leaderConfirmationLatency) {
        this(nodeId, capturedAtEpochMs, leader, seriesCount, usedBytes, capacityBytes, openHandles, writeBatches,
                samplesWritten, samplesFailed, checkpoints, flushes, reads, writeBatchLatency, checkpointLatency,
                readLatency, errorsByStatus, blobStats, migrationsIn, migrationsOut, reconcileAdopted,
                reconcileOrphansDeleted, reconcileUnplaced, reconcileMissing, reconcileLastDurationMs,
                leaderConfirmations, leaderConfirmationLatency, 0L, 0L, 0L, 0L);
    }
}
