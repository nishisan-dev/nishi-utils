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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import dev.nishisan.utils.oss.metrics.BlobVolumeStats;

import java.util.Objects;

/**
 * Resumo por nó de um {@link BlobVolumeStats}, agregando os arrays por shard
 * em totais/máximos simples — o suficiente para {@link NodeMetricsSnapshot},
 * sem carregar o detalhamento por shard pela rede a cada tick.
 *
 * @param shardCount        número de shards do volume
 * @param usedBytes         soma do uso líquido de todos os shards
 * @param capacityBytes     soma da capacidade atual de todos os shards
 * @param maxFillRatio      maior {@code fillRatio} entre os shards, em [0,1]
 * @param catalogEntryCount total de entradas vivas no catálogo do volume
 * @param walBytes          tamanho atual do journal do catálogo do volume
 * @param writeMode         modo de escrita dos shards ({@code MMAP}|{@code PWRITE}); {@code null} quando o nó
 *                          que reportou roda uma versão anterior à 8.13.0 (modo desconhecido)
 * @param bytesWritten      bytes lógicos entregues a {@code writeAt} em todos os shards desde a abertura do
 *                          volume (contador acumulado, não persistido); {@code 0} quando o nó reportante é
 *                          anterior à 8.13.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BlobVolumeSummary(
        int shardCount,
        long usedBytes,
        long capacityBytes,
        double maxFillRatio,
        int catalogEntryCount,
        long walBytes,
        String writeMode,
        long bytesWritten) {

    /** Construtor de compatibilidade (forma da 8.12.0): modo de escrita desconhecido e {@code bytesWritten} zero. */
    public BlobVolumeSummary(int shardCount, long usedBytes, long capacityBytes, double maxFillRatio,
                             int catalogEntryCount, long walBytes) {
        this(shardCount, usedBytes, capacityBytes, maxFillRatio, catalogEntryCount, walBytes, null, 0L);
    }

    /** Deriva o resumo a partir do {@link BlobVolumeStats} bruto de um {@code BlobVolume}. */
    public static BlobVolumeSummary from(BlobVolumeStats stats) {
        Objects.requireNonNull(stats, "stats");
        return new BlobVolumeSummary(stats.shardCount(), sum(stats.shardUsedBytes()), sum(stats.shardCapacityBytes()),
                max(stats.fillRatioPerShard()), stats.catalogEntryCount(), stats.walBytes(),
                stats.writeMode().name(), stats.bytesWritten());
    }

    private static long sum(long[] values) {
        long total = 0L;
        for (long value : values) {
            total += value;
        }
        return total;
    }

    private static double max(double[] values) {
        double max = 0.0;
        for (double value : values) {
            if (value > max) {
                max = value;
            }
        }
        return max;
    }
}
