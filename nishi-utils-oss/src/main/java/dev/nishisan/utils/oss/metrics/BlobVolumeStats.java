package dev.nishisan.utils.oss.metrics;

import dev.nishisan.utils.oss.blob.VolumeWriteMode;

import java.util.Objects;

/**
 * Snapshot imutável dos gauges operacionais de um blob volume, lido sob demanda
 * (modelo <em>pull</em>) via {@code BlobVolume.stats()} / {@code BlobStorage.stats()}.
 * Desacopla a coleta da frequência de checkpoint — tipicamente alimenta
 * {@code Gauge}s do Micrometer por <em>poll</em> de baixa frequência.
 *
 * <p>Os arrays são indexados por {@code shardId} ({@code [0, shardCount)}) e
 * representam uma fotografia consistente no instante da chamada. {@code shardUsedBytes}
 * é o <strong>uso líquido</strong> (soma de {@code regionBytes} das séries vivas no
 * shard) — recua quando regiões são liberadas, ao contrário de um <em>high-water</em>
 * por cursor de bump.</p>
 *
 * @param shardCount         número de shards do volume
 * @param shardCapacityBytes capacidade atual (em disco) de cada shard
 * @param shardUsedBytes     uso líquido (soma de regiões vivas) de cada shard
 * @param seriesPerShard     contagem de séries vivas em cada shard
 * @param fillRatioPerShard  {@code shardUsedBytes / shardCapacityBytes} de cada shard, em [0,1]
 * @param catalogEntryCount  total de entradas vivas no catálogo
 * @param catalogImageBytes  tamanho do snapshot durável do catálogo ({@code catalog.bin})
 * @param walBytes           tamanho atual do journal do catálogo ({@code catalog.wal})
 * @param writeMode          modo de escrita dos shards do volume
 * @param bytesWritten       bytes lógicos entregues a {@code writeAt} em todos os shards desde a
 *                           abertura do volume (contador acumulado, não persistido)
 * @param declaredCapacityBytes capacidade lógica declarada do volume ({@code capacityBytes});
 *                           {@code 0} = não declarada ou desconhecida (ex.: snapshot de nó mais antigo)
 * @param liveBytes          soma dos bytes de região das séries vivas no catálogo
 * @param reservedBytes      bytes lógicos de crescimento reservados (reservas de admissão em aberto)
 */
public record BlobVolumeStats(int shardCount, long[] shardCapacityBytes, long[] shardUsedBytes,
                              long[] seriesPerShard, double[] fillRatioPerShard,
                              int catalogEntryCount, long catalogImageBytes, long walBytes,
                              VolumeWriteMode writeMode, long bytesWritten,
                              long declaredCapacityBytes, long liveBytes, long reservedBytes) {

    public BlobVolumeStats {
        Objects.requireNonNull(writeMode, "writeMode é obrigatório");
    }

    /**
     * Compatibilidade com a forma da 8.13 (com modo de escrita e bytes escritos): capacidade declarada,
     * bytes vivos e reservados ficam em {@code 0}.
     */
    public BlobVolumeStats(int shardCount, long[] shardCapacityBytes, long[] shardUsedBytes,
                           long[] seriesPerShard, double[] fillRatioPerShard,
                           int catalogEntryCount, long catalogImageBytes, long walBytes,
                           VolumeWriteMode writeMode, long bytesWritten) {
        this(shardCount, shardCapacityBytes, shardUsedBytes, seriesPerShard, fillRatioPerShard,
                catalogEntryCount, catalogImageBytes, walBytes, writeMode, bytesWritten, 0L, 0L, 0L);
    }

    /** Compatibilidade: snapshot sem modo de escrita (assume {@link VolumeWriteMode#MMAP}) e sem bytes escritos. */
    public BlobVolumeStats(int shardCount, long[] shardCapacityBytes, long[] shardUsedBytes,
                           long[] seriesPerShard, double[] fillRatioPerShard,
                           int catalogEntryCount, long catalogImageBytes, long walBytes) {
        this(shardCount, shardCapacityBytes, shardUsedBytes, seriesPerShard, fillRatioPerShard,
                catalogEntryCount, catalogImageBytes, walBytes, VolumeWriteMode.MMAP, 0L);
    }

    /**
     * Fill sobre a capacidade <strong>declarada</strong>: {@code (liveBytes + reservedBytes) / declaredCapacityBytes},
     * a mesma conta usada pela recusa de admissão ({@code CapacityBudget}). Diferente de
     * {@code fillRatioPerShard}, que é por shard sobre o tamanho atual do arquivo (dente de serra, não serve
     * para alerta).
     *
     * @return o fill; {@code 0.0} quando não há capacidade declarada ({@code 0} = desconhecido)
     */
    public double fillOfDeclaredCapacity() {
        return declaredCapacityBytes > 0
                ? (double) (liveBytes + reservedBytes) / declaredCapacityBytes
                : 0.0;
    }
}
