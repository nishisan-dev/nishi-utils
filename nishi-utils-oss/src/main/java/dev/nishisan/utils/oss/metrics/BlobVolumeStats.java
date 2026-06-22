package dev.nishisan.utils.oss.metrics;

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
 */
public record BlobVolumeStats(int shardCount, long[] shardCapacityBytes, long[] shardUsedBytes,
                              long[] seriesPerShard, double[] fillRatioPerShard,
                              int catalogEntryCount, long catalogImageBytes, long walBytes) {
}
