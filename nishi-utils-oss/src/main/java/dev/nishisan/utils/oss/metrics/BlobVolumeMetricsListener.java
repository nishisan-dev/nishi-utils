package dev.nishisan.utils.oss.metrics;

/**
 * Recebe notificações de eventos operacionais de um <strong>blob volume</strong>
 * (camada {@code SHARDED_BLOB}). Complementa o {@link NgrrdMetricsListener}, que é
 * por-série/qualidade: este listener é único por volume e observa a camada física
 * (shards, regiões, checkpoints) para <em>capacity planning</em> e alertas em
 * escala. As implementações tipicamente forward para Micrometer, logs ou JMX.
 *
 * <p><strong>Contrato de thread-safety:</strong> os callbacks de
 * {@link #onShardGrow}, {@link #onRegionAllocate} e {@link #onRegionFree} são
 * invocados <em>segurando o lock estrutural do volume</em>, que serializa todas as
 * alocações de milhares de séries. As implementações <strong>devem</strong> ser
 * thread-safe e <strong>baratas/non-blocking</strong> (O(1), sem I/O nem locks
 * próprios) — bloquear aqui serializa o volume inteiro. {@link #onCheckpoint} é
 * invocado fora do lock.</p>
 */
public interface BlobVolumeMetricsListener {

    /** Um shard cresceu em disco de {@code oldCapacityBytes} para {@code newCapacityBytes}. */
    default void onShardGrow(int shardId, long oldCapacityBytes, long newCapacityBytes) {
    }

    /** Uma nova região de {@code regionBytes} foi alocada no shard {@code shardId}. */
    default void onRegionAllocate(int shardId, long regionBytes) {
    }

    /** Uma região de {@code regionBytes} foi liberada (delete ou realocação) no shard {@code shardId}. */
    default void onRegionFree(int shardId, long regionBytes) {
    }

    /** Um checkpoint do catálogo concluiu em {@code durationMs} cobrindo {@code entryCount} entradas vivas. */
    default void onCheckpoint(long durationMs, int entryCount) {
    }
}
