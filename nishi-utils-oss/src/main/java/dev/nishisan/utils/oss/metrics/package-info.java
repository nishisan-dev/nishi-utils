/**
 * Métricas de qualidade e observabilidade do ngrrd.
 *
 * <p>{@link dev.nishisan.utils.oss.metrics.NgrrdMetrics} expõe os contadores e
 * gauges declarados em {@code spec.quality.emitMetrics} do YAML
 * ({@code missing_ratio}, {@code ingest_lag_sec}, {@code late_sample_count},
 * {@code counter_reset_count}, {@code wrap_detected_count}). A interface
 * {@link dev.nishisan.utils.oss.metrics.NgrrdMetricsListener} permite plugar
 * Micrometer, JMX ou um logger externo; sua forma canônica recebe o
 * {@code seriesKey} para atribuir eventos à série de origem.</p>
 *
 * <p>Para a camada de <strong>blob volume</strong> ({@code SHARDED_BLOB}), a
 * observabilidade <em>operacional</em> (independente da qualidade por-série) é dada
 * por {@link dev.nishisan.utils.oss.metrics.BlobVolumeMetricsListener} (eventos push:
 * grow de shard, alloc/free de região, checkpoint),
 * {@link dev.nishisan.utils.oss.metrics.BlobVolumeMetrics} (coletor acumulador) e
 * {@link dev.nishisan.utils.oss.metrics.BlobVolumeStats} (gauges pull: fill ratio e
 * séries por shard, tamanho de catálogo e WAL).</p>
 */
package dev.nishisan.utils.oss.metrics;
