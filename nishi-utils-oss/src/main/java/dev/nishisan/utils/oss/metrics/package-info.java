/**
 * Métricas de qualidade e observabilidade do ngrrd.
 *
 * <p>{@link dev.nishisan.utils.oss.metrics.NgrrdMetrics} expõe os contadores e
 * gauges declarados em {@code spec.quality.emitMetrics} do YAML
 * ({@code missing_ratio}, {@code ingest_lag_sec}, {@code late_sample_count},
 * {@code counter_reset_count}, {@code wrap_detected_count}). A interface
 * {@link dev.nishisan.utils.oss.metrics.NgrrdMetricsListener} permite plugar
 * Micrometer, JMX ou um logger externo.</p>
 */
package dev.nishisan.utils.oss.metrics;
