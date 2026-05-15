/**
 * Pacote raiz do formato <strong>ngrrd</strong> (Nishi Grid Round-Robin Database).
 *
 * <p>Oferece persistência de séries temporais com paridade conceitual ao RRD clássico
 * (Data Sources tipados, RRAs com step/rows/CF/XFF, detecção de counter reset/wrap,
 * derivações automáticas) e suporte a backends pluggable (disco local e Object Storage
 * S3-compatível).</p>
 *
 * <p>Definições são declarativas via YAML (apiVersion {@code ngrrd/v1}, kind
 * {@code MetricSeriesDefinition}).</p>
 */
package dev.nishisan.utils.oss;
