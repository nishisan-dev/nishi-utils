/**
 * Ingestão e persistência da série ngrrd no objeto único (NGRR).
 *
 * <p>O componente público é {@link dev.nishisan.utils.oss.writer.NgrrdWriter},
 * responsável por receber {@link dev.nishisan.utils.oss.api.Sample}s, aplicar a
 * engine pura ({@code engine.CounterDeriver} → {@code engine.PrimaryDataPoint}
 * → consolidação por RRA) e manter os ring buffers in-place do arquivo de série
 * via {@link dev.nishisan.utils.oss.storage.SeriesChannel}.</p>
 *
 * <p>A consolidação é contínua (estilo {@code cdp_prep} do RRDtool) e o
 * {@code checkpoint()} materializa o CDP em progresso como parcial, tornando o
 * estado durável (fsync no disco / PUT no S3).</p>
 */
package dev.nishisan.utils.oss.writer;
