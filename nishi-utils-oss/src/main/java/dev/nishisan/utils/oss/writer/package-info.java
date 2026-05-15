/**
 * Ingestão e fechamento de blocos do ngrrd.
 *
 * <p>O componente público é {@link dev.nishisan.utils.oss.writer.NgrrdWriter},
 * responsável por receber {@link dev.nishisan.utils.oss.api.Sample}s, aplicar a
 * engine pura ({@code engine.CounterDeriver} → {@code engine.PrimaryDataPoint}
 * → {@code engine.RraConsolidator}) e persistir os blocos consolidados via
 * {@link dev.nishisan.utils.oss.storage.NgrrdStorage}.</p>
 *
 * <p>Já {@link dev.nishisan.utils.oss.writer.ManifestUpdater} grava snapshots
 * versionados do manifesto a cada {@code manifestPolicy.intervalSec}.</p>
 */
package dev.nishisan.utils.oss.writer;
