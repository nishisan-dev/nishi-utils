package dev.nishisan.utils.oss.format;

import dev.nishisan.utils.oss.engine.PrimaryDataPoint;

/**
 * Estado vivo mutável da série (a parte do arquivo NGRR que é sobrescrita in
 * place): equivale ao {@code live_head}/{@code pdp_prep}/{@code cdp_prep}/
 * {@code rra_ptr} do RRDtool. É a fonte de verdade em memória do writer e o que
 * o {@link SeriesFileCodec} serializa na seção de live-state.
 *
 * <ul>
 *     <li>{@code lastUpEpochMs} — timestamp da última amostra observada
 *     ({@code last_up}).</li>
 *     <li>{@code counterPrevValue}/{@code counterPrevTsMs} — último valor por DS
 *     raw de cada coluna ({@code last_ds}), alimenta o {@code CounterDeriver};
 *     {@code NaN} = sem amostra anterior.</li>
 *     <li>{@code pdp} — acumulador do step base em progresso por coluna
 *     ({@code pdp_prep}); {@code pdpSlotSec} = slot que está acumulando
 *     ({@code -1} = nenhum).</li>
 *     <li>{@code curRow}/{@code curRowEpochSec} — ponteiro do ring por archive
 *     ({@code rra_ptr}); {@code -1} = vazio.</li>
 *     <li>{@code cdpPartial}/{@code cdpFolded}/{@code cdpMissing} — consolidação
 *     em progresso por {@code (archive, coluna)} ({@code cdp_prep}).</li>
 * </ul>
 *
 * <p>Não é thread-safe — apenas a worker thread do writer a acessa.</p>
 */
public final class SeriesLiveState {

    private final int d;
    private final int a;

    public long lastUpEpochMs;
    public final double[] counterPrevValue;   // [d]
    public final long[] counterPrevTsMs;      // [d]
    public final PrimaryDataPoint[] pdp;      // [d]
    public final long[] pdpSlotSec;           // [d]
    public final int[] curRow;                // [a]
    public final long[] curRowEpochSec;       // [a]
    public final double[] cdpPartial;         // [a*d]
    public final int[] cdpFolded;             // [a*d]
    public final int[] cdpMissing;            // [a*d]

    public SeriesLiveState(int columnCount, int archiveCount) {
        this.d = columnCount;
        this.a = archiveCount;
        this.lastUpEpochMs = 0L;
        this.counterPrevValue = new double[d];
        this.counterPrevTsMs = new long[d];
        this.pdp = new PrimaryDataPoint[d];
        this.pdpSlotSec = new long[d];
        this.curRow = new int[a];
        this.curRowEpochSec = new long[a];
        this.cdpPartial = new double[a * d];
        this.cdpFolded = new int[a * d];
        this.cdpMissing = new int[a * d];
        reset();
    }

    /** Zera para o estado "vazio" (nenhuma amostra ainda). */
    public void reset() {
        lastUpEpochMs = 0L;
        for (int i = 0; i < d; i++) {
            counterPrevValue[i] = Double.NaN;
            counterPrevTsMs[i] = 0L;
            pdp[i] = new PrimaryDataPoint();
            pdpSlotSec[i] = -1L;
        }
        for (int x = 0; x < a; x++) {
            curRow[x] = -1;
            curRowEpochSec[x] = -1L;
        }
        for (int x = 0; x < a * d; x++) {
            cdpPartial[x] = Double.NaN;
            cdpFolded[x] = 0;
            cdpMissing[x] = 0;
        }
    }

    public int columnCount() {
        return d;
    }

    public int archiveCount() {
        return a;
    }

    /** Índice linear do acumulador de CDP de {@code (archive, coluna)}. */
    public int cdpIndex(int archive, int col) {
        return archive * d + col;
    }
}
