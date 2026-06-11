package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.engine.PrimaryDataPoint;

import java.util.Map;

/**
 * Estado durável do {@link NgrrdWriter} no modo de persistência incremental.
 *
 * <p>Captura tudo que é necessário para reconstruir a janela do bloco aberto e a
 * continuidade da derivação de counter ao reabrir o handle (semântica
 * rrdtool-like, análoga ao {@code last_ds}/{@code last_up}):</p>
 *
 * <ul>
 *     <li>{@code blockStartEpochSec} — início (alinhado) do bloco aberto.</li>
 *     <li>{@code baseStepSec}/{@code blockSizeSec} — geometria da janela, usada
 *     para validar compatibilidade com a definição corrente na reabertura.</li>
 *     <li>{@code counterPrev} — último valor observado por DS <em>raw</em>
 *     (alimenta o {@code CounterDeriver}).</li>
 *     <li>{@code buckets} — acumuladores ({@link PrimaryDataPoint.Memento}) por
 *     slot, por DS <em>derivado</em>.</li>
 * </ul>
 *
 * <p>Serializado por {@link SeriesStateCodec}.</p>
 */
public record SeriesWriterState(
        long blockStartEpochSec,
        int baseStepSec,
        int blockSizeSec,
        Map<String, BlockWindow.CounterPrev> counterPrev,
        Map<String, PrimaryDataPoint.Memento[]> buckets) {

    public SeriesWriterState {
        counterPrev = counterPrev == null ? Map.of() : Map.copyOf(counterPrev);
        buckets = buckets == null ? Map.of() : Map.copyOf(buckets);
    }

    /** Número de slots esperado para a geometria persistida. */
    public int slots() {
        return blockSizeSec / baseStepSec;
    }
}
