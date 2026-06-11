package dev.nishisan.utils.oss.engine;

import dev.nishisan.utils.oss.api.ConsolidationFunction;

/**
 * Acumulador mutável de um Primary Data Point (PDP) — bucket de tamanho
 * {@code baseStepSec}.
 *
 * <p>Coleta múltiplas amostras dentro da mesma janela e expõe todas as funções
 * de consolidação clássicas (AVERAGE, MAX, MIN, LAST) sobre o mesmo material.
 * A contagem de amostras ausentes é registrada explicitamente para alimentar o
 * cálculo de XFF no nível do RRA.</p>
 */
public final class PrimaryDataPoint {

    private double sum;
    private int count;
    private double min;
    private double max;
    private double last;
    private int missing;

    public PrimaryDataPoint() {
        reset();
    }

    public void reset() {
        this.sum = 0.0;
        this.count = 0;
        this.min = Double.POSITIVE_INFINITY;
        this.max = Double.NEGATIVE_INFINITY;
        this.last = Double.NaN;
        this.missing = 0;
    }

    public void add(double value) {
        if (Double.isNaN(value)) {
            missing++;
            return;
        }
        sum += value;
        count++;
        if (value < min) {
            min = value;
        }
        if (value > max) {
            max = value;
        }
        last = value;
    }

    public int observedCount() {
        return count;
    }

    public int missingCount() {
        return missing;
    }

    public boolean isEmpty() {
        return count == 0;
    }

    public double consolidate(ConsolidationFunction cf) {
        if (isEmpty()) {
            return Double.NaN;
        }
        return switch (cf) {
            case AVERAGE -> sum / count;
            case MAX -> max;
            case MIN -> min;
            case LAST -> last;
        };
    }

    /**
     * Memento imutável do estado interno do acumulador, usado para persistir e
     * reconstruir a janela aberta no modo de persistência incremental.
     */
    public record Memento(double sum, int count, double min, double max, double last, int missing) {
    }

    /** Captura o estado interno atual para serialização. */
    public Memento snapshot() {
        return new Memento(sum, count, min, max, last, missing);
    }

    /** Reconstrói um acumulador a partir de um {@link Memento} persistido. */
    public static PrimaryDataPoint restore(Memento m) {
        PrimaryDataPoint pdp = new PrimaryDataPoint();
        pdp.sum = m.sum();
        pdp.count = m.count();
        pdp.min = m.min();
        pdp.max = m.max();
        pdp.last = m.last();
        pdp.missing = m.missing();
        return pdp;
    }
}
