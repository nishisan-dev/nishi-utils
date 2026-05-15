package dev.nishisan.utils.oss.engine;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PrimaryDataPointTest {

    @Test
    void consolidaAverage() {
        PrimaryDataPoint pdp = new PrimaryDataPoint();
        pdp.add(10.0);
        pdp.add(20.0);
        pdp.add(30.0);
        assertEquals(20.0, pdp.consolidate(ConsolidationFunction.AVERAGE), 1e-9);
    }

    @Test
    void consolidaMaxMinLast() {
        PrimaryDataPoint pdp = new PrimaryDataPoint();
        pdp.add(5.0);
        pdp.add(2.0);
        pdp.add(9.0);
        pdp.add(7.0);
        assertEquals(9.0, pdp.consolidate(ConsolidationFunction.MAX), 1e-9);
        assertEquals(2.0, pdp.consolidate(ConsolidationFunction.MIN), 1e-9);
        assertEquals(7.0, pdp.consolidate(ConsolidationFunction.LAST), 1e-9);
    }

    @Test
    void contabilizaMissingPorNaN() {
        PrimaryDataPoint pdp = new PrimaryDataPoint();
        pdp.add(Double.NaN);
        pdp.add(10.0);
        pdp.add(Double.NaN);
        assertEquals(1, pdp.observedCount());
        assertEquals(2, pdp.missingCount());
    }

    @Test
    void vazioRetornaNaN() {
        PrimaryDataPoint pdp = new PrimaryDataPoint();
        assertTrue(Double.isNaN(pdp.consolidate(ConsolidationFunction.AVERAGE)));
    }

    @Test
    void resetVoltaAoEstadoInicial() {
        PrimaryDataPoint pdp = new PrimaryDataPoint();
        pdp.add(1.0);
        pdp.reset();
        assertEquals(0, pdp.observedCount());
        assertTrue(pdp.isEmpty());
    }
}
