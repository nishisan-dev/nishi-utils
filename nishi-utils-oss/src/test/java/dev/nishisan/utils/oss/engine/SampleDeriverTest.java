package dev.nishisan.utils.oss.engine;

import dev.nishisan.utils.oss.api.DataSourceType;
import dev.nishisan.utils.oss.definition.DataSourceDef;
import dev.nishisan.utils.oss.definition.ResetPolicyDef;
import dev.nishisan.utils.oss.engine.CounterDeriver.CounterDeriverResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Paridade RRD: cada tipo de DS deriva conforme sua semântica mesmo sem bloco
 * {@code derive}.
 */
class SampleDeriverTest {

    private static final long T0 = 1_000_000L;
    private static final long T1 = T0 + 300_000L; // ΔT = 300s

    @Test
    void gaugeSemDerivePersisteValorComoEsta() {
        DataSourceDef gauge = new DataSourceDef("temp", DataSourceType.GAUGE,
                null, 900, null, null, null, null, null);
        CounterDeriverResult r = SampleDeriver.derive(gauge, Double.NaN, 0L, 42.5, T1);
        assertEquals(CounterDeriver.Flag.OK, r.flag());
        assertEquals(42.5, r.value(), 1e-9);
    }

    @Test
    void gaugeForaDaFaixaViraNaN() {
        DataSourceDef gauge = new DataSourceDef("pct", DataSourceType.GAUGE,
                null, 900, 0.0, 100.0, null, null, null);
        assertTrue(Double.isNaN(SampleDeriver.derive(gauge, Double.NaN, 0L, 150.0, T1).value()));
        assertTrue(Double.isNaN(SampleDeriver.derive(gauge, Double.NaN, 0L, -1.0, T1).value()));
        assertEquals(73.0, SampleDeriver.derive(gauge, Double.NaN, 0L, 73.0, T1).value(), 1e-9);
    }

    @Test
    void counterSemDeriveUsaTaxaPadraoDeltaPorDeltaT() {
        DataSourceDef counter = new DataSourceDef("c", DataSourceType.COUNTER,
                64, 900, null, null, new ResetPolicyDef(true, 0.90), null, null);
        // delta = 300 em 300s → 1.0/s
        CounterDeriverResult r = SampleDeriver.derive(counter, 1_000.0, T0, 1_300.0, T1);
        assertEquals(CounterDeriver.Flag.OK, r.flag());
        assertEquals(1.0, r.value(), 1e-9);
    }

    @Test
    void deriveSemDerivePermiteTaxaNegativa() {
        DataSourceDef derive = new DataSourceDef("d", DataSourceType.DERIVE,
                null, 900, null, null, null, null, null);
        // (700 - 1000) / 300 = -1.0/s — DERIVE não detecta reset.
        CounterDeriverResult r = SampleDeriver.derive(derive, 1_000.0, T0, 700.0, T1);
        assertEquals(CounterDeriver.Flag.OK, r.flag());
        assertEquals(-1.0, r.value(), 1e-9);
    }

    @Test
    void absoluteSemDeriveDivideValorAtualPeloIntervalo() {
        DataSourceDef absolute = new DataSourceDef("a", DataSourceType.ABSOLUTE,
                null, 900, null, null, null, null, null);
        // 600 / 300 = 2.0/s (não usa o valor anterior).
        CounterDeriverResult r = SampleDeriver.derive(absolute, Double.NaN, T0, 600.0, T1);
        assertEquals(CounterDeriver.Flag.OK, r.flag());
        assertEquals(2.0, r.value(), 1e-9);
    }

    @Test
    void taxaNaPrimeiraAmostraEhIndefinida() {
        DataSourceDef derive = new DataSourceDef("d", DataSourceType.DERIVE,
                null, 900, null, null, null, null, null);
        CounterDeriverResult r = SampleDeriver.derive(derive, Double.NaN, 0L, 700.0, T1);
        assertEquals(CounterDeriver.Flag.FIRST_SAMPLE, r.flag());
        assertTrue(Double.isNaN(r.value()));
    }
}
