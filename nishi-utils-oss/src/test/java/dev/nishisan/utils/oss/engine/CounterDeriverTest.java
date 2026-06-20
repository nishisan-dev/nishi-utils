package dev.nishisan.utils.oss.engine;

import dev.nishisan.utils.oss.api.DataSourceType;
import dev.nishisan.utils.oss.api.ResetBehavior;
import dev.nishisan.utils.oss.api.WrapBehavior;
import dev.nishisan.utils.oss.definition.DataSourceDef;
import dev.nishisan.utils.oss.definition.DeriveDef;
import dev.nishisan.utils.oss.definition.DeriveOutputDef;
import dev.nishisan.utils.oss.definition.ResetPolicyDef;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CounterDeriverTest {

    @Test
    void primeiraAmostraRetornaFirstSample() {
        CounterDeriver.CounterDeriverResult r = CounterDeriver.derive(
                ds64(), Double.NaN, 0L, 1000.0, 1_000_000L);
        assertEquals(CounterDeriver.Flag.FIRST_SAMPLE, r.flag());
        assertTrue(Double.isNaN(r.value()));
    }

    @Test
    void deltaPositivoAplicaFormula() {
        long prevMs = 1_000_000L;
        long currMs = prevMs + 300_000L;     // deltaT = 300s
        double prev = 1_000_000.0;
        double curr = 1_000_000.0 + 3_000.0; // 3000 octets em 300s = 80 bps
        CounterDeriver.CounterDeriverResult r = CounterDeriver.derive(
                ds64(), prev, prevMs, curr, currMs);
        assertEquals(CounterDeriver.Flag.OK, r.flag());
        assertEquals(80.0, r.value(), 1e-6);
    }

    @Test
    void quedaGrandeDetectadaComoReset() {
        long prevMs = 1_000_000L;
        long currMs = prevMs + 300_000L;
        double prev = 1_000_000.0;
        double curr = 10.0; // queda > 90%
        CounterDeriver.CounterDeriverResult r = CounterDeriver.derive(
                ds64(), prev, prevMs, curr, currMs);
        assertEquals(CounterDeriver.Flag.RESET, r.flag());
        assertTrue(Double.isNaN(r.value()));
    }

    @Test
    void wrapDe32BitsAplicaCorrecao() {
        DataSourceDef ds = ds32();
        long prevMs = 1_000_000L;
        long currMs = prevMs + 300_000L;
        double prev = Math.pow(2, 32) - 1000.0;
        double curr = 2000.0; // wrap atravessou: deveria voltar a ~3000 octets
        CounterDeriver.CounterDeriverResult r = CounterDeriver.derive(ds, prev, prevMs, curr, currMs);
        assertEquals(CounterDeriver.Flag.WRAP, r.flag());
        // 3000 octets em 300s = 80 bps
        assertEquals(80.0, r.value(), 1e-6);
    }

    @Test
    void quedaPequenaAbaixoDoThresholdClassificaComoSpikeDownNaoReset() {
        // ds64 tem maxResetDeltaRatio=0.90 → quedas < 90% não são reset.
        long prevMs = 1_000_000L;
        long currMs = prevMs + 300_000L;
        double prev = 1_000_000.0;
        double curr = 950_000.0; // queda de 5% — bem abaixo do threshold 90%
        CounterDeriver.CounterDeriverResult r = CounterDeriver.derive(
                ds64(), prev, prevMs, curr, currMs);
        assertEquals(CounterDeriver.Flag.SPIKE_DOWN, r.flag());
        assertTrue(Double.isNaN(r.value()));
    }

    @Test
    void quedaNoLimiteDoThresholdAindaContaComoReset() {
        long prevMs = 1_000_000L;
        long currMs = prevMs + 300_000L;
        double prev = 1_000_000.0;
        double curr = 100_000.0; // queda exatamente de 90%
        CounterDeriver.CounterDeriverResult r = CounterDeriver.derive(
                ds64(), prev, prevMs, curr, currMs);
        assertEquals(CounterDeriver.Flag.RESET, r.flag());
    }

    @Test
    void prevValueZeroForcaQualquerNegativoAReset() {
        // Sem base para calcular ratio; tratamos como reset por convenção.
        DataSourceDef ds = ds64();
        long prevMs = 1_000_000L;
        long currMs = prevMs + 300_000L;
        CounterDeriver.CounterDeriverResult r = CounterDeriver.derive(
                ds, 0.0, prevMs, -1.0, currMs);
        assertEquals(CounterDeriver.Flag.RESET, r.flag());
    }

    @Test
    void clampNegativeToZeroForcaResultadoNaoNegativo() {
        // Constrói DS com formula que poderia ser negativa
        DataSourceDef ds = new DataSourceDef(
                "x", DataSourceType.COUNTER, 64, 900, 0.0, null,
                new ResetPolicyDef(false, 0.9),
                new DeriveDef(new DeriveOutputDef(
                        "rate", "u", "delta - 1000",
                        true, ResetBehavior.UNKNOWN, WrapBehavior.AUTO)), null);
        long prevMs = 1L;
        long currMs = 301_000L;
        CounterDeriver.CounterDeriverResult r = CounterDeriver.derive(ds, 100.0, prevMs, 200.0, currMs);
        assertEquals(CounterDeriver.Flag.OK, r.flag());
        // delta=100, formula = 100 - 1000 = -900, clamp = 0
        assertEquals(0.0, r.value(), 1e-9);
    }

    private static DataSourceDef ds64() {
        return new DataSourceDef(
                "in_octets", DataSourceType.COUNTER, 64, 900, 0.0, null,
                new ResetPolicyDef(true, 0.90),
                new DeriveDef(new DeriveOutputDef(
                        "in_bps", "bit/s", "delta * 8 / deltaT",
                        true, ResetBehavior.UNKNOWN, WrapBehavior.AUTO)), null);
    }

    private static DataSourceDef ds32() {
        return new DataSourceDef(
                "in_errors", DataSourceType.COUNTER, 32, 900, 0.0, null,
                new ResetPolicyDef(true, 0.90),
                new DeriveDef(new DeriveOutputDef(
                        "in_eps", "errors/s", "delta * 8 / deltaT",
                        true, ResetBehavior.UNKNOWN, WrapBehavior.AUTO)), null);
    }
}
