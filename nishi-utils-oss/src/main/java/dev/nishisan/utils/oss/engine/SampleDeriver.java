package dev.nishisan.utils.oss.engine;

import dev.nishisan.utils.oss.definition.DataSourceDef;
import dev.nishisan.utils.oss.engine.CounterDeriver.CounterDeriverResult;
import dev.nishisan.utils.oss.engine.CounterDeriver.Flag;

/**
 * Despacha a transformação de uma amostra para o valor a persistir conforme o
 * {@link dev.nishisan.utils.oss.api.DataSourceType}, em paridade com o RRDtool:
 * o <strong>tipo</strong> do DS define a transformação e o bloco {@code derive}
 * é opcional (refina apenas COUNTER/DERIVE com fórmula/clamp/wrap/reset).
 *
 * <ul>
 *   <li>{@code GAUGE} — valor as-is; sem derivação.</li>
 *   <li>{@code COUNTER} — taxa via {@link CounterDeriver} (delta/Δt com detecção
 *       de wrap e reset).</li>
 *   <li>{@code DERIVE} — taxa delta/Δt permitindo valores negativos, sem
 *       wrap/reset.</li>
 *   <li>{@code ABSOLUTE} — valor/Δt (contador zerado a cada leitura).</li>
 * </ul>
 *
 * <p>Em todos os tipos, valores fora de {@code [min, max]} (quando declarados)
 * viram {@code NaN} (UNKNOWN), também em paridade com o RRDtool — é um filtro de
 * faixa, não um truncamento.</p>
 */
public final class SampleDeriver {

    private SampleDeriver() {
    }

    /**
     * Deriva o valor a arquivar a partir da amostra atual e do estado anterior.
     *
     * @param ds        definição do Data Source (determina a transformação)
     * @param prevValue valor bruto anterior (NaN = primeira amostra)
     * @param prevTsMs  timestamp anterior em ms ({@code <= 0} = primeira amostra)
     * @param currValue valor bruto atual
     * @param currTsMs  timestamp atual em ms
     * @return valor derivado + flag descritiva (reuso de {@link CounterDeriverResult})
     */
    public static CounterDeriverResult derive(DataSourceDef ds, double prevValue, long prevTsMs,
                                              double currValue, long currTsMs) {
        return switch (ds.type()) {
            case GAUGE -> new CounterDeriverResult(withinRange(ds, currValue), Flag.OK);
            case COUNTER -> CounterDeriver.derive(ds, prevValue, prevTsMs, currValue, currTsMs);
            case DERIVE -> deriveRate(ds, prevValue, prevTsMs, currValue, currTsMs);
            case ABSOLUTE -> absoluteRate(ds, prevTsMs, currValue, currTsMs);
        };
    }

    /** DERIVE: taxa delta/Δt permitindo valores negativos; sem wrap/reset. */
    private static CounterDeriverResult deriveRate(DataSourceDef ds, double prevValue, long prevTsMs,
                                                   double currValue, long currTsMs) {
        if (Double.isNaN(prevValue) || prevTsMs <= 0) {
            return new CounterDeriverResult(Double.NaN, Flag.FIRST_SAMPLE);
        }
        if (currTsMs <= prevTsMs) {
            return new CounterDeriverResult(Double.NaN, Flag.NON_MONOTONIC_TIME);
        }
        double deltaT = (currTsMs - prevTsMs) / 1000.0;
        return new CounterDeriverResult(withinRange(ds, (currValue - prevValue) / deltaT), Flag.OK);
    }

    /** ABSOLUTE: contador zerado a cada leitura → valor/Δt (não usa o valor anterior). */
    private static CounterDeriverResult absoluteRate(DataSourceDef ds, long prevTsMs,
                                                     double currValue, long currTsMs) {
        if (prevTsMs <= 0) {
            return new CounterDeriverResult(Double.NaN, Flag.FIRST_SAMPLE);
        }
        if (currTsMs <= prevTsMs) {
            return new CounterDeriverResult(Double.NaN, Flag.NON_MONOTONIC_TIME);
        }
        double deltaT = (currTsMs - prevTsMs) / 1000.0;
        return new CounterDeriverResult(withinRange(ds, currValue / deltaT), Flag.OK);
    }

    /** Aplica o filtro de faixa min/max: fora dos limites declarados → {@code NaN}. */
    private static double withinRange(DataSourceDef ds, double value) {
        if (Double.isNaN(value)) {
            return value;
        }
        if (ds.min() != null && value < ds.min()) {
            return Double.NaN;
        }
        if (ds.max() != null && value > ds.max()) {
            return Double.NaN;
        }
        return value;
    }
}
