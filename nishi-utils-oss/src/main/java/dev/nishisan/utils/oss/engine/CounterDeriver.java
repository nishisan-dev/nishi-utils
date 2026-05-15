package dev.nishisan.utils.oss.engine;

import dev.nishisan.utils.oss.api.ResetBehavior;
import dev.nishisan.utils.oss.api.WrapBehavior;
import dev.nishisan.utils.oss.definition.DataSourceDef;

import java.util.Map;

/**
 * Aplica a derivação automática counter → rate.
 *
 * <p>Função pura (sem estado): recebe valor anterior + atual com seus
 * timestamps e a {@link DataSourceDef} respectiva, devolve um
 * {@link CounterDeriverResult} com o valor derivado e uma {@link Flag} indicando
 * se houve reset, wrap ou se o ponto deve virar {@code NaN}.</p>
 *
 * <p>Política:</p>
 * <ol>
 *     <li>{@code delta = current - previous}.</li>
 *     <li>Se {@code delta >= 0}: aplica a fórmula com bindings
 *     {@code delta} e {@code deltaT} (em segundos).</li>
 *     <li>Se {@code delta < 0} e {@link dev.nishisan.utils.oss.definition.ResetPolicyDef#detectCounterReset()}:
 *     <ul>
 *         <li>Se {@code |delta| >= previous * maxResetDeltaRatio} → RESET, aplica
 *         {@code onReset} ({@link ResetBehavior#UNKNOWN} = {@code NaN};
 *         {@link ResetBehavior#ZERO} = 0; {@link ResetBehavior#CARRY} = NaN para
 *         este ponto, sem alterar o histórico).</li>
 *         <li>Senão, presumimos WRAP: {@code wrappedDelta = (2^bits) - previous + current},
 *         aplicado conforme {@code onWrap} ({@link WrapBehavior#AUTO} usa a
 *         fórmula; {@link WrapBehavior#UNKNOWN} = {@code NaN}).</li>
 *     </ul>
 *     </li>
 *     <li>Se {@code clampNegativeToZero}, valores negativos finais são truncados a 0.</li>
 * </ol>
 */
public final class CounterDeriver {

    private CounterDeriver() {
    }

    /**
     * Calcula a derivação para uma transição de counter.
     *
     * @param ds          definição do Data Source (deve ter {@code derive.output} setado)
     * @param prevValue   valor anterior do counter (NaN = primeira amostra)
     * @param prevTsMs    timestamp anterior em ms
     * @param currValue   valor atual
     * @param currTsMs    timestamp atual em ms (deve ser &gt; prevTsMs)
     * @return resultado com valor derivado + flag descritiva
     */
    public static CounterDeriverResult derive(DataSourceDef ds,
                                              double prevValue,
                                              long prevTsMs,
                                              double currValue,
                                              long currTsMs) {
        if (ds.derive() == null || ds.derive().output() == null) {
            throw new IllegalArgumentException("DS sem derive.output não pode ser derivado: " + ds.name());
        }
        if (Double.isNaN(prevValue) || prevTsMs <= 0) {
            return new CounterDeriverResult(Double.NaN, Flag.FIRST_SAMPLE);
        }
        if (currTsMs <= prevTsMs) {
            return new CounterDeriverResult(Double.NaN, Flag.NON_MONOTONIC_TIME);
        }
        double deltaT = (currTsMs - prevTsMs) / 1000.0;
        double delta = currValue - prevValue;
        if (delta >= 0) {
            return finalize(ds, delta, deltaT, Flag.OK);
        }
        // delta negativo: tenta interpretar como wrap antes de declarar reset.
        boolean detectReset = ds.resetPolicy() != null && ds.resetPolicy().detectCounterReset();
        Integer counterBits = ds.counterBits();
        WrapBehavior onWrap = ds.derive().output().onWrap();
        ResetBehavior onReset = ds.derive().output().onReset();
        if (detectReset && counterBits != null) {
            double maxValue = Math.pow(2.0, counterBits);
            double wrappedDelta = maxValue - prevValue + currValue;
            // Wrap plausível: o "ganho real" precisa ser pequeno o bastante para
            // representar a continuidade após overflow (não a queda de um reset).
            // Limiar: < metade do domínio.
            if (wrappedDelta >= 0 && wrappedDelta < maxValue / 2.0) {
                if (onWrap == WrapBehavior.UNKNOWN) {
                    return new CounterDeriverResult(Double.NaN, Flag.WRAP);
                }
                return finalize(ds, wrappedDelta, deltaT, Flag.WRAP);
            }
        }
        // Não é wrap plausível: tratar como reset.
        if (!detectReset) {
            // Sem política, delta negativo é apenas NaN.
            return new CounterDeriverResult(Double.NaN, Flag.RESET);
        }
        double resetValue = switch (onReset == null ? ResetBehavior.UNKNOWN : onReset) {
            case UNKNOWN, CARRY -> Double.NaN;
            case ZERO -> 0.0;
        };
        return new CounterDeriverResult(resetValue, Flag.RESET);
    }

    private static CounterDeriverResult finalize(DataSourceDef ds, double delta, double deltaT, Flag flag) {
        double rate = FormulaEvaluator.evaluate(
                ds.derive().output().formula(),
                Map.of("delta", delta, "deltaT", deltaT));
        if (ds.derive().output().clampNegativeToZero() && rate < 0) {
            rate = 0.0;
        }
        return new CounterDeriverResult(rate, flag);
    }

    /**
     * Flag descritiva do resultado.
     */
    public enum Flag {
        OK,
        FIRST_SAMPLE,
        NON_MONOTONIC_TIME,
        RESET,
        WRAP
    }

    /**
     * Resultado da derivação.
     *
     * @param value valor derivado (NaN quando indeterminado)
     * @param flag  motivo/estado
     */
    public record CounterDeriverResult(double value, Flag flag) {
    }
}
