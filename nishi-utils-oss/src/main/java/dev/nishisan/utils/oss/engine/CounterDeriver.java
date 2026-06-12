package dev.nishisan.utils.oss.engine;

import dev.nishisan.utils.oss.api.ResetBehavior;
import dev.nishisan.utils.oss.api.WrapBehavior;
import dev.nishisan.utils.oss.definition.DataSourceDef;
import dev.nishisan.utils.oss.definition.DeriveOutputDef;

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

    /** Fórmula de taxa padrão quando o DS não declara {@code derive.output}. */
    private static final String DEFAULT_RATE_FORMULA = "delta / deltaT";

    private CounterDeriver() {
    }

    /**
     * Calcula a derivação para uma transição de counter.
     *
     * @param ds          definição do Data Source ({@code derive.output} opcional —
     *                    ausente usa defaults de taxa: fórmula {@code delta/deltaT})
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
        if (Double.isNaN(prevValue) || prevTsMs <= 0) {
            return new CounterDeriverResult(Double.NaN, Flag.FIRST_SAMPLE);
        }
        if (currTsMs <= prevTsMs) {
            return new CounterDeriverResult(Double.NaN, Flag.NON_MONOTONIC_TIME);
        }
        // Sem derive.output (paridade RRD), a taxa usa defaults: fórmula delta/Δt,
        // sem clamp, wrap AUTO, reset UNKNOWN.
        DeriveOutputDef out = ds.derive() == null ? null : ds.derive().output();
        String formula = out != null && out.formula() != null ? out.formula() : DEFAULT_RATE_FORMULA;
        boolean clampNegativeToZero = out != null && out.clampNegativeToZero();
        WrapBehavior onWrap = out != null && out.onWrap() != null ? out.onWrap() : WrapBehavior.AUTO;
        ResetBehavior onReset = out != null && out.onReset() != null ? out.onReset() : ResetBehavior.UNKNOWN;

        double deltaT = (currTsMs - prevTsMs) / 1000.0;
        double delta = currValue - prevValue;
        if (delta >= 0) {
            return finalize(formula, clampNegativeToZero, delta, deltaT, Flag.OK);
        }
        // delta negativo: tenta interpretar como wrap antes de declarar reset.
        boolean detectReset = ds.resetPolicy() != null && ds.resetPolicy().detectCounterReset();
        Integer counterBits = ds.counterBits();
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
                return finalize(formula, clampNegativeToZero, wrappedDelta, deltaT, Flag.WRAP);
            }
        }
        // Não é wrap plausível: avalia maxResetDeltaRatio antes de classificar como reset.
        if (!detectReset) {
            // Sem política, delta negativo é apenas NaN.
            return new CounterDeriverResult(Double.NaN, Flag.RESET);
        }
        // dropRatio em [0, +inf): fração do valor anterior que foi "perdida".
        // Ex.: prev=100, curr=10 → dropRatio = 0.90 (queda de 90%).
        // prevValue<=0 não permite cálculo significativo da razão; assumimos queda total.
        double maxResetRatio = ds.resetPolicy().maxResetDeltaRatio();
        double dropRatio = prevValue > 0 ? (-delta / prevValue) : Double.POSITIVE_INFINITY;
        if (dropRatio < maxResetRatio) {
            // Queda menor que o threshold — provavelmente ruído ou amostra fora
            // de ordem, não um reset legítimo. Devolve NaN sem contar como reset.
            return new CounterDeriverResult(Double.NaN, Flag.SPIKE_DOWN);
        }
        double resetValue = switch (onReset) {
            case UNKNOWN, CARRY -> Double.NaN;
            case ZERO -> 0.0;
        };
        return new CounterDeriverResult(resetValue, Flag.RESET);
    }

    private static CounterDeriverResult finalize(String formula, boolean clampNegativeToZero,
                                                 double delta, double deltaT, Flag flag) {
        double rate = FormulaEvaluator.evaluate(formula, Map.of("delta", delta, "deltaT", deltaT));
        if (clampNegativeToZero && rate < 0) {
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
        WRAP,
        /**
         * Queda negativa cujo magnitude é menor que
         * {@link dev.nishisan.utils.oss.definition.ResetPolicyDef#maxResetDeltaRatio()}.
         * Tratada como amostra inválida (NaN), <strong>não</strong> como reset.
         */
        SPIKE_DOWN
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
