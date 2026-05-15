package dev.nishisan.utils.oss.engine;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.definition.RraDef;

import java.util.List;

/**
 * Consolida uma sequência de PDPs (do step base) em um único CDP (do step do RRA),
 * respeitando o filtro de XFF.
 *
 * <p>Para um CDP cobrir {@code N = rra.stepSec / baseStepSec} PDPs:</p>
 * <ul>
 *     <li>se {@code missing / N > xff}, o CDP é {@code NaN};</li>
 *     <li>caso contrário, aplica-se a {@link ConsolidationFunction} sobre os PDPs
 *     consolidados (ignorando os {@code NaN} dentro do bucket).</li>
 * </ul>
 *
 * <p>Stateless por chamada — instâncias não retêm estado entre CDPs.</p>
 */
public final class RraConsolidator {

    private RraConsolidator() {
    }

    /**
     * Consolida uma janela de PDPs em um CDP.
     *
     * @param rra         definição do RRA (usado para xff)
     * @param cf          função de consolidação alvo (entre as listadas em
     *                    {@code rra.cf()})
     * @param pdpValues   valores já consolidados no step base, na ordem temporal
     * @param missingPdps quantos PDPs foram considerados {@code NaN} na janela
     * @return CDP resultante (NaN se XFF estourado ou todos PDPs ausentes)
     */
    public static double consolidate(RraDef rra,
                                     ConsolidationFunction cf,
                                     List<Double> pdpValues,
                                     int missingPdps) {
        if (pdpValues == null || pdpValues.isEmpty()) {
            return Double.NaN;
        }
        int total = pdpValues.size() + missingPdps;
        if (total <= 0) {
            return Double.NaN;
        }
        double missingRatio = (double) missingPdps / total;
        if (missingRatio > rra.xff()) {
            return Double.NaN;
        }
        return switch (cf) {
            case AVERAGE -> average(pdpValues);
            case MAX -> max(pdpValues);
            case MIN -> min(pdpValues);
            case LAST -> pdpValues.getLast();
        };
    }

    private static double average(List<Double> values) {
        double sum = 0.0;
        int count = 0;
        for (Double v : values) {
            if (!Double.isNaN(v)) {
                sum += v;
                count++;
            }
        }
        return count == 0 ? Double.NaN : sum / count;
    }

    private static double max(List<Double> values) {
        double max = Double.NEGATIVE_INFINITY;
        boolean any = false;
        for (Double v : values) {
            if (!Double.isNaN(v)) {
                any = true;
                if (v > max) {
                    max = v;
                }
            }
        }
        return any ? max : Double.NaN;
    }

    private static double min(List<Double> values) {
        double min = Double.POSITIVE_INFINITY;
        boolean any = false;
        for (Double v : values) {
            if (!Double.isNaN(v)) {
                any = true;
                if (v < min) {
                    min = v;
                }
            }
        }
        return any ? min : Double.NaN;
    }
}
