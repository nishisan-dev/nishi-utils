package dev.nishisan.utils.oss.engine;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.definition.RraDef;

import java.util.List;
import java.util.Optional;

/**
 * Escolhe o RRA mais adequado a uma {@link ViewQuery}.
 *
 * <p>Algoritmo:</p>
 * <ol>
 *     <li>Filtra RRAs cujo {@code cf} inclui a CF solicitada.</li>
 *     <li>Entre os candidatos, prefere o de maior {@code stepSec} que ainda seja
 *     {@code <= targetStepSec} — garante o menor número de pontos sem perder
 *     resolução abaixo do alvo.</li>
 *     <li>Se nenhum RRA tem {@code stepSec <= targetStepSec}, cai para o RRA
 *     de menor {@code stepSec} disponível.</li>
 *     <li>Em caso de empate em {@code stepSec}, prefere o que tem maior
 *     {@code rows × stepSec} (cobertura temporal).</li>
 * </ol>
 *
 * <p>A {@link ViewQuery#window()} influencia ranking via cobertura mas não filtra
 * candidatos — preferimos retornar resultado parcial a falhar sem dados.</p>
 */
public final class BestFitSelector {

    private BestFitSelector() {
    }

    public static Optional<RraDef> pick(ViewQuery query, List<RraDef> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return Optional.empty();
        }
        ConsolidationFunction wantedCf = query.cf();
        int target = query.targetStepSec();
        long windowSec = query.window().getSeconds();

        RraDef bestFit = null;
        RraDef smallest = null;
        for (RraDef rra : candidates) {
            if (!rra.cf().contains(wantedCf)) {
                continue;
            }
            if (smallest == null || rra.stepSec() < smallest.stepSec()
                    || (rra.stepSec() == smallest.stepSec() && coverage(rra) > coverage(smallest))) {
                smallest = rra;
            }
            if (rra.stepSec() <= target) {
                if (bestFit == null
                        || rra.stepSec() > bestFit.stepSec()
                        || (rra.stepSec() == bestFit.stepSec() && coverage(rra) > coverage(bestFit))) {
                    bestFit = rra;
                }
            }
        }
        // Preferimos o bestFit; se não cobrir a janela e houver outro de step
        // maior que cubra, ele vence — limitamos pelo target apenas se cobertura ok.
        if (bestFit != null && coverage(bestFit) >= windowSec) {
            return Optional.of(bestFit);
        }
        if (smallest != null) {
            // Procura um candidato com step >= target porém cobertura >= windowSec.
            RraDef coverer = null;
            for (RraDef rra : candidates) {
                if (!rra.cf().contains(wantedCf)) {
                    continue;
                }
                if (coverage(rra) >= windowSec
                        && (coverer == null || rra.stepSec() < coverer.stepSec())) {
                    coverer = rra;
                }
            }
            if (coverer != null) {
                return Optional.of(coverer);
            }
        }
        return Optional.ofNullable(bestFit != null ? bestFit : smallest);
    }

    private static long coverage(RraDef rra) {
        return (long) rra.rows() * rra.stepSec();
    }
}
