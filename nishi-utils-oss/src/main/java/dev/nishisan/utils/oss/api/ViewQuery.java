package dev.nishisan.utils.oss.api;

import java.time.Duration;
import java.util.Objects;

/**
 * Parâmetros de uma leitura.
 *
 * @param window         janela total a recuperar (ex.: {@code Duration.ofDays(1)})
 * @param targetStepSec  granularidade desejada (segundos)
 * @param cf             função de consolidação preferida
 * @param maxPoints      limite máximo de pontos retornados (downsample uniforme)
 */
public record ViewQuery(
        Duration window,
        int targetStepSec,
        ConsolidationFunction cf,
        int maxPoints
) {
    public ViewQuery {
        Objects.requireNonNull(window, "window é obrigatório");
        Objects.requireNonNull(cf, "cf é obrigatório");
        if (targetStepSec <= 0) {
            throw new IllegalArgumentException("targetStepSec deve ser > 0");
        }
        if (maxPoints <= 0) {
            throw new IllegalArgumentException("maxPoints deve ser > 0");
        }
    }
}
