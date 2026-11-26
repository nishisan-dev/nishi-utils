/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.oss.cluster.node;

import java.time.Duration;
import java.util.Objects;

/**
 * Política do TTL de ociosidade por série: {@code max(handleIdleTtl, stepFactor × baseStepSec)}.
 *
 * <p>Uma série escrita no ritmo do seu passo base fica ociosa por exatamente um passo entre duas escritas; com
 * o TTL global menor que o passo, ela seria fechada (checkpoint + close) logo antes da próxima escrita e
 * reaberta pela autocura, a cada passo. Este helper é o único ponto que calcula o TTL efetivo, para que todo
 * caminho que cria uma entrada aberta (abertura, reabertura pela autocura e, adiante, a reabertura pelo
 * journal) preencha o mesmo valor.</p>
 */
final class IdleTtlPolicy {

    private IdleTtlPolicy() {
    }

    /**
     * TTL de ociosidade efetivo de uma série: {@code max(handleIdleTtl, stepFactor × baseStepSec)}, em
     * milissegundos. Com {@code stepFactor == 0} ou {@code baseStepSec <= 0} vale só {@code handleIdleTtl}. O
     * produto satura em {@link Long#MAX_VALUE} em vez de estourar.
     *
     * @param handleIdleTtl TTL global ({@code ngrrd.handleIdleTtl})
     * @param stepFactor    fator {@code k} ({@code ngrrd.handleIdleTtlStepFactor}), &gt;= 0
     * @param baseStepSec   {@code spec.time.baseStepSec} da definição, em segundos
     */
    static long effectiveIdleTtlMs(Duration handleIdleTtl, int stepFactor, int baseStepSec) {
        Objects.requireNonNull(handleIdleTtl, "handleIdleTtl");
        long globalMs = handleIdleTtl.toMillis();
        if (stepFactor <= 0 || baseStepSec <= 0) {
            return globalMs;
        }
        long stepMs;
        try {
            stepMs = Math.multiplyExact(Math.multiplyExact((long) stepFactor, (long) baseStepSec), 1000L);
        } catch (ArithmeticException e) {
            stepMs = Long.MAX_VALUE;
        }
        return Math.max(globalMs, stepMs);
    }
}
