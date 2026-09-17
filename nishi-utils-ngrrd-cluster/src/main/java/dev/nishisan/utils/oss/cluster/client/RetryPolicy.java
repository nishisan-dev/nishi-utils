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

package dev.nishisan.utils.oss.cluster.client;

import java.time.Duration;
import java.util.Objects;

/**
 * Política de retentativa com backoff exponencial e teto, usada por
 * {@link PlacementResolver}, {@link WriteDispatcher} e {@code RemoteSeriesHandle}
 * para as esperas de líder/{@code MIGRATING}.
 *
 * @param timeout    prazo total desde o início da operação até desistir
 * @param backoffMin backoff da primeira retentativa
 * @param backoffMax teto do backoff exponencial
 */
public record RetryPolicy(Duration timeout, Duration backoffMin, Duration backoffMax) {

    public RetryPolicy {
        Objects.requireNonNull(timeout, "timeout é obrigatório");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("timeout deve ser > 0");
        }
        Objects.requireNonNull(backoffMin, "backoffMin é obrigatório");
        if (backoffMin.isNegative() || backoffMin.isZero()) {
            throw new IllegalArgumentException("backoffMin deve ser > 0");
        }
        Objects.requireNonNull(backoffMax, "backoffMax é obrigatório");
        if (backoffMax.compareTo(backoffMin) < 0) {
            throw new IllegalArgumentException("backoffMax deve ser >= backoffMin");
        }
    }

    /**
     * Backoff exponencial com teto para a {@code attempt}-ésima retentativa
     * (1-based): {@code backoffMin * 2^(attempt - 1)}, nunca acima de
     * {@code backoffMax}. {@code attempt <= 1} devolve {@code backoffMin}.
     */
    public Duration backoffFor(int attempt) {
        if (attempt <= 1) {
            return backoffMin;
        }
        // Cap do expoente para não estourar long em tentativas muito altas — qualquer
        // expoente que já exceda backoffMax satura no teto de qualquer forma.
        int shift = Math.min(attempt - 1, 62);
        long millis = backoffMin.toMillis();
        long factor = 1L << shift;
        long candidateMillis = millis > Long.MAX_VALUE / Math.max(factor, 1L)
                ? Long.MAX_VALUE
                : millis * factor;
        Duration candidate = Duration.ofMillis(candidateMillis);
        return candidate.compareTo(backoffMax) > 0 ? backoffMax : candidate;
    }

    /** Indica se {@code timeout} já se esgotou entre {@code startedAtMs} e {@code nowMs}. */
    public boolean exhausted(long startedAtMs, long nowMs) {
        return (nowMs - startedAtMs) >= timeout.toMillis();
    }
}
