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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RetryPolicyTest {

    @Test
    void primeiraTentativaUsaBackoffMinimo() {
        RetryPolicy policy = new RetryPolicy(Duration.ofMinutes(1), Duration.ofMillis(100), Duration.ofSeconds(2));

        assertEquals(Duration.ofMillis(100), policy.backoffFor(1));
        assertEquals(Duration.ofMillis(100), policy.backoffFor(0));
    }

    @Test
    void backoffCresceExponencialmenteAteOTeto() {
        RetryPolicy policy = new RetryPolicy(Duration.ofMinutes(1), Duration.ofMillis(100), Duration.ofSeconds(2));

        assertEquals(Duration.ofMillis(100), policy.backoffFor(1));
        assertEquals(Duration.ofMillis(200), policy.backoffFor(2));
        assertEquals(Duration.ofMillis(400), policy.backoffFor(3));
        assertEquals(Duration.ofMillis(800), policy.backoffFor(4));
        // 100ms * 2^4 = 1600ms, ainda abaixo do teto de 2s.
        assertEquals(Duration.ofMillis(1600), policy.backoffFor(5));
        // 100ms * 2^5 = 3200ms, satura no teto.
        assertEquals(Duration.ofSeconds(2), policy.backoffFor(6));
        assertEquals(Duration.ofSeconds(2), policy.backoffFor(100));
    }

    @Test
    void exhaustedIndicaQuandoOPrazoTotalJaPassou() {
        RetryPolicy policy = new RetryPolicy(Duration.ofSeconds(10), Duration.ofMillis(100), Duration.ofSeconds(1));

        assertFalse(policy.exhausted(1_000L, 5_000L));
        assertTrue(policy.exhausted(1_000L, 11_000L));
        assertTrue(policy.exhausted(1_000L, 11_001L));
    }

    @Test
    void construtorRejeitaTimeoutNaoPositivo() {
        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicy(Duration.ZERO, Duration.ofMillis(100), Duration.ofSeconds(1)));
    }

    @Test
    void construtorRejeitaBackoffMaxMenorQueBackoffMin() {
        assertThrows(IllegalArgumentException.class,
                () -> new RetryPolicy(Duration.ofSeconds(10), Duration.ofSeconds(1), Duration.ofMillis(500)));
    }
}
