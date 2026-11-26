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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IdleTtlPolicyTest {

    private static final Duration GLOBAL = Duration.ofMinutes(15);

    @Test
    void passoMaiorQueMetadeDoTtlEstendeOTtl() {
        assertEquals(Duration.ofMinutes(30).toMillis(), IdleTtlPolicy.effectiveIdleTtlMs(GLOBAL, 2, 900));
    }

    @Test
    void passoCurtoMantemOTtlGlobal() {
        assertEquals(GLOBAL.toMillis(), IdleTtlPolicy.effectiveIdleTtlMs(GLOBAL, 2, 300));
        assertEquals(GLOBAL.toMillis(), IdleTtlPolicy.effectiveIdleTtlMs(GLOBAL, 2, 450));
    }

    @Test
    void fatorZeroOuPassoInvalidoUsaSoOTtlGlobal() {
        assertEquals(GLOBAL.toMillis(), IdleTtlPolicy.effectiveIdleTtlMs(GLOBAL, 0, 900));
        assertEquals(GLOBAL.toMillis(), IdleTtlPolicy.effectiveIdleTtlMs(GLOBAL, 2, 0));
        assertEquals(GLOBAL.toMillis(), IdleTtlPolicy.effectiveIdleTtlMs(GLOBAL, 2, -5));
    }

    @Test
    void produtoGrandeSaturaEmVezDeEstourar() {
        assertEquals(Long.MAX_VALUE, IdleTtlPolicy.effectiveIdleTtlMs(GLOBAL, Integer.MAX_VALUE, Integer.MAX_VALUE));
    }
}
