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

package dev.nishisan.utils.oss.cluster.protocol;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.ViewQuery;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Cobre a validação do construtor compacto de {@link ReadRequest}, inclusive o
 * ponto em que ela é mais estrita que {@link ViewQuery}: {@code windowMs} zero
 * é rejeitado aqui, mas sobrevive ao construtor compacto de {@link ViewQuery}.
 */
class ReadRequestTest {

    private static ReadRequest request(long windowMs, int targetStepSec, int maxPoints) {
        return new ReadRequest("series-1", "in_octets", windowMs, targetStepSec,
                ConsolidationFunction.AVERAGE, maxPoints, null);
    }

    @Test
    void aceitaValoresPositivosEmTodosOsCampos() {
        assertDoesNotThrow(() -> request(60_000L, 60, 500));
    }

    @Test
    void rejeitaWindowMsZero() {
        assertThrows(IllegalArgumentException.class, () -> request(0L, 60, 500));
    }

    @Test
    void rejeitaWindowMsNegativo() {
        assertThrows(IllegalArgumentException.class, () -> request(-1L, 60, 500));
    }

    @Test
    void rejeitaTargetStepSecZero() {
        assertThrows(IllegalArgumentException.class, () -> request(60_000L, 0, 500));
    }

    @Test
    void rejeitaTargetStepSecNegativo() {
        assertThrows(IllegalArgumentException.class, () -> request(60_000L, -30, 500));
    }

    @Test
    void rejeitaMaxPointsZero() {
        assertThrows(IllegalArgumentException.class, () -> request(60_000L, 60, 0));
    }

    @Test
    void rejeitaMaxPointsNegativo() {
        assertThrows(IllegalArgumentException.class, () -> request(60_000L, 60, -10));
    }

    @Test
    void rejeitaSeriesKeyNulo() {
        assertThrows(NullPointerException.class,
                () -> new ReadRequest(null, "in_octets", 60_000L, 60, ConsolidationFunction.AVERAGE, 500, null));
    }

    @Test
    void rejeitaDsNameNulo() {
        assertThrows(NullPointerException.class,
                () -> new ReadRequest("series-1", null, 60_000L, 60, ConsolidationFunction.AVERAGE, 500, null));
    }

    @Test
    void rejeitaCfNulo() {
        assertThrows(NullPointerException.class,
                () -> new ReadRequest("series-1", "in_octets", 60_000L, 60, null, 500, null));
    }

    @Test
    void viewQueryToleraJanelaZeroDiferentementeDeReadRequest() {
        // ViewQuery não valida o próprio window além de não-nulo: janela zero passa por lá,
        // mas é rejeitada por ReadRequest (validação deliberadamente mais estrita).
        assertDoesNotThrow(() -> new ViewQuery(Duration.ZERO, 60, ConsolidationFunction.AVERAGE, 500));
        assertThrows(IllegalArgumentException.class, () -> request(0L, 60, 500));
    }

    @Test
    void toViewQueryReconstroiOsMesmosCampos() {
        ReadRequest req = request(3_600_000L, 30, 200);
        ViewQuery query = req.toViewQuery();
        assertEquals(Duration.ofMillis(3_600_000L), query.window());
        assertEquals(30, query.targetStepSec());
        assertEquals(ConsolidationFunction.AVERAGE, query.cf());
        assertEquals(200, query.maxPoints());
    }
}
