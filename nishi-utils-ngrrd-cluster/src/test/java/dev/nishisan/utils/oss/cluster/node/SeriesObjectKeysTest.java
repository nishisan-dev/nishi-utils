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

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Cobre {@link SeriesObjectKeys} — BAIXO-D do Refuter (M4, rodada 3): normalização de barras no prefixo. */
class SeriesObjectKeysTest {

    @Test
    void prefixoComOuSemBarraProduzAMesmaChaveFisica() {
        assertEquals(SeriesObjectKeys.objectKey("series", "s1"), SeriesObjectKeys.objectKey("series/", "s1"));
        assertEquals(SeriesObjectKeys.objectKey("series", "s1"), SeriesObjectKeys.objectKey("/series/", "s1"));
        assertEquals("series/s1.ngrr", SeriesObjectKeys.objectKey("series", "s1"));
        assertEquals("series/s1.ngrr", SeriesObjectKeys.objectKey("series/", "s1"));
    }

    @Test
    void prefixWithSlashNormalizaAntesDeAnexarABarra() {
        assertEquals("series/", SeriesObjectKeys.prefixWithSlash("series"));
        assertEquals("series/", SeriesObjectKeys.prefixWithSlash("series/"));
        assertEquals("series/", SeriesObjectKeys.prefixWithSlash("/series/"));
    }

    @Test
    void seriesKeyOfFuncionaIgualComOuSemBarraNoPrefix() {
        String objectKey = "series/s1.ngrr";

        Optional<String> viaSemBarra = SeriesObjectKeys.seriesKeyOf(objectKey, "series");
        Optional<String> viaComBarra = SeriesObjectKeys.seriesKeyOf(objectKey, "series/");

        assertEquals(Optional.of("s1"), viaSemBarra);
        assertEquals(viaSemBarra, viaComBarra);
    }
}
