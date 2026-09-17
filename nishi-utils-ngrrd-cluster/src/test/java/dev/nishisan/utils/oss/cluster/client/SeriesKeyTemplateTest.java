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

import java.io.InputStream;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Cobre {@link SeriesKeyTemplate} com os mesmos casos que
 * {@code Ngrrd.resolveSeriesKey} já cobre no {@code nishi-utils-oss}
 * (replicação intencional da semântica — ver Javadoc da classe).
 */
class SeriesKeyTemplateTest {

    @Test
    void resolveExpandeTemplateComMultiplasTags() {
        String key = SeriesKeyTemplate.resolve(
                "device:{deviceId}/iface:{interfaceId}",
                Map.of("deviceId", "r1", "interfaceId", "eth0"));

        assertEquals("device:r1/iface:eth0", key);
    }

    @Test
    void resolveTemplateSemPlaceholderDevolveLiteral() {
        assertEquals("serie-fixa", SeriesKeyTemplate.resolve("serie-fixa", Map.of()));
    }

    @Test
    void resolveLancaExcecaoQuandoTagObrigatoriaEstaAusente() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> SeriesKeyTemplate.resolve("device:{deviceId}", Map.of()));
        assertEquals("Tag obrigatória ausente para seriesKey: deviceId", ex.getMessage());
    }

    @Test
    void resolveLancaExcecaoQuandoPlaceholderNaoEstaFechado() {
        assertThrows(IllegalArgumentException.class,
                () -> SeriesKeyTemplate.resolve("device:{deviceId", Map.of("deviceId", "r1")));
    }

    @Test
    void templateOfLeATagDeIdentidadeDaDefinicaoYaml() throws Exception {
        String yaml;
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-blob.yaml")) {
            yaml = new String(in.readAllBytes());
        }

        assertEquals("device:{deviceId}/iface:{interfaceId}", SeriesKeyTemplate.templateOf(yaml));
    }
}
