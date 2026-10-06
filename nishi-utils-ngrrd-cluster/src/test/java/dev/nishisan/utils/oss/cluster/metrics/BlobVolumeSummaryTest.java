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

package dev.nishisan.utils.oss.cluster.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec;
import dev.nishisan.utils.oss.blob.VolumeWriteMode;
import dev.nishisan.utils.oss.metrics.BlobVolumeStats;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * {@link BlobVolumeSummary} trafega em JSON entre nós/clientes de versões diferentes durante o rolling
 * deploy: o JSON de um nó anterior à 8.13.0 não traz {@code writeMode}/{@code bytesWritten}, e o de um
 * nó novo lido por um consumidor antigo traz campos que ele desconhece.
 */
class BlobVolumeSummaryTest {

    private final ObjectMapper transportMapper = JacksonMessageCodec.createDefaultMapper();

    @Test
    void fromPreencheModoEBytesEscritosDoStats() {
        BlobVolumeStats stats = new BlobVolumeStats(2, new long[]{1_000L, 1_000L}, new long[]{100L, 300L},
                new long[]{1L, 3L}, new double[]{0.1, 0.3}, 4, 512L, 64L, VolumeWriteMode.PWRITE, 4_096L);

        BlobVolumeSummary summary = BlobVolumeSummary.from(stats);

        assertEquals("PWRITE", summary.writeMode());
        assertEquals(4_096L, summary.bytesWritten());
        assertEquals(400L, summary.usedBytes());
        assertEquals(2_000L, summary.capacityBytes());
        assertEquals(0.3, summary.maxFillRatio());
    }

    @Test
    void roundTripPreservaModoEBytesEscritos() throws IOException {
        BlobVolumeSummary original = new BlobVolumeSummary(4, 10_485_760L, 104_857_600L, 0.42, 350, 8_192L,
                "PWRITE", 987_654L);

        for (ObjectMapper mapper : new ObjectMapper[]{transportMapper, new ObjectMapper()}) {
            BlobVolumeSummary roundTripped = mapper.readValue(mapper.writeValueAsString(original),
                    BlobVolumeSummary.class);
            assertEquals(original, roundTripped);
            assertEquals("PWRITE", roundTripped.writeMode());
            assertEquals(987_654L, roundTripped.bytesWritten());
        }
    }

    @Test
    void jsonDeNoAnteriorSemOsCamposNovosDesserializaComoDesconhecido() throws IOException {
        String legacy = "{\"shardCount\":4,\"usedBytes\":10,\"capacityBytes\":100,\"maxFillRatio\":0.1,"
                + "\"catalogEntryCount\":7,\"walBytes\":32}";

        for (ObjectMapper mapper : new ObjectMapper[]{transportMapper, new ObjectMapper()}) {
            BlobVolumeSummary summary = mapper.readValue(legacy, BlobVolumeSummary.class);
            assertNull(summary.writeMode(), "modo desconhecido (nó anterior à 8.13.0)");
            assertEquals(0L, summary.bytesWritten());
            assertEquals(4, summary.shardCount());
            assertEquals(7, summary.catalogEntryCount());
        }
    }

    @Test
    void jsonComCamposDesconhecidosETolerado() throws IOException {
        String future = "{\"shardCount\":4,\"usedBytes\":10,\"capacityBytes\":100,\"maxFillRatio\":0.1,"
                + "\"catalogEntryCount\":7,\"walBytes\":32,\"writeMode\":\"MMAP\",\"bytesWritten\":5,"
                + "\"campoDeUmaVersaoFutura\":{\"x\":1}}";

        // inclusive com um ObjectMapper estrito (default do Jackson): a anotação do record dispensa a config do mapper
        for (ObjectMapper mapper : new ObjectMapper[]{transportMapper, new ObjectMapper()}) {
            BlobVolumeSummary summary = mapper.readValue(future, BlobVolumeSummary.class);
            assertEquals("MMAP", summary.writeMode());
            assertEquals(5L, summary.bytesWritten());
        }
    }

    @Test
    void construtorDeCompatibilidadeDaForma8_12_0MarcaModoDesconhecido() {
        BlobVolumeSummary summary = new BlobVolumeSummary(1, 100L, 1_000L, 0.1, 5, 0L);

        assertNull(summary.writeMode());
        assertEquals(0L, summary.bytesWritten());
    }
}
