package dev.nishisan.utils.oss.format;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.engine.PrimaryDataPoint;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre a geometria e o codec do arquivo de série único (NGRR): offsets
 * determinísticos, round-trip da seção estática e da live-state, integridade por
 * CRC e endereçamento das células de ring.
 */
class SeriesFileCodecTest {

    private static final String YAML = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata:
              name: codec-test
            spec:
              time:
                baseStepSec: 300
              dataSources:
                - name: in_octets
                  type: COUNTER
                  counterBits: 64
                  heartbeatSec: 900
                  derive:
                    output: {name: in_bps, formula: "delta * 8 / deltaT"}
                - name: out_octets
                  type: COUNTER
                  counterBits: 64
                  heartbeatSec: 900
                  derive:
                    output: {name: out_bps, formula: "delta * 8 / deltaT"}
              archives:
                appliesTo: {include: [in_bps, out_bps]}
                rras:
                  - {name: rra_5m, stepSec: 300, rows: 4, cf: [AVERAGE, MAX], xff: 0.5}
            """;

    private SeriesGeometry geometry() {
        NgrrdDefinition def = NgrrdYamlLoader.parse(YAML, k -> null);
        return new SeriesGeometry(def);
    }

    @Test
    void geometriaCalculaColunasArchivesEOffsets() {
        SeriesGeometry geo = geometry();
        assertEquals(300, geo.baseStepSec());
        assertEquals(2, geo.columnCount());
        assertEquals(2, geo.archiveCount());
        assertEquals(0, geo.columnIndex("in_bps"));
        assertEquals(1, geo.columnIndex("out_bps"));
        assertEquals(-1, geo.columnIndex("nope"));
        assertEquals(0, geo.archiveIndex("rra_5m", ConsolidationFunction.AVERAGE));
        assertEquals(1, geo.archiveIndex("rra_5m", ConsolidationFunction.MAX));
        assertEquals(-1, geo.archiveIndex("rra_5m", ConsolidationFunction.MIN));

        assertEquals(0L, geo.liveStateOffset() % 8, "live-state deve ser 8-align");
        assertEquals(0L, geo.ringDataOffset() % 8, "ring-data deve ser 8-align");
        assertTrue(geo.ringDataOffset() >= geo.liveStateOffset() + geo.liveStateBytes());
        assertEquals(SeriesGeometry.liveStateBytes(2, 2), geo.liveStateBytes());

        // 2 archives × 4 rows × 2 cols × 8 bytes = 128 bytes de ring.
        assertEquals(geo.ringDataOffset() + 128L, geo.fileTotalBytes());
        assertEquals(geo.ringDataOffset(), geo.cellOffset(0, 0, 0));
        assertEquals(geo.ringDataOffset() + 8L, geo.cellOffset(0, 0, 1));
        assertEquals(geo.ringDataOffset() + 16L, geo.cellOffset(0, 1, 0));
        assertEquals(geo.ringDataOffset() + 64L, geo.cellOffset(1, 0, 0));
    }

    @Test
    void imagemInicialDecodificaCabecalhoELiveStateVazia() {
        SeriesGeometry geo = geometry();
        byte[] hash = new byte[DefinitionHash.BYTES];
        Arrays.fill(hash, (byte) 0xAB);

        byte[] image = SeriesFileCodec.buildInitialImage(geo, hash);
        assertEquals((int) geo.fileTotalBytes(), image.length);

        SeriesHeader header = SeriesFileCodec.decodeFixedHeader(image);
        assertEquals(SeriesFileCodec.CURRENT_VERSION, header.formatVersion());
        assertEquals(300, header.baseStepSec());
        assertEquals(2, header.columnCount());
        assertEquals(2, header.archiveCount());
        assertArrayEquals(hash, header.definitionHash());
        assertEquals(geo.liveStateOffset(), header.liveStateOffset());
        assertEquals(geo.fileTotalBytes(), header.fileTotalBytes());

        byte[] liveBytes = Arrays.copyOfRange(image, (int) geo.liveStateOffset(),
                (int) (geo.liveStateOffset() + geo.liveStateBytes()));
        SeriesLiveState st = SeriesFileCodec.decodeLiveState(geo, liveBytes);
        assertEquals(0L, st.lastUpEpochMs);
        assertTrue(Double.isNaN(st.counterPrevValue[0]));
        assertEquals(-1, st.curRow[0]);
        assertEquals(-1L, st.curRowEpochSec[1]);
        assertEquals(-1L, st.pdpSlotSec[0]);

        // Toda célula de ring começa NaN.
        double firstCell = SeriesFileCodec.readDouble(
                Arrays.copyOfRange(image, (int) geo.cellOffset(0, 0, 0),
                        (int) geo.cellOffset(0, 0, 0) + 8));
        assertTrue(Double.isNaN(firstCell));
    }

    @Test
    void liveStateRoundTrip() {
        SeriesGeometry geo = geometry();
        SeriesLiveState st = new SeriesLiveState(2, 2);
        st.lastUpEpochMs = 1_747_339_200_000L;
        st.counterPrevValue[0] = 1_000_000.0;
        st.counterPrevTsMs[0] = 1_747_339_200_000L;
        st.pdp[0].add(10.0);
        st.pdp[0].add(20.0);
        st.pdpSlotSec[0] = 1_747_339_200L;
        st.curRow[0] = 2;
        st.curRowEpochSec[0] = 1_747_339_800L;
        int idx = st.cdpIndex(1, 1);
        st.cdpPartial[idx] = 42.0;
        st.cdpFolded[idx] = 3;
        st.cdpMissing[idx] = 1;

        byte[] encoded = SeriesFileCodec.encodeLiveState(geo, st);
        assertEquals((int) geo.liveStateBytes(), encoded.length);
        SeriesLiveState back = SeriesFileCodec.decodeLiveState(geo, encoded);

        assertEquals(1_747_339_200_000L, back.lastUpEpochMs);
        assertEquals(1_000_000.0, back.counterPrevValue[0]);
        assertEquals(1_747_339_200_000L, back.counterPrevTsMs[0]);
        assertEquals(15.0, back.pdp[0].consolidate(ConsolidationFunction.AVERAGE));
        assertEquals(1_747_339_200L, back.pdpSlotSec[0]);
        assertEquals(2, back.curRow[0]);
        assertEquals(1_747_339_800L, back.curRowEpochSec[0]);
        assertEquals(42.0, back.cdpPartial[idx]);
        assertEquals(3, back.cdpFolded[idx]);
        assertEquals(1, back.cdpMissing[idx]);
    }

    @Test
    void crcDeLiveStateCorrompidoLancaExcecao() {
        SeriesGeometry geo = geometry();
        byte[] encoded = SeriesFileCodec.encodeLiveState(geo, new SeriesLiveState(2, 2));
        encoded[0] ^= 0x7F; // corrompe o byte de lastUpEpochMs
        assertThrows(NgrrdFormatException.class, () -> SeriesFileCodec.decodeLiveState(geo, encoded));
    }

    @Test
    void crcDeCabecalhoCorrompidoLancaExcecao() {
        SeriesGeometry geo = geometry();
        byte[] hash = new byte[DefinitionHash.BYTES];
        byte[] image = SeriesFileCodec.buildInitialImage(geo, hash);
        image[8] ^= 0x7F; // corrompe baseStepSec dentro da área coberta pelo CRC
        assertThrows(NgrrdFormatException.class, () -> SeriesFileCodec.decodeFixedHeader(image));
    }

    @Test
    void mementoDePdpReconstroiConsolidacao() {
        PrimaryDataPoint pdp = new PrimaryDataPoint();
        pdp.add(5.0);
        pdp.add(15.0);
        PrimaryDataPoint restored = PrimaryDataPoint.restore(pdp.snapshot());
        assertEquals(10.0, restored.consolidate(ConsolidationFunction.AVERAGE));
        assertEquals(15.0, restored.consolidate(ConsolidationFunction.MAX));
    }
}
