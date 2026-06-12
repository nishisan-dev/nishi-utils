package dev.nishisan.utils.oss.migration;

import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.format.SeriesFileCodec;
import dev.nishisan.utils.oss.format.SeriesGeometry;
import dev.nishisan.utils.oss.format.SeriesHeader;
import dev.nishisan.utils.oss.format.SeriesLiveState;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeometryMigratorTest {

    private static final String ONE_COL = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata:
              name: migrate-test
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
              archives:
                rras:
                  - {name: rra_5m, stepSec: 300, rows: 4, cf: [AVERAGE], xff: 0.5}
            """;

    private static final String TWO_COL = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata:
              name: migrate-test
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
                - name: temperature
                  type: GAUGE
                  heartbeatSec: 900
              archives:
                rras:
                  - {name: rra_5m, stepSec: 300, rows: 4, cf: [AVERAGE], xff: 0.5}
            """;

    private static SeriesGeometry geometry(String yaml) {
        return new SeriesGeometry(NgrrdYamlLoader.parse(yaml, k -> null));
    }

    @Test
    void adicaoDeDataSourcePreservaHistoriaEContinuidade() {
        SeriesGeometry oldGeo = geometry(ONE_COL);
        byte[] oldImage = SeriesFileCodec.buildInitialImage(oldGeo, oldGeo.geometryHash(), 1);

        // Dados conhecidos no ring (archive 0, coluna 0) e estado vivo populado.
        ByteBuffer ob = ByteBuffer.wrap(oldImage).order(ByteOrder.BIG_ENDIAN);
        ob.putDouble((int) oldGeo.cellOffset(0, 0, 0), 11.0);
        ob.putDouble((int) oldGeo.cellOffset(0, 1, 0), 22.0);
        SeriesLiveState live = new SeriesLiveState(1, oldGeo.archiveCount());
        live.lastUpEpochMs = 1_000_000L;
        live.curRow[0] = 1;
        live.curRowEpochSec[0] = 1500L;
        live.counterPrevValue[0] = 7.0;
        live.counterPrevTsMs[0] = 999_000L;
        byte[] lb = SeriesFileCodec.encodeLiveState(oldGeo, live);
        System.arraycopy(lb, 0, oldImage, (int) oldGeo.liveStateOffset(), lb.length);

        SeriesGeometry newGeo = geometry(TWO_COL);
        byte[] newImage = GeometryMigrator.migrate(oldGeo, newGeo, oldImage, newGeo.geometryHash(), 2);

        SeriesHeader h = SeriesFileCodec.decodeFixedHeader(newImage);
        assertEquals(2, h.schemaRevision());
        assertArrayEquals(newGeo.geometryHash(), h.definitionHash());
        assertEquals((int) newGeo.fileTotalBytes(), newImage.length);

        int inBps = newGeo.columnIndex("in_bps");
        int temp = newGeo.columnIndex("temperature");
        ByteBuffer nb = ByteBuffer.wrap(newImage).order(ByteOrder.BIG_ENDIAN);
        // História da coluna preexistente preservada na nova posição de coluna.
        assertEquals(11.0, nb.getDouble((int) newGeo.cellOffset(0, 0, inBps)));
        assertEquals(22.0, nb.getDouble((int) newGeo.cellOffset(0, 1, inBps)));
        // Coluna nova começa em NaN.
        assertTrue(Double.isNaN(nb.getDouble((int) newGeo.cellOffset(0, 0, temp))));
        assertTrue(Double.isNaN(nb.getDouble((int) newGeo.cellOffset(0, 1, temp))));

        SeriesLiveState back = SeriesFileCodec.decodeLiveState(newGeo,
                Arrays.copyOfRange(newImage, (int) newGeo.liveStateOffset(),
                        (int) (newGeo.liveStateOffset() + newGeo.liveStateBytes())));
        assertEquals(1_000_000L, back.lastUpEpochMs);
        assertEquals(1, back.curRow[0]);
        assertEquals(1500L, back.curRowEpochSec[0]);
        assertEquals(7.0, back.counterPrevValue[inBps]);
        assertTrue(Double.isNaN(back.counterPrevValue[temp]));
    }

    @Test
    void mudancaDeBaseStepRejeitada() {
        SeriesGeometry oldGeo = geometry(ONE_COL);
        byte[] oldImage = SeriesFileCodec.buildInitialImage(oldGeo, oldGeo.geometryHash(), 1);
        SeriesGeometry newGeo = geometry(ONE_COL
                .replace("baseStepSec: 300", "baseStepSec: 600")
                .replace("stepSec: 300", "stepSec: 600"));
        assertThrows(IllegalStateException.class,
                () -> GeometryMigrator.migrate(oldGeo, newGeo, oldImage, newGeo.geometryHash(), 2));
    }

    @Test
    void reamostragemDeArchiveRejeitada() {
        SeriesGeometry oldGeo = geometry(ONE_COL);
        byte[] oldImage = SeriesFileCodec.buildInitialImage(oldGeo, oldGeo.geometryHash(), 1);
        SeriesGeometry newGeo = geometry(ONE_COL.replace("rows: 4", "rows: 8"));
        assertThrows(IllegalStateException.class,
                () -> GeometryMigrator.migrate(oldGeo, newGeo, oldImage, newGeo.geometryHash(), 2));
    }
}
