package dev.nishisan.utils.oss.migration;

import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.format.SeriesGeometry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeometryDiffTest {

    private static final String BASE = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata:
              name: diff-test
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

    // BASE + 1 GAUGE sem derive; archives e baseStep inalterados.
    private static final String WITH_GAUGE = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata:
              name: diff-test
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
    void adicaoDeDataSourceEhMigravelEstruturalmente() {
        GeometryDiff diff = GeometryDiff.between(geometry(BASE), geometry(WITH_GAUGE));
        assertTrue(diff.structurallyMigratable());
        assertFalse(diff.baseStepChanged());
        assertEquals(1, diff.addedColumns().size());
        assertEquals("temperature", diff.addedColumns().get(0).name());
        assertEquals(1, diff.keptColumns().size());
        assertTrue(diff.removedColumns().isEmpty());
        assertTrue(diff.resampledArchives().isEmpty());

        String report = GeometryChangeReport.render("device:r1/iface:eth0", diff, 1, 2,
                geometry(WITH_GAUGE).fileTotalBytes());
        assertTrue(report.contains("+ temperature"));
        assertTrue(report.contains("POSSÍVEL"));
        assertTrue(report.contains("schemaRevision: arquivo=1, definition=2"));
    }

    @Test
    void mudancaDeRowsExigeReamostragemENaoEhMigravel() {
        String moreRows = BASE.replace("rows: 4", "rows: 8");

        GeometryDiff diff = GeometryDiff.between(geometry(BASE), geometry(moreRows));
        assertFalse(diff.structurallyMigratable());
        assertEquals(1, diff.resampledArchives().size());
        assertEquals("rra_5m", diff.resampledArchives().get(0).rraName());

        String report = GeometryChangeReport.render("s", diff, 1, 2, 1024L);
        assertTrue(report.contains("não migrável"));
    }

    @Test
    void mudancaDeBaseStepNaoEhMigravel() {
        String biggerBase = BASE.replace("baseStepSec: 300", "baseStepSec: 600")
                .replace("stepSec: 300", "stepSec: 600");

        GeometryDiff diff = GeometryDiff.between(geometry(BASE), geometry(biggerBase));
        assertTrue(diff.baseStepChanged());
        assertFalse(diff.structurallyMigratable());
    }

    @Test
    void formatacaoDeBytesEstavel() {
        assertEquals("512 B", GeometryChangeReport.humanBytes(512));
        assertEquals("2.0 KB", GeometryChangeReport.humanBytes(2048));
        assertEquals("1.0 MB", GeometryChangeReport.humanBytes(1024 * 1024));
        assertEquals("11.0 GB", GeometryChangeReport.humanBytes(11L * 1024 * 1024 * 1024));
    }
}
