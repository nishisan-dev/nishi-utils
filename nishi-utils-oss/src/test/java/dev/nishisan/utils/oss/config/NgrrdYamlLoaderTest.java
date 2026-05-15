package dev.nishisan.utils.oss.config;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataSourceType;
import dev.nishisan.utils.oss.api.FallbackSource;
import dev.nishisan.utils.oss.api.LateSampleAction;
import dev.nishisan.utils.oss.api.ResetBehavior;
import dev.nishisan.utils.oss.api.SelectionStrategy;
import dev.nishisan.utils.oss.api.StorageBackendType;
import dev.nishisan.utils.oss.api.TimestampAlignment;
import dev.nishisan.utils.oss.api.WrapBehavior;
import dev.nishisan.utils.oss.definition.DataSourceDef;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.definition.PresetDef;
import dev.nishisan.utils.oss.definition.RraDef;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NgrrdYamlLoaderTest {

    @Test
    void carregaDefinicaoCompletaDoEnunciado() {
        NgrrdDefinition def = loadResource("/iface-traffic-errors-v1.yaml");

        assertEquals("ngrrd/v1", def.apiVersion());
        assertEquals("MetricSeriesDefinition", def.kind());
        assertEquals("iface-traffic-errors-v1", def.metadata().name());

        assertEquals(300, def.spec().time().baseStepSec());
        assertEquals(21600, def.spec().time().blockSizeSec());
        assertEquals(TimestampAlignment.EPOCH, def.spec().time().timestampAlignment());
        assertEquals(LateSampleAction.BUCKET_IF_POSSIBLE, def.spec().time().lateSamplePolicy().onLate());
        assertEquals("NaN", def.spec().time().missingValue());

        assertEquals(5, def.spec().identity().tags().size());
        assertEquals("device:{deviceId}/iface:{interfaceId}", def.spec().identity().seriesKeyTemplate());

        assertEquals(4, def.spec().dataSources().size());
        DataSourceDef inOctets = def.spec().dataSources().getFirst();
        assertEquals("in_octets", inOctets.name());
        assertEquals(DataSourceType.COUNTER, inOctets.type());
        assertEquals(64, inOctets.counterBits());
        assertEquals(900, inOctets.heartbeatSec());
        assertEquals(0.0, inOctets.min());
        assertNull(inOctets.max());
        assertTrue(inOctets.resetPolicy().detectCounterReset());
        assertEquals(0.90, inOctets.resetPolicy().maxResetDeltaRatio(), 1e-9);
        assertEquals("in_bps", inOctets.derive().output().name());
        assertEquals("delta * 8 / deltaT", inOctets.derive().output().formula());
        assertEquals(ResetBehavior.UNKNOWN, inOctets.derive().output().onReset());
        assertEquals(WrapBehavior.AUTO, inOctets.derive().output().onWrap());

        assertEquals(3, def.spec().archives().rras().size());
        RraDef firstRra = def.spec().archives().rras().getFirst();
        assertEquals("rra_5m_30d", firstRra.name());
        assertEquals(300, firstRra.stepSec());
        assertEquals(8640, firstRra.rows());
        assertEquals(0.50, firstRra.xff(), 1e-9);
        assertEquals(2, firstRra.cf().size());
        assertEquals(ConsolidationFunction.AVERAGE, firstRra.cf().getFirst());

        assertEquals(SelectionStrategy.BEST_FIT, def.spec().views().selection().strategy());
        assertEquals(2000, def.spec().views().selection().maxPointsDefault());
        assertEquals(FallbackSource.RAW, def.spec().views().selection().fallbackOrder().getFirst());
        assertEquals(FallbackSource.AGG, def.spec().views().selection().fallbackOrder().get(1));

        PresetDef daily = def.spec().views().presets().stream()
                .filter(p -> p.name().equals("daily"))
                .findFirst()
                .orElseThrow();
        assertEquals("P1D", daily.window());
        assertEquals(300, daily.targetStepSec());
        assertEquals(ConsolidationFunction.AVERAGE, daily.cf());
        assertEquals(400, daily.maxPoints());

        assertEquals(StorageBackendType.OBJECT_STORAGE, def.spec().storage().backend());
        assertEquals("raw", def.spec().storage().objectNaming().rawPrefix());
        assertEquals("{seriesKey}/{ds}/{stepSec}/{blockStartEpoch}",
                def.spec().storage().writePolicy().idempotency().key());
        assertEquals(900, def.spec().storage().manifestPolicy().intervalSec());

        assertEquals(5, def.spec().quality().emitMetrics().size());
        assertTrue(def.spec().quality().emitMetrics().contains("counter_reset_count"));
    }

    @Test
    void aplicaInterpolacaoDeVariaveisAntesDoParse() {
        String yaml = """
                apiVersion: ngrrd/v1
                kind: MetricSeriesDefinition
                metadata:
                  name: ${SERIES_NAME:default-series}
                spec:
                  time:
                    baseStepSec: 60
                    blockSizeSec: 3600
                    timestampAlignment: epoch
                    lateSamplePolicy:
                      maxLatenessSec: 60
                      onLate: bucket_if_possible
                    missingValue: NaN
                  dataSources:
                    - name: cpu
                      type: GAUGE
                      heartbeatSec: 120
                  archives:
                    appliesTo:
                      include: [cpu]
                    rras:
                      - name: rra_1m_1d
                        stepSec: 60
                        rows: 1440
                        cf: [AVERAGE]
                        xff: 0.5
                  views:
                    selection:
                      strategy: best_fit
                      maxPointsDefault: 1000
                      fallbackOrder: [raw, agg]
                    presets: []
                  storage:
                    backend: localDisk
                    objectNaming:
                      scheme: deterministic
                      rawPrefix: raw
                      aggPrefix: agg
                      manifestPrefix: manifest
                      schemaPrefix: schema
                    writePolicy:
                      mode: append_only
                      idempotency:
                        key: ""
                        onConflict: verify_or_replace_if_identical
                    manifestPolicy:
                      updateMode: periodic_snapshot
                      intervalSec: 60
                  quality:
                    emitMetrics: []
                """;
        NgrrdDefinition def = NgrrdYamlLoader.parse(yaml, name -> "from-env-" + name);
        assertNotNull(def);
        assertEquals("from-env-SERIES_NAME", def.metadata().name());
    }

    private static NgrrdDefinition loadResource(String path) {
        try (InputStream is = NgrrdYamlLoaderTest.class.getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalStateException("resource não encontrado: " + path);
            }
            return NgrrdYamlLoader.load(is, n -> null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
