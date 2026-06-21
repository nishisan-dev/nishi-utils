package dev.nishisan.utils.oss.config;

import dev.nishisan.utils.oss.definition.DataSourceDef;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NgrrdDefinitionValidatorTest {

    @Test
    void aceitaDefinicaoDoEnunciado() {
        NgrrdDefinition def = loadResource("/iface-traffic-errors-v1.yaml");
        assertDoesNotThrow(() -> NgrrdDefinitionValidator.validate(def));
    }

    @Test
    void rejeitaApiVersionDesconhecida() {
        NgrrdDefinition def = parse(baseYaml().replace("apiVersion: ngrrd/v1", "apiVersion: foo/v1"));
        NgrrdDefinitionException ex = assertThrows(NgrrdDefinitionException.class,
                () -> NgrrdDefinitionValidator.validate(def));
        assertTrue(ex.getMessage().contains("apiVersion"));
    }

    @Test
    void rejeitaRraStepNaoMultiplo() {
        NgrrdDefinition def = parse(baseYaml()
                .replace("stepSec: 60\n", "stepSec: 75\n"));
        NgrrdDefinitionException ex = assertThrows(NgrrdDefinitionException.class,
                () -> NgrrdDefinitionValidator.validate(def));
        assertTrue(ex.getMessage().contains("stepSec"));
    }

    @Test
    void rejeitaXffForaDoIntervalo() {
        NgrrdDefinition def = parse(baseYaml()
                .replace("xff: 0.5", "xff: 1.2"));
        NgrrdDefinitionException ex = assertThrows(NgrrdDefinitionException.class,
                () -> NgrrdDefinitionValidator.validate(def));
        assertTrue(ex.getMessage().contains("xff"));
    }

    @Test
    void rejeitaIncludeReferenciandoDsInexistente() {
        NgrrdDefinition def = parse(baseYaml()
                .replace("include: [cpu]", "include: [cpu, foo]"));
        NgrrdDefinitionException ex = assertThrows(NgrrdDefinitionException.class,
                () -> NgrrdDefinitionValidator.validate(def));
        assertTrue(ex.getMessage().contains("foo"));
    }

    @Test
    void rejeitaPresetSemRraResolvivel() {
        String yamlWithPreset = baseYaml().replace(
                "    presets: []",
                "    presets:\n"
                        + "      - name: tiny\n"
                        + "        window: PT1H\n"
                        + "        targetStepSec: 30\n"
                        + "        cf: AVERAGE\n"
                        + "        maxPoints: 50\n"
                        + "        series: [cpu]");
        NgrrdDefinition def = parse(yamlWithPreset);
        NgrrdDefinitionException ex = assertThrows(NgrrdDefinitionException.class,
                () -> NgrrdDefinitionValidator.validate(def));
        assertTrue(ex.getMessage().contains("tiny"));
    }

    @Test
    void rejeitaWindowComMesOuAno() {
        // P1M e P1Y são Period (meses/anos), não Duration — não conseguem ser
        // interpretados consistentemente em segundos pelo ViewExecutor em runtime.
        String yamlWithMonthlyWindow = baseYaml().replace(
                "    presets: []",
                "    presets:\n"
                        + "      - name: monthly\n"
                        + "        window: P1M\n"
                        + "        targetStepSec: 60\n"
                        + "        cf: AVERAGE\n"
                        + "        maxPoints: 50\n"
                        + "        series: [cpu]");
        NgrrdDefinition def = parse(yamlWithMonthlyWindow);
        NgrrdDefinitionException ex = assertThrows(NgrrdDefinitionException.class,
                () -> NgrrdDefinitionValidator.validate(def));
        assertTrue(ex.getMessage().contains("preset.window inválido"),
                "mensagem esperada mencionando preset.window, obteve: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("P1M"));
    }

    @Test
    void rejeitaWindowDeAno() {
        String yaml = baseYaml().replace(
                "    presets: []",
                "    presets:\n"
                        + "      - name: yearly\n"
                        + "        window: P1Y\n"
                        + "        targetStepSec: 60\n"
                        + "        cf: AVERAGE\n"
                        + "        maxPoints: 50\n"
                        + "        series: [cpu]");
        NgrrdDefinition def = parse(yaml);
        assertThrows(NgrrdDefinitionException.class,
                () -> NgrrdDefinitionValidator.validate(def));
    }

    @Test
    void aceitaWindowEmDiasMesmoQueLonga() {
        // P365D é Duration válida (365 dias em segundos), diferente de P1Y.
        String yaml = baseYaml().replace(
                "    presets: []",
                "    presets:\n"
                        + "      - name: yearly\n"
                        + "        window: P365D\n"
                        + "        targetStepSec: 60\n"
                        + "        cf: AVERAGE\n"
                        + "        maxPoints: 50\n"
                        + "        series: [cpu]");
        NgrrdDefinition def = parse(yaml);
        assertDoesNotThrow(() -> NgrrdDefinitionValidator.validate(def));
    }

    @Test
    void rejeitaFormulaInvalida() {
        String yaml = """
                apiVersion: ngrrd/v1
                kind: MetricSeriesDefinition
                metadata:
                  name: bad-formula
                spec:
                  time:
                    baseStepSec: 60
                    blockSizeSec: 3600
                  dataSources:
                    - name: bytes
                      type: COUNTER
                      counterBits: 64
                      heartbeatSec: 120
                      resetPolicy:
                        detectCounterReset: true
                        maxResetDeltaRatio: 0.9
                      derive:
                        output:
                          name: rate
                          unit: "B/s"
                          formula: "delta * 8 ; rm -rf /"
                          clampNegativeToZero: true
                          onReset: unknown
                          onWrap: auto
                  archives:
                    rras:
                      - name: r
                        stepSec: 60
                        rows: 60
                        cf: [AVERAGE]
                        xff: 0.5
                """;
        NgrrdDefinition def = NgrrdYamlLoader.parse(yaml, n -> null);
        NgrrdDefinitionException ex = assertThrows(NgrrdDefinitionException.class,
                () -> NgrrdDefinitionValidator.validate(def));
        assertTrue(ex.getMessage().contains("formula"));
    }

    @Test
    void rejeitaTemplateComTagNaoDeclarada() {
        String yamlWithIdentity = baseYaml().replace(
                "  dataSources:",
                "  identity:\n"
                        + "    seriesKeyTemplate: \"host:{hostName}\"\n"
                        + "    tags:\n"
                        + "      - name: deviceId\n"
                        + "  dataSources:");
        NgrrdDefinition def = parse(yamlWithIdentity);
        NgrrdDefinitionException ex = assertThrows(NgrrdDefinitionException.class,
                () -> NgrrdDefinitionValidator.validate(def));
        assertTrue(ex.getMessage().contains("hostName"));
    }

    @Test
    void rejeitaDictionarioArquivadoApenasComAverage() {
        // cpu vira DS de estado (tem dictionary), mas as RRAs só consolidam por
        // AVERAGE — média de estados (1/0) é lixo. O validator deve barrar.
        String yaml = baseYaml().replace(
                "      type: GAUGE\n      heartbeatSec: 120\n",
                "      type: GAUGE\n      heartbeatSec: 120\n"
                        + "      dictionary:\n        1: up\n        2: down\n");
        NgrrdDefinition def = parse(yaml);
        NgrrdDefinitionException ex = assertThrows(NgrrdDefinitionException.class,
                () -> NgrrdDefinitionValidator.validate(def));
        assertTrue(ex.getMessage().contains("cpu"),
                "mensagem deve citar o DS de estado, obteve: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("AVERAGE"),
                "mensagem deve citar AVERAGE, obteve: " + ex.getMessage());
    }

    @Test
    void aceitaDictionarioComRraNaoAverage() {
        // Mesma cpu com dictionary, mas agora há um CF não-AVERAGE (LAST) cobrindo-a.
        String yaml = baseYaml()
                .replace(
                        "      type: GAUGE\n      heartbeatSec: 120\n",
                        "      type: GAUGE\n      heartbeatSec: 120\n"
                                + "      dictionary:\n        1: up\n        2: down\n")
                .replace("cf: [AVERAGE]", "cf: [AVERAGE, LAST]");
        NgrrdDefinition def = parse(yaml);
        assertDoesNotThrow(() -> NgrrdDefinitionValidator.validate(def));
    }

    @Test
    void rejeitaDictionarioEmExcludeArquivadoApenasComAverage() {
        // appliesTo.exclude NAO e aplicado na geometria de escrita
        // (SeriesGeometry.columnsOf so honra include), entao um DS de estado
        // listado em exclude CONTINUA arquivado. O guard deve barra-lo mesmo
        // assim — paridade com a escrita; senao o estado seria gravado com AVERAGE.
        String yaml = baseYaml()
                .replace(
                        "      type: GAUGE\n      heartbeatSec: 120\n",
                        "      type: GAUGE\n      heartbeatSec: 120\n"
                                + "      dictionary:\n        1: up\n        2: down\n")
                .replace("      include: [cpu]\n", "      include: [cpu]\n      exclude: [cpu]\n");
        NgrrdDefinition def = parse(yaml);
        NgrrdDefinitionException ex = assertThrows(NgrrdDefinitionException.class,
                () -> NgrrdDefinitionValidator.validate(def));
        assertTrue(ex.getMessage().contains("cpu"),
                "mensagem deve citar o DS de estado, obteve: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("AVERAGE"),
                "mensagem deve citar AVERAGE, obteve: " + ex.getMessage());
    }

    @Test
    void dicionarioEhLidoDoYaml() {
        String yaml = """
                apiVersion: ngrrd/v1
                kind: MetricSeriesDefinition
                metadata:
                  name: with-dictionary
                spec:
                  time:
                    baseStepSec: 60
                    blockSizeSec: 3600
                  dataSources:
                    - name: oper_status
                      type: GAUGE
                      heartbeatSec: 120
                      dictionary:
                        1: up
                        2: down
                  archives:
                    rras:
                      - name: r
                        stepSec: 60
                        rows: 60
                        cf: [LAST]
                        xff: 0.5
                """;
        NgrrdDefinition def = parse(yaml);
        DataSourceDef ds = def.spec().dataSources().get(0);
        assertEquals(Map.of(1, "up", 2, "down"), ds.dictionary());
    }

    private static String baseYaml() {
        return """
                apiVersion: ngrrd/v1
                kind: MetricSeriesDefinition
                metadata:
                  name: minimal
                spec:
                  time:
                    baseStepSec: 60
                    blockSizeSec: 3600
                  dataSources:
                    - name: cpu
                      type: GAUGE
                      heartbeatSec: 120
                  archives:
                    appliesTo:
                      include: [cpu]
                    rras:
                      - name: rra_1m
                        stepSec: 60
                        rows: 60
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
    }

    private static NgrrdDefinition parse(String yaml) {
        return NgrrdYamlLoader.parse(yaml, n -> null);
    }

    private static NgrrdDefinition loadResource(String path) {
        try (InputStream is = NgrrdDefinitionValidatorTest.class.getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalStateException("resource não encontrado: " + path);
            }
            return NgrrdYamlLoader.load(is, n -> null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
