package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataPoint;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.storage.StorageFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contrato da redução de pontos na leitura (o antigo {@code downsample}).
 *
 * <p>A implementação original amostrava um índice por bucket
 * ({@code floor(i*n/maxPoints)}) e descartava o resto, o que jogava fora as
 * {@code ceil(n/maxPoints)-1} amostras MAIS RECENTES do range. Numa série de
 * estado isso apagava uma queda recém-ocorrida; num contador, subestimava o
 * pico em janelas longas. Estes testes fixam o comportamento agregado.</p>
 */
class NgrrdReadReductionTest {

    private static final int STEP_SEC = 300;
    private static final long T0_SEC = 1_700_000_400L; // alinhado a 300s
    private static final int AMOSTRAS = 300;

    private static final String YAML = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata:
              name: read-reduction
            spec:
              time:
                baseStepSec: 300
              identity:
                seriesKeyTemplate: "sensor:{id}"
                tags:
                  - name: id
              dataSources:
                - name: level
                  type: GAUGE
                  heartbeatSec: 900
              archives:
                rras:
                  - {name: rra_5m, stepSec: 300, rows: 400, cf: [AVERAGE, MAX, LAST], xff: 0.5}
              storage:
                backend: localDisk
                objectNaming: {scheme: deterministic, seriesPrefix: series, schemaPrefix: schema}
            """;

    private static ViewQuery query(ConsolidationFunction cf, int maxPoints) {
        return new ViewQuery(Duration.ofSeconds((long) AMOSTRAS * STEP_SEC), STEP_SEC, cf, maxPoints);
    }

    private static long fimExclusivoMs() {
        return (T0_SEC + (long) AMOSTRAS * STEP_SEC) * 1000L;
    }

    /** Escreve {@code AMOSTRAS} pontos; {@code valores.get(i)} é o valor do índice i. */
    private static NgrrdHandle serieCom(Path dir, List<Double> valores) {
        StorageFactory.StorageBindings bindings = StorageFactory.StorageBindings.forLocalDisk(dir);
        NgrrdHandle h = Ngrrd.fromYaml(YAML, bindings, Map.of("id", "s1"), null,
                Ngrrd.OpenOptions.defaults());
        for (int i = 0; i < valores.size(); i++) {
            h.write("level", new Sample((T0_SEC + (long) i * STEP_SEC) * 1000L, valores.get(i)));
        }
        h.checkpoint();
        return h;
    }

    private static List<Double> rampa() {
        return java.util.stream.IntStream.range(0, AMOSTRAS).mapToDouble(i -> i).boxed().toList();
    }

    private static List<Double> valores(SeriesResult r) {
        return r.points().stream().map(DataPoint::value).filter(v -> !Double.isNaN(v)).toList();
    }

    @Test
    void reducaoPreservaAAmostraMaisRecenteDoRange(@TempDir Path dir) {
        try (NgrrdHandle h = serieCom(dir, rampa())) {
            SeriesResult r = h.read("level", query(ConsolidationFunction.LAST, 120), fimExclusivoMs());

            assertTrue(r.points().size() <= 120, "respeita maxPoints: " + r.points().size());
            // A rampa termina em AMOSTRAS-1. Com decimacao por indice esse valor sumia.
            assertTrue(valores(r).contains((double) (AMOSTRAS - 1)),
                    "ultima amostra do range deve sobreviver a reducao; obtido=" + valores(r));
        }
    }

    @Test
    void maxPreservaOPicoDoBucket(@TempDir Path dir) {
        List<Double> v = new java.util.ArrayList<>(rampa());
        v.set(137, 9999.0); // pico isolado, num indice que a decimacao antiga pulava
        try (NgrrdHandle h = serieCom(dir, v)) {
            SeriesResult r = h.read("level", query(ConsolidationFunction.MAX, 60), fimExclusivoMs());
            assertTrue(valores(r).contains(9999.0),
                    "cf=MAX deve preservar o pico do bucket; obtido=" + valores(r));
        }
    }

    @Test
    void averagePreservaAMediaDoRange(@TempDir Path dir) {
        try (NgrrdHandle h = serieCom(dir, rampa())) {
            SeriesResult r = h.read("level", query(ConsolidationFunction.AVERAGE, 60), fimExclusivoMs());
            double mediaLida = valores(r).stream().mapToDouble(Double::doubleValue).average().orElseThrow();
            double mediaReal = (AMOSTRAS - 1) / 2.0;
            assertEquals(mediaReal, mediaLida, mediaReal * 0.01,
                    "cf=AVERAGE deve preservar a media do range");
        }
    }

    @Test
    void quedaDeEstadoNoFimDaJanelaSobrevive(@TempDir Path dir) {
        // Reproduz o caso real: oper_status 1=up por toda a janela, 2=down nas duas
        // ultimas amostras. Com decimacao por indice, ambas caiam fora da resposta e
        // a tira de estado exibia "up 100% / 0 transicoes".
        List<Double> v = new java.util.ArrayList<>(java.util.Collections.nCopies(AMOSTRAS, 1.0));
        v.set(AMOSTRAS - 2, 2.0);
        v.set(AMOSTRAS - 1, 2.0);
        try (NgrrdHandle h = serieCom(dir, v)) {
            SeriesResult r = h.read("level", query(ConsolidationFunction.MAX, 120), fimExclusivoMs());
            assertTrue(valores(r).contains(2.0),
                    "o down no fim da janela nao pode desaparecer na reducao; obtido=" + valores(r));
        }
    }

    @Test
    void semReducaoQuandoORangeCabeEmMaxPoints(@TempDir Path dir) {
        try (NgrrdHandle h = serieCom(dir, rampa())) {
            SeriesResult r = h.read("level", query(ConsolidationFunction.AVERAGE, 10_000), fimExclusivoMs());
            List<Double> lidos = valores(r);
            assertEquals(AMOSTRAS, lidos.size(), "passthrough deve devolver todas as amostras");
            assertEquals(STEP_SEC, r.stepSec(), "sem agregacao, stepSec e o step do RRA");
        }
    }

    @Test
    void stepSecRefleteOEspacamentoRealAposAgregacao(@TempDir Path dir) {
        try (NgrrdHandle h = serieCom(dir, rampa())) {
            SeriesResult r = h.read("level", query(ConsolidationFunction.AVERAGE, 100), fimExclusivoMs());
            // 300 amostras em 100 buckets => 3 amostras por bucket => 900s.
            assertEquals(900, r.stepSec(),
                    "stepSec deve informar o espacamento real, nao o do RRA");
        }
    }
}
