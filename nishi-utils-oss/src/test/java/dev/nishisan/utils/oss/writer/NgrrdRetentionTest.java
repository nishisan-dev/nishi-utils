package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.format.NgrrdManifest;
import dev.nishisan.utils.oss.storage.LocalDiskStorage;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Valida que o {@link NgrrdWriter} respeita a janela {@code rows × stepSec}
 * de cada RRA — blocos cujo final em segundos cai fora dessa janela são
 * removidos tanto do Storage quanto da lista interna usada pelo
 * {@link ManifestUpdater}. Sem essa expiração, o ngrrd cresceria
 * indefinidamente e perderia paridade conceitual com o RRDtool.
 */
class NgrrdRetentionTest {

    /**
     * YAML mínimo desenhado especificamente para tornar a janela observável:
     *
     * <ul>
     *     <li>baseStepSec = 60 s (1 min)</li>
     *     <li>blockSizeSec = 120 s (2 PDPs/bloco)</li>
     *     <li>RRA com stepSec=120 e rows=3 → janela = 360 s (3 blocos)</li>
     * </ul>
     *
     * Ingerindo 5 blocos sequenciais, esperamos sobrar apenas os 3 mais novos.
     */
    private static final String TINY_YAML = """
            apiVersion: ngrrd/v1
            kind: MetricSeriesDefinition
            metadata:
              name: retention-test
            spec:
              time:
                baseStepSec: 60
                blockSizeSec: 120
              identity:
                seriesKeyTemplate: "s:{id}"
                tags:
                  - name: id
              dataSources:
                - name: cpu
                  type: GAUGE
                  heartbeatSec: 120
                  derive:
                    output:
                      name: cpu_pct
                      unit: "%"
                      formula: "delta"
                      clampNegativeToZero: false
                      onReset: unknown
                      onWrap: auto
              archives:
                appliesTo:
                  include: [cpu_pct]
                rras:
                  - name: rra_2m_3
                    stepSec: 120
                    rows: 3
                    cf: [AVERAGE]
                    xff: 0.5
              views:
                selection:
                  strategy: best_fit
                  maxPointsDefault: 100
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

    private static final long BLOCK_START_SEC = 1_747_339_200L;
    private static final long BLOCK_START_MS = BLOCK_START_SEC * 1000L;
    private static final int BLOCK_MS = 120_000;

    @Test
    void blocosForaDaJanelaDeRetencaoSaoRemovidosDoStorageEDoRegistro(@TempDir Path tempDir) {
        NgrrdDefinition def = NgrrdYamlLoader.parse(TINY_YAML, k -> null);
        NgrrdStorage storage = new LocalDiskStorage(tempDir);

        try (NgrrdWriter writer = new NgrrdWriter(def, storage, "s:r1")) {
            // 5 blocos de 2 min, cada um com 2 amostras dentro.
            for (int block = 0; block < 5; block++) {
                long base = BLOCK_START_MS + (long) block * BLOCK_MS;
                writer.write("cpu", new Sample(base, 10.0 + block));
                writer.write("cpu", new Sample(base + 60_000L, 20.0 + block));
            }
            writer.flush();

            List<PersistedBlock> remaining = writer.persistedBlocks();
            // RRA tem rows=3; após 5 blocos, sobram os 3 mais novos.
            assertEquals(3, remaining.size(),
                    "esperava 3 blocos remanescentes, obteve " + remaining.size()
                            + ": " + remaining);
            long minBlockStart = remaining.stream()
                    .mapToLong(PersistedBlock::blockStartEpoch).min().orElseThrow();
            long maxBlockStart = remaining.stream()
                    .mapToLong(PersistedBlock::blockStartEpoch).max().orElseThrow();
            // Os 3 mais novos: blocos #2, #3, #4 a partir de BLOCK_START_SEC.
            assertEquals(BLOCK_START_SEC + 2L * 120, minBlockStart);
            assertEquals(BLOCK_START_SEC + 4L * 120, maxBlockStart);

            // Arquivos físicos dos blocos antigos sumiram do disco.
            for (int block = 0; block < 2; block++) {
                String oldKey = "agg/s:r1/rra_2m_3__cpu_pct__AVERAGE/120/"
                        + (BLOCK_START_SEC + (long) block * 120) + ".ngrrd";
                assertFalse(storage.exists(oldKey),
                        "bloco expirado ainda presente no storage: " + oldKey);
            }

            // Manifesto reflete o estado pós-expiração.
            NgrrdManifest snapshot = new ManifestUpdater(writer, storage, "h", 60).writeSnapshot();
            assertEquals(1, snapshot.rras().size());
            assertEquals(3, snapshot.rras().get(0).blocks().size());
        }
    }

    @Test
    void semExpiracaoQuandoVolumeAindaCabeNaJanela(@TempDir Path tempDir) {
        NgrrdDefinition def = NgrrdYamlLoader.parse(TINY_YAML, k -> null);
        NgrrdStorage storage = new LocalDiskStorage(tempDir);

        try (NgrrdWriter writer = new NgrrdWriter(def, storage, "s:r1")) {
            // Apenas 2 blocos — abaixo da janela rows=3.
            for (int block = 0; block < 2; block++) {
                long base = BLOCK_START_MS + (long) block * BLOCK_MS;
                writer.write("cpu", new Sample(base, 10.0));
                writer.write("cpu", new Sample(base + 60_000L, 20.0));
            }
            writer.flush();

            assertTrue(writer.persistedBlocks().size() == 2,
                    "esperava 2 blocos retidos (abaixo da janela)");
        }
    }
}
