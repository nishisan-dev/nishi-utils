package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.format.ManifestCodec;
import dev.nishisan.utils.oss.metrics.NgrrdMetrics;
import dev.nishisan.utils.oss.metrics.NgrrdMetricsListener;
import dev.nishisan.utils.oss.reader.NgrrdReader;
import dev.nishisan.utils.oss.reader.ViewExecutor;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.StorageFactory;
import dev.nishisan.utils.oss.writer.ManifestUpdater;
import dev.nishisan.utils.oss.writer.NgrrdWriter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/**
 * Façade pública do formato <strong>ngrrd</strong>.
 *
 * <p>Compila um pipeline pronto para uso a partir de uma definição YAML +
 * vínculos de storage. {@link #fromYaml(Path, StorageFactory.StorageBindings)}
 * carrega+valida o YAML, instancia o backend, sobe o writer e o manifest
 * updater em background, e devolve um {@link NgrrdHandle} para escrita/leitura
 * por série.</p>
 *
 * <p>Uso típico:</p>
 * <pre>{@code
 * Path yaml = Path.of("series.yaml");
 * StorageFactory.StorageBindings bindings = StorageFactory.StorageBindings.forLocalDisk(tmp);
 * try (NgrrdHandle handle = Ngrrd.fromYaml(yaml, bindings,
 *         Map.of("deviceId","r1","interfaceId","eth0"))) {
 *     handle.write("in_octets", new Sample(System.currentTimeMillis(), 12345L));
 *     SeriesResult daily = handle.read("daily").get("in_bps");
 * }
 * }</pre>
 */
public final class Ngrrd {

    private Ngrrd() {
    }

    /**
     * Identificador do formato persistido em headers de bloco e manifestos.
     *
     * @return string fixa "ngrrd/v1"
     */
    public static String apiVersion() {
        return "ngrrd/v1";
    }

    public static NgrrdHandle fromYaml(Path yamlFile,
                                       StorageFactory.StorageBindings bindings,
                                       Map<String, String> tags) {
        Objects.requireNonNull(yamlFile, "yamlFile é obrigatório");
        String raw;
        try {
            raw = Files.readString(yamlFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException("Falha ao ler YAML em " + yamlFile, e);
        }
        return fromYaml(raw, bindings, tags, null);
    }

    public static NgrrdHandle fromYaml(InputStream input,
                                       StorageFactory.StorageBindings bindings,
                                       Map<String, String> tags) {
        Objects.requireNonNull(input, "input é obrigatório");
        try {
            return fromYaml(new String(input.readAllBytes(), StandardCharsets.UTF_8),
                    bindings, tags, null);
        } catch (IOException e) {
            throw new IllegalArgumentException("Falha ao ler YAML do InputStream", e);
        }
    }

    public static NgrrdHandle fromYaml(String yamlContent,
                                       StorageFactory.StorageBindings bindings,
                                       Map<String, String> tags,
                                       NgrrdMetricsListener metricsListener) {
        Objects.requireNonNull(yamlContent, "yamlContent é obrigatório");
        Objects.requireNonNull(bindings, "bindings é obrigatório");
        Objects.requireNonNull(tags, "tags é obrigatório");

        NgrrdDefinition def = NgrrdYamlLoader.parse(yamlContent, System::getenv);
        dev.nishisan.utils.oss.config.NgrrdDefinitionValidator.validate(def);
        String definitionHash = ManifestCodec.computeDefinitionHash(yamlContent);

        NgrrdStorage storage = StorageFactory.from(def.spec().storage(), bindings);
        String seriesKey = resolveSeriesKey(def.spec().identity().seriesKeyTemplate(), tags);

        NgrrdMetrics metrics = new NgrrdMetrics(metricsListener);
        NgrrdWriter writer = new NgrrdWriter(def, storage, seriesKey, metrics);
        ManifestUpdater updater = new ManifestUpdater(writer, storage, definitionHash,
                def.spec().storage().manifestPolicy().intervalSec());
        updater.start();

        return new DefaultHandle(def, storage, seriesKey, writer, updater, metrics, Map.copyOf(tags));
    }

    static String resolveSeriesKey(String template, Map<String, String> tags) {
        StringBuilder out = new StringBuilder(template.length());
        int i = 0;
        while (i < template.length()) {
            char c = template.charAt(i);
            if (c == '{') {
                int end = template.indexOf('}', i);
                if (end < 0) {
                    throw new IllegalArgumentException(
                            "Placeholder não fechado em seriesKeyTemplate: " + template);
                }
                String name = template.substring(i + 1, end);
                String value = tags.get(name);
                if (value == null) {
                    throw new IllegalArgumentException(
                            "Tag obrigatória ausente para seriesKey: " + name);
                }
                out.append(value);
                i = end + 1;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    private static final class DefaultHandle implements NgrrdHandle {

        private final NgrrdDefinition definition;
        private final NgrrdStorage storage;
        private final String seriesKey;
        private final NgrrdWriter writer;
        private final ManifestUpdater updater;
        private final NgrrdMetrics metrics;
        private final NgrrdReader reader;
        private final ViewExecutor viewExecutor;
        private final Map<String, String> tags;
        private volatile boolean closed;

        DefaultHandle(NgrrdDefinition def, NgrrdStorage storage, String seriesKey,
                      NgrrdWriter writer, ManifestUpdater updater, NgrrdMetrics metrics,
                      Map<String, String> tags) {
            this.definition = def;
            this.storage = storage;
            this.seriesKey = seriesKey;
            this.writer = writer;
            this.updater = updater;
            this.metrics = metrics;
            this.reader = new NgrrdReader(def, storage, seriesKey);
            this.viewExecutor = new ViewExecutor(def, storage);
            this.tags = tags;
        }

        @Override
        public String seriesKey() {
            return seriesKey;
        }

        public NgrrdMetrics metrics() {
            return metrics;
        }

        @Override
        public void write(String dsName, Sample sample) {
            writer.write(dsName, sample);
        }

        @Override
        public void flush() {
            writer.flush();
            updater.writeSnapshot();
        }

        @Override
        public SeriesResult read(String dsName, ViewQuery query) {
            return reader.read(dsName, query);
        }

        @Override
        public Map<String, SeriesResult> read(String presetName) {
            return viewExecutor.run(presetName, tags);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                writer.flush();
            } finally {
                try {
                    updater.writeSnapshot();
                } finally {
                    try {
                        updater.close();
                    } finally {
                        writer.close();
                        if (storage instanceof AutoCloseable ac) {
                            try {
                                ac.close();
                            } catch (Exception e) {
                                // log opcional — não bloqueia shutdown.
                            }
                        }
                    }
                }
            }
        }
    }
}
