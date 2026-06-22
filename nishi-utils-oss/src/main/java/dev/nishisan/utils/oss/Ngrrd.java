package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.StorageBackendType;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdUri;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.definition.StorageSpec;
import dev.nishisan.utils.oss.metrics.NgrrdMetrics;
import dev.nishisan.utils.oss.metrics.NgrrdMetricsListener;
import dev.nishisan.utils.oss.reader.NgrrdReader;
import dev.nishisan.utils.oss.reader.ViewExecutor;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.StorageFactory;
import dev.nishisan.utils.oss.writer.NgrrdWriter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Façade pública do formato <strong>ngrrd</strong>.
 *
 * <p>Compila um pipeline pronto para uso a partir de uma definição YAML +
 * vínculos de storage. {@link #fromYaml(Path, StorageFactory.StorageBindings, Map)}
 * carrega+valida o YAML, instancia o backend, abre o writer sobre o objeto único
 * da série e devolve um {@link NgrrdHandle} para escrita/leitura por série.</p>
 *
 * <p>Uso típico:</p>
 * <pre>{@code
 * Path yaml = Path.of("series.yaml");
 * StorageFactory.StorageBindings bindings = StorageFactory.StorageBindings.forLocalDisk(tmp);
 * try (NgrrdHandle handle = Ngrrd.fromYaml(yaml, bindings,
 *         Map.of("deviceId","r1","interfaceId","eth0"))) {
 *     handle.write("in_octets", new Sample(System.currentTimeMillis(), 12345L));
 *     handle.checkpoint();
 *     SeriesResult daily = handle.read("daily").get("in_bps");
 * }
 * }</pre>
 */
public final class Ngrrd {

    private Ngrrd() {
    }

    /**
     * Identificador do formato persistido em headers e schema.
     *
     * @return string fixa "ngrrd/v1"
     */
    public static String apiVersion() {
        return "ngrrd/v1";
    }

    public static NgrrdHandle fromYaml(Path yamlFile,
                                       StorageFactory.StorageBindings bindings,
                                       Map<String, String> tags) {
        return fromYaml(yamlFile, bindings, tags, OpenOptions.defaults());
    }

    public static NgrrdHandle fromYaml(Path yamlFile,
                                       StorageFactory.StorageBindings bindings,
                                       Map<String, String> tags,
                                       OpenOptions options) {
        Objects.requireNonNull(yamlFile, "yamlFile é obrigatório");
        String raw;
        try {
            raw = Files.readString(yamlFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException("Falha ao ler YAML em " + yamlFile, e);
        }
        return fromYaml(raw, bindings, tags, null, options);
    }

    public static NgrrdHandle fromYaml(InputStream input,
                                       StorageFactory.StorageBindings bindings,
                                       Map<String, String> tags) {
        return fromYaml(input, bindings, tags, OpenOptions.defaults());
    }

    public static NgrrdHandle fromYaml(InputStream input,
                                       StorageFactory.StorageBindings bindings,
                                       Map<String, String> tags,
                                       OpenOptions options) {
        Objects.requireNonNull(input, "input é obrigatório");
        try {
            return fromYaml(new String(input.readAllBytes(), StandardCharsets.UTF_8),
                    bindings, tags, null, options);
        } catch (IOException e) {
            throw new IllegalArgumentException("Falha ao ler YAML do InputStream", e);
        }
    }

    public static NgrrdHandle fromYaml(String yamlContent,
                                       StorageFactory.StorageBindings bindings,
                                       Map<String, String> tags,
                                       NgrrdMetricsListener metricsListener) {
        return fromYaml(yamlContent, bindings, tags, metricsListener, OpenOptions.defaults());
    }

    public static NgrrdHandle fromYaml(String yamlContent,
                                       StorageFactory.StorageBindings bindings,
                                       Map<String, String> tags,
                                       NgrrdMetricsListener metricsListener,
                                       OpenOptions options) {
        Objects.requireNonNull(yamlContent, "yamlContent é obrigatório");
        Objects.requireNonNull(bindings, "bindings é obrigatório");
        Objects.requireNonNull(tags, "tags é obrigatório");
        Objects.requireNonNull(options, "options é obrigatório");

        NgrrdDefinition def = NgrrdYamlLoader.parse(yamlContent, System::getenv);
        dev.nishisan.utils.oss.config.NgrrdDefinitionValidator.validate(def);
        String seriesKey = resolveSeriesKey(def.spec().identity().seriesKeyTemplate(), tags);
        return buildHandle(def, bindings, seriesKey, metricsListener, options);
    }

    // ------------------------------------------------------------ blob locator

    /**
     * Abre uma série de um blob volume pelo locator {@code ngrrd://<volume>/<path>}:
     * o {@code <path>} é usado diretamente como {@code seriesKey} (modo locator),
     * dispensando {@code tags}+{@code seriesKeyTemplate}.
     */
    public static NgrrdHandle open(BlobVolumeRegistry registry, NgrrdUri locator, String yamlContent) {
        return open(registry, locator, yamlContent, OpenOptions.defaults());
    }

    public static NgrrdHandle open(BlobVolumeRegistry registry, NgrrdUri locator, String yamlContent,
                                   OpenOptions options) {
        Objects.requireNonNull(registry, "registry é obrigatório");
        Objects.requireNonNull(locator, "locator é obrigatório");
        return open(registry.require(locator.volume()), locator, yamlContent, options);
    }

    public static NgrrdHandle open(BlobVolume volume, NgrrdUri locator, String yamlContent) {
        return open(volume, locator, yamlContent, OpenOptions.defaults());
    }

    public static NgrrdHandle open(BlobVolume volume, NgrrdUri locator, String yamlContent, OpenOptions options) {
        Objects.requireNonNull(volume, "volume é obrigatório");
        Objects.requireNonNull(locator, "locator é obrigatório");
        Objects.requireNonNull(yamlContent, "yamlContent é obrigatório");
        Objects.requireNonNull(options, "options é obrigatório");
        NgrrdDefinition def = NgrrdYamlLoader.parse(yamlContent, System::getenv);
        dev.nishisan.utils.oss.config.NgrrdDefinitionValidator.validate(def);
        // Propaga o listener de qualidade default do volume a cada handle (coleta
        // central por série); null quando não configurado preserva o comportamento atual.
        return buildHandle(def, volume.bindings(), locator.seriesPath(), volume.qualityListener(), options);
    }

    private static NgrrdHandle buildHandle(NgrrdDefinition def, StorageFactory.StorageBindings bindings,
                                           String seriesKey, NgrrdMetricsListener metricsListener,
                                           OpenOptions options) {
        StorageSpec storageSpec = def.spec().storage();
        // Durabilidade efetiva: override de abertura > default do YAML > FSYNC.
        Durability durability = resolveDurability(options, storageSpec);
        // Fail-fast antes de instanciar o backend/abrir o writer (evita GET no
        // S3): no OBJECT_STORAGE o force() é o próprio PUT (publicação), logo
        // OS_CACHE nunca publicaria a série.
        if (storageSpec.backend() == StorageBackendType.OBJECT_STORAGE
                && durability == Durability.OS_CACHE) {
            throw new IllegalArgumentException(
                    "durability OS_CACHE não é suportada com backend OBJECT_STORAGE: no S3 o "
                            + "force() é o próprio PUT (publicação); desligá-lo nunca publicaria "
                            + "a série. Use FSYNC ou backend localDisk.");
        }

        NgrrdStorage storage = StorageFactory.from(storageSpec, bindings);
        // Tratamento de mudança de geometria: override de abertura > YAML > FAIL.
        OnGeometryChange onGeometryChange = resolveGeometryChange(options, storageSpec);

        NgrrdMetrics metrics = new NgrrdMetrics(metricsListener);
        // Lock por handle compartilhado entre writer e leitores: garante o
        // contrato de 1 writer + N readers do NgrrdHandle.
        ReadWriteLock seriesLock = new ReentrantReadWriteLock();
        NgrrdWriter writer = new NgrrdWriter(def, storage, seriesKey, metrics, seriesLock,
                durability, onGeometryChange);

        // O volume SHARDED_BLOB é compartilhado entre handles; quem o fecha é o
        // BlobVolumeRegistry, não o handle individual.
        boolean ownsStorage = storageSpec.backend() != StorageBackendType.SHARDED_BLOB;
        return new DefaultHandle(def, storage, seriesKey, writer, metrics, seriesLock, ownsStorage);
    }

    static Durability resolveDurability(OpenOptions options, StorageSpec storageSpec) {
        if (options.durability() != null) {
            return options.durability();
        }
        if (storageSpec.durability() != null) {
            return storageSpec.durability();
        }
        return Durability.FSYNC;
    }

    static OnGeometryChange resolveGeometryChange(OpenOptions options, StorageSpec storageSpec) {
        if (options.onGeometryChange() != null) {
            return options.onGeometryChange();
        }
        if (storageSpec.onGeometryChange() != null) {
            return storageSpec.onGeometryChange();
        }
        return OnGeometryChange.FAIL;
    }

    /**
     * Opções de abertura de um {@link NgrrdHandle}. Independem da forma da série
     * (descrita no YAML) e variam por deployment/execução.
     *
     * <p>Campos {@code null} significam "usar o default do YAML"
     * ({@code spec.storage.*}), que por sua vez recaem nos defaults globais
     * ({@link Durability#FSYNC}, {@link OnGeometryChange#FAIL}). Um valor não-nulo
     * sobrescreve o YAML — permitindo, por exemplo, abrir em produção com
     * {@link OnGeometryChange#FAIL} e rodar um job de manutenção com
     * {@link OnGeometryChange#MIGRATE}.</p>
     */
    public record OpenOptions(Durability durability, OnGeometryChange onGeometryChange) {

        public static OpenOptions defaults() {
            return new OpenOptions(null, null);
        }

        public static OpenOptions durability(Durability durability) {
            return new OpenOptions(durability, null);
        }

        public static OpenOptions onGeometryChange(OnGeometryChange onGeometryChange) {
            return new OpenOptions(null, onGeometryChange);
        }

        public static OpenOptions of(Durability durability, OnGeometryChange onGeometryChange) {
            return new OpenOptions(durability, onGeometryChange);
        }
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

        private final NgrrdStorage storage;
        private final String seriesKey;
        private final NgrrdWriter writer;
        private final NgrrdMetrics metrics;
        private final NgrrdReader reader;
        private final ViewExecutor viewExecutor;
        private final boolean ownsStorage;
        private volatile boolean closed;

        DefaultHandle(NgrrdDefinition def, NgrrdStorage storage, String seriesKey,
                      NgrrdWriter writer, NgrrdMetrics metrics,
                      ReadWriteLock seriesLock, boolean ownsStorage) {
            this.storage = storage;
            this.seriesKey = seriesKey;
            this.writer = writer;
            this.metrics = metrics;
            this.reader = new NgrrdReader(def, storage, seriesKey, seriesLock);
            this.viewExecutor = new ViewExecutor(def, storage, seriesKey, seriesLock);
            this.ownsStorage = ownsStorage;
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
        }

        @Override
        public void checkpoint() {
            writer.checkpoint();
        }

        @Override
        public SeriesResult read(String dsName, ViewQuery query) {
            return reader.read(dsName, query);
        }

        @Override
        public SeriesResult read(String dsName, ViewQuery query, long endExclusiveEpochMs) {
            return reader.read(dsName, query, endExclusiveEpochMs);
        }

        @Override
        public Map<String, SeriesResult> read(String presetName) {
            return viewExecutor.run(presetName);
        }

        @Override
        public Map<String, SeriesResult> read(String presetName, long endExclusiveEpochMs) {
            return viewExecutor.run(presetName, endExclusiveEpochMs);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                writer.close();
            } finally {
                // Volumes SHARDED_BLOB são compartilhados (ownsStorage=false) e
                // fechados pelo BlobVolumeRegistry, não por handle.
                if (ownsStorage && storage instanceof AutoCloseable ac) {
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
