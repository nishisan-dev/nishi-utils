package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.StorageBackendType;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdUri;
import dev.nishisan.utils.oss.config.NgrrdDefinitionValidator;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.definition.StorageSpec;
import dev.nishisan.utils.oss.metrics.NgrrdMetrics;
import dev.nishisan.utils.oss.metrics.NgrrdMetricsListener;
import dev.nishisan.utils.oss.reader.NgrrdReader;
import dev.nishisan.utils.oss.reader.ViewExecutor;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.SeriesChannelProvider;
import dev.nishisan.utils.oss.storage.StorageFactory;
import dev.nishisan.utils.oss.storage.StorageKey;
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

    // ------------------------------------------------------------ exists

    /**
     * Indica se a série existe no volume, sem I/O de criação: equivalente a
     * {@link #open(BlobVolumeRegistry, NgrrdUri, String)}, mas apenas consulta.
     */
    public static boolean exists(BlobVolumeRegistry registry, NgrrdUri locator, String yamlContent) {
        Objects.requireNonNull(registry, "registry é obrigatório");
        Objects.requireNonNull(locator, "locator é obrigatório");
        return exists(registry.require(locator.volume()), locator, yamlContent);
    }

    /**
     * Indica se a série existe no volume, sem I/O de criação: equivalente a
     * {@link #open(BlobVolume, NgrrdUri, String)}, mas apenas consulta. No
     * backend sharded blob é um lookup no catálogo em memória do volume (sem
     * acessar disco).
     */
    public static boolean exists(BlobVolume volume, NgrrdUri locator, String yamlContent) {
        Objects.requireNonNull(volume, "volume é obrigatório");
        Objects.requireNonNull(locator, "locator é obrigatório");
        Objects.requireNonNull(yamlContent, "yamlContent é obrigatório");
        NgrrdDefinition def = NgrrdYamlLoader.parse(yamlContent, System::getenv);
        NgrrdDefinitionValidator.validate(def);
        return existsInStorage(def.spec().storage(), volume.bindings(), locator.seriesPath());
    }

    /**
     * Indica se a série existe, sem I/O de criação: equivalente a
     * {@link #fromYaml(String, StorageFactory.StorageBindings, Map, NgrrdMetricsListener)},
     * mas apenas consulta. O YAML é necessário só para derivar o
     * {@code seriesPrefix} e resolver o {@code seriesKey} a partir das tags.
     */
    public static boolean exists(String yamlContent, StorageFactory.StorageBindings bindings,
                                 Map<String, String> tags) {
        Objects.requireNonNull(yamlContent, "yamlContent é obrigatório");
        Objects.requireNonNull(bindings, "bindings é obrigatório");
        Objects.requireNonNull(tags, "tags é obrigatório");
        NgrrdDefinition def = NgrrdYamlLoader.parse(yamlContent, System::getenv);
        NgrrdDefinitionValidator.validate(def);
        String seriesKey = resolveSeriesKey(def.spec().identity().seriesKeyTemplate(), tags);
        return existsInStorage(def.spec().storage(), bindings, seriesKey);
    }

    /**
     * Resolve a chave física via {@link StorageKey#series} e responde via
     * {@link SeriesChannelProvider#seriesExists} quando o backend suporta
     * (todos os backends atuais suportam); sem esse fallback, cai para
     * {@link NgrrdStorage#exists}. Nenhum objeto é criado, aberto ou
     * pré-alocado. Fecha o storage recém-instanciado quando ele não é
     * compartilhado (mesma regra de {@code DefaultHandle.close}: volumes
     * {@code SHARDED_BLOB} são geridos pelo {@link BlobVolumeRegistry}).
     */
    private static boolean existsInStorage(StorageSpec storageSpec, StorageFactory.StorageBindings bindings,
                                           String seriesKey) {
        NgrrdStorage storage = StorageFactory.from(storageSpec, bindings);
        boolean ownsStorage = storageSpec.backend() != StorageBackendType.SHARDED_BLOB;
        boolean result;
        try {
            String storageKey = StorageKey.series(storageSpec.objectNaming(), seriesKey);
            result = storage instanceof SeriesChannelProvider provider
                    ? provider.seriesExists(storageKey)
                    : storage.exists(storageKey);
        } catch (RuntimeException e) {
            closeOwnedStorage(storage, ownsStorage, e);
            throw e;
        }
        closeOwnedStorage(storage, ownsStorage, null);
        return result;
    }

    /**
     * Fecha um storage próprio (não compartilhado — ver {@code ownsStorage}: um
     * volume {@code SHARDED_BLOB} é gerido pelo {@link BlobVolumeRegistry}, não
     * por este método) após um uso pontual: falha ao abrir o writer ou consulta
     * de {@link #exists}. Sem isso, um backend com recurso próprio (ex.:
     * {@code S3Storage} com seu {@code S3Client}) vazaria a cada chamada que não
     * chega a devolver um {@link NgrrdHandle} para o chamador fechar.
     *
     * <p>Quando {@code primaryFailure} não é {@code null} (falha ao construir o
     * writer), uma falha de {@code close()} vira
     * {@link Throwable#addSuppressed(Throwable)} nela — nunca mascara a causa
     * original. Sem falha primária (consulta de {@link #exists} bem-sucedida),
     * a falha de close é apenas registrada: não há exceção em curso para
     * anexar, e o resultado da consulta já foi obtido.</p>
     */
    static void closeOwnedStorage(NgrrdStorage storage, boolean ownsStorage,
                                  RuntimeException primaryFailure) {
        if (!ownsStorage || !(storage instanceof AutoCloseable ac)) {
            return;
        }
        try {
            ac.close();
        } catch (Exception closeFailure) {
            if (primaryFailure != null) {
                primaryFailure.addSuppressed(closeFailure);
            } else {
                System.err.println("ngrrd: falha ao fechar storage: " + closeFailure);
            }
        }
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
        // O volume SHARDED_BLOB é compartilhado entre handles; quem o fecha é o
        // BlobVolumeRegistry, não este método nem o handle individual.
        boolean ownsStorage = storageSpec.backend() != StorageBackendType.SHARDED_BLOB;
        // Tratamento de mudança de geometria: override de abertura > YAML > FAIL.
        OnGeometryChange onGeometryChange = resolveGeometryChange(options, storageSpec);

        NgrrdMetrics metrics = new NgrrdMetrics(metricsListener);
        // Lock por handle compartilhado entre writer e leitores: garante o
        // contrato de 1 writer + N readers do NgrrdHandle.
        ReadWriteLock seriesLock = new ReentrantReadWriteLock();
        NgrrdWriter writer;
        try {
            writer = new NgrrdWriter(def, storage, seriesKey, metrics, seriesLock,
                    durability, onGeometryChange, System::currentTimeMillis, options.createIfMissing());
        } catch (RuntimeException e) {
            // SeriesNotFoundException com createIfMissing=false é um caminho
            // esperado e frequente (varredura de catálogo): sem fechar aqui, um
            // storage próprio (S3Storage com seu S3Client) vazaria a cada chave
            // ausente — o writer nunca chegou a existir para o chamador fechar.
            closeOwnedStorage(storage, ownsStorage, e);
            throw e;
        }

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
     * <p>{@code durability}/{@code onGeometryChange} {@code null} significam
     * "usar o default do YAML" ({@code spec.storage.*}), que por sua vez recaem
     * nos defaults globais ({@link Durability#FSYNC}, {@link OnGeometryChange#FAIL}).
     * Um valor não-nulo sobrescreve o YAML — permitindo, por exemplo, abrir em
     * produção com {@link OnGeometryChange#FAIL} e rodar um job de manutenção com
     * {@link OnGeometryChange#MIGRATE}.</p>
     *
     * @param createIfMissing quando {@code true} (default), abrir uma série
     *                        inexistente a cria do zero — comportamento atual.
     *                        Quando {@code false}, nenhum objeto é criado/pré-alocado:
     *                        série ausente faz o {@code open} lançar
     *                        {@link SeriesNotFoundException} em vez de
     *                        materializar uma série vazia (no modo local, com
     *                        {@link SeriesNotFoundException.Reason#ABSENT}; no
     *                        cluster, {@code NOT_PLACED} ou
     *                        {@code MISSING_ON_OWNER}). No modo local, o
     *                        handle de uma série existente abre normalmente
     *                        (leitura e escrita). No cluster
     *                        ({@code NgrrdClusterClient.open}), {@code false}
     *                        abre um handle SOMENTE LEITURA — escrita, flush e
     *                        checkpoint lançam {@link IllegalStateException} e
     *                        o {@code close()} é local; se a chave já tiver um
     *                        handle gravável aberto no cliente, o {@code open}
     *                        devolve uma vista somente leitura sobre ele, cujo
     *                        {@code close()} nunca fecha o gravável; um
     *                        {@code open} com criação posterior abre um
     *                        gravável novo no lugar do somente leitura. Exige
     *                        storages que anunciem {@code open.createIfMissing}:
     *                        contra um storage de versão anterior o cluster
     *                        falha com {@code UNSUPPORTED_BY_NODE} em vez de
     *                        arriscar criar a série (atualize os storages antes
     *                        dos clientes).
     */
    public record OpenOptions(Durability durability, OnGeometryChange onGeometryChange, boolean createIfMissing) {

        public OpenOptions(Durability durability, OnGeometryChange onGeometryChange) {
            this(durability, onGeometryChange, true);
        }

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

        /** Devolve uma cópia com {@code createIfMissing} alterado; demais campos preservados. */
        public OpenOptions withCreateIfMissing(boolean createIfMissing) {
            return new OpenOptions(durability, onGeometryChange, createIfMissing);
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
