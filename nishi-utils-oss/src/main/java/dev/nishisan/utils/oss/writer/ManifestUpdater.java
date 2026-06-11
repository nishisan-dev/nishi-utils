package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.format.ManifestBlock;
import dev.nishisan.utils.oss.format.ManifestCodec;
import dev.nishisan.utils.oss.format.NgrrdManifest;
import dev.nishisan.utils.oss.format.RraManifest;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.StorageKey;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Grava snapshots versionados do manifesto a cada
 * {@code manifestPolicy.intervalSec}, transformando a lista de blocos
 * persistidos pelo {@link NgrrdWriter} em {@link NgrrdManifest} e gravando em
 * {@code manifestPrefix/<seriesKey>/v{N}.yaml}.
 *
 * <p>A versão monotônica {@code N} é descoberta na inicialização listando o
 * prefixo e a partir daí é mantida em memória.</p>
 */
public final class ManifestUpdater implements AutoCloseable {

    /** No modo incremental mantém apenas as N versões mais recentes do manifesto. */
    private static final int INCREMENTAL_KEEP_VERSIONS = 2;

    private final NgrrdWriter writer;
    private final NgrrdStorage storage;
    private final String definitionHash;
    private final long intervalSec;

    /**
     * {@code true} no modo incremental: os snapshots são disparados pelo
     * {@code checkpoint()} (não há thread agendada por handle) e versões antigas
     * são podadas a cada escrita.
     */
    private final boolean incremental;

    private final ScheduledExecutorService scheduler;
    private final AtomicInteger nextVersion = new AtomicInteger();

    public ManifestUpdater(NgrrdWriter writer, NgrrdStorage storage,
                           String definitionHash, long intervalSec) {
        this(writer, storage, definitionHash, intervalSec, false);
    }

    public ManifestUpdater(NgrrdWriter writer, NgrrdStorage storage,
                           String definitionHash, long intervalSec, boolean incremental) {
        this.writer = Objects.requireNonNull(writer, "writer é obrigatório");
        this.storage = Objects.requireNonNull(storage, "storage é obrigatório");
        this.definitionHash = Objects.requireNonNull(definitionHash, "definitionHash é obrigatório");
        if (intervalSec <= 0) {
            throw new IllegalArgumentException("intervalSec deve ser > 0: " + intervalSec);
        }
        this.intervalSec = intervalSec;
        this.incremental = incremental;
        this.nextVersion.set(discoverNextVersion());

        // No modo incremental não há thread agendada por handle (reduz o churn de
        // threads sob eviction): os snapshots vêm do checkpoint do consumer.
        this.scheduler = incremental ? null : Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ngrrd-manifest-" + writer.seriesKey());
            t.setDaemon(true);
            return t;
        });
    }

    /** Inicia o agendamento (chamar uma única vez); no-op no modo incremental. */
    public void start() {
        if (scheduler == null) {
            return;
        }
        scheduler.scheduleAtFixedRate(this::writeSnapshotSafe,
                intervalSec, intervalSec, TimeUnit.SECONDS);
    }

    /**
     * Grava um snapshot imediatamente (sincronicamente) — útil em testes e no
     * shutdown ordenado do writer.
     */
    public NgrrdManifest writeSnapshot() {
        int version = nextVersion.getAndIncrement();
        if (version <= 0) {
            version = 1;
            nextVersion.set(2);
        }
        NgrrdManifest manifest = buildManifest(version);
        byte[] yaml = ManifestCodec.writeYaml(manifest);
        String key = StorageKey.manifestVersion(
                writer.definition().spec().storage().objectNaming(),
                writer.seriesKey(),
                version);
        storage.atomicReplace(key, yaml);
        if (incremental) {
            pruneOldVersions(version);
        }
        return manifest;
    }

    /**
     * Remove versões de manifesto anteriores a {@code currentVersion -
     * INCREMENTAL_KEEP_VERSIONS}. Como o reader sempre lê a versão máxima, manter
     * só as últimas evita que checkpoints frequentes acumulem milhares de
     * {@code vN.yaml}.
     */
    private void pruneOldVersions(int currentVersion) {
        int threshold = currentVersion - INCREMENTAL_KEEP_VERSIONS;
        if (threshold <= 0) {
            return;
        }
        var naming = writer.definition().spec().storage().objectNaming();
        String prefix = StorageKey.manifestPrefix(naming, writer.seriesKey());
        for (String existing : storage.list(prefix)) {
            int slash = existing.lastIndexOf('/');
            String name = slash >= 0 ? existing.substring(slash + 1) : existing;
            if (!(name.startsWith("v") && name.endsWith(".yaml"))) {
                continue;
            }
            try {
                int n = Integer.parseInt(name.substring(1, name.length() - ".yaml".length()));
                if (n <= threshold) {
                    storage.delete(existing);
                }
            } catch (NumberFormatException ignored) {
                // arquivo fora do padrão — ignora.
            }
        }
    }

    @Override
    public void close() {
        if (scheduler == null) {
            return;
        }
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
    }

    private void writeSnapshotSafe() {
        try {
            writeSnapshot();
        } catch (RuntimeException e) {
            System.err.println("ngrrd-manifest-updater error: " + e);
        }
    }

    private NgrrdManifest buildManifest(int version) {
        Map<String, List<ManifestBlock>> byRra = new LinkedHashMap<>();
        Map<String, Integer> stepByRra = new LinkedHashMap<>();
        List<PersistedBlock> blocks = writer.persistedBlocks();
        // Ordena por rra → blockStart → ds → cf para snapshots determinísticos.
        blocks = new ArrayList<>(blocks);
        blocks.sort(Comparator
                .comparing(PersistedBlock::rraName)
                .thenComparingLong(PersistedBlock::blockStartEpoch)
                .thenComparing(PersistedBlock::dsName)
                .thenComparing(PersistedBlock::cf));
        for (PersistedBlock pb : blocks) {
            byRra.computeIfAbsent(pb.rraName(), k -> new ArrayList<>())
                    .add(new ManifestBlock(
                            pb.blockStartEpoch(),
                            pb.rows(),
                            pb.crc32(),
                            pb.storageKey(),
                            pb.dsName(),
                            pb.cf()));
            stepByRra.putIfAbsent(pb.rraName(), pb.stepSec());
        }
        List<RraManifest> rras = new ArrayList<>(byRra.size());
        for (Map.Entry<String, List<ManifestBlock>> e : byRra.entrySet()) {
            rras.add(new RraManifest(e.getKey(), stepByRra.get(e.getKey()), e.getValue()));
        }
        return new NgrrdManifest(version, writer.seriesKey(), definitionHash, rras);
    }

    private int discoverNextVersion() {
        String prefix = StorageKey.manifestPrefix(
                writer.definition().spec().storage().objectNaming(),
                writer.seriesKey());
        List<String> keys = storage.list(prefix);
        int maxFound = 0;
        for (String key : keys) {
            int slash = key.lastIndexOf('/');
            String name = slash >= 0 ? key.substring(slash + 1) : key;
            if (name.startsWith("v") && name.endsWith(".yaml")) {
                String number = name.substring(1, name.length() - ".yaml".length());
                try {
                    int n = Integer.parseInt(number);
                    if (n > maxFound) {
                        maxFound = n;
                    }
                } catch (NumberFormatException ignored) {
                    // arquivo fora do padrão — ignora.
                }
            }
        }
        return maxFound + 1;
    }
}
