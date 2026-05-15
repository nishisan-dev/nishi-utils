package dev.nishisan.utils.oss.reader;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataPoint;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.definition.RraDef;
import dev.nishisan.utils.oss.engine.BestFitSelector;
import dev.nishisan.utils.oss.format.BlockCodec;
import dev.nishisan.utils.oss.format.EncodedBlock;
import dev.nishisan.utils.oss.format.ManifestBlock;
import dev.nishisan.utils.oss.format.ManifestCodec;
import dev.nishisan.utils.oss.format.NgrrdManifest;
import dev.nishisan.utils.oss.format.RraManifest;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.StorageKey;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Materializa séries temporais persistidas no Storage para uma janela e
 * granularidade-alvo. Opera sobre o manifesto versionado mais recente; nunca
 * varre o storage diretamente, mantendo o manifesto como fonte de verdade.
 */
public final class NgrrdReader {

    private final NgrrdDefinition definition;
    private final NgrrdStorage storage;
    private final String seriesKey;

    public NgrrdReader(NgrrdDefinition definition, NgrrdStorage storage, String seriesKey) {
        this.definition = Objects.requireNonNull(definition, "definition é obrigatório");
        this.storage = Objects.requireNonNull(storage, "storage é obrigatório");
        this.seriesKey = Objects.requireNonNull(seriesKey, "seriesKey é obrigatório");
    }

    /**
     * Executa uma leitura para o DS derivado informado dentro do range
     * {@code [endExclusiveEpochMs - window, endExclusiveEpochMs)}.
     */
    public SeriesResult read(String dsName, ViewQuery query, long endExclusiveEpochMs) {
        Objects.requireNonNull(dsName, "dsName é obrigatório");
        Objects.requireNonNull(query, "query é obrigatório");

        Optional<RraDef> rra = BestFitSelector.pick(query, definition.spec().archives().rras());
        if (rra.isEmpty()) {
            return new SeriesResult(dsName, null, query.cf(), query.targetStepSec(), List.of());
        }

        long endExclusiveSec = endExclusiveEpochMs / 1000L;
        long startInclusiveSec = endExclusiveSec - query.window().getSeconds();

        Optional<NgrrdManifest> manifest = loadLatestManifest();
        if (manifest.isEmpty()) {
            return new SeriesResult(dsName, rra.get().name(), query.cf(),
                    rra.get().stepSec(), List.of());
        }

        List<ManifestBlock> blocks = filterBlocks(manifest.get(), rra.get(), dsName, query.cf());
        List<DataPoint> raw = decodeAndConcat(blocks, rra.get().stepSec(),
                startInclusiveSec, endExclusiveSec);
        List<DataPoint> downsampled = downsample(raw, query.maxPoints());
        return new SeriesResult(dsName, rra.get().name(), query.cf(),
                rra.get().stepSec(), downsampled);
    }

    /** Variante de conveniência que usa {@link System#currentTimeMillis()} como fim do range. */
    public SeriesResult read(String dsName, ViewQuery query) {
        return read(dsName, query, System.currentTimeMillis());
    }

    /** Carrega a versão de manifesto mais recente disponível no Storage. */
    public Optional<NgrrdManifest> loadLatestManifest() {
        String prefix = StorageKey.manifestPrefix(
                definition.spec().storage().objectNaming(), seriesKey);
        List<String> keys = storage.list(prefix);
        int maxVersion = -1;
        String bestKey = null;
        for (String key : keys) {
            int slash = key.lastIndexOf('/');
            String name = slash >= 0 ? key.substring(slash + 1) : key;
            if (!(name.startsWith("v") && name.endsWith(".yaml"))) {
                continue;
            }
            String number = name.substring(1, name.length() - ".yaml".length());
            try {
                int n = Integer.parseInt(number);
                if (n > maxVersion) {
                    maxVersion = n;
                    bestKey = key;
                }
            } catch (NumberFormatException ignored) {
                // ignora arquivos fora do padrão.
            }
        }
        if (bestKey == null) {
            return Optional.empty();
        }
        return storage.get(bestKey).map(ManifestCodec::readYaml);
    }

    private List<ManifestBlock> filterBlocks(NgrrdManifest manifest, RraDef rra,
                                             String dsName, ConsolidationFunction cf) {
        for (RraManifest rm : manifest.rras()) {
            if (!rm.rraName().equals(rra.name())) {
                continue;
            }
            List<ManifestBlock> matching = new ArrayList<>();
            for (ManifestBlock mb : rm.blocks()) {
                if (Objects.equals(mb.dsName(), dsName) && mb.cf() == cf) {
                    matching.add(mb);
                }
            }
            matching.sort(Comparator.comparingLong(ManifestBlock::blockStartEpoch));
            return matching;
        }
        return List.of();
    }

    private List<DataPoint> decodeAndConcat(List<ManifestBlock> blocks, int stepSec,
                                            long startInclusiveSec, long endExclusiveSec) {
        List<DataPoint> result = new ArrayList<>();
        for (ManifestBlock mb : blocks) {
            long blockEnd = mb.blockStartEpoch() + (long) mb.rows() * stepSec;
            if (blockEnd <= startInclusiveSec) {
                continue;
            }
            if (mb.blockStartEpoch() >= endExclusiveSec) {
                continue;
            }
            byte[] bytes = storage.get(mb.storageKey())
                    .orElseThrow(() -> new IllegalStateException(
                            "Bloco listado no manifesto não existe no storage: " + mb.storageKey()));
            EncodedBlock block = BlockCodec.decode(bytes);
            double[] payload = block.payload();
            for (int i = 0; i < payload.length; i++) {
                long tsSec = block.header().blockStartEpoch() + (long) i * block.header().stepSec();
                if (tsSec < startInclusiveSec || tsSec >= endExclusiveSec) {
                    continue;
                }
                result.add(new DataPoint(tsSec * 1000L, payload[i]));
            }
        }
        return result;
    }

    private List<DataPoint> downsample(List<DataPoint> points, int maxPoints) {
        if (points.size() <= maxPoints) {
            return points;
        }
        List<DataPoint> out = new ArrayList<>(maxPoints);
        double step = (double) points.size() / maxPoints;
        for (int i = 0; i < maxPoints; i++) {
            int idx = (int) Math.floor(i * step);
            if (idx >= points.size()) {
                idx = points.size() - 1;
            }
            out.add(points.get(idx));
        }
        return out;
    }
}
