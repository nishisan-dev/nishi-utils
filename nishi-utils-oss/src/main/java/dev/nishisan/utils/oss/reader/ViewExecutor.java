package dev.nishisan.utils.oss.reader;

import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.definition.PresetDef;
import dev.nishisan.utils.oss.storage.NgrrdStorage;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Resolve presets declarativos do YAML ({@link PresetDef}) em {@link ViewQuery}s
 * executadas pelo {@link NgrrdReader}, retornando um {@link SeriesResult} por
 * DS listado no preset.
 *
 * <p>Opera sobre uma <strong>única série</strong> já resolvida ({@code seriesKey}):
 * o key é o mesmo do handle (resolvido por tags+template no modo compat, ou
 * diretamente pelo locator {@code ngrrd://...}), evitando re-resolução.</p>
 */
public final class ViewExecutor {

    private final NgrrdDefinition definition;
    private final NgrrdStorage storage;
    private final String seriesKey;
    private final ReadWriteLock seriesLock;

    public ViewExecutor(NgrrdDefinition definition, NgrrdStorage storage, String seriesKey) {
        this(definition, storage, seriesKey, new ReentrantReadWriteLock());
    }

    /**
     * Variante com {@link ReadWriteLock} compartilhado com o writer da série
     * (mesmo processo), repassado aos {@link NgrrdReader}s criados por execução.
     */
    public ViewExecutor(NgrrdDefinition definition, NgrrdStorage storage, String seriesKey,
                        ReadWriteLock seriesLock) {
        this.definition = Objects.requireNonNull(definition, "definition é obrigatório");
        this.storage = Objects.requireNonNull(storage, "storage é obrigatório");
        this.seriesKey = Objects.requireNonNull(seriesKey, "seriesKey é obrigatório");
        this.seriesLock = Objects.requireNonNull(seriesLock, "seriesLock é obrigatório");
    }

    public Map<String, SeriesResult> run(String presetName) {
        return run(presetName, System.currentTimeMillis());
    }

    public Map<String, SeriesResult> run(String presetName, long endExclusiveEpochMs) {
        Objects.requireNonNull(presetName, "presetName é obrigatório");

        PresetDef preset = definition.spec().views().presets().stream()
                .filter(p -> p.name().equals(presetName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Preset desconhecido: " + presetName));

        NgrrdReader reader = new NgrrdReader(definition, storage, seriesKey, seriesLock);
        ViewQuery query = new ViewQuery(
                Duration.parse(preset.window()),
                preset.targetStepSec(),
                preset.cf(),
                preset.maxPoints());

        Map<String, SeriesResult> out = new LinkedHashMap<>();
        for (String ds : preset.series()) {
            out.put(ds, reader.read(ds, query, endExclusiveEpochMs));
        }
        return out;
    }
}
