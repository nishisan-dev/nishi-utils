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
 */
public final class ViewExecutor {

    private final NgrrdDefinition definition;
    private final NgrrdStorage storage;
    private final ReadWriteLock seriesLock;

    public ViewExecutor(NgrrdDefinition definition, NgrrdStorage storage) {
        this(definition, storage, new ReentrantReadWriteLock());
    }

    /**
     * Variante com {@link ReadWriteLock} compartilhado com o writer da série
     * (mesmo processo), repassado aos {@link NgrrdReader}s criados por execução.
     */
    public ViewExecutor(NgrrdDefinition definition, NgrrdStorage storage,
                        ReadWriteLock seriesLock) {
        this.definition = Objects.requireNonNull(definition, "definition é obrigatório");
        this.storage = Objects.requireNonNull(storage, "storage é obrigatório");
        this.seriesLock = Objects.requireNonNull(seriesLock, "seriesLock é obrigatório");
    }

    public Map<String, SeriesResult> run(String presetName, Map<String, String> tags) {
        return run(presetName, tags, System.currentTimeMillis());
    }

    public Map<String, SeriesResult> run(String presetName, Map<String, String> tags,
                                         long endExclusiveEpochMs) {
        Objects.requireNonNull(presetName, "presetName é obrigatório");
        Map<String, String> safeTags = Objects.requireNonNullElse(tags, Map.of());

        PresetDef preset = definition.spec().views().presets().stream()
                .filter(p -> p.name().equals(presetName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Preset desconhecido: " + presetName));

        String seriesKey = resolveSeriesKey(safeTags);
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

    private String resolveSeriesKey(Map<String, String> tags) {
        String template = definition.spec().identity().seriesKeyTemplate();
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
}
