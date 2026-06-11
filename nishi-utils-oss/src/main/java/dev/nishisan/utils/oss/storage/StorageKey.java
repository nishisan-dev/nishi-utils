package dev.nishisan.utils.oss.storage;

import dev.nishisan.utils.oss.definition.ObjectNaming;

import java.util.Objects;

/**
 * Construtor de chaves determinísticas para os objetos persistidos pelo ngrrd.
 *
 * <p>Convenção (a partir de {@code spec.storage.objectNaming}):</p>
 *
 * <pre>
 * raw block       : {rawPrefix}/{seriesKey}/{dsName}/{stepSec}/{blockStartEpoch}.ngrrd
 * aggregated block: {aggPrefix}/{seriesKey}/{rraName}/{stepSec}/{blockStartEpoch}.ngrrd
 * manifest version: {manifestPrefix}/{seriesKey}/v{version}.yaml
 * schema snapshot : {schemaPrefix}/{definitionName}.yaml
 * </pre>
 *
 * <p>Os prefixos NÃO incluem barra final. A classe nunca produz {@code //}.</p>
 */
public final class StorageKey {

    private static final String BLOCK_EXT = ".ngrrd";
    private static final String YAML_EXT = ".yaml";
    private static final String STATE_EXT = ".state";

    private StorageKey() {
    }

    public static String rawBlock(ObjectNaming naming, String seriesKey, String dsName,
                                  int stepSec, long blockStartEpoch) {
        Objects.requireNonNull(naming, "naming é obrigatório");
        return join(naming.rawPrefix(), seriesKey, dsName, Integer.toString(stepSec),
                blockStartEpoch + BLOCK_EXT);
    }

    public static String aggBlock(ObjectNaming naming, String seriesKey, String rraName,
                                  int stepSec, long blockStartEpoch) {
        Objects.requireNonNull(naming, "naming é obrigatório");
        return join(naming.aggPrefix(), seriesKey, rraName, Integer.toString(stepSec),
                blockStartEpoch + BLOCK_EXT);
    }

    public static String manifestVersion(ObjectNaming naming, String seriesKey, int version) {
        Objects.requireNonNull(naming, "naming é obrigatório");
        if (version <= 0) {
            throw new IllegalArgumentException("version deve ser > 0: " + version);
        }
        return join(naming.manifestPrefix(), seriesKey, "v" + version + YAML_EXT);
    }

    public static String manifestPrefix(ObjectNaming naming, String seriesKey) {
        Objects.requireNonNull(naming, "naming é obrigatório");
        return join(naming.manifestPrefix(), seriesKey);
    }

    public static String schemaSnapshot(ObjectNaming naming, String definitionName) {
        Objects.requireNonNull(naming, "naming é obrigatório");
        return join(naming.schemaPrefix(), definitionName + YAML_EXT);
    }

    /**
     * Estado do writer no modo incremental: {@code {statePrefix}/{seriesKey}/writer.state}.
     * Carrega o último valor por DS raw e os acumuladores da janela aberta para
     * reconstruir o writer na reabertura do handle (semântica rrdtool-like).
     */
    public static String seriesState(ObjectNaming naming, String seriesKey) {
        Objects.requireNonNull(naming, "naming é obrigatório");
        return join(naming.statePrefixOrDefault(), seriesKey, "writer" + STATE_EXT);
    }

    private static String join(String... segments) {
        StringBuilder sb = new StringBuilder();
        for (String segment : segments) {
            if (segment == null || segment.isEmpty()) {
                continue;
            }
            String trimmed = trimSlashes(segment);
            if (trimmed.isEmpty()) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append('/');
            }
            sb.append(trimmed);
        }
        return sb.toString();
    }

    private static String trimSlashes(String s) {
        int start = 0;
        int end = s.length();
        while (start < end && s.charAt(start) == '/') {
            start++;
        }
        while (end > start && s.charAt(end - 1) == '/') {
            end--;
        }
        return s.substring(start, end);
    }
}
