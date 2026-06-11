package dev.nishisan.utils.oss.storage;

import dev.nishisan.utils.oss.definition.ObjectNaming;

import java.util.Objects;

/**
 * Construtor de chaves determinísticas para os objetos persistidos pelo ngrrd.
 *
 * <p>No formato de série única (NGRR) há apenas dois tipos de objeto:</p>
 *
 * <pre>
 * série            : {seriesPrefix}/{seriesKey}.ngrr
 * snapshot schema  : {schemaPrefix}/{definitionName}.yaml
 * </pre>
 *
 * <p>Os prefixos NÃO incluem barra final. A classe nunca produz {@code //}.</p>
 */
public final class StorageKey {

    private static final String SERIES_EXT = ".ngrr";
    private static final String YAML_EXT = ".yaml";

    private StorageKey() {
    }

    /**
     * Chave do objeto único da série no formato NGRR:
     * {@code {seriesPrefix}/{seriesKey}.ngrr}. É o único objeto de dados por
     * série (paridade com o arquivo {@code .rrd} do RRDtool).
     */
    public static String series(ObjectNaming naming, String seriesKey) {
        Objects.requireNonNull(naming, "naming é obrigatório");
        return join(naming.seriesPrefixOrDefault(), seriesKey + SERIES_EXT);
    }

    public static String schemaSnapshot(ObjectNaming naming, String definitionName) {
        Objects.requireNonNull(naming, "naming é obrigatório");
        return join(naming.schemaPrefixOrDefault(), definitionName + YAML_EXT);
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
