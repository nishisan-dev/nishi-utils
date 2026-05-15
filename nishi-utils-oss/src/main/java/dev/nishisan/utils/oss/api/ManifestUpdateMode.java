package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Política de atualização do manifesto versionado.
 *
 * <p>{@link #PERIODIC_SNAPSHOT} grava um novo {@code manifest/<seriesKey>/v{N}.yaml}
 * a cada {@code intervalSec}, mantendo histórico recuperável.</p>
 */
public enum ManifestUpdateMode {
    PERIODIC_SNAPSHOT;

    @JsonCreator
    public static ManifestUpdateMode from(String value) {
        if (value == null) {
            return null;
        }
        return ManifestUpdateMode.valueOf(value.trim().toUpperCase());
    }
}
