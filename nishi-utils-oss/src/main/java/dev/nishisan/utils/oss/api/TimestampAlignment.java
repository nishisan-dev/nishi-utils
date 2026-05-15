package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Estratégia de alinhamento temporal dos pontos da série.
 *
 * <p>{@link #EPOCH} significa que os timestamps consolidados são múltiplos de
 * {@code baseStepSec} desde 1970-01-01 UTC (alinhamento determinístico, padrão RRD).</p>
 */
public enum TimestampAlignment {
    EPOCH;

    @JsonCreator
    public static TimestampAlignment from(String value) {
        if (value == null) {
            return null;
        }
        return TimestampAlignment.valueOf(value.trim().toUpperCase());
    }
}
