package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Comportamento aplicado ao valor derivado quando wrap (overflow) é detectado.
 *
 * <ul>
 *     <li>{@link #AUTO} — usa {@code counterBits} para corrigir o delta
 *     somando 2^32 ou 2^64 conforme apropriado.</li>
 *     <li>{@link #UNKNOWN} — emite NaN ao detectar wrap.</li>
 * </ul>
 */
public enum WrapBehavior {
    AUTO,
    UNKNOWN;

    @JsonCreator
    public static WrapBehavior from(String value) {
        if (value == null) {
            return null;
        }
        return WrapBehavior.valueOf(value.trim().toUpperCase());
    }
}
