package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Comportamento aplicado ao valor derivado quando um counter reset é detectado.
 *
 * <ul>
 *     <li>{@link #UNKNOWN} — emite NaN para o CDP afetado (default RRD).</li>
 *     <li>{@link #ZERO} — emite 0.</li>
 *     <li>{@link #CARRY} — assume continuidade ignorando o reset (uso restrito).</li>
 * </ul>
 */
public enum ResetBehavior {
    UNKNOWN,
    ZERO,
    CARRY;

    @JsonCreator
    public static ResetBehavior from(String value) {
        if (value == null) {
            return null;
        }
        return ResetBehavior.valueOf(value.trim().toUpperCase());
    }
}
