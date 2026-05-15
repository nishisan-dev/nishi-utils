package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Esquema de nomeação de objetos no storage.
 *
 * <p>{@link #DETERMINISTIC} garante que a chave de cada bloco é função pura de
 * (seriesKey, ds, stepSec, blockStartEpoch), tornando idempotente o reprocessamento.</p>
 */
public enum NamingScheme {
    DETERMINISTIC;

    @JsonCreator
    public static NamingScheme from(String value) {
        if (value == null) {
            return null;
        }
        return NamingScheme.valueOf(value.trim().toUpperCase());
    }
}
