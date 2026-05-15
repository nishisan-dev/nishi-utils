package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Modo de escrita aceito pelo storage backend.
 *
 * <p>{@link #APPEND_ONLY} é o único modo suportado: blocos nunca são reescritos
 * parcialmente — são gravados como objeto inteiro identificado pela chave
 * determinística.</p>
 */
public enum WriteMode {
    APPEND_ONLY;

    @JsonCreator
    public static WriteMode from(String value) {
        if (value == null) {
            return null;
        }
        return WriteMode.valueOf(value.trim().toUpperCase());
    }
}
