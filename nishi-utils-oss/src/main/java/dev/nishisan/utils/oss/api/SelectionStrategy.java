package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Estratégia usada por {@code BestFitSelector} para escolher o RRA mais adequado
 * a uma {@code ViewQuery}.
 */
public enum SelectionStrategy {
    BEST_FIT;

    @JsonCreator
    public static SelectionStrategy from(String value) {
        if (value == null) {
            return null;
        }
        return SelectionStrategy.valueOf(value.trim().toUpperCase());
    }
}
