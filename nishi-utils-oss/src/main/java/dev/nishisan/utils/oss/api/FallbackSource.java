package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Fontes possíveis quando o RRA preferido não cobre a janela solicitada.
 *
 * <p>Ordem da lista {@code fallbackOrder} no YAML define a prioridade durante a
 * resolução: por exemplo, {@code [raw, agg]} tenta primeiro pontos brutos e cai
 * para arquivos agregados se não houver cobertura suficiente.</p>
 */
public enum FallbackSource {
    RAW,
    AGG;

    @JsonCreator
    public static FallbackSource from(String value) {
        if (value == null) {
            return null;
        }
        return FallbackSource.valueOf(value.trim().toUpperCase());
    }
}
