package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Política para amostras que chegam fora do step esperado.
 *
 * <ul>
 *     <li>{@link #BUCKET_IF_POSSIBLE} — encaixa no bloco aberto se ainda houver
 *     espaço dentro de {@code maxLatenessSec}.</li>
 *     <li>{@link #LATE_DROP} — descarta a amostra e incrementa {@code late_sample_count}.</li>
 * </ul>
 */
public enum LateSampleAction {
    BUCKET_IF_POSSIBLE,
    LATE_DROP;

    @JsonCreator
    public static LateSampleAction from(String value) {
        if (value == null) {
            return null;
        }
        return LateSampleAction.valueOf(value.trim().toUpperCase());
    }
}
