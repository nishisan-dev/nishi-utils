package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Modo de persistência do {@code NgrrdWriter}.
 *
 * <ul>
 *     <li>{@link #BLOCK_ROLLOVER} — comportamento histórico: a janela do bloco
 *     vive apenas em memória e só é materializada no rollover do bloco (ao
 *     cruzar {@code blockSizeSec}) ou em {@code flush()}/{@code close()}. O
 *     estado do counter é volátil; reabrir o handle perde a continuidade da
 *     derivação. Default quando o campo é omitido (retrocompatível).</li>
 *     <li>{@link #INCREMENTAL} — semântica estilo rrdtool: a janela do bloco é
 *     <em>write-through</em>. O writer recarrega o estado da série no
 *     {@code open} (último valor por DS raw + acumuladores por slot) e o
 *     persiste a cada {@code checkpoint()}, gravando o bloco aberto com
 *     {@code FLAG_PARTIAL}. Handles ficam stateless na reabertura — o counter e
 *     a janela sobrevivem a eviction/restart — e o dado fica legível antes do
 *     rollover.</li>
 * </ul>
 */
public enum PersistenceMode {
    BLOCK_ROLLOVER,
    INCREMENTAL;

    /**
     * Aceita as formas {@code blockRollover}, {@code block_rollover} e
     * {@code incremental} (case-insensitive), conforme escritas no YAML.
     */
    @JsonCreator
    public static PersistenceMode from(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim().toLowerCase().replace("_", "").replace("-", "");
        return switch (normalized) {
            case "blockrollover" -> BLOCK_ROLLOVER;
            case "incremental" -> INCREMENTAL;
            default -> throw new IllegalArgumentException("persistenceMode inválido: " + value);
        };
    }
}
