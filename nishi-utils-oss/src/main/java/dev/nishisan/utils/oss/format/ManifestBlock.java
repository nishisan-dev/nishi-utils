package dev.nishisan.utils.oss.format;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Entrada de bloco dentro de um {@link RraManifest}. Referencia um objeto
 * persistido no Storage e carrega a metadata mínima para uso pelo
 * {@code NgrrdReader} (sem precisar baixar o bloco para descobrir a janela).
 *
 * <p>{@code crc32} é armazenado em {@code long} para representar fielmente o
 * uint32 sem sinal usado no header binário.</p>
 *
 * @param blockStartEpoch epoch (segundos UTC) alinhado ao stepSec do RRA
 * @param rows            número de células no bloco
 * @param crc32           CRC32 unsigned do bloco persistido
 * @param storageKey      chave completa do objeto no Storage
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ManifestBlock(
        long blockStartEpoch,
        int rows,
        long crc32,
        String storageKey) {

    @JsonCreator
    public ManifestBlock(
            @JsonProperty("blockStartEpoch") long blockStartEpoch,
            @JsonProperty("rows") int rows,
            @JsonProperty("crc32") long crc32,
            @JsonProperty("storageKey") String storageKey) {
        this.blockStartEpoch = blockStartEpoch;
        this.rows = rows;
        this.crc32 = crc32;
        this.storageKey = storageKey;
    }
}
