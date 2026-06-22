package dev.nishisan.utils.oss.storage.blob;

import java.util.Objects;

/**
 * Entrada do catálogo do blob volume: mapeia a {@code catalogKey} (=
 * {@code series/<seriesKey>.ngrr}) para a região física onde a imagem {@code .ngrr}
 * reside. Ver {@code doc/oss/ngrrd-blob-volume.md} §6.
 *
 * @param key          catalogKey (UTF-8), idêntica à storage key passada a openSeries
 * @param shardId      shard onde a região reside (autoritativo)
 * @param regionOffset offset absoluto na data region do shard (≥ headerBytes)
 * @param regionBytes  tamanho da região física/slot (= align(objectBytes))
 * @param objectBytes  tamanho lógico do objeto (= fileTotalBytes da série); o que {@code get} devolve
 * @param state        LIVE ou DELETED (tombstone reaproveitável)
 */
public record CatalogEntry(String key, int shardId, long regionOffset, long regionBytes,
                           long objectBytes, State state) {

    public CatalogEntry {
        Objects.requireNonNull(key, "key é obrigatória");
        Objects.requireNonNull(state, "state é obrigatório");
    }

    /** Estado de uma entrada do catálogo (codificado como u8). */
    public enum State {
        LIVE((byte) 1),
        DELETED((byte) 2);

        private final byte code;

        State(byte code) {
            this.code = code;
        }

        public byte code() {
            return code;
        }

        public static State fromCode(byte code) {
            return switch (code) {
                case 1 -> LIVE;
                case 2 -> DELETED;
                default -> throw new BlobVolumeException("estado de entrada desconhecido: " + code);
            };
        }
    }
}
