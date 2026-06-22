package dev.nishisan.utils.oss.storage.blob;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Roteamento determinístico de uma série para um shard do volume (algoritmo
 * {@code SHA256_PREFIX64}, v1).
 *
 * <p>{@code shard = uint64(SHA-256(catalogKey)[0..8] big-endian) mod N}. Reusa
 * SHA-256 (estável, bem distribuído e portável) para que a ferramenta de migração
 * Python ({@code hashlib.sha256}) calcule exatamente o mesmo shard. Ver
 * {@code doc/oss/ngrrd-blob-volume.md} §8.</p>
 *
 * <p>Stateless e thread-safe.</p>
 */
public final class BlobRouting {

    /** Identificador do algoritmo de roteamento gravado em {@code volume.meta}. */
    public static final int ALGORITHM_SHA256_PREFIX64 = 1;

    /** Versão do roteamento gravada em {@code volume.meta}. */
    public static final int ROUTING_VERSION = 1;

    private BlobRouting() {
    }

    /**
     * Shard preferencial para a {@code catalogKey} (= {@code series/<seriesKey>.ngrr}).
     *
     * @param catalogKey chave do objeto, exatamente como passada a {@code openSeries}
     * @param shardCount número de shards do volume (&gt; 0)
     * @return índice do shard em {@code [0, shardCount)}
     */
    public static int shardFor(String catalogKey, int shardCount) {
        Objects.requireNonNull(catalogKey, "catalogKey é obrigatório");
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount deve ser > 0: " + shardCount);
        }
        byte[] digest = sha256(catalogKey);
        long h64 = 0L;
        for (int i = 0; i < 8; i++) {
            h64 = (h64 << 8) | (digest[i] & 0xFFL);
        }
        return (int) Long.remainderUnsigned(h64, shardCount);
    }

    private static byte[] sha256(String value) {
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            return sha.digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponível na JVM", e);
        }
    }
}
