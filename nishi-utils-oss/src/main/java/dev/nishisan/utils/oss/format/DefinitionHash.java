package dev.nishisan.utils.oss.format;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Hash SHA-256 do YAML original da definição. É gravado no cabeçalho do arquivo
 * de série ({@link SeriesFileCodec}) para detectar incompatibilidade silenciosa
 * entre a geometria persistida e a definição em uso: divergência de hash força
 * a recriação do arquivo (clean cut).
 *
 * <p>Stateless e thread-safe.</p>
 */
public final class DefinitionHash {

    /** Tamanho do digest SHA-256 em bytes. */
    public static final int BYTES = 32;

    private DefinitionHash() {
    }

    /** Digest SHA-256 (32 bytes) do YAML da definição. */
    public static byte[] sha256(String definitionYaml) {
        Objects.requireNonNull(definitionYaml, "definitionYaml é obrigatório");
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            return sha.digest(definitionYaml.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponível na JVM", e);
        }
    }

    /** Representação hex do {@link #sha256(String)}, útil para logs. */
    public static String hex(String definitionYaml) {
        return HexFormat.of().formatHex(sha256(definitionYaml));
    }
}
