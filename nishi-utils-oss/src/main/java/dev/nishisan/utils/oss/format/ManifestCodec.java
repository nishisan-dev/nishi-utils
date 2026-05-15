package dev.nishisan.utils.oss.format;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Codec YAML do manifesto versionado. Serializa e desserializa instâncias de
 * {@link NgrrdManifest} usando Jackson YAML; stateless e thread-safe.
 *
 * <p>Também expõe {@link #computeDefinitionHash(String)} para gerar o
 * {@code definitionHash} consumido pelo writer ao persistir um novo
 * manifesto.</p>
 */
public final class ManifestCodec {

    private static final ObjectMapper MAPPER;

    static {
        YAMLFactory factory = new YAMLFactory()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);
        MAPPER = new ObjectMapper(factory)
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    private ManifestCodec() {
    }

    /** Serializa o manifesto em YAML UTF-8. */
    public static byte[] writeYaml(NgrrdManifest manifest) {
        Objects.requireNonNull(manifest, "manifest é obrigatório");
        try {
            return MAPPER.writeValueAsBytes(manifest);
        } catch (IOException e) {
            throw new NgrrdFormatException("Falha ao serializar manifesto YAML", e);
        }
    }

    /** Desserializa o manifesto a partir de bytes YAML UTF-8. */
    public static NgrrdManifest readYaml(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes é obrigatório");
        try {
            return MAPPER.readValue(bytes, NgrrdManifest.class);
        } catch (IOException e) {
            throw new NgrrdFormatException("Falha ao desserializar manifesto YAML", e);
        }
    }

    /** Conveniência: desserializa de uma string já decodificada. */
    public static NgrrdManifest readYaml(String yaml) {
        return readYaml(yaml.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Calcula SHA-256 hex do YAML original da definição. O resultado é gravado
     * em {@link NgrrdManifest#definitionHash()} para detectar incompatibilidades
     * silenciosas entre o manifesto persistido e a definição em uso.
     */
    public static String computeDefinitionHash(String definitionYaml) {
        Objects.requireNonNull(definitionYaml, "definitionYaml é obrigatório");
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            byte[] digest = sha.digest(definitionYaml.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 indisponível na JVM", e);
        }
    }
}
