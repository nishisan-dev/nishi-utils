package dev.nishisan.utils.oss.format;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Snapshot versionado do estado de uma série: lista de blocos persistidos por
 * RRA. É a fonte de verdade para o {@code NgrrdReader} — leitura não consulta
 * {@code Storage.list(prefix)}, apenas o manifesto.
 *
 * <p>Cada versão é gravada como {@code manifest/<seriesKey>/v{N}.yaml}.</p>
 *
 * @param version         versão monotônica crescente do snapshot
 * @param seriesKey       chave da série (resolvida via {@code IdentitySpec.seriesKeyTemplate})
 * @param definitionHash  SHA-256 hex do YAML original (detecta mudança de schema)
 * @param rras            estado de cada RRA da definição
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NgrrdManifest(
        int version,
        String seriesKey,
        String definitionHash,
        List<RraManifest> rras) {

    @JsonCreator
    public NgrrdManifest(
            @JsonProperty("version") int version,
            @JsonProperty("seriesKey") String seriesKey,
            @JsonProperty("definitionHash") String definitionHash,
            @JsonProperty("rras") List<RraManifest> rras) {
        if (version <= 0) {
            throw new IllegalArgumentException("version deve ser > 0: " + version);
        }
        this.version = version;
        this.seriesKey = Objects.requireNonNull(seriesKey, "seriesKey é obrigatório");
        this.definitionHash = Objects.requireNonNull(definitionHash, "definitionHash é obrigatório");
        this.rras = rras == null ? List.of() : List.copyOf(rras);
    }
}
