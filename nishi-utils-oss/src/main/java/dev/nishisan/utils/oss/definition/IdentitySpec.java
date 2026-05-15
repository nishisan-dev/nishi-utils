package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Modelo de identidade da série: template para a chave lógica e tags suportadas.
 *
 * <p>Exemplo de template: {@code "device:{deviceId}/iface:{interfaceId}"} —
 * placeholders correspondem aos {@code tags[].name}.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record IdentitySpec(
        @JsonProperty("seriesKeyTemplate") String seriesKeyTemplate,
        @JsonProperty("tags") List<IdentityTag> tags
) {
    public IdentitySpec {
        tags = Objects.requireNonNullElse(tags, List.of());
    }
}
