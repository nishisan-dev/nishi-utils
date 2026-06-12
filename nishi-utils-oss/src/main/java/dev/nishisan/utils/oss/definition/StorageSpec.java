package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.StorageBackendType;

/**
 * Bloco {@code storage}: backend, nomeação do objeto único da série, política
 * de escrita e durabilidade do checkpoint.
 *
 * <p>{@code durability} é opcional: ausente no YAML desserializa como
 * {@code null}, resolvido como {@link Durability#FSYNC} (default) no ponto de
 * abertura. Pode ser sobrescrito por abertura via {@code Ngrrd.OpenOptions}.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record StorageSpec(
        @JsonProperty("backend") StorageBackendType backend,
        @JsonProperty("objectNaming") ObjectNaming objectNaming,
        @JsonProperty("writePolicy") WritePolicy writePolicy,
        @JsonProperty("durability") Durability durability
) {
}
