package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.StorageBackendType;

/**
 * Bloco {@code storage}: backend, nomeação do objeto único da série e política
 * de escrita.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record StorageSpec(
        @JsonProperty("backend") StorageBackendType backend,
        @JsonProperty("objectNaming") ObjectNaming objectNaming,
        @JsonProperty("writePolicy") WritePolicy writePolicy
) {
}
