package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Metadados da definição (nome lógico, descrição livre).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NgrrdMetadata(
        @JsonProperty("name") String name,
        @JsonProperty("description") String description
) {
}
