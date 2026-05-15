package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Tag declarada no bloco {@code identity.tags} da definição.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record IdentityTag(
        @JsonProperty("name") String name
) {
}
