package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.NamingScheme;

/**
 * Convenções de prefixos usadas para construir chaves de objetos no storage.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ObjectNaming(
        @JsonProperty("scheme") NamingScheme scheme,
        @JsonProperty("rawPrefix") String rawPrefix,
        @JsonProperty("aggPrefix") String aggPrefix,
        @JsonProperty("manifestPrefix") String manifestPrefix,
        @JsonProperty("schemaPrefix") String schemaPrefix
) {
}
