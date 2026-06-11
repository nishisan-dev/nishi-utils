package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.NamingScheme;

/**
 * Convenções de prefixos usadas para construir chaves de objetos no storage.
 *
 * @param statePrefix prefixo do estado de série persistido pelo writer no modo
 *                    incremental ({@code last value} + acumuladores da janela);
 *                    ausente ⇒ {@code "state"}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ObjectNaming(
        @JsonProperty("scheme") NamingScheme scheme,
        @JsonProperty("rawPrefix") String rawPrefix,
        @JsonProperty("aggPrefix") String aggPrefix,
        @JsonProperty("manifestPrefix") String manifestPrefix,
        @JsonProperty("schemaPrefix") String schemaPrefix,
        @JsonProperty("statePrefix") String statePrefix
) {

    /** Construtor de compatibilidade (sem {@code statePrefix}). */
    public ObjectNaming(NamingScheme scheme, String rawPrefix, String aggPrefix,
                        String manifestPrefix, String schemaPrefix) {
        this(scheme, rawPrefix, aggPrefix, manifestPrefix, schemaPrefix, null);
    }

    /** Prefixo de estado efetivo: {@code "state"} quando omitido. */
    public String statePrefixOrDefault() {
        return statePrefix == null || statePrefix.isBlank() ? "state" : statePrefix;
    }
}
