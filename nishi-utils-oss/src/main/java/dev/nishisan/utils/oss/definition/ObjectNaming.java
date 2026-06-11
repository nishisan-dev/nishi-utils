package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.NamingScheme;

/**
 * Convenções de prefixos usadas para construir chaves de objetos no storage.
 *
 * @param seriesPrefix prefixo do objeto único da série ({@code .ngrr}); ausente
 *                     ⇒ {@code "series"}
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
        @JsonProperty("statePrefix") String statePrefix,
        @JsonProperty("seriesPrefix") String seriesPrefix
) {

    /** Construtor de compatibilidade (sem {@code statePrefix}/{@code seriesPrefix}). */
    public ObjectNaming(NamingScheme scheme, String rawPrefix, String aggPrefix,
                        String manifestPrefix, String schemaPrefix) {
        this(scheme, rawPrefix, aggPrefix, manifestPrefix, schemaPrefix, null, null);
    }

    /** Construtor de compatibilidade (sem {@code seriesPrefix}). */
    public ObjectNaming(NamingScheme scheme, String rawPrefix, String aggPrefix,
                        String manifestPrefix, String schemaPrefix, String statePrefix) {
        this(scheme, rawPrefix, aggPrefix, manifestPrefix, schemaPrefix, statePrefix, null);
    }

    /** Prefixo de estado efetivo: {@code "state"} quando omitido. */
    public String statePrefixOrDefault() {
        return statePrefix == null || statePrefix.isBlank() ? "state" : statePrefix;
    }

    /** Prefixo do objeto único da série: {@code "series"} quando omitido. */
    public String seriesPrefixOrDefault() {
        return seriesPrefix == null || seriesPrefix.isBlank() ? "series" : seriesPrefix;
    }
}
