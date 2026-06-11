package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.NamingScheme;

/**
 * Convenções de prefixos usadas para construir as chaves de objetos no storage.
 *
 * <p>No formato de série única (NGRR) só existem dois objetos por definição: o
 * objeto da série ({@code seriesPrefix}, um por {@code seriesKey}) e o snapshot
 * opcional de schema ({@code schemaPrefix}).</p>
 *
 * @param scheme       esquema de nomeação (atualmente {@code DETERMINISTIC})
 * @param schemaPrefix prefixo do snapshot de schema; ausente ⇒ {@code "schema"}
 * @param seriesPrefix prefixo do objeto único da série ({@code .ngrr}); ausente
 *                     ⇒ {@code "series"}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ObjectNaming(
        @JsonProperty("scheme") NamingScheme scheme,
        @JsonProperty("schemaPrefix") String schemaPrefix,
        @JsonProperty("seriesPrefix") String seriesPrefix
) {

    /** Prefixo de schema efetivo: {@code "schema"} quando omitido. */
    public String schemaPrefixOrDefault() {
        return schemaPrefix == null || schemaPrefix.isBlank() ? "schema" : schemaPrefix;
    }

    /** Prefixo do objeto único da série: {@code "series"} quando omitido. */
    public String seriesPrefixOrDefault() {
        return seriesPrefix == null || seriesPrefix.isBlank() ? "series" : seriesPrefix;
    }
}
