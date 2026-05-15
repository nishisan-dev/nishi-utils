package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Raiz de uma definição ngrrd carregada de YAML.
 *
 * <p>Modelo imutável; instâncias são construídas pelo {@code NgrrdYamlLoader}.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NgrrdDefinition(
        @JsonProperty("apiVersion") String apiVersion,
        @JsonProperty("kind") String kind,
        @JsonProperty("metadata") NgrrdMetadata metadata,
        @JsonProperty("spec") NgrrdSpec spec
) {
    public NgrrdDefinition {
        Objects.requireNonNull(spec, "spec é obrigatório");
        Objects.requireNonNull(metadata, "metadata é obrigatório");
    }
}
