package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Filtro de DS aplicáveis ao bloco de archives.
 *
 * @param include nomes dos DS (raw ou derivados) que terão RRAs gerados; lista
 *                vazia significa "todos"
 * @param exclude nomes a serem removidos da lista resolvida
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AppliesTo(
        @JsonProperty("include") List<String> include,
        @JsonProperty("exclude") List<String> exclude
) {
    public AppliesTo {
        include = Objects.requireNonNullElse(include, List.of());
        exclude = Objects.requireNonNullElse(exclude, List.of());
    }
}
