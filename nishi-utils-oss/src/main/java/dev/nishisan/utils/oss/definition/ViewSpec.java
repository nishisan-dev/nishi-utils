package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Bloco {@code views}: estratégia de seleção de RRA e presets nomeados.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ViewSpec(
        @JsonProperty("selection") ViewSelection selection,
        @JsonProperty("presets") List<PresetDef> presets
) {
    public ViewSpec {
        presets = Objects.requireNonNullElse(presets, List.of());
    }
}
