package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Bloco {@code archives}: conjunto de RRAs e o filtro de DS aos quais eles se aplicam.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ArchiveSpec(
        @JsonProperty("appliesTo") AppliesTo appliesTo,
        @JsonProperty("rras") List<RraDef> rras
) {
    public ArchiveSpec {
        rras = Objects.requireNonNullElse(rras, List.of());
    }
}
