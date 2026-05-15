package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Bloco {@code spec} de uma definição ngrrd.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NgrrdSpec(
        @JsonProperty("time") TimeSpec time,
        @JsonProperty("identity") IdentitySpec identity,
        @JsonProperty("dataSources") List<DataSourceDef> dataSources,
        @JsonProperty("archives") ArchiveSpec archives,
        @JsonProperty("views") ViewSpec views,
        @JsonProperty("storage") StorageSpec storage,
        @JsonProperty("quality") QualitySpec quality
) {
    public NgrrdSpec {
        dataSources = Objects.requireNonNullElse(dataSources, List.of());
    }
}
