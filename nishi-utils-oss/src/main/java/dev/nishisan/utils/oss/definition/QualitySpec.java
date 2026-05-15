package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Bloco {@code quality}: nomes das métricas que devem ser emitidas pela engine
 * (ex.: {@code missing_ratio}, {@code counter_reset_count}).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record QualitySpec(
        @JsonProperty("emitMetrics") List<String> emitMetrics
) {
    public QualitySpec {
        emitMetrics = Objects.requireNonNullElse(emitMetrics, List.of());
    }
}
