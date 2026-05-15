package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuração de derivação automática a partir de um Data Source.
 *
 * <p>O caso típico é {@code octets → bps}: {@code formula: "delta * 8 / deltaT"}.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DeriveDef(
        @JsonProperty("output") DeriveOutputDef output
) {
}
