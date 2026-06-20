package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.DataSourceType;

import java.util.Map;
import java.util.Objects;

/**
 * Definição de um Data Source: tipo, heartbeat, faixa válida, política de reset
 * e derivação opcional (counter → rate).
 *
 * @param name           nome lógico do DS (ex.: {@code in_octets})
 * @param type           {@link DataSourceType}
 * @param counterBits    largura do counter (32 ou 64) — relevante apenas para
 *                       {@link DataSourceType#COUNTER} e detecção de wrap
 * @param heartbeatSec   tempo máximo (s) sem amostra antes do PDP virar NaN
 * @param min            limite inferior aceito (null = sem limite)
 * @param max            limite superior aceito (null = sem limite)
 * @param resetPolicy    política de detecção/aplicação de counter reset
 * @param derive         derivação automática (counter → rate) ou null
 * @param dictionary     mapa opcional ordinal→label para DS de estado/enum
 *                       (ex.: {@code ifOperStatus}: 1=up, 2=down). Apenas
 *                       descritivo: não entra na geometria binária e humaniza a
 *                       leitura. Um DS com dictionary deve ser consolidado por
 *                       LAST/MAX (nunca apenas AVERAGE) — ver
 *                       {@link dev.nishisan.utils.oss.config.NgrrdDefinitionValidator}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DataSourceDef(
        @JsonProperty("name") String name,
        @JsonProperty("type") DataSourceType type,
        @JsonProperty("counterBits") Integer counterBits,
        @JsonProperty("heartbeatSec") int heartbeatSec,
        @JsonProperty("min") Double min,
        @JsonProperty("max") Double max,
        @JsonProperty("resetPolicy") ResetPolicyDef resetPolicy,
        @JsonProperty("derive") DeriveDef derive,
        @JsonProperty("dictionary") Map<Integer, String> dictionary
) {
    public DataSourceDef {
        dictionary = Objects.requireNonNullElse(dictionary, Map.of());
    }
}
