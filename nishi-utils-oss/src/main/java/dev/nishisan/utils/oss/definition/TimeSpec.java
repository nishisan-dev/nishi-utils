package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.TimestampAlignment;

/**
 * Configuração temporal da série: step base, alinhamento dos timestamps
 * consolidados e política de amostras atrasadas.
 *
 * <p>No formato de série única (NGRR) não há {@code blockSizeSec}: a consolidação
 * é contínua e a retenção é o ring buffer de cada RRA ({@code rows × stepSec}),
 * em paridade com o RRDtool.</p>
 *
 * @param baseStepSec        step base (PDP) em segundos; ex.: 300 (5 min)
 * @param timestampAlignment alinhamento dos buckets — atualmente apenas EPOCH
 * @param lateSamplePolicy   política para amostras fora do step
 * @param missingValue       representação de valor ausente; o valor literal "NaN"
 *                           é o padrão e deixa NaN nos pontos consolidados
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TimeSpec(
        @JsonProperty("baseStepSec") int baseStepSec,
        @JsonProperty("timestampAlignment") TimestampAlignment timestampAlignment,
        @JsonProperty("lateSamplePolicy") LateSamplePolicy lateSamplePolicy,
        @JsonProperty("missingValue") String missingValue
) {
}
