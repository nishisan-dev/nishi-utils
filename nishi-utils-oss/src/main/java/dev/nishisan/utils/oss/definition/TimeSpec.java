package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.TimestampAlignment;

/**
 * Configuração temporal da série: step base, tamanho do bloco temporal,
 * alinhamento dos timestamps consolidados e política de amostras atrasadas.
 *
 * @param baseStepSec        step base (PDP) em segundos; ex.: 300 (5 min)
 * @param blockSizeSec       tamanho do bloco temporal em segundos; ex.: 21600 (6h)
 * @param timestampAlignment alinhamento dos buckets — atualmente apenas EPOCH
 * @param lateSamplePolicy   política para amostras fora do step
 * @param missingValue       representação de valor ausente; o valor literal "NaN"
 *                           é o padrão e deixa NaN nos pontos consolidados
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TimeSpec(
        @JsonProperty("baseStepSec") int baseStepSec,
        @JsonProperty("blockSizeSec") int blockSizeSec,
        @JsonProperty("timestampAlignment") TimestampAlignment timestampAlignment,
        @JsonProperty("lateSamplePolicy") LateSamplePolicy lateSamplePolicy,
        @JsonProperty("missingValue") String missingValue
) {
}
