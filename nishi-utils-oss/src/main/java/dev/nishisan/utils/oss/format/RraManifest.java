package dev.nishisan.utils.oss.format;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Bloco do manifesto que agrupa todos os blocos persistidos para um RRA
 * específico de uma série. A ordem da lista de blocos é preservada e segue
 * monotônica crescente em {@code blockStartEpoch}.
 *
 * @param rraName nome do RRA conforme {@code spec.archives.rras[].name}
 * @param stepSec resolução em segundos (raw = {@code time.baseStepSec})
 * @param blocks  blocos persistidos
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RraManifest(String rraName, int stepSec, List<ManifestBlock> blocks) {

    @JsonCreator
    public RraManifest(
            @JsonProperty("rraName") String rraName,
            @JsonProperty("stepSec") int stepSec,
            @JsonProperty("blocks") List<ManifestBlock> blocks) {
        this.rraName = rraName;
        this.stepSec = stepSec;
        this.blocks = blocks == null ? List.of() : List.copyOf(blocks);
    }
}
