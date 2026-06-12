package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.api.StorageBackendType;

/**
 * Bloco {@code storage}: backend, nomeação do objeto único da série, política
 * de escrita, durabilidade do checkpoint e tratamento de mudança de geometria.
 *
 * <p>{@code durability} e {@code onGeometryChange} são opcionais: ausentes no
 * YAML desserializam como {@code null} e são resolvidos no ponto de abertura
 * ({@link Durability#FSYNC} e {@link OnGeometryChange#FAIL}, respectivamente).
 * Ambos podem ser sobrescritos por abertura via {@code Ngrrd.OpenOptions}.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record StorageSpec(
        @JsonProperty("backend") StorageBackendType backend,
        @JsonProperty("objectNaming") ObjectNaming objectNaming,
        @JsonProperty("writePolicy") WritePolicy writePolicy,
        @JsonProperty("durability") Durability durability,
        @JsonProperty("onGeometryChange") OnGeometryChange onGeometryChange
) {
}
