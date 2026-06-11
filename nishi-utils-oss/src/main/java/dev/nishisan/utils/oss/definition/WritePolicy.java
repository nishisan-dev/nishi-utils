package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.WriteMode;

/**
 * Política de escrita: modo (append-only) e regras de idempotência das chaves
 * determinísticas.
 *
 * <p>No formato de série única (NGRR) a persistência é sempre incremental
 * (rrdtool-like): o estado vivo é reidratado na reabertura e o {@code checkpoint()}
 * materializa o CDP em progresso. Não há mais {@code persistenceMode} nem
 * rollover de blocos.</p>
 *
 * @param mode        modo de escrita do storage (append-only)
 * @param idempotency idempotência das chaves determinísticas
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WritePolicy(
        @JsonProperty("mode") WriteMode mode,
        @JsonProperty("idempotency") IdempotencyDef idempotency
) {
}
