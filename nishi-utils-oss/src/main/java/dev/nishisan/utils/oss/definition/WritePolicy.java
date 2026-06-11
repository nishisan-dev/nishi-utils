package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import dev.nishisan.utils.oss.api.PersistenceMode;
import dev.nishisan.utils.oss.api.WriteMode;

/**
 * Política de escrita: modo (append-only), regras de idempotência e modo de
 * persistência da janela do bloco.
 *
 * @param mode             modo de escrita do storage (append-only)
 * @param idempotency      idempotência das chaves determinísticas
 * @param persistenceMode  {@link PersistenceMode}; ausente ⇒ {@code BLOCK_ROLLOVER}
 * @param persistLastValue persiste o último valor por DS raw para sobreviver à
 *                         reabertura do handle; ausente ⇒ derivado do
 *                         {@code persistenceMode} (true em {@code INCREMENTAL})
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record WritePolicy(
        @JsonProperty("mode") WriteMode mode,
        @JsonProperty("idempotency") IdempotencyDef idempotency,
        @JsonProperty("persistenceMode") PersistenceMode persistenceMode,
        @JsonProperty("persistLastValue") Boolean persistLastValue
) {

    /** Construtor de compatibilidade (sem campos de persistência incremental). */
    public WritePolicy(WriteMode mode, IdempotencyDef idempotency) {
        this(mode, idempotency, null, null);
    }

    /** Modo efetivo: {@code BLOCK_ROLLOVER} quando omitido. */
    public PersistenceMode persistenceModeOrDefault() {
        return persistenceMode == null ? PersistenceMode.BLOCK_ROLLOVER : persistenceMode;
    }

    /** {@code true} quando o último valor por DS raw deve ser persistido. */
    public boolean persistLastValueEnabled() {
        if (persistLastValue != null) {
            return persistLastValue;
        }
        return persistenceModeOrDefault() == PersistenceMode.INCREMENTAL;
    }
}
