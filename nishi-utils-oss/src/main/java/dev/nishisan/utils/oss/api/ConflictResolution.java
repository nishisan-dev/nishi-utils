package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Estratégia de resolução quando um bloco já existe no storage para a mesma chave.
 *
 * <ul>
 *     <li>{@link #VERIFY_OR_REPLACE_IF_IDENTICAL} — lê o existente, compara
 *     checksum; se idêntico, deixa como está; senão, substitui (idempotência
 *     forte para reprocessamentos legítimos).</li>
 *     <li>{@link #FAIL_IF_EXISTS} — aborta a operação.</li>
 *     <li>{@link #OVERWRITE} — sobrescreve sempre, sem verificação.</li>
 * </ul>
 */
public enum ConflictResolution {
    VERIFY_OR_REPLACE_IF_IDENTICAL,
    FAIL_IF_EXISTS,
    OVERWRITE;

    @JsonCreator
    public static ConflictResolution from(String value) {
        if (value == null) {
            return null;
        }
        return ConflictResolution.valueOf(value.trim().toUpperCase());
    }
}
