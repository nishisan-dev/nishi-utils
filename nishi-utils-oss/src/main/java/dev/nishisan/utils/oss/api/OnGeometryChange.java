package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Política aplicada quando, ao abrir uma série, a geometria da definição diverge
 * da gravada no objeto {@code .ngrr} (mudança de DS, de archives ou do step base).
 *
 * <p>O gatilho é a divergência do {@code geometryHash}; a aplicação da política é
 * <strong>condicionada</strong> ao incremento explícito de
 * {@code metadata.schemaRevision}: se a geometria mudou mas a revisão não foi
 * incrementada, a abertura sempre falha (independente desta política), pois a
 * mudança é tratada como não intencional.</p>
 */
public enum OnGeometryChange {

    /**
     * Aborta a abertura com um relatório do que mudou, preservando a série
     * existente. É o padrão: um restart nunca destrói história por omissão.
     */
    FAIL,

    /**
     * Reescreve a série preservando a história compatível: remapeia colunas e
     * archives por nome (coluna nova começa em {@code NaN}). Só cobre mudanças
     * estruturais (sem reamostragem); alterar o step base ou o {@code stepSec}/
     * {@code rows} de um archive existente não é migrável e recai em falha.
     */
    MIGRATE,

    /**
     * Descarta a série antiga e cria uma nova vazia (clean cut) com a geometria
     * nova. Perde toda a história — escolha explícita.
     */
    RECREATE;

    @JsonCreator
    public static OnGeometryChange from(String value) {
        if (value == null) {
            return null;
        }
        return OnGeometryChange.valueOf(value.trim().toUpperCase());
    }
}
