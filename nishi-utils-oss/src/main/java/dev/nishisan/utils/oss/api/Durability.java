package dev.nishisan.utils.oss.api;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Política de durabilidade do {@code checkpoint()}/{@code flush()} do writer.
 *
 * <p>Controla se cada checkpoint força a escrita até o armazenamento durável
 * ({@code fsync} no disco) além de materializar os bytes do objeto da série.</p>
 *
 * <p>Modelo conceitualmente análogo ao {@code RelayDurability} do core
 * ({@code dev.nishisan.utils.ngrid.replication}); {@code GROUP_COMMIT} (coalescer
 * N checkpoints num único force) fica para uma evolução futura.</p>
 */
public enum Durability {

    /**
     * Força a escrita em todo checkpoint ({@code fsync} no disco / {@code PUT} no
     * S3). Durabilidade local mais forte, menor throughput. É o padrão.
     */
    FSYNC,

    /**
     * Disco local: o checkpoint materializa os bytes (legíveis por leitores no
     * mesmo processo via page cache do SO) mas <strong>pula</strong> o
     * {@code fsync}; o SO descarrega no seu próprio ritmo. A janela de perda é um
     * crash abrupto — um {@code close()} limpo ainda descarrega o pendente.
     *
     * <p><strong>Não suportado</strong> em {@code OBJECT_STORAGE}: no S3 o
     * {@code force()} é o próprio {@code PUT} (publicação), de modo que pular o
     * force nunca publicaria a série. A abertura rejeita essa combinação.</p>
     */
    OS_CACHE;

    @JsonCreator
    public static Durability from(String value) {
        if (value == null) {
            return null;
        }
        return Durability.valueOf(value.trim().toUpperCase());
    }
}
