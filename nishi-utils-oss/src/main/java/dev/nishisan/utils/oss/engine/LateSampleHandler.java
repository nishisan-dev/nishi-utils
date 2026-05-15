package dev.nishisan.utils.oss.engine;

import dev.nishisan.utils.oss.api.LateSampleAction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.definition.LateSamplePolicy;

/**
 * Decide o destino de uma amostra com base no timestamp do sample, na janela
 * do bloco atualmente aberto e na política configurada.
 *
 * <p>{@link Outcome}:</p>
 * <ul>
 *     <li>{@link Outcome#IN_TIME} — amostra dentro do bloco aberto.</li>
 *     <li>{@link Outcome#FUTURE} — amostra à frente do bloco atual (precisa
 *     rotacionar).</li>
 *     <li>{@link Outcome#BUCKETED_LATE} — amostra atrasada porém ainda dentro
 *     de {@code maxLatenessSec} a partir do {@code blockStart}; a política
 *     {@link LateSampleAction#BUCKET_IF_POSSIBLE} encaixa.</li>
 *     <li>{@link Outcome#DROPPED} — atraso excede tolerância ou política é
 *     {@link LateSampleAction#LATE_DROP}.</li>
 * </ul>
 */
public final class LateSampleHandler {

    private LateSampleHandler() {
    }

    /**
     * Classifica uma amostra contra o bloco aberto.
     *
     * @param sample          amostra recebida
     * @param blockStartMs    início do bloco aberto (epoch ms, alinhado a baseStep)
     * @param blockSizeMs     tamanho do bloco em ms
     * @param policy          política configurada (pode ser {@code null} → comportamento default)
     * @return classificação
     */
    public static Outcome classify(Sample sample,
                                   long blockStartMs,
                                   long blockSizeMs,
                                   LateSamplePolicy policy) {
        long ts = sample.tsEpochMs();
        long blockEndExclusive = blockStartMs + blockSizeMs;
        if (ts >= blockEndExclusive) {
            return Outcome.FUTURE;
        }
        if (ts >= blockStartMs) {
            return Outcome.IN_TIME;
        }
        long latenessMs = blockStartMs - ts;
        long maxLatenessMs = (policy == null ? 0L : (long) policy.maxLatenessSec()) * 1000L;
        LateSampleAction action = policy == null ? LateSampleAction.LATE_DROP : policy.onLate();
        if (action == LateSampleAction.BUCKET_IF_POSSIBLE && latenessMs <= maxLatenessMs) {
            return Outcome.BUCKETED_LATE;
        }
        return Outcome.DROPPED;
    }

    /** Destino da amostra. */
    public enum Outcome {
        IN_TIME,
        FUTURE,
        BUCKETED_LATE,
        DROPPED
    }
}
