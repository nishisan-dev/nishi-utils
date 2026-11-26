package dev.nishisan.utils.oss.cluster.node;

import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;

/**
 * Causa de uma tentativa de exclusão de série que falhou. Alimenta as métricas
 * {@code seriesDelete.attemptFailed.<CAUSA>} do nó.
 */
enum DeleteFailureCause {
    /** O nó não é o líder sincronizado ({@code NO_LEADER} em {@code ensureLeader}). */
    NO_LEADER,
    /** RPC de fase da exclusão estourou o prazo. */
    TIMEOUT,
    /** Participante recusou o prepare, não tem status/capacidade {@code series.delete} ou falhou na fase. */
    PARTICIPANT,
    /** A reserva de exclusão mudou durante a operação ({@link StaleReservationException}). */
    STALE,
    /** Qualquer outra exceção. */
    OTHER;

    /** Reserva de exclusão obsoleta: outra geração ou outra operação ocupa o placement. */
    static final class StaleReservationException extends IllegalStateException {
        StaleReservationException() { super("stale deletion reservation"); }
    }

    /** Status de um nó do catálogo ausente ao montar a lista de participantes. */
    static final class MissingNodeStatusException extends IllegalStateException {
        MissingNodeStatusException(String nodeId) { super("missing status: " + nodeId); }
    }

    /** Classifica a falha de uma tentativa de exclusão. */
    static DeleteFailureCause of(Throwable failure) {
        if (failure instanceof StaleReservationException) return STALE;
        if (failure instanceof MissingNodeStatusException) return PARTICIPANT;
        if (failure instanceof NgrrdClusterException cluster) {
            ErrorCode code = cluster.code();
            if (code == ErrorCode.NO_LEADER) return NO_LEADER;
            if (code == ErrorCode.TIMEOUT) return TIMEOUT;
            if (code == ErrorCode.REMOTE_ERROR || code == ErrorCode.UNSUPPORTED_BY_NODE) return PARTICIPANT;
        }
        return OTHER;
    }
}
