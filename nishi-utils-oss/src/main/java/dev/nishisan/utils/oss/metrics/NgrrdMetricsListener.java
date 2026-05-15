package dev.nishisan.utils.oss.metrics;

/**
 * Recebe notificações de eventos de qualidade emitidos pelo {@link NgrrdMetrics}.
 * As implementações tipicamente forward para Micrometer, logs estruturados ou
 * JMX. Implementações devem ser thread-safe.
 */
public interface NgrrdMetricsListener {

    /** Sample atrasada além do {@code maxLatenessSec} e descartada/aceita conforme política. */
    default void onLateSample(String dsName, long latenessSec) {
    }

    /** Counter reset detectado para o DS raw. */
    default void onCounterReset(String dsName) {
    }

    /** Wrap (overflow) plausível detectado para o DS raw. */
    default void onWrapDetected(String dsName) {
    }

    /** Sample chegou {@code lagSec} segundos após o esperado pelo bucket. */
    default void onIngestLag(String dsName, long lagSec) {
    }

    /** Bloco fechado com proporção {@code ratio} de PDPs ausentes. */
    default void onBlockClosed(String rraName, String dsName, double missingRatio) {
    }
}
