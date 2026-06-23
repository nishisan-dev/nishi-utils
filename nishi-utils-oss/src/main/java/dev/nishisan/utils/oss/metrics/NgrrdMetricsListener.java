package dev.nishisan.utils.oss.metrics;

/**
 * Recebe notificações de eventos de qualidade emitidos pelo {@link NgrrdMetrics}.
 * As implementações tipicamente forward para Micrometer, logs estruturados ou
 * JMX. Implementações devem ser thread-safe.
 *
 * <p><strong>Forma canônica vs. legada:</strong> cada evento tem uma forma
 * <em>canônica</em> que recebe o {@code seriesKey} como primeiro parâmetro —
 * essencial quando um único listener é compartilhado por milhares de séries de um
 * blob volume e precisa atribuir o evento à série de origem. As variantes
 * <em>legadas</em> (sem {@code seriesKey}) são mantidas para compatibilidade: o
 * produtor sempre chama a forma canônica e o {@code default} canônico delega à
 * legada, de modo que implementações que só sobrescrevem a forma antiga continuam
 * recebendo os eventos (com {@code seriesKey} descartado).</p>
 */
public interface NgrrdMetricsListener {

    // ------------------------------------------------------------ forma canônica

    /** Sample atrasada além do {@code maxLatenessSec} e descartada/aceita conforme política. */
    default void onLateSample(String seriesKey, String dsName, long latenessSec) {
        onLateSample(dsName, latenessSec);
    }

    /** Counter reset detectado para o DS raw. */
    default void onCounterReset(String seriesKey, String dsName) {
        onCounterReset(dsName);
    }

    /** Wrap (overflow) plausível detectado para o DS raw. */
    default void onWrapDetected(String seriesKey, String dsName) {
        onWrapDetected(dsName);
    }

    /** Sample chegou {@code lagSec} segundos após o instante do recebimento esperado. */
    default void onIngestLag(String seriesKey, String dsName, long lagSec) {
        onIngestLag(dsName, lagSec);
    }

    /** Bloco fechado com proporção {@code missingRatio} de PDPs ausentes. */
    default void onBlockClosed(String seriesKey, String rraName, String dsName, double missingRatio) {
        onBlockClosed(rraName, dsName, missingRatio);
    }

    // --------------------------------------------------------------- forma legada

    /** Variante legada (sem {@code seriesKey}); ver a forma canônica {@link #onLateSample(String, String, long)}. */
    default void onLateSample(String dsName, long latenessSec) {
    }

    /** Variante legada (sem {@code seriesKey}); ver a forma canônica {@link #onCounterReset(String, String)}. */
    default void onCounterReset(String dsName) {
    }

    /** Variante legada (sem {@code seriesKey}); ver a forma canônica {@link #onWrapDetected(String, String)}. */
    default void onWrapDetected(String dsName) {
    }

    /** Variante legada (sem {@code seriesKey}); ver a forma canônica {@link #onIngestLag(String, String, long)}. */
    default void onIngestLag(String dsName, long lagSec) {
    }

    /** Variante legada (sem {@code seriesKey}); ver a forma canônica {@link #onBlockClosed(String, String, String, double)}. */
    default void onBlockClosed(String rraName, String dsName, double missingRatio) {
    }
}
