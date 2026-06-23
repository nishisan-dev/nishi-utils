package dev.nishisan.utils.oss.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coletor de métricas de qualidade do ngrrd. Mantém os contadores e gauges
 * declarados em {@code spec.quality.emitMetrics}:
 *
 * <ul>
 *     <li>{@code late_sample_count} — incrementa em cada sample classificada
 *     como tardia que não couber no bloco corrente.</li>
 *     <li>{@code counter_reset_count} — counter reset detectado.</li>
 *     <li>{@code wrap_detected_count} — wrap (overflow) detectado.</li>
 *     <li>{@code ingest_lag_sec} — último lag em segundos observado entre o
 *     timestamp da sample e o instante do recebimento.</li>
 *     <li>{@code missing_ratio} — última proporção de PDPs ausentes observada
 *     em um bloco fechado, em [0.0, 1.0].</li>
 * </ul>
 *
 * <p>Implementa a forma <em>canônica</em> (com {@code seriesKey}) de cada callback,
 * acumulando localmente e fazendo forward canônico. As variantes legadas delegam à
 * canônica com {@code seriesKey == null}, de modo que chamadas antigas também são
 * contabilizadas exatamente uma vez.</p>
 *
 * <p>Thread-safe (todos os campos são atômicos).</p>
 */
public final class NgrrdMetrics implements NgrrdMetricsListener {

    private final AtomicLong lateSampleCount = new AtomicLong();
    private final AtomicLong counterResetCount = new AtomicLong();
    private final AtomicLong wrapDetectedCount = new AtomicLong();
    private final AtomicLong lastIngestLagSec = new AtomicLong();
    private final AtomicReference<Double> lastMissingRatio = new AtomicReference<>(0.0);
    private final NgrrdMetricsListener forward;

    public NgrrdMetrics() {
        this(null);
    }

    public NgrrdMetrics(NgrrdMetricsListener forward) {
        this.forward = forward;
    }

    public long lateSampleCount() {
        return lateSampleCount.get();
    }

    public long counterResetCount() {
        return counterResetCount.get();
    }

    public long wrapDetectedCount() {
        return wrapDetectedCount.get();
    }

    public long lastIngestLagSec() {
        return lastIngestLagSec.get();
    }

    public double lastMissingRatio() {
        return lastMissingRatio.get();
    }

    // ------------------------------------------------------------ forma canônica

    @Override
    public void onLateSample(String seriesKey, String dsName, long latenessSec) {
        lateSampleCount.incrementAndGet();
        if (forward != null) {
            forward.onLateSample(seriesKey, dsName, latenessSec);
        }
    }

    @Override
    public void onCounterReset(String seriesKey, String dsName) {
        counterResetCount.incrementAndGet();
        if (forward != null) {
            forward.onCounterReset(seriesKey, dsName);
        }
    }

    @Override
    public void onWrapDetected(String seriesKey, String dsName) {
        wrapDetectedCount.incrementAndGet();
        if (forward != null) {
            forward.onWrapDetected(seriesKey, dsName);
        }
    }

    @Override
    public void onIngestLag(String seriesKey, String dsName, long lagSec) {
        lastIngestLagSec.set(lagSec);
        if (forward != null) {
            forward.onIngestLag(seriesKey, dsName, lagSec);
        }
    }

    @Override
    public void onBlockClosed(String seriesKey, String rraName, String dsName, double missingRatio) {
        lastMissingRatio.set(missingRatio);
        if (forward != null) {
            forward.onBlockClosed(seriesKey, rraName, dsName, missingRatio);
        }
    }

    // --------------------------------------------------------------- forma legada

    @Override
    public void onLateSample(String dsName, long latenessSec) {
        onLateSample(null, dsName, latenessSec);
    }

    @Override
    public void onCounterReset(String dsName) {
        onCounterReset(null, dsName);
    }

    @Override
    public void onWrapDetected(String dsName) {
        onWrapDetected(null, dsName);
    }

    @Override
    public void onIngestLag(String dsName, long lagSec) {
        onIngestLag(null, dsName, lagSec);
    }

    @Override
    public void onBlockClosed(String rraName, String dsName, double missingRatio) {
        onBlockClosed(null, rraName, dsName, missingRatio);
    }
}
