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

    @Override
    public void onLateSample(String dsName, long latenessSec) {
        lateSampleCount.incrementAndGet();
        if (forward != null) {
            forward.onLateSample(dsName, latenessSec);
        }
    }

    @Override
    public void onCounterReset(String dsName) {
        counterResetCount.incrementAndGet();
        if (forward != null) {
            forward.onCounterReset(dsName);
        }
    }

    @Override
    public void onWrapDetected(String dsName) {
        wrapDetectedCount.incrementAndGet();
        if (forward != null) {
            forward.onWrapDetected(dsName);
        }
    }

    @Override
    public void onIngestLag(String dsName, long lagSec) {
        lastIngestLagSec.set(lagSec);
        if (forward != null) {
            forward.onIngestLag(dsName, lagSec);
        }
    }

    @Override
    public void onBlockClosed(String rraName, String dsName, double missingRatio) {
        lastMissingRatio.set(missingRatio);
        if (forward != null) {
            forward.onBlockClosed(rraName, dsName, missingRatio);
        }
    }
}
