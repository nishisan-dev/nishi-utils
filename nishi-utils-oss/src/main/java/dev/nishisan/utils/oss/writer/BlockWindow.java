package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.engine.PrimaryDataPoint;

import java.util.HashMap;
import java.util.Map;

/**
 * Estado mutável do bloco temporal aberto no {@code NgrrdWriter}. Mantém um
 * array de {@link PrimaryDataPoint} (um por slot de step base) para cada DS
 * derivado, plus o último valor de counter por DS raw para alimentar o
 * {@code CounterDeriver} entre amostras.
 *
 * <p>Esta classe não é thread-safe — apenas a worker thread do writer a acessa.</p>
 */
final class BlockWindow {

    private final long blockStartEpochSec;
    private final long blockEndExclusiveEpochSec;
    private final int baseStepSec;
    private final int slots;
    private final Map<String, PrimaryDataPoint[]> bucketsByDerivedDs = new HashMap<>();
    private final Map<String, CounterPrev> counterPrevByRawDs = new HashMap<>();

    BlockWindow(long blockStartEpochSec, int baseStepSec, int blockSizeSec) {
        if (blockStartEpochSec % baseStepSec != 0) {
            throw new IllegalArgumentException(
                    "blockStartEpochSec=" + blockStartEpochSec + " não alinhado a baseStepSec=" + baseStepSec);
        }
        if (blockSizeSec % baseStepSec != 0) {
            throw new IllegalArgumentException(
                    "blockSizeSec=" + blockSizeSec + " não múltiplo de baseStepSec=" + baseStepSec);
        }
        this.blockStartEpochSec = blockStartEpochSec;
        this.blockEndExclusiveEpochSec = blockStartEpochSec + blockSizeSec;
        this.baseStepSec = baseStepSec;
        this.slots = blockSizeSec / baseStepSec;
    }

    long blockStartEpochSec() {
        return blockStartEpochSec;
    }

    long blockEndExclusiveEpochSec() {
        return blockEndExclusiveEpochSec;
    }

    int slots() {
        return slots;
    }

    boolean contains(long tsEpochSec) {
        return tsEpochSec >= blockStartEpochSec && tsEpochSec < blockEndExclusiveEpochSec;
    }

    int slotIndexFor(long tsEpochSec) {
        long delta = tsEpochSec - blockStartEpochSec;
        return (int) (delta / baseStepSec);
    }

    PrimaryDataPoint[] bucketsFor(String derivedDsName) {
        return bucketsByDerivedDs.computeIfAbsent(derivedDsName, name -> {
            PrimaryDataPoint[] arr = new PrimaryDataPoint[slots];
            for (int i = 0; i < slots; i++) {
                arr[i] = new PrimaryDataPoint();
            }
            return arr;
        });
    }

    void addSampleDerived(String derivedDsName, long tsEpochSec, double value) {
        int slot = slotIndexFor(tsEpochSec);
        bucketsFor(derivedDsName)[slot].add(value);
    }

    /**
     * Instala um array de PDPs já reconstituído para um DS derivado (usado na
     * reidratação do estado durável no modo incremental).
     */
    void restoreBucket(String derivedDsName, PrimaryDataPoint[] pdps) {
        if (pdps.length != slots) {
            throw new IllegalArgumentException(
                    "PDPs=" + pdps.length + " incompatível com slots=" + slots + " em " + derivedDsName);
        }
        bucketsByDerivedDs.put(derivedDsName, pdps);
    }

    CounterPrev counterPrev(String rawDsName) {
        return counterPrevByRawDs.get(rawDsName);
    }

    void setCounterPrev(String rawDsName, CounterPrev prev) {
        counterPrevByRawDs.put(rawDsName, prev);
    }

    Map<String, PrimaryDataPoint[]> bucketsSnapshot() {
        return bucketsByDerivedDs;
    }

    Map<String, CounterPrev> counterPrevSnapshot() {
        return counterPrevByRawDs;
    }

    /** Último valor observado de um counter raw, para alimentar a próxima derivação. */
    record CounterPrev(double value, long tsEpochMs) {
    }
}
