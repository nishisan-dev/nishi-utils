package dev.nishisan.utils.oss.engine;

/**
 * Helpers de alinhamento temporal (epoch-aligned, padrão RRD).
 *
 * <p>Todos os métodos trabalham em milissegundos UTC e retornam valores que são
 * múltiplos exatos do passo desde 1970-01-01.</p>
 */
public final class TimeBucket {

    private TimeBucket() {
    }

    /**
     * Alinha um timestamp para o início do bucket correspondente.
     *
     * @param tsEpochMs timestamp em milissegundos
     * @param stepMs    tamanho do bucket em milissegundos
     * @return o maior múltiplo de {@code stepMs} {@code <= tsEpochMs}
     */
    public static long alignDown(long tsEpochMs, long stepMs) {
        if (stepMs <= 0) {
            throw new IllegalArgumentException("stepMs deve ser > 0");
        }
        if (tsEpochMs >= 0) {
            return tsEpochMs - (tsEpochMs % stepMs);
        }
        // suporte a timestamps negativos (antes de epoch) por completude
        long mod = tsEpochMs % stepMs;
        return mod == 0 ? tsEpochMs : tsEpochMs - mod - stepMs;
    }

    /**
     * Próximo múltiplo de {@code stepMs} estritamente maior que {@code tsEpochMs}.
     */
    public static long alignUp(long tsEpochMs, long stepMs) {
        long down = alignDown(tsEpochMs, stepMs);
        return down == tsEpochMs ? tsEpochMs + stepMs : down + stepMs;
    }

    /**
     * Quantidade de buckets entre dois timestamps já alinhados.
     */
    public static long bucketCount(long fromAlignedMs, long toExclusiveMs, long stepMs) {
        if (toExclusiveMs < fromAlignedMs) {
            return 0;
        }
        return (toExclusiveMs - fromAlignedMs) / stepMs;
    }
}
