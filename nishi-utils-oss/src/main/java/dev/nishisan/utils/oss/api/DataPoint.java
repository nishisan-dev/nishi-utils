package dev.nishisan.utils.oss.api;

/**
 * Ponto consolidado retornado por {@link dev.nishisan.utils.oss.api.SeriesResult}.
 *
 * @param tsEpochMs timestamp do CDP em milissegundos desde epoch UTC
 * @param value     valor consolidado; {@link Double#NaN} = unknown/missing
 */
public record DataPoint(long tsEpochMs, double value) {
}
