package dev.nishisan.utils.oss.api;

/**
 * Amostra crua entregue à engine na ingestão.
 *
 * @param tsEpochMs timestamp em milissegundos desde epoch UTC
 * @param value     valor observado; pode ser {@link Double#NaN} para representar
 *                  ausência explícita
 */
public record Sample(long tsEpochMs, double value) {
}
