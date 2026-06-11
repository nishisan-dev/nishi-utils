/**
 * Leitura e materialização de séries do ngrrd.
 *
 * <p>{@link dev.nishisan.utils.oss.reader.NgrrdReader} resolve a melhor RRA via
 * {@code BestFitSelector}, lê os ring buffers diretamente do objeto único da
 * série ({@code SeriesChannel}, sem manifesto), reconstrói os timestamps pelo
 * ponteiro do ring e devolve um {@code SeriesResult} já recortado para a janela
 * solicitada e respeitando {@code ViewQuery.maxPoints}.</p>
 *
 * <p>{@link dev.nishisan.utils.oss.reader.ViewExecutor} traduz
 * {@code PresetDef} em {@code ViewQuery} e dispara as leituras dos DSs
 * listados no preset.</p>
 */
package dev.nishisan.utils.oss.reader;
