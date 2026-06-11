package dev.nishisan.utils.oss.storage;

/**
 * Capacidade de um {@link NgrrdStorage} de abrir um {@link SeriesChannel} para o
 * objeto único da série. Implementada por {@code LocalDiskStorage} (FileChannel
 * in-place) e {@code S3Storage} (imagem em memória + PUT).
 */
public interface SeriesChannelProvider {

    /** Abre (criando se necessário) o canal do objeto de série em {@code key}. */
    SeriesChannel openSeries(String key);

    /** Indica se o objeto de série em {@code key} já existe. */
    boolean seriesExists(String key);
}
