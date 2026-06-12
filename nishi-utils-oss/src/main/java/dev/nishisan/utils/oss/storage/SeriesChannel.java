package dev.nishisan.utils.oss.storage;

/**
 * Canal de acesso aleatório a um único objeto de série (o arquivo NGRR).
 * Abstrai a diferença entre os backends: em disco local as escritas são
 * <strong>in-place</strong> (apenas as regiões alteradas, fiel ao RRDtool); em
 * Object Storage (S3) o objeto inteiro é regravado em {@link #force()} (a imagem
 * é mantida em memória e sofre read-modify-write).
 *
 * <p>Invariante de uso: <strong>um writer por série</strong>. O reader abre seu
 * próprio canal: no S3 ele é um snapshot imutável (GET); no disco é uma visão
 * viva do mesmo arquivo — a consistência entre leitor e writer concorrentes no
 * mesmo processo é coordenada pelo {@code ReadWriteLock} compartilhado do
 * {@code NgrrdHandle}, não pelo canal.</p>
 */
public interface SeriesChannel extends AutoCloseable {

    /** Tamanho atual do objeto em bytes. */
    long size();

    /**
     * Garante que o objeto tenha exatamente {@code totalBytes} de tamanho
     * (pré-alocação). Idempotente quando já está nesse tamanho.
     */
    void allocate(long totalBytes);

    /** Lê {@code len} bytes a partir de {@code offset}. */
    byte[] readRegion(long offset, int len);

    /** Escreve {@code data} a partir de {@code offset} (in-place no disco). */
    void writeRegion(long offset, byte[] data);

    /** Torna durável o que foi escrito (fsync no disco / PUT no S3). */
    void force();

    /** Faz {@link #force()} se necessário e libera o recurso. */
    @Override
    void close();
}
