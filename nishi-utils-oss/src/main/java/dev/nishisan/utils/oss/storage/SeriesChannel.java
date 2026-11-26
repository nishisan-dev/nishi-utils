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

    /**
     * Prepara o force: captura o que precisa ser forçado e limpa a marca de sujo.
     * Chamado só pela thread do writer (a mesma que chama {@link #writeRegion}).
     *
     * <p>O padrão faz o {@link #force()} inline, na thread chamadora, e devolve
     * {@link ForceTicket#DONE}: é o caso do disco local e do S3. O S3 nunca sai da
     * thread do writer, porque o PUT relê a imagem em memória, que o writer continua
     * mutando. Um canal que devolve um ticket destacado ({@link ForceTicket#detached()})
     * permite ao writer executar o force em outra thread.</p>
     *
     * @return {@link ForceTicket#DONE} se o force já foi feito (ou não era
     *         necessário); senão, o ticket com o force capturado
     */
    default ForceTicket prepareForce() {
        force();
        return ForceTicket.DONE;
    }

    /** Faz {@link #force()} se necessário e libera o recurso. */
    @Override
    void close();

    /**
     * Force capturado por {@link #prepareForce()}, executável fora da thread do
     * writer.
     */
    interface ForceTicket {

        /** Ticket de um force já concluído (ou desnecessário): {@link #run()} não faz nada. */
        ForceTicket DONE = () -> {
        };

        /**
         * Executa o force capturado; pode rodar em qualquer thread. Em falha, a
         * implementação volta a marcar o canal como sujo antes de relançar, para que
         * o próximo force (ou o {@code close()}) tente de novo.
         */
        void run();

        /** Indica se o force ainda precisa ser executado por {@link #run()}. */
        default boolean detached() {
            return this != DONE;
        }
    }
}
