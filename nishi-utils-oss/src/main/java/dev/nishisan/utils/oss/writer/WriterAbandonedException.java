package dev.nishisan.utils.oss.writer;

/**
 * O {@link NgrrdWriter} foi abandonado no fechamento: o {@code close()} encontrou um force
 * destacado preso além do teto de espera e encerrou a worker sem o checkpoint final. As
 * amostras aplicadas desde o último checkpoint concluído podem ter sido perdidas, e o writer
 * não pode mais ser usado nem fechado com sucesso: todo {@code close()} seguinte, concorrente ou
 * posterior, lança esta exceção, mesmo depois de o force terminar.
 *
 * <p>É um tipo próprio para que o chamador distinga esse caso de outras falhas de fechamento e
 * descarte a referência ao writer (ou ao handle) conscientemente; a série volta a ser usável
 * por um novo open, cujo primeiro checkpoint força a região de novo.</p>
 */
public class WriterAbandonedException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    public WriterAbandonedException(String message, Throwable cause) {
        super(message, cause);
    }
}
