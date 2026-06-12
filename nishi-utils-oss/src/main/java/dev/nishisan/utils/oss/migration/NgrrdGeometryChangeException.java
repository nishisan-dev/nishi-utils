package dev.nishisan.utils.oss.migration;

/**
 * Lançada quando, ao abrir uma série, a geometria diverge da gravada e a
 * política/disciplina configurada impede prosseguir sem perda de dados:
 * {@code onGeometryChange=FAIL}, mudança não acompanhada de incremento de
 * {@code schemaRevision}, ou {@code MIGRATE} sobre uma mudança que exige
 * reamostragem.
 *
 * <p>A mensagem carrega o relatório legível do que mudou (ver
 * {@link GeometryChangeReport}).</p>
 */
public class NgrrdGeometryChangeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NgrrdGeometryChangeException(String message) {
        super(message);
    }

    public NgrrdGeometryChangeException(String message, Throwable cause) {
        super(message, cause);
    }
}
