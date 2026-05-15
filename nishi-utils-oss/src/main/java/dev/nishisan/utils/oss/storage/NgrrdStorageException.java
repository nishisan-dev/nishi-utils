package dev.nishisan.utils.oss.storage;

/**
 * Sinalizada quando uma operação de storage falha (IO, rede, key inexistente
 * quando esperada, etc.). Embrulha exceções específicas do backend.
 */
public class NgrrdStorageException extends RuntimeException {

    public NgrrdStorageException(String message) {
        super(message);
    }

    public NgrrdStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
