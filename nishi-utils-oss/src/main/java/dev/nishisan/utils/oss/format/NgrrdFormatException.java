package dev.nishisan.utils.oss.format;

/**
 * Sinalizada quando um bloco ou manifesto serializado é inválido (magic errado,
 * versão não suportada, CRC32 divergente, payload truncado, etc.).
 */
public class NgrrdFormatException extends RuntimeException {

    public NgrrdFormatException(String message) {
        super(message);
    }

    public NgrrdFormatException(String message, Throwable cause) {
        super(message, cause);
    }
}
