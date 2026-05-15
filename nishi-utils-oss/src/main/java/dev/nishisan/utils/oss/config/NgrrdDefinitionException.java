package dev.nishisan.utils.oss.config;

/**
 * Lançada quando uma definição ngrrd não pode ser carregada (sintaxe YAML
 * inválida, falha de IO) ou falha em alguma regra de validação semântica.
 */
public class NgrrdDefinitionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NgrrdDefinitionException(String message) {
        super(message);
    }

    public NgrrdDefinitionException(String message, Throwable cause) {
        super(message, cause);
    }
}
