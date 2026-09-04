package dev.nishisan.utils.oss.api;

/**
 * Lançada quando uma {@link ViewQuery} não pode ser atendida por ser inválida
 * para a definição da série — distinto de "não há dados no range".
 *
 * <p>Caso principal: a {@link ConsolidationFunction} pedida não é declarada por
 * nenhuma RRA da definição. Antes esse cenário devolvia uma lista vazia de
 * pontos, indistinguível de uma janela sem amostras, e chegava ao cliente HTTP
 * como {@code 200} com arrays vazios.</p>
 */
public class NgrrdQueryException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public NgrrdQueryException(String message) {
        super(message);
    }

    public NgrrdQueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
