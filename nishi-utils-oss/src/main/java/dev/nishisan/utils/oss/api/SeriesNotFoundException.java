package dev.nishisan.utils.oss.api;

/**
 * Lançada ao abrir uma série com {@code createIfMissing=false} (ver
 * {@code Ngrrd.OpenOptions}) quando ela não existe no storage.
 *
 * <p><strong>Não</strong> é falha de transporte, conectividade ou timeout — um
 * {@code catch} dessas categorias nunca deve capturá-la por engano. Sinaliza
 * exatamente o caso "consultado com sucesso, série ausente".</p>
 */
public class SeriesNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String seriesKey;

    public SeriesNotFoundException(String seriesKey) {
        this(seriesKey, "Série inexistente: " + seriesKey);
    }

    public SeriesNotFoundException(String seriesKey, String message) {
        super(message);
        this.seriesKey = seriesKey;
    }

    /** Chave lógica da série que não foi encontrada. */
    public String seriesKey() {
        return seriesKey;
    }
}
