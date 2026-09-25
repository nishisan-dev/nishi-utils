package dev.nishisan.utils.oss.api;

import java.util.Objects;

/**
 * Lançada ao abrir (ou ler, no cluster) uma série com {@code createIfMissing=false} (ver
 * {@code Ngrrd.OpenOptions}) quando ela não existe. {@link #reason()} diz de onde veio a ausência.
 *
 * <p><strong>Não</strong> é falha de transporte, conectividade ou timeout — um
 * {@code catch} dessas categorias nunca deve capturá-la por engano. Sinaliza
 * exatamente o caso "consultado com sucesso, série ausente".</p>
 */
public class SeriesNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /** Origem da ausência confirmada. */
    public enum Reason {
        /**
         * Cluster: o líder confirmou que não há placement para a chave — a série não existe no catálogo
         * do cluster.
         */
        NOT_PLACED,
        /**
         * Cluster: há placement para a chave e o dono confirmou (consultando o líder) que o arquivo da
         * série não existe no storage. É uma inconsistência do cluster (ex.: disco perdido ou cliente que
         * caiu entre o posicionamento e a criação) — NÃO significa que a série deixou de existir no
         * catálogo.
         */
        MISSING_ON_OWNER,
        /** Modo local: o objeto da série não existe no storage. */
        ABSENT
    }

    private final String seriesKey;
    private final Reason reason;

    public SeriesNotFoundException(String seriesKey, Reason reason) {
        this(seriesKey, reason, "Série inexistente: " + seriesKey + " (" + reason + ")");
    }

    public SeriesNotFoundException(String seriesKey, Reason reason, String message) {
        super(message);
        this.seriesKey = seriesKey;
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    /** Chave lógica da série que não foi encontrada. */
    public String seriesKey() {
        return seriesKey;
    }

    /** De onde veio a ausência — ver {@link Reason}. */
    public Reason reason() {
        return reason;
    }
}
