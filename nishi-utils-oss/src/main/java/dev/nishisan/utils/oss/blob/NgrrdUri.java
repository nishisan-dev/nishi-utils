package dev.nishisan.utils.oss.blob;

import java.net.URI;
import java.util.Objects;

/**
 * Locator de série no namespace de blob volumes: {@code ngrrd://<volume>/<seriesPath>}.
 *
 * <p>{@code <volume>} é o nome lógico do volume (resolvido contra um
 * {@link BlobVolumeRegistry}); {@code <seriesPath>} é usado <strong>diretamente</strong>
 * como {@code seriesKey} (modo locator). É um value type imutável, sem dependência
 * de {@code java.nio.file} (o literal {@code Path.of("ngrrd://...")} não despacharia
 * para um provider custom; ver decisão de arquitetura no plano).</p>
 *
 * @param volume     nome do volume (não vazio)
 * @param seriesPath caminho lógico da série = seriesKey (não vazio, sem {@code ..})
 */
public record NgrrdUri(String volume, String seriesPath) {

    public static final String SCHEME = "ngrrd";
    private static final String PREFIX = SCHEME + "://";

    public NgrrdUri {
        Objects.requireNonNull(volume, "volume é obrigatório");
        Objects.requireNonNull(seriesPath, "seriesPath é obrigatório");
        if (volume.isBlank()) {
            throw new IllegalArgumentException("volume vazio no ngrrd URI");
        }
        if (seriesPath.isBlank()) {
            throw new IllegalArgumentException("seriesPath vazio no ngrrd URI");
        }
        if (seriesPath.startsWith("/")) {
            throw new IllegalArgumentException("seriesPath não pode começar com '/': " + seriesPath);
        }
        for (String segment : seriesPath.split("/")) {
            if (segment.equals("..")) {
                throw new IllegalArgumentException("seriesPath não pode conter '..': " + seriesPath);
            }
        }
    }

    public static NgrrdUri of(String volume, String seriesPath) {
        return new NgrrdUri(volume, seriesPath);
    }

    /** Parseia {@code ngrrd://<volume>/<seriesPath>}, normalizando barras. */
    public static NgrrdUri parse(String uri) {
        Objects.requireNonNull(uri, "uri é obrigatório");
        if (!uri.startsWith(PREFIX)) {
            throw new IllegalArgumentException("URI não usa o esquema ngrrd://: " + uri);
        }
        String rest = uri.substring(PREFIX.length());
        int slash = rest.indexOf('/');
        if (slash < 0) {
            throw new IllegalArgumentException("URI sem seriesPath: " + uri);
        }
        String volume = rest.substring(0, slash);
        String rawPath = rest.substring(slash + 1);
        int start = 0;
        int end = rawPath.length();
        while (start < end && rawPath.charAt(start) == '/') {
            start++;
        }
        while (end > start && rawPath.charAt(end - 1) == '/') {
            end--;
        }
        String path = rawPath.substring(start, end);
        if (path.isEmpty()) {
            throw new IllegalArgumentException("URI sem seriesPath: " + uri);
        }
        return new NgrrdUri(volume, path);
    }

    public static NgrrdUri parse(URI uri) {
        return parse(uri.toString());
    }

    @Override
    public String toString() {
        return PREFIX + volume + "/" + seriesPath;
    }
}
