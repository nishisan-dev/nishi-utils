package dev.nishisan.utils.oss.config;

import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Expansão de placeholders {@code ${VAR}} e {@code ${VAR:default}} em strings.
 *
 * <p>Aplicado ao texto bruto do YAML antes do parse. O resolver é injetado pelo
 * chamador (tipicamente {@code System::getenv}), permitindo testes determinísticos.</p>
 *
 * <p>Comportamento:</p>
 * <ul>
 *     <li>{@code ${VAR}} sem default — se ausente, mantém o placeholder original
 *     (não lança); o validator detectará e reportará.</li>
 *     <li>{@code ${VAR:default}} — usa {@code default} quando a variável é
 *     ausente ou vazia.</li>
 *     <li>Suporta caracteres {@code [A-Za-z0-9_.-]} no nome e qualquer texto sem
 *     {@code }} no default.</li>
 * </ul>
 */
public final class VariableInterpolator {

    private static final Pattern PATTERN = Pattern.compile("\\$\\{([A-Za-z0-9_.-]+)(?::([^}]*))?\\}");

    private VariableInterpolator() {
    }

    /**
     * Substitui placeholders no texto.
     *
     * @param raw      texto original (ex.: conteúdo de um arquivo YAML)
     * @param resolver função que mapeia nome → valor (ou null se não definido)
     * @return novo texto com substituições aplicadas
     */
    public static String interpolate(String raw, Function<String, String> resolver) {
        if (raw == null || raw.isEmpty()) {
            return raw;
        }
        Matcher matcher = PATTERN.matcher(raw);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            String name = matcher.group(1);
            String fallback = matcher.group(2);
            String resolved = resolver.apply(name);
            String replacement;
            if (resolved != null && !resolved.isEmpty()) {
                replacement = resolved;
            } else if (fallback != null) {
                replacement = fallback;
            } else {
                replacement = matcher.group(0);
            }
            matcher.appendReplacement(out, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(out);
        return out.toString();
    }
}
