/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.oss.cluster.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dev.nishisan.utils.oss.config.VariableInterpolator;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Suporte compartilhado por {@code StorageNodeConfig.fromYaml}/{@code NgrrdClusterConfig.fromYaml}:
 * o {@link ObjectMapper} YAML (com módulo jsr310) e o parser tolerante de {@link Duration} usado nos
 * dois YAMLs deste módulo — mesma convenção de sufixos de {@code NGridConfigLoader} do core
 * ({@code 10s}, {@code 500ms}, {@code 5m}, {@code 2h}, {@code 1d}), com fallback para ISO-8601
 * ({@code PT10S}).
 */
public final class NgrrdYamlSupport {

    private static final Pattern SUFFIX_PATTERN = Pattern.compile("(\\d+)\\s*(ms|s|m|h|d)");

    private static final ObjectMapper MAPPER;

    static {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER = mapper;
    }

    private NgrrdYamlSupport() {
    }

    /** {@link ObjectMapper} YAML compartilhado — nunca reconfigurar fora desta classe. */
    public static ObjectMapper mapper() {
        return MAPPER;
    }

    /** Expande {@code ${VAR}}/{@code ${VAR:default}} no texto bruto do YAML. */
    public static String interpolate(String raw, Function<String, String> envResolver) {
        return VariableInterpolator.interpolate(raw, envResolver);
    }

    /**
     * Converte uma duração textual em {@link Duration}: sufixos {@code ms/s/m/h/d} (ex.: {@code 10s},
     * {@code 500ms}, {@code 5m}), ou ISO-8601 ({@code PT10S}) como fallback. {@code null}/vazio devolve
     * {@code null} (o chamador decide o default).
     *
     * @throws IllegalArgumentException se {@code raw} não é vazio nem reconhecido em nenhum dos dois formatos
     */
    public static Duration duration(String raw, String fieldName) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String trimmed = raw.trim();
        Matcher matcher = SUFFIX_PATTERN.matcher(trimmed.toLowerCase(java.util.Locale.ROOT));
        if (matcher.matches()) {
            long value = Long.parseLong(matcher.group(1));
            return switch (matcher.group(2)) {
                case "ms" -> Duration.ofMillis(value);
                case "s" -> Duration.ofSeconds(value);
                case "m" -> Duration.ofMinutes(value);
                case "h" -> Duration.ofHours(value);
                case "d" -> Duration.ofDays(value);
                default -> throw new IllegalArgumentException(fieldName + ": unidade de duração desconhecida em '"
                        + raw + "'");
            };
        }
        try {
            return Duration.parse(trimmed.toUpperCase(java.util.Locale.ROOT));
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(fieldName + ": duração inválida: '" + raw
                    + "' (use algo como '10s', '500ms', '5m', '2h' ou ISO-8601 'PT10S')", e);
        }
    }
}
