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

package dev.nishisan.utils.oss.cluster.client;

import dev.nishisan.utils.oss.config.NgrrdYamlLoader;

import java.util.Map;

/**
 * Resolve o {@code seriesKey} de uma série a partir do
 * {@code seriesKeyTemplate} da definição e das tags fornecidas pelo chamador.
 *
 * <p>{@link #resolve} replica <strong>exatamente</strong> a semântica de
 * {@code Ngrrd.resolveSeriesKey} ({@code nishi-utils-oss}, package-private):
 * este módulo vive num pacote diferente e não pode chamá-lo diretamente, então
 * a lógica é duplicada aqui de propósito, não reinventada — qualquer alteração
 * de semântica lá deve ser replicada aqui.</p>
 */
public final class SeriesKeyTemplate {

    private SeriesKeyTemplate() {
    }

    /**
     * Expande {@code template} substituindo cada {@code {tagName}} pelo valor
     * correspondente em {@code tags}.
     *
     * @throws IllegalArgumentException se houver um placeholder não fechado ou
     *                                   uma tag obrigatória ausente
     */
    public static String resolve(String template, Map<String, String> tags) {
        StringBuilder out = new StringBuilder(template.length());
        int i = 0;
        while (i < template.length()) {
            char c = template.charAt(i);
            if (c == '{') {
                int end = template.indexOf('}', i);
                if (end < 0) {
                    throw new IllegalArgumentException(
                            "Placeholder não fechado em seriesKeyTemplate: " + template);
                }
                String name = template.substring(i + 1, end);
                String value = tags.get(name);
                if (value == null) {
                    throw new IllegalArgumentException(
                            "Tag obrigatória ausente para seriesKey: " + name);
                }
                out.append(value);
                i = end + 1;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    /** {@code seriesKeyTemplate} declarado na definição YAML, resolvendo {@code ${VAR}} de ambiente. */
    public static String templateOf(String yaml) {
        return NgrrdYamlLoader.parse(yaml, System::getenv).spec().identity().seriesKeyTemplate();
    }
}
