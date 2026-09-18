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

package dev.nishisan.utils.oss.cluster.node;

import java.util.Objects;
import java.util.Optional;

/**
 * Conversão entre {@code seriesKey} e a chave física do objeto único da série no {@code BlobStorage}
 * ({@code {prefix}/{seriesKey}.ngrr}) — compartilhada por {@link StorageRequestHandler} (checagem de
 * existência sem abrir handle) e {@link LocalReconciler} (varredura do volume).
 *
 * <p>O sufixo {@code .ngrr} mirra a constante privada {@code StorageKey.SERIES_EXT} do
 * {@code nishi-utils-oss} — não exposta publicamente por aquele módulo, então duplicada aqui de
 * propósito (não pode ser importada). Qualquer mudança de convenção lá precisa ser replicada aqui.</p>
 *
 * <p>Válido só porque todas as definições servidas por um mesmo storage node são obrigadas a usar o
 * mesmo {@code seriesPrefix} (ver Javadoc de {@link StorageNodeConfig#seriesObjectPrefix()} e a
 * validação em {@link StorageRequestHandler}) — sem essa invariante, não haveria como recuperar o
 * {@code seriesKey} de volta a partir da chave física sem reabrir cada definição.</p>
 *
 * <p>BAIXO-D do Refuter: {@code prefix} é normalizado (barras iniciais/finais removidas) antes de
 * qualquer uso — {@code "series"} e {@code "series/"} (ou {@code "/series/"}) produzem exatamente a
 * mesma chave física, mesma convenção de {@code StorageNodeConfig#seriesObjectPrefix} (normalizado lá
 * também, na validação do record).</p>
 */
final class SeriesObjectKeys {

    private static final String SUFFIX = ".ngrr";

    private SeriesObjectKeys() {
    }

    /** Chave física do objeto único da série: {@code {prefix}/{seriesKey}.ngrr} ({@code prefix} normalizado). */
    static String objectKey(String prefix, String seriesKey) {
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(seriesKey, "seriesKey");
        return prefixWithSlash(prefix) + seriesKey + SUFFIX;
    }

    /** {@code prefix} normalizado com uma única barra final — usado como argumento de {@code BlobStorage#list}. */
    static String prefixWithSlash(String prefix) {
        Objects.requireNonNull(prefix, "prefix");
        return normalize(prefix) + "/";
    }

    /**
     * Extrai o {@code seriesKey} de uma chave física, se ela seguir a convenção
     * {@code {prefix}/{seriesKey}.ngrr} ({@code prefix} normalizado); {@link Optional#empty()} para
     * qualquer outra chave (ex.: snapshots de schema noutro prefixo) — silenciosamente ignorada pelo
     * chamador.
     */
    static Optional<String> seriesKeyOf(String objectKey, String prefix) {
        Objects.requireNonNull(objectKey, "objectKey");
        String withSlash = prefixWithSlash(prefix);
        if (!objectKey.startsWith(withSlash) || !objectKey.endsWith(SUFFIX)) {
            return Optional.empty();
        }
        return Optional.of(objectKey.substring(withSlash.length(), objectKey.length() - SUFFIX.length()));
    }

    /**
     * Remove barras (`/`) iniciais e finais de {@code prefix} — {@code "series"} e {@code "/series/"}
     * são o mesmo prefixo. Package-private: também usado por {@link StorageNodeConfig} para normalizar
     * {@code seriesObjectPrefix} já na validação do record (BAIXO-D do Refuter), sem duplicar a lógica.
     */
    static String normalize(String prefix) {
        int start = 0;
        int end = prefix.length();
        while (start < end && prefix.charAt(start) == '/') {
            start++;
        }
        while (end > start && prefix.charAt(end - 1) == '/') {
            end--;
        }
        return prefix.substring(start, end);
    }
}
