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

package dev.nishisan.utils.oss.cluster.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Resultado de {@link NgrrdClusterClient#release(java.util.Collection, ReleaseOptions)}: um desfecho por chave pedida
 * (chaves repetidas aparecem uma vez), na ordem em que foram pedidas.
 *
 * <p>Uso na entrega de partições entre clientes: confirme o offset da partição só se {@link #allReleased()}. Senão,
 * não confirme (ou confirme até a última marca concluída sem falha) e registre {@link #notReleased()}.</p>
 *
 * <p>Releases sobrepostos da mesma série (no mesmo cliente) veem, cada um, as falhas que a sua fronteira cobre: a
 * mesma falha aparece em todo release ainda ativo cuja fronteira a cubra, e as contagens não devem ser somadas entre
 * releases. Um release que começa depois de outro já ter colhido a série não vê a falha de novo, e o release que
 * admitiu a escrita sempre a recebe.</p>
 *
 * @param series desfecho por chave de série
 * @since 8.15.0
 */
public record ReleaseResult(Map<String, SeriesRelease> series) {

    public ReleaseResult {
        Objects.requireNonNull(series, "series");
        series.forEach((key, release) -> {
            Objects.requireNonNull(key, "series key");
            Objects.requireNonNull(release, "series release");
        });
        series = Collections.unmodifiableMap(new LinkedHashMap<>(series));
    }

    /** {@code true} se toda série terminou {@link ReleaseStatus#RELEASED} ou {@link ReleaseStatus#NOT_HELD}. */
    public boolean allReleased() {
        return notReleased().isEmpty();
    }

    /** Chaves cujo desfecho não é {@link ReleaseStatus#RELEASED} nem {@link ReleaseStatus#NOT_HELD}, na ordem pedida. */
    public Set<String> notReleased() {
        Set<String> keys = new LinkedHashSet<>();
        series.forEach((key, release) -> {
            if (release.status() != ReleaseStatus.RELEASED && release.status() != ReleaseStatus.NOT_HELD) {
                keys.add(key);
            }
        });
        return Collections.unmodifiableSet(keys);
    }

    /**
     * Soma de {@link SeriesRelease#failedSamples()} de todas as séries deste resultado. Com releases concorrentes da
     * mesma série, a mesma falha aparece em cada um deles: não some esta contagem entre releases.
     */
    public long failedSamples() {
        long total = 0L;
        for (SeriesRelease release : series.values()) {
            total += release.failedSamples();
        }
        return total;
    }
}
