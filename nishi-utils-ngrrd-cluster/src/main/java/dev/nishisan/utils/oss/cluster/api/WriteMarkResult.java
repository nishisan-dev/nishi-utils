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

import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Desfecho de uma {@link WriteMark}: o que aconteceu com as escritas admitidas até a fronteira da marca e
 * ainda não reportadas por uma marca anterior.
 *
 * @param markId                identificador da marca ({@link WriteMark#id()})
 * @param samplesAdmitted       amostras admitidas no buffer entre a marca anterior e esta. <strong>Aproximado
 *                              na fronteira</strong>: uma escrita admitida em paralelo ao {@code mark()} pode
 *                              ser contada na janela vizinha. Serve para métrica e taxa de erro, não para
 *                              conciliação exata
 * @param samplesFailed         amostras com resposta final diferente de {@code OK} atribuídas a esta marca —
 *                              exato. Um {@code ERROR} parcial do nó conta o grupo inteiro da série no lote
 *                              (o nó pode ter gravado as primeiras), então é um teto de amostras perdidas
 * @param failuresByStatus      {@code samplesFailed} por status final; uma geração substituída conta como
 *                              {@link SeriesStatus#SERIES_DELETED}
 * @param failedSeriesSample    até {@value #MAX_FAILED_SERIES_SAMPLE} séries distintas com falha, na ordem em
 *                              que as falhas foram atribuídas
 * @param failedSeriesTruncated {@code true} se houve mais séries com falha do que as listadas
 * @since 8.12.0
 */
public record WriteMarkResult(long markId, long samplesAdmitted, long samplesFailed,
        Map<SeriesStatus, Long> failuresByStatus, List<String> failedSeriesSample, boolean failedSeriesTruncated) {

    /** Teto de {@link #failedSeriesSample()}. */
    public static final int MAX_FAILED_SERIES_SAMPLE = 100;

    public WriteMarkResult {
        if (samplesAdmitted < 0) {
            throw new IllegalArgumentException("samplesAdmitted deve ser >= 0: " + samplesAdmitted);
        }
        if (samplesFailed < 0) {
            throw new IllegalArgumentException("samplesFailed deve ser >= 0: " + samplesFailed);
        }
        failuresByStatus = Map.copyOf(Objects.requireNonNull(failuresByStatus, "failuresByStatus"));
        failedSeriesSample = List.copyOf(Objects.requireNonNull(failedSeriesSample, "failedSeriesSample"));
        if (failedSeriesSample.size() > MAX_FAILED_SERIES_SAMPLE) {
            throw new IllegalArgumentException("failedSeriesSample acima de " + MAX_FAILED_SERIES_SAMPLE);
        }
    }

    /** {@code true} se nenhuma escrita atribuída a esta marca falhou. */
    public boolean succeeded() {
        return samplesFailed == 0;
    }
}
