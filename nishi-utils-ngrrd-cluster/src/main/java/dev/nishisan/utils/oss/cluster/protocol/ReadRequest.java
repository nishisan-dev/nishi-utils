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

package dev.nishisan.utils.oss.cluster.protocol;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.ViewQuery;

import java.time.Duration;
import java.util.Objects;

/**
 * Pedido de leitura de uma série ao seu dono. Espelha {@link ViewQuery} em
 * campos primitivos (sem {@link Duration}/{@link java.time.Instant}) para um
 * round-trip estável no protocolo do cluster.
 *
 * <p>Valida os invariantes do construtor compacto de {@link ViewQuery}
 * ({@code dsName}/{@code cf} obrigatórios, {@code targetStepSec}/{@code maxPoints}
 * {@code > 0}) e, de propósito, é <strong>mais estrita</strong> num ponto:
 * {@code windowMs} também precisa ser {@code > 0} aqui, enquanto o construtor
 * compacto de {@link ViewQuery} não rejeita uma janela zero (ou negativa) — só
 * verifica que {@code window} não é {@code null}. A validação extra existe para
 * que o erro de um pedido malformado estoure no cliente, ao montar o
 * {@code ReadRequest}, e não depois de uma viagem de rede até o dono da série.</p>
 *
 * @param seriesKey            chave lógica da série
 * @param dsName               data source a ler
 * @param windowMs             janela total a recuperar, em milissegundos
 * @param targetStepSec        granularidade desejada, em segundos
 * @param cf                   função de consolidação preferida
 * @param maxPoints            limite máximo de pontos retornados
 * @param endExclusiveEpochMs  fim exclusivo da janela; {@code null} = agora
 */
public record ReadRequest(
        String seriesKey,
        String dsName,
        long windowMs,
        int targetStepSec,
        ConsolidationFunction cf,
        int maxPoints,
        Long endExclusiveEpochMs) {

    public ReadRequest {
        Objects.requireNonNull(seriesKey, "seriesKey é obrigatório");
        Objects.requireNonNull(dsName, "dsName é obrigatório");
        Objects.requireNonNull(cf, "cf é obrigatório");
        if (windowMs <= 0) {
            throw new IllegalArgumentException("windowMs deve ser > 0");
        }
        if (targetStepSec <= 0) {
            throw new IllegalArgumentException("targetStepSec deve ser > 0");
        }
        if (maxPoints <= 0) {
            throw new IllegalArgumentException("maxPoints deve ser > 0");
        }
    }

    /** Reconstrói a {@link ViewQuery} equivalente a este pedido. */
    public ViewQuery toViewQuery() {
        return new ViewQuery(Duration.ofMillis(windowMs), targetStepSec, cf, maxPoints);
    }

    /** Constrói o pedido a partir de uma {@link ViewQuery} já montada pelo cliente. */
    public static ReadRequest of(String seriesKey, String dsName, ViewQuery query, Long endExclusiveEpochMs) {
        return new ReadRequest(seriesKey, dsName, query.window().toMillis(), query.targetStepSec(),
                query.cf(), query.maxPoints(), endExclusiveEpochMs);
    }
}
