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

import java.util.Objects;
import java.util.Set;

/**
 * Resposta de {@link Commands#SERIES_EXISTS_BATCH}.
 *
 * @param status  {@link SeriesStatus#OK} em caso de sucesso; {@link SeriesStatus#ERROR} para falha
 *                de aplicação
 * @param present chaves do pedido que existem fisicamente no volume local do respondente; uma chave do
 *                pedido ausente deste conjunto significa que o respondente não a possui. {@code null}
 *                só ocorre numa resposta malformada (ex.: campo ausente na deserialização de um payload
 *                de outra versão) — nunca produzido por {@link #ok(Set)}, e o cliente trata esse caso
 *                como falha da página, nunca como "todas as chaves ausentes"
 * @param message detalhe legível do erro, ou {@code null}
 */
public record SeriesExistsBatchResponse(SeriesStatus status, Set<String> present, String message) {

    /** Resposta de sucesso, com o subconjunto de chaves presentes no volume local; {@code present} nunca {@code null}. */
    public static SeriesExistsBatchResponse ok(Set<String> present) {
        return new SeriesExistsBatchResponse(SeriesStatus.OK, Set.copyOf(Objects.requireNonNull(present, "present")),
                null);
    }

    /** Resposta de falha de aplicação. */
    public static SeriesExistsBatchResponse error(String message) {
        return new SeriesExistsBatchResponse(SeriesStatus.ERROR, Set.of(), message);
    }
}
