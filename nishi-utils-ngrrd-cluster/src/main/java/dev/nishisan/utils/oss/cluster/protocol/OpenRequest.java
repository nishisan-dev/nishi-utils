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

import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;

import java.util.Map;
import java.util.Objects;

/**
 * Pedido ao dono para abrir (ou confirmar aberta) uma série local.
 *
 * @param seriesKey         chave lógica da série
 * @param yaml              definição YAML da série
 * @param tags              tags associadas à série; nunca {@code null} (vazio quando ausente)
 * @param durability        política de durabilidade do writer
 * @param onGeometryChange  política aplicada quando a geometria da definição diverge da gravada
 * @param placementHint     placement retornado pelo {@code ngrrd.place} que originou este open —
 *                          permite ao dono aceitar a requisição mesmo que seu catálogo local ainda
 *                          não tenha replicado a entrada recém-criada pelo líder
 * @param createIfMissing   {@code true} cria a série quando ausente (comportamento atual);
 *                          {@code false} exige que a série já exista, respondendo
 *                          {@link SeriesStatus#NOT_FOUND} caso contrário; {@code null} equivale a
 *                          {@code true} — compatível com clientes anteriores a esta issue, que não
 *                          enviam o campo. Use {@link #createIfMissingOrDefault()} para normalizar.
 */
public record OpenRequest(
        String seriesKey,
        String yaml,
        Map<String, String> tags,
        Durability durability,
        OnGeometryChange onGeometryChange,
        SeriesPlacement placementHint,
        Boolean createIfMissing) {

    public OpenRequest {
        tags = Map.copyOf(Objects.requireNonNullElse(tags, Map.of()));
    }

    /**
     * Construtor de compatibilidade com a assinatura de 6 argumentos anterior a esta issue —
     * {@code createIfMissing} fica {@code null} (equivalente a {@code true}).
     */
    public OpenRequest(String seriesKey, String yaml, Map<String, String> tags, Durability durability,
            OnGeometryChange onGeometryChange, SeriesPlacement placementHint) {
        this(seriesKey, yaml, tags, durability, onGeometryChange, placementHint, null);
    }

    /**
     * Normaliza {@link #createIfMissing()}: {@code null} (cliente antigo, sem o campo) equivale a
     * {@code true}.
     */
    public boolean createIfMissingOrDefault() {
        return createIfMissing == null || createIfMissing;
    }
}
