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

package dev.nishisan.utils.oss.cluster.placement;

import java.util.Optional;

/**
 * Decide, entre os storage nodes candidatos, qual deve receber uma nova série.
 * Só o líder invoca — decisão baseada na visão local (mais atual) do catálogo.
 * Interface isolada para permitir trocar a estratégia em testes.
 */
public interface PlacementPolicy {

    /**
     * Escolhe o storage node alvo para uma nova série.
     *
     * @param ctx contexto com o status conhecido dos nós e a carga pendente
     * @return o {@code nodeId} escolhido, ou {@link Optional#empty()} se nenhum
     *         candidato está disponível
     */
    Optional<String> choose(PlacementContext ctx);
}
