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

import java.util.Map;
import java.util.Objects;

/**
 * Resultado de {@link NgrrdClusterClient#triggerRebalance()}: o que o líder planejou no ciclo disparado.
 *
 * @param planned              movimentos planejados; {@code -1} quando desconhecido (implementação do
 *                             cliente sem acesso às contagens — ver {@link #unknown()})
 * @param started              movimentos submetidos às migrações; {@code -1} quando desconhecido
 * @param excludedDestinations nós excluídos como destino neste ciclo por causa da réplica do catálogo
 *                             (issue #177), com o motivo; vazio quando nenhum foi excluído ou o líder é
 *                             anterior à 8.7.0; nunca {@code null}
 */
public record RebalanceTrigger(int planned, int started, Map<String, String> excludedDestinations) {

    private static final RebalanceTrigger UNKNOWN = new RebalanceTrigger(-1, -1, Map.of());

    public RebalanceTrigger {
        excludedDestinations = Map.copyOf(Objects.requireNonNullElse(excludedDestinations, Map.of()));
    }

    /** Disparo confirmado sem as contagens (default de {@link NgrrdClusterClient#triggerRebalance()}). */
    public static RebalanceTrigger unknown() {
        return UNKNOWN;
    }

    /** Se {@link #planned()} e {@link #started()} são conhecidos. */
    public boolean countsKnown() {
        return planned >= 0 && started >= 0;
    }
}
