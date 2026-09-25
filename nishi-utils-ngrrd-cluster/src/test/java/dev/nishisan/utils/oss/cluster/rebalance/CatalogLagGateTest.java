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

package dev.nishisan.utils.oss.cluster.rebalance;

import dev.nishisan.utils.oss.cluster.catalog.CatalogReplicaStatus;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CatalogLagGateTest {

    private static final long MAX_LAG = 1_000L;

    @Test
    void noSemCampoDeVersaoAnteriorEhElegivel() {
        assertEquals(Optional.empty(), CatalogLagGate.exclusionReason(status(null), MAX_LAG));
    }

    @Test
    void liderEReplicaEmDiaSaoElegiveis() {
        assertEquals(Optional.empty(), CatalogLagGate.exclusionReason(status(CatalogReplicaStatus.ofLeader()), 0L));
        assertEquals(Optional.empty(), CatalogLagGate.exclusionReason(status(follower(1_000L)), MAX_LAG));
        assertEquals(Optional.empty(), CatalogLagGate.exclusionReason(status(follower(0L)), 0L));
    }

    @Test
    void lagAcimaDoLimiteExcluiComOMotivo() {
        assertEquals(Optional.of("lag=12345>1000"),
                CatalogLagGate.exclusionReason(status(follower(12_345L)), MAX_LAG));
        assertEquals(Optional.of("lag=1>0"), CatalogLagGate.exclusionReason(status(follower(1L)), 0L));
    }

    @Test
    void lagDesconhecidoSincronizacaoEBootstrapExcluem() {
        assertEquals(Optional.of("lag desconhecido"),
                CatalogLagGate.exclusionReason(status(CatalogReplicaStatus.from(false, null)), MAX_LAG));
        assertEquals(Optional.of("sincronizando"), CatalogLagGate.exclusionReason(
                status(new CatalogReplicaStatus(false, 0L, 50L, 51L, true, false, false)), MAX_LAG));
        assertEquals(Optional.of("bootstrap pendente"), CatalogLagGate.exclusionReason(
                status(new CatalogReplicaStatus(false, 0L, 50L, 51L, false, true, false)), MAX_LAG));
    }

    @Test
    void limiteNegativoDesligaAPorta() {
        assertEquals(Optional.empty(), CatalogLagGate.exclusionReason(status(follower(99_999L)), -1L));
        assertEquals(Optional.empty(),
                CatalogLagGate.exclusionReason(status(CatalogReplicaStatus.from(false, null)), -1L));
    }

    private static CatalogReplicaStatus follower(long lag) {
        return new CatalogReplicaStatus(false, lag, 50_000L, 50_001L - lag, false, false, true);
    }

    private static StorageNodeStatus status(CatalogReplicaStatus replica) {
        return new StorageNodeStatus("storage-x", NodeState.ACTIVE, 0, 0, 0, 1L, DistributionMode.COUNT, 1, 0,
                StorageCapabilities.ALL, replica);
    }
}
