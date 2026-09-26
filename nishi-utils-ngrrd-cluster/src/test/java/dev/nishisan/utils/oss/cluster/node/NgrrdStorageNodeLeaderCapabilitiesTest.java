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

import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.client.NodeCapabilities;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre a conferência de {@code catalog.lookup} do líder feita pelo storage antes de confirmar um
 * redirecionamento (issue #177): só a réplica local de {@code ngrrd.nodes} é lida e a resposta negativa é
 * imediata — o chamador segura um lock de coordenação e responde pela réplica local se a conferência falhar.
 */
class NgrrdStorageNodeLeaderCapabilitiesTest {

    private static final Duration MAX_WAIT = Duration.ofSeconds(2);
    private static final long FAST_MS = 500L;

    @Test
    void liderSemACapacidadeFalhaNaHora() {
        StorageNodeStatus legacy = new StorageNodeStatus("storage-leader", NodeState.ACTIVE, 0, 0, 0, 1L);
        NodeCapabilities capabilities = NgrrdStorageNode.leaderCapabilitiesFromLocal(
                id -> Optional.ofNullable(Map.of("storage-leader", legacy).get(id)));

        long startedAt = System.nanoTime();
        NgrrdClusterException e = assertThrows(NgrrdClusterException.class,
                () -> capabilities.require("storage-leader", StorageCapabilities.CATALOG_LOOKUP, MAX_WAIT));

        assertEquals(ErrorCode.UNSUPPORTED_BY_NODE, e.code());
        assertTrue(elapsedMs(startedAt) < FAST_MS, "deveria falhar sem esperar: " + elapsedMs(startedAt) + " ms");
    }

    @Test
    void liderSemStatusNaReplicaLocalFalhaNaHora() {
        NodeCapabilities capabilities = NgrrdStorageNode.leaderCapabilitiesFromLocal(id -> Optional.empty());

        long startedAt = System.nanoTime();
        NgrrdClusterException e = assertThrows(NgrrdClusterException.class,
                () -> capabilities.require("storage-leader", StorageCapabilities.CATALOG_LOOKUP, MAX_WAIT));

        assertEquals(ErrorCode.UNSUPPORTED_BY_NODE, e.code());
        assertTrue(elapsedMs(startedAt) < FAST_MS, "deveria falhar sem esperar: " + elapsedMs(startedAt) + " ms");
    }

    @Test
    void liderComACapacidadePassa() {
        StorageNodeStatus current = new StorageNodeStatus("storage-leader", NodeState.ACTIVE, 0, 0, 0, 1L,
                null, 1, 0, StorageCapabilities.ALL);
        NodeCapabilities capabilities = NgrrdStorageNode.leaderCapabilitiesFromLocal(
                id -> Optional.ofNullable(Map.of("storage-leader", current).get(id)));

        assertDoesNotThrow(() -> capabilities.require("storage-leader", StorageCapabilities.CATALOG_LOOKUP, MAX_WAIT));
    }

    private static long elapsedMs(long startedAtNanos) {
        return (System.nanoTime() - startedAtNanos) / 1_000_000L;
    }
}
