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

package dev.nishisan.utils.oss.cluster.client;

import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Cobre {@link NodeCapabilities}: réplica local primeiro, leitura forte só com o status ausente. */
class NodeCapabilitiesTest {

    private final AtomicInteger strongReads = new AtomicInteger();

    @Test
    void statusLocalComACapacidadeSegueSemLeituraForte() {
        NodeCapabilities capabilities = new NodeCapabilities(
                id -> Optional.of(CapabilityFixtures.status(id, StorageCapabilities.ALL)), this::noStrongStatus);

        assertDoesNotThrow(() -> capabilities.require("storage-a", StorageCapabilities.CATALOG_LOOKUP));
        assertEquals(0, strongReads.get());
    }

    @Test
    void statusLocalSemACapacidadeFalhaComUnsupportedByNode() {
        NodeCapabilities capabilities = new NodeCapabilities(
                id -> Optional.of(CapabilityFixtures.status(id, Set.of())), this::noStrongStatus);

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> capabilities.require("storage-a", StorageCapabilities.CATALOG_LOOKUP));

        assertEquals(ErrorCode.UNSUPPORTED_BY_NODE, ex.code());
        assertTrue(ex.getMessage().contains("storage-a"), ex.getMessage());
        assertTrue(ex.getMessage().contains("catalog.lookup"), ex.getMessage());
        assertEquals(0, strongReads.get(), "o status local presente decide sozinho");
    }

    @Test
    void statusLocalAusenteConfereComUmaLeituraForte() {
        NodeCapabilities capabilities = new NodeCapabilities(id -> Optional.empty(), id -> {
            strongReads.incrementAndGet();
            return Optional.of(CapabilityFixtures.status(id, StorageCapabilities.ALL));
        });

        assertDoesNotThrow(() -> capabilities.require("storage-a", StorageCapabilities.OPEN_CREATE_IF_MISSING));
        assertEquals(1, strongReads.get());
    }

    @Test
    void statusAusenteTambemNoLiderFalhaComUnsupportedByNode() {
        NodeCapabilities capabilities = new NodeCapabilities(id -> Optional.empty(), this::noStrongStatus);

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> capabilities.require("storage-a", StorageCapabilities.OPEN_CREATE_IF_MISSING));

        assertEquals(ErrorCode.UNSUPPORTED_BY_NODE, ex.code());
        assertEquals(1, strongReads.get());
    }

    @Test
    void falhaDaLeituraForteNaoViraUnsupportedByNode() {
        NodeCapabilities capabilities = new NodeCapabilities(id -> Optional.empty(), id -> {
            throw new IllegalStateException("sem líder");
        });

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> capabilities.require("storage-a", StorageCapabilities.CATALOG_LOOKUP));

        assertNotEquals(ErrorCode.UNSUPPORTED_BY_NODE, ex.code(), "sem confirmar o status, não se sabe se falta");
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    private Optional<StorageNodeStatus> noStrongStatus(String nodeId) {
        strongReads.incrementAndGet();
        return Optional.empty();
    }
}
