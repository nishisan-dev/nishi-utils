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

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.api.SeriesInfo;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.protocol.CatalogLookupRequest;
import dev.nishisan.utils.oss.cluster.protocol.CatalogLookupResponse;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link SeriesExistence} (lógica de {@code exists}/{@code find} do cliente) com
 * {@link PlacementLookup} fake e {@link CatalogLookupClient} real sobre {@link RecordingClusterRpc}
 * — sem {@code NGridNode} real.
 */
class ClientExistenceTest {

    private static final Duration MAX_WAIT = Duration.ofSeconds(2);
    private static final NodeId LEADER = NodeId.of("storage-leader");

    private RecordingClusterRpc rpc;
    private FakePlacementLookup placementLookup;
    private SeriesExistence existence;

    private void newExistence(int batchSize) {
        newExistence(batchSize, CapabilityFixtures.advertisingAll());
    }

    private void newExistence(int batchSize, NodeCapabilities capabilities) {
        rpc = new RecordingClusterRpc(NodeId.of("client-under-test"));
        rpc.leader(LEADER);
        placementLookup = new FakePlacementLookup();
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(5), Duration.ofMillis(50));
        CatalogLookupClient catalogLookupClient = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), batchSize,
                capabilities);
        existence = new SeriesExistence(placementLookup, catalogLookupClient);
    }

    @Test
    void existsEFindComLiderSemCatalogLookupFalhamRapidoSemRpc() {
        newExistence(2_000, CapabilityFixtures.advertising(Set.of(StorageCapabilities.OPEN_CREATE_IF_MISSING)));

        NgrrdClusterException onExists = assertThrows(NgrrdClusterException.class,
                () -> existence.exists("m1", MAX_WAIT));
        NgrrdClusterException onBatch = assertThrows(NgrrdClusterException.class,
                () -> existence.exists(List.of("m1", "m2"), MAX_WAIT));
        NgrrdClusterException onFind = assertThrows(NgrrdClusterException.class,
                () -> existence.find("m1", MAX_WAIT));

        for (NgrrdClusterException ex : List.of(onExists, onBatch, onFind)) {
            assertEquals(ErrorCode.UNSUPPORTED_BY_NODE, ex.code());
            assertTrue(ex.getMessage().contains(LEADER.value()), ex.getMessage());
            assertTrue(ex.getMessage().contains(StorageCapabilities.CATALOG_LOOKUP), ex.getMessage());
        }
        assertTrue(rpc.calls().isEmpty(), "nenhum CATALOG_LOOKUP a um líder que não o anuncia");
    }

    @Test
    void existsComStatusDoLiderAusenteLocalmenteConfereNaLeituraForteESegue() {
        AtomicInteger strongReads = new AtomicInteger();
        newExistence(2_000, new NodeCapabilities(nodeId -> Optional.empty(), nodeId -> {
            strongReads.incrementAndGet();
            return Optional.of(CapabilityFixtures.status(nodeId, StorageCapabilities.ALL));
        }));
        rpc.respondDefault((cmd, body) -> CatalogLookupResponse.ok(
                Map.of("m1", SeriesPlacement.active("storage-a", 1L))));

        assertTrue(existence.exists("m1", MAX_WAIT));
        assertEquals(1, strongReads.get());
        assertEquals(1, rpc.calls().size());
    }

    @Test
    void existsSoComHitsLocaisNaoConfereCapacidade() {
        newExistence(2_000, CapabilityFixtures.unused());
        placementLookup.cache("s1", SeriesPlacement.active("storage-a", 1L));

        assertTrue(existence.exists("s1", MAX_WAIT));
        assertTrue(existence.find("s1", MAX_WAIT).isPresent());
    }

    @Test
    void existsComHitLocalNaoFazRpc() {
        newExistence(2_000);
        placementLookup.cache("s1", SeriesPlacement.active("storage-a", 1L));

        boolean result = existence.exists("s1", MAX_WAIT);

        assertTrue(result);
        assertTrue(rpc.calls().isEmpty(), "hit local não deveria disparar RPC nenhum");
    }

    @Test
    void existsComMissesConsultaLiderEmLote() {
        newExistence(2_000);
        rpc.respondDefault((cmd, body) -> {
            CatalogLookupRequest request = (CatalogLookupRequest) body;
            Map<String, SeriesPlacement> found = new HashMap<>();
            for (String key : request.seriesKeys()) {
                found.put(key, SeriesPlacement.active("storage-a", 1L));
            }
            return CatalogLookupResponse.ok(found);
        });

        Map<String, Boolean> result = existence.exists(List.of("m1", "m2", "m3"), MAX_WAIT);

        assertEquals(Map.of("m1", true, "m2", true, "m3", true), result);
        assertEquals(1, rpc.calls().size(), "3 misses cabem numa única página de CATALOG_LOOKUP");
        assertEquals(Commands.CATALOG_LOOKUP, rpc.calls().get(0).command());
    }

    @Test
    void existsAusenteNoLiderDevolveFalse() {
        newExistence(2_000);
        rpc.respondDefault((cmd, body) -> CatalogLookupResponse.ok(Map.of()));

        boolean result = existence.exists("inexistente", MAX_WAIT);

        assertFalse(result);
    }

    @Test
    void existsSemLiderLancaExcecaoENuncaFalse() {
        newExistence(2_000);
        rpc.leader(null);

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> existence.exists("s1", Duration.ofMillis(100)));
        assertEquals(ErrorCode.NO_LEADER, ex.code());
    }

    @Test
    void existsDevolveTodasAsChavesPedidas() {
        newExistence(2_000);
        placementLookup.cache("hit", SeriesPlacement.active("storage-a", 1L));
        rpc.respondDefault((cmd, body) -> {
            CatalogLookupRequest request = (CatalogLookupRequest) body;
            Map<String, SeriesPlacement> found = new HashMap<>();
            if (request.seriesKeys().contains("miss-presente")) {
                found.put("miss-presente", SeriesPlacement.active("storage-a", 1L));
            }
            return CatalogLookupResponse.ok(found);
        });

        Map<String, Boolean> result = existence.exists(List.of("hit", "miss-presente", "miss-ausente"), MAX_WAIT);

        assertEquals(Set.of("hit", "miss-presente", "miss-ausente"), result.keySet());
        assertEquals(Boolean.TRUE, result.get("hit"));
        assertEquals(Boolean.TRUE, result.get("miss-presente"));
        assertEquals(Boolean.FALSE, result.get("miss-ausente"));
    }

    @Test
    void existsMigrandoEhTrue() {
        newExistence(2_000);
        SeriesPlacement migrating = SeriesPlacement.migrating(SeriesPlacement.active("storage-a", 1L),
                "storage-b", "mig-1", 2L);
        placementLookup.cache("s1", migrating);

        assertTrue(existence.exists("s1", MAX_WAIT));
    }

    @Test
    void findDevolveDonoEstadoEAlvo() {
        newExistence(2_000);
        SeriesPlacement migrating = SeriesPlacement.migrating(SeriesPlacement.active("storage-a", 1L),
                "storage-b", "mig-1", 2L);
        placementLookup.cache("s1", migrating);

        Optional<SeriesInfo> result = existence.find("s1", MAX_WAIT);

        assertTrue(result.isPresent());
        SeriesInfo info = result.get();
        assertEquals("s1", info.seriesKey());
        assertEquals("storage-a", info.ownerNodeId());
        assertEquals(PlacementState.MIGRATING, info.state());
        assertEquals("storage-b", info.targetNodeId());
        assertTrue(rpc.calls().isEmpty(), "hit local não deveria disparar RPC nenhum");
    }

    @Test
    void findAusenteDevolveVazio() {
        newExistence(2_000);
        rpc.respondDefault((cmd, body) -> CatalogLookupResponse.ok(Map.of()));

        assertTrue(existence.find("inexistente", MAX_WAIT).isEmpty());
    }

    /** {@link PlacementLookup} fake: só {@code placementCached} tem comportamento programável. */
    private static final class FakePlacementLookup implements PlacementLookup {
        private final Map<String, SeriesPlacement> cached = new ConcurrentHashMap<>();

        void cache(String seriesKey, SeriesPlacement placement) {
            cached.put(seriesKey, placement);
        }

        @Override
        public SeriesPlacement resolve(String seriesKey, String definitionHashHex) {
            throw new UnsupportedOperationException("não usado por SeriesExistence");
        }

        @Override
        public SeriesPlacement resolveExisting(String seriesKey, Duration maxWait) {
            throw new UnsupportedOperationException("não usado por SeriesExistence");
        }

        @Override
        public SeriesPlacement resolveExistingAtLeader(String seriesKey, Duration maxWait) {
            throw new UnsupportedOperationException("não usado por SeriesExistence");
        }

        @Override
        public Optional<SeriesPlacement> placementCached(String seriesKey) {
            return Optional.ofNullable(cached.get(seriesKey));
        }

        @Override
        public void invalidate(String seriesKey) {
            cached.remove(seriesKey);
        }

        @Override
        public void noteOwner(String seriesKey, String ownerNodeId) {
            cached.put(seriesKey, SeriesPlacement.active(ownerNodeId, 0L));
        }
    }
}
