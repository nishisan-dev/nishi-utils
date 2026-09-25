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
import dev.nishisan.utils.oss.cluster.api.SeriesVerification;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.CatalogLookupRequest;
import dev.nishisan.utils.oss.cluster.protocol.CatalogLookupResponse;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesExistsBatchResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link SeriesVerifier} (lógica de {@code verify} do cliente) com {@link PlacementLookup} fake e
 * {@link CatalogLookupClient} real sobre {@link RecordingClusterRpc} — mesma técnica de
 * {@link ClientExistenceTest}, sem {@code NGridNode} real.
 */
class SeriesVerifierTest {

    private static final Duration MAX_WAIT = Duration.ofSeconds(2);
    private static final NodeId LEADER = NodeId.of("storage-leader");
    private static final NodeId NODE_A = NodeId.of("storage-a");
    private static final NodeId NODE_B = NodeId.of("storage-b");

    private RecordingClusterRpc rpc;
    private FakePlacementLookup placementLookup;
    private SeriesVerifier verifier;

    private void newVerifier(int batchSize) {
        newVerifier(batchSize, CapabilityFixtures.advertisingAll());
    }

    private void newVerifier(int batchSize, NodeCapabilities capabilities) {
        rpc = new RecordingClusterRpc(NodeId.of("client-under-test"));
        rpc.leader(LEADER);
        placementLookup = new FakePlacementLookup();
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(5), Duration.ofMillis(50));
        CatalogLookupClient catalogLookupClient = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), 2_000,
                capabilities);
        verifier = new SeriesVerifier(placementLookup, catalogLookupClient, rpc, capabilities, MAX_WAIT, MAX_WAIT,
                batchSize);
    }

    @Test
    void agrupaPorDonoEPagina() {
        newVerifier(2);
        for (int i = 0; i < 5; i++) {
            placementLookup.cache("nodeA-" + i, SeriesPlacement.active(NODE_A.value(), 1L));
        }
        placementLookup.cache("nodeB-0", SeriesPlacement.active(NODE_B.value(), 1L));
        rpc.respondDefault((cmd, body) -> {
            SeriesExistsBatchRequest request = (SeriesExistsBatchRequest) body;
            return SeriesExistsBatchResponse.ok(Set.copyOf(request.seriesKeys()));
        });

        Map<String, SeriesVerification> result = verifier.verify(
                List.of("nodeA-0", "nodeA-1", "nodeA-2", "nodeA-3", "nodeA-4", "nodeB-0"));

        assertEquals(Map.of(
                "nodeA-0", SeriesVerification.PRESENT,
                "nodeA-1", SeriesVerification.PRESENT,
                "nodeA-2", SeriesVerification.PRESENT,
                "nodeA-3", SeriesVerification.PRESENT,
                "nodeA-4", SeriesVerification.PRESENT,
                "nodeB-0", SeriesVerification.PRESENT), result);
        List<Integer> pageSizesForNodeA = rpc.calls().stream()
                .filter(call -> call.target().equals(NODE_A))
                .map(call -> ((SeriesExistsBatchRequest) call.body()).seriesKeys().size())
                .toList();
        assertEquals(List.of(2, 2, 1), pageSizesForNodeA, "5 chaves em páginas de 2 -> tamanhos 2,2,1");
        long callsToNodeB = rpc.calls().stream().filter(call -> call.target().equals(NODE_B)).count();
        assertEquals(1, callsToNodeB);
        for (var call : rpc.calls()) {
            assertEquals(Commands.SERIES_EXISTS_BATCH, call.command());
        }
    }

    @Test
    void semPlacementEhNotPlaced() {
        newVerifier(2_000);
        rpc.respondDefault((cmd, body) -> CatalogLookupResponse.ok(Map.of()));

        Map<String, SeriesVerification> result = verifier.verify(List.of("inexistente"));

        assertEquals(SeriesVerification.NOT_PLACED, result.get("inexistente"));
        assertEquals(1, rpc.calls().size());
        assertEquals(Commands.CATALOG_LOOKUP, rpc.calls().get(0).command());
    }

    @Test
    void ausenteNoDonoEhMissingOnOwner() {
        newVerifier(2_000);
        placementLookup.cache("s1", SeriesPlacement.active(NODE_A.value(), 1L));
        rpc.respondNext((cmd, body) -> SeriesExistsBatchResponse.ok(Set.of()));
        rpc.respondNext((cmd, body) -> CatalogLookupResponse.ok(
                Map.of("s1", SeriesPlacement.active(NODE_A.value(), 1L))));

        Map<String, SeriesVerification> result = verifier.verify(List.of("s1"));

        assertEquals(SeriesVerification.MISSING_ON_OWNER, result.get("s1"));
        assertEquals(2, rpc.calls().size());
        assertEquals(Commands.SERIES_EXISTS_BATCH, rpc.calls().get(0).command());
        assertEquals(NODE_A, rpc.calls().get(0).target());
        assertEquals(Commands.CATALOG_LOOKUP, rpc.calls().get(1).command());
    }

    @Test
    void donoMudouDuranteVerificacaoReperguntaAoNovoDono() {
        newVerifier(2_000);
        placementLookup.cache("s1", SeriesPlacement.active(NODE_A.value(), 1L));
        rpc.respondNext((cmd, body) -> SeriesExistsBatchResponse.ok(Set.of()));
        rpc.respondNext((cmd, body) -> CatalogLookupResponse.ok(
                Map.of("s1", SeriesPlacement.active(NODE_B.value(), 2L))));
        rpc.respondNext((cmd, body) -> SeriesExistsBatchResponse.ok(Set.of("s1")));

        Map<String, SeriesVerification> result = verifier.verify(List.of("s1"));

        assertEquals(SeriesVerification.PRESENT, result.get("s1"));
        assertEquals(3, rpc.calls().size());
        assertEquals(NODE_A, rpc.calls().get(0).target());
        assertEquals(Commands.CATALOG_LOOKUP, rpc.calls().get(1).command());
        assertEquals(NODE_B, rpc.calls().get(2).target());
        assertEquals(Commands.SERIES_EXISTS_BATCH, rpc.calls().get(2).command());
    }

    @Test
    void donoMudouEContinuaAusenteEhMissingOnOwner() {
        newVerifier(2_000);
        placementLookup.cache("s1", SeriesPlacement.active(NODE_A.value(), 1L));
        rpc.respondNext((cmd, body) -> SeriesExistsBatchResponse.ok(Set.of()));
        rpc.respondNext((cmd, body) -> CatalogLookupResponse.ok(
                Map.of("s1", SeriesPlacement.active(NODE_B.value(), 2L))));
        rpc.respondNext((cmd, body) -> SeriesExistsBatchResponse.ok(Set.of()));

        Map<String, SeriesVerification> result = verifier.verify(List.of("s1"));

        assertEquals(SeriesVerification.MISSING_ON_OWNER, result.get("s1"));
    }

    @Test
    void falhaNumNoMarcaSoSuasChavesComoUnverified() {
        newVerifier(2_000);
        placementLookup.cache("keyA", SeriesPlacement.active(NODE_A.value(), 1L));
        placementLookup.cache("keyB", SeriesPlacement.active(NODE_B.value(), 1L));
        rpc.respondNext((cmd, body) -> {
            throw new NgrrdClusterException(ErrorCode.TIMEOUT, "timeout simulado no dono");
        });
        rpc.respondNext((cmd, body) -> SeriesExistsBatchResponse.ok(Set.of("keyB")));

        Map<String, SeriesVerification> result = verifier.verify(List.of("keyA", "keyB"));

        assertEquals(SeriesVerification.UNVERIFIED, result.get("keyA"));
        assertEquals(SeriesVerification.PRESENT, result.get("keyB"));
    }

    @Test
    void falhaNumaPaginaDoMeioMarcaSoAquelaPaginaComoUnverified() {
        newVerifier(2);
        for (int i = 0; i < 5; i++) {
            placementLookup.cache("p-" + i, SeriesPlacement.active(NODE_A.value(), 1L));
        }
        // Falha NÃO-timeout (status ERROR) na página do meio — prova o isolamento por página, sem
        // cortar a página seguinte (diferente de um TIMEOUT, ver timeoutNumaPaginaCortaAsPaginasRestantesDoMesmoNo).
        rpc.respondNext((cmd, body) ->
                SeriesExistsBatchResponse.ok(Set.copyOf(((SeriesExistsBatchRequest) body).seriesKeys())));
        rpc.respondNext((cmd, body) -> SeriesExistsBatchResponse.error("falha simulada na página do meio"));
        rpc.respondNext((cmd, body) ->
                SeriesExistsBatchResponse.ok(Set.copyOf(((SeriesExistsBatchRequest) body).seriesKeys())));

        Map<String, SeriesVerification> result =
                verifier.verify(List.of("p-0", "p-1", "p-2", "p-3", "p-4"));

        assertEquals(SeriesVerification.PRESENT, result.get("p-0"));
        assertEquals(SeriesVerification.PRESENT, result.get("p-1"));
        assertEquals(SeriesVerification.UNVERIFIED, result.get("p-2"));
        assertEquals(SeriesVerification.UNVERIFIED, result.get("p-3"));
        assertEquals(SeriesVerification.PRESENT, result.get("p-4"), "a 3a página segue normalmente após a falha isolada da 2a");
        assertEquals(3, rpc.calls().size(), "as 3 páginas foram todas chamadas — falha ERROR não corta o nó");
    }

    @Test
    void timeoutNumaPaginaCortaAsPaginasRestantesDoMesmoNo() {
        newVerifier(2);
        for (int i = 0; i < 5; i++) {
            placementLookup.cache("p-" + i, SeriesPlacement.active(NODE_A.value(), 1L));
        }
        placementLookup.cache("nodeB-0", SeriesPlacement.active(NODE_B.value(), 1L));
        rpc.respondNext((cmd, body) ->
                SeriesExistsBatchResponse.ok(Set.copyOf(((SeriesExistsBatchRequest) body).seriesKeys())));
        rpc.respondNext((cmd, body) -> {
            throw new NgrrdClusterException(ErrorCode.TIMEOUT, "timeout simulado no dono");
        });
        // Sem 3a resposta programada: se a implementação chamar a 3a página do mesmo nó, o
        // RecordingClusterRpc lança IllegalStateException ("nenhuma resposta programada") e o teste falha.
        rpc.respondDefault((cmd, body) -> SeriesExistsBatchResponse.ok(Set.of("nodeB-0")));

        Map<String, SeriesVerification> result =
                verifier.verify(List.of("p-0", "p-1", "p-2", "p-3", "p-4", "nodeB-0"));

        assertEquals(SeriesVerification.PRESENT, result.get("p-0"));
        assertEquals(SeriesVerification.PRESENT, result.get("p-1"));
        assertEquals(SeriesVerification.UNVERIFIED, result.get("p-2"), "página que sofreu o TIMEOUT");
        assertEquals(SeriesVerification.UNVERIFIED, result.get("p-3"), "mesma página do TIMEOUT");
        assertEquals(SeriesVerification.UNVERIFIED, result.get("p-4"),
                "página seguinte do MESMO nó, cortada sem RPC depois do TIMEOUT");
        assertEquals(SeriesVerification.PRESENT, result.get("nodeB-0"), "outro nó segue normalmente");
        long callsToNodeA = rpc.calls().stream().filter(call -> call.target().equals(NODE_A)).count();
        assertEquals(2, callsToNodeA, "só as 2 primeiras páginas do nó A foram chamadas — a 3a foi cortada");
    }

    @Test
    void respostaComStatusDeErroNaoViraAusencia() {
        newVerifier(2_000);
        placementLookup.cache("erro", SeriesPlacement.active(NODE_A.value(), 1L));
        placementLookup.cache("statusNulo", SeriesPlacement.active(NODE_B.value(), 1L));
        rpc.respondNext((cmd, body) -> SeriesExistsBatchResponse.error("falha simulada"));
        rpc.respondNext((cmd, body) -> new SeriesExistsBatchResponse(null, Set.of(), null));

        Map<String, SeriesVerification> result = verifier.verify(List.of("erro", "statusNulo"));

        assertEquals(SeriesVerification.UNVERIFIED, result.get("erro"));
        assertEquals(SeriesVerification.UNVERIFIED, result.get("statusNulo"));
        assertEquals(2, rpc.calls().size(), "nenhuma reconfirmação no líder para chaves UNVERIFIED");
        for (var call : rpc.calls()) {
            assertEquals(Commands.SERIES_EXISTS_BATCH, call.command());
        }
    }

    @Test
    void respostaOkSemPresentNaoViraTodasAusentes() {
        newVerifier(2_000);
        placementLookup.cache("s1", SeriesPlacement.active(NODE_A.value(), 1L));
        rpc.respondNext((cmd, body) -> new SeriesExistsBatchResponse(SeriesStatus.OK, null, null));

        Map<String, SeriesVerification> result = verifier.verify(List.of("s1"));

        assertEquals(SeriesVerification.UNVERIFIED, result.get("s1"));
        assertEquals(1, rpc.calls().size(), "um OK sem present é tratado como falha da página, não como ausência");
    }

    @Test
    void semCapacidadeNoDonoMarcaChavesComoUnverifiedSemRpc() {
        newVerifier(2_000, CapabilityFixtures.advertisingByNode(Map.of(NODE_A.value(), Set.of())));
        placementLookup.cache("s1", SeriesPlacement.active(NODE_A.value(), 1L));

        Map<String, SeriesVerification> result = verifier.verify(List.of("s1"));

        assertEquals(SeriesVerification.UNVERIFIED, result.get("s1"));
        assertTrue(rpc.calls().isEmpty(), "sem a capacidade, nenhum RPC deveria ser enviado ao dono");
    }

    @Test
    void falhaNoLookupDoLiderLancaExcecao() {
        newVerifier(2_000);
        rpc.respondDefault((cmd, body) -> CatalogLookupResponse.error("falha simulada no líder"));

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> verifier.verify(List.of("inexistente")));

        assertEquals(ErrorCode.REMOTE_ERROR, ex.code());
    }

    @Test
    void falhaNoLookupDeReconfirmacaoDosAusentesLancaExcecao() {
        newVerifier(2_000);
        placementLookup.cache("s1", SeriesPlacement.active(NODE_A.value(), 1L));
        rpc.respondNext((cmd, body) -> SeriesExistsBatchResponse.ok(Set.of())); // ausente no dono
        rpc.respondNext((cmd, body) -> CatalogLookupResponse.error("falha simulada na reconfirmação"));

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> verifier.verify(List.of("s1")));

        assertEquals(ErrorCode.REMOTE_ERROR, ex.code());
    }

    @Test
    void devolveTodasAsChaves() {
        newVerifier(2_000);
        placementLookup.cache("presente", SeriesPlacement.active(NODE_A.value(), 1L));
        placementLookup.cache("ausente", SeriesPlacement.active(NODE_A.value(), 1L));
        rpc.respondDefault((cmd, body) -> {
            if (Commands.CATALOG_LOOKUP.equals(cmd)) {
                CatalogLookupRequest request = (CatalogLookupRequest) body;
                Map<String, SeriesPlacement> found = request.seriesKeys().contains("ausente")
                        ? Map.of("ausente", SeriesPlacement.active(NODE_A.value(), 1L))
                        : Map.of();
                return CatalogLookupResponse.ok(found);
            }
            SeriesExistsBatchRequest request = (SeriesExistsBatchRequest) body;
            Set<String> present = request.seriesKeys().contains("presente") ? Set.of("presente") : Set.of();
            return SeriesExistsBatchResponse.ok(present);
        });

        Map<String, SeriesVerification> result = verifier.verify(
                List.of("presente", "ausente", "sem-placement"));

        assertEquals(Set.of("presente", "ausente", "sem-placement"), result.keySet());
        assertEquals(SeriesVerification.PRESENT, result.get("presente"));
        assertEquals(SeriesVerification.MISSING_ON_OWNER, result.get("ausente"));
        assertEquals(SeriesVerification.NOT_PLACED, result.get("sem-placement"));
    }

    /** {@link PlacementLookup} fake: só {@code placementCached} tem comportamento programável. */
    private static final class FakePlacementLookup implements PlacementLookup {
        private final Map<String, SeriesPlacement> cached = new ConcurrentHashMap<>();

        void cache(String seriesKey, SeriesPlacement placement) {
            cached.put(seriesKey, placement);
        }

        @Override
        public SeriesPlacement resolve(String seriesKey, String definitionHashHex) {
            throw new UnsupportedOperationException("não usado por SeriesVerifier");
        }

        @Override
        public SeriesPlacement resolveExisting(String seriesKey, Duration maxWait) {
            throw new UnsupportedOperationException("não usado por SeriesVerifier");
        }

        @Override
        public SeriesPlacement resolveExistingAtLeader(String seriesKey, Duration maxWait) {
            throw new UnsupportedOperationException("não usado por SeriesVerifier");
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
