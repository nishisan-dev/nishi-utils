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
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.CatalogLookupRequest;
import dev.nishisan.utils.oss.cluster.protocol.CatalogLookupResponse;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link CatalogLookupClient} com {@link RecordingClusterRpc} fake — mesma técnica de
 * {@code PlacementResolverTest}, sem {@code ClusterCoordinator}/{@code Transport} reais.
 */
class CatalogLookupClientTest {

    private static final NodeId LEADER = NodeId.of("leader-1");
    private static final NodeId CLIENT = NodeId.of("client-under-test");

    private RecordingClusterRpc rpc;
    private RetryPolicy retry;

    @BeforeEach
    void setUp() {
        rpc = new RecordingClusterRpc(CLIENT);
        rpc.leader(LEADER);
        retry = new RetryPolicy(Duration.ofSeconds(2), Duration.ofMillis(10), Duration.ofMillis(100));
    }

    @Test
    void paginaEmLotesDoTamanhoConfigurado() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            keys.add("series-" + i);
        }
        List<String> expectedPage1 = keys.subList(0, 2000);
        List<String> expectedPage2 = keys.subList(2000, 4000);
        List<String> expectedPage3 = keys.subList(4000, 5000);
        rpc.respondNext((cmd, body) -> {
            assertEquals(Commands.CATALOG_LOOKUP, cmd);
            assertEquals(new CatalogLookupRequest(expectedPage1), body);
            return CatalogLookupResponse.ok(Map.of());
        });
        rpc.respondNext((cmd, body) -> {
            assertEquals(new CatalogLookupRequest(expectedPage2), body);
            return CatalogLookupResponse.ok(Map.of());
        });
        rpc.respondNext((cmd, body) -> {
            assertEquals(new CatalogLookupRequest(expectedPage3), body);
            return CatalogLookupResponse.ok(Map.of());
        });
        CatalogLookupClient client = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), 2000);

        Map<String, SeriesPlacement> found = client.lookup(keys, Duration.ofSeconds(1));

        assertEquals(Map.of(), found);
        assertEquals(3, rpc.calls().size());
        assertEquals(LEADER, rpc.calls().get(0).target());
        assertEquals(LEADER, rpc.calls().get(1).target());
        assertEquals(LEADER, rpc.calls().get(2).target());
    }

    @Test
    void deduplicaChaves() {
        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);
        rpc.respondNext((cmd, body) -> {
            assertEquals(new CatalogLookupRequest(List.of("a", "b", "c")), body);
            return CatalogLookupResponse.ok(Map.of("a", placed));
        });
        CatalogLookupClient client = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), 10);

        Map<String, SeriesPlacement> found = client.lookup(List.of("a", "b", "a", "c", "b"), Duration.ofSeconds(1));

        assertEquals(Map.of("a", placed), found);
        assertEquals(1, rpc.calls().size());
    }

    @Test
    void seguePistaDeNotLeader() {
        NodeId secondLeader = NodeId.of("leader-2");
        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);
        rpc.respondNext((cmd, body) -> CatalogLookupResponse.notLeader(secondLeader.value()));
        rpc.respondNext((cmd, body) -> CatalogLookupResponse.ok(Map.of("series-1", placed)));
        CatalogLookupClient client = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), 2000);

        Map<String, SeriesPlacement> found = client.lookup(List.of("series-1"), Duration.ofSeconds(1));

        assertEquals(Map.of("series-1", placed), found);
        assertEquals(2, rpc.calls().size());
        assertEquals(LEADER, rpc.calls().get(0).target());
        assertEquals(secondLeader, rpc.calls().get(1).target());
    }

    @Test
    void retentaFalhaDeTransporteDentroDoPrazo() {
        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);
        rpc.respondNext((cmd, body) -> {
            throw new NgrrdClusterException(ErrorCode.TIMEOUT, "timeout simulado");
        });
        rpc.respondNext((cmd, body) -> CatalogLookupResponse.ok(Map.of("series-1", placed)));
        CatalogLookupClient client = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), 2000);

        Map<String, SeriesPlacement> found = client.lookup(List.of("series-1"), Duration.ofSeconds(1));

        assertEquals(Map.of("series-1", placed), found);
        assertEquals(2, rpc.calls().size());
        assertEquals(LEADER, rpc.calls().get(0).target());
        assertEquals(LEADER, rpc.calls().get(1).target());
    }

    @Test
    @Timeout(2)
    void semLiderLancaNoLeader() {
        rpc.leader(null);
        CatalogLookupClient client = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), 2000);

        NgrrdClusterException failure = assertThrows(NgrrdClusterException.class,
                () -> client.lookup(List.of("series-1"), Duration.ofMillis(30)));

        assertTrue(failure.code() == ErrorCode.NO_LEADER || failure.code() == ErrorCode.TIMEOUT);
        assertTrue(rpc.calls().isEmpty());
    }

    @Test
    void erroDoLiderLancaRemoteError() {
        rpc.respondNext((cmd, body) -> CatalogLookupResponse.error("lote maior que o permitido"));
        CatalogLookupClient client = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), 2000);

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> client.lookup(List.of("series-1"), Duration.ofSeconds(1)));

        assertEquals(ErrorCode.REMOTE_ERROR, ex.code());
    }

    @Test
    void falhaNumaPaginaNaoDevolveResultadoParcial() {
        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);
        rpc.respondNext((cmd, body) -> CatalogLookupResponse.ok(Map.of("k1", placed, "k2", placed)));
        rpc.respondNext((cmd, body) -> CatalogLookupResponse.error("falha simulada na segunda página"));
        CatalogLookupClient client = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), 2);

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> client.lookup(List.of("k1", "k2", "k3", "k4"), Duration.ofSeconds(1)));

        assertEquals(ErrorCode.REMOTE_ERROR, ex.code());
        assertEquals(2, rpc.calls().size());
    }

    @Test
    void listaVaziaNaoFazRpc() {
        CatalogLookupClient client = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), 2000);

        Map<String, SeriesPlacement> found = client.lookup(List.of(), Duration.ofSeconds(1));

        assertEquals(Map.of(), found);
        assertTrue(rpc.calls().isEmpty());
    }

    @Test
    void respostaNulaDoLiderLancaRemoteError() {
        rpc.respondNext((cmd, body) -> null);
        CatalogLookupClient client = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), 2000);

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> client.lookup(List.of("series-1"), Duration.ofSeconds(1)));

        assertEquals(ErrorCode.REMOTE_ERROR, ex.code());
    }

    @Test
    void statusDesconhecidoNaRespostaLancaRemoteError() {
        // O codec lê um enum desconhecido (ex.: líder de versão mais nova) como status == null.
        rpc.respondNext((cmd, body) -> new CatalogLookupResponse(null, null, null, null));
        CatalogLookupClient client = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), 2000);

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> client.lookup(List.of("series-1"), Duration.ofSeconds(1)));

        assertEquals(ErrorCode.REMOTE_ERROR, ex.code());
    }

    @Test
    void notLeaderSemHintVoltaAConsultarLeaderIdEConclui() {
        NodeId fallbackLeader = NodeId.of("leader-2");
        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);
        rpc.respondNext((cmd, body) -> {
            rpc.leader(fallbackLeader);
            return CatalogLookupResponse.notLeader(null);
        });
        rpc.respondNext((cmd, body) -> CatalogLookupResponse.ok(Map.of("series-1", placed)));
        CatalogLookupClient client = new CatalogLookupClient(rpc, retry, Clock.systemUTC(), 2000);

        Map<String, SeriesPlacement> found = client.lookup(List.of("series-1"), Duration.ofSeconds(1));

        assertEquals(Map.of("series-1", placed), found);
        assertEquals(2, rpc.calls().size());
        assertEquals(LEADER, rpc.calls().get(0).target());
        assertEquals(fallbackLeader, rpc.calls().get(1).target());
    }

    @Test
    void prazoCompartilhadoEntrePaginasFalhaComTimeoutSemResultadoParcial() {
        long[] now = {0};
        Clock clock = new Clock() {
            public ZoneId getZone() { return ZoneOffset.UTC; }
            public Clock withZone(ZoneId zone) { return this; }
            public Instant instant() { return Instant.ofEpochMilli(now[0]); }
            public long millis() { return now[0]; }
        };
        SeriesPlacement placed = SeriesPlacement.active("storage-a", 1_000L);
        rpc.respondNext((cmd, body) -> {
            // A primeira página consome quase todo o orçamento de 100ms compartilhado entre as páginas.
            now[0] += 100;
            return CatalogLookupResponse.ok(Map.of("k1", placed));
        });
        CatalogLookupClient client = new CatalogLookupClient(rpc, retry, clock, 2);

        NgrrdClusterException ex = assertThrows(NgrrdClusterException.class,
                () -> client.lookup(List.of("k1", "k2", "k3", "k4"), Duration.ofMillis(100)));

        assertEquals(ErrorCode.TIMEOUT, ex.code());
        assertEquals(1, rpc.calls().size(),
                "a segunda página não deveria sequer chamar o líder com o prazo já esgotado");
    }
}
