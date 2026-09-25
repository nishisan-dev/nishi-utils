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
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.ReadResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchResponse;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre o caminho direto (fora da reabertura assíncrona do {@link WriteDispatcher} em {@code NOT_OPEN}):
 * {@link RemoteSeriesHandle} descobrindo {@link SeriesNotFoundException} numa operação síncrona
 * ({@code read}) precisa avisar o dispatcher para falhar escritas ainda no buffer — sem isso, uma
 * escrita bufferizada chegaria a {@code NOT_OPEN} mais tarde e tentaria reabrir via o reopener, que não
 * encontra mais o handle (já removido do mapa do cliente) e entraria em retentativa para sempre.
 */
class DirectNotFoundNotifiesDispatcherTest {

    private static final String SERIES_KEY = "gone";
    private static final NodeId OWNER_A = NodeId.of("storage-a");

    @Test
    void readDescobreNotFoundDiretoFalhaEscritaNoBufferSemNovasTentativasEFlushAllDasDemaisSeriesSegue() {
        RecordingClusterRpc rpc = new RecordingClusterRpc(NodeId.of("client-under-test"));
        ConcurrentMap<String, RemoteSeriesHandle> handles = new ConcurrentHashMap<>();
        FakePlacementLookup lookup = new FakePlacementLookup(OWNER_A.value());
        RetryPolicy retry = new RetryPolicy(Duration.ofSeconds(5), Duration.ofMillis(5), Duration.ofMillis(50));

        // batchMaxSamples alto e batchMaxDelay bem longo: a única escrita de "gone" fica no buffer, sem
        // ser drenada sozinha, até o read() síncrono descobrir a série inexistente.
        WriteDispatcher dispatcher = new WriteDispatcher(rpc, lookup, retry, 10, Duration.ofSeconds(30), 1_000,
                NgrrdClusterConfig.BufferFullPolicy.BLOCK, Duration.ofSeconds(5),
                key -> {
                    RemoteSeriesHandle handle = handles.get(key);
                    return handle != null && handle.reopen();
                },
                (key, newOwner) -> {
                    RemoteSeriesHandle handle = handles.get(key);
                    if (handle != null) {
                        handle.ownerChanged(newOwner);
                    }
                }, Clock.systemUTC(), null, null);
        try {
            RemoteSeriesHandle handle = new RemoteSeriesHandle(SERIES_KEY, "yaml: fake", "hash-1", Map.of(),
                    Ngrrd.OpenOptions.defaults().withCreateIfMissing(false), lookup, rpc, dispatcher, retry,
                    Duration.ofSeconds(5), Duration.ofSeconds(5), Clock.systemUTC(), handles::remove);
            handles.put(SERIES_KEY, handle);

            rpc.respondNext((cmd, body) -> new SeriesStatusResponse(SeriesStatus.OK, OWNER_A.value(), null));
            handle.open();

            rpc.respondNext((cmd, body) -> new ReadResponse(SeriesStatus.NOT_OPEN, null, null, null));
            rpc.respondDefault((cmd, body) -> new SeriesStatusResponse(SeriesStatus.NOT_FOUND, OWNER_A.value(), null));

            handle.write("in_octets", new Sample(1L, 1.0));

            SeriesNotFoundException ex = assertThrows(SeriesNotFoundException.class, () -> handle.read("in_octets",
                    new ViewQuery(Duration.ofHours(1), 300, ConsolidationFunction.AVERAGE, 100)));
            assertEquals(SERIES_KEY, ex.seriesKey());

            assertNull(handles.get(SERIES_KEY), "handle deveria ter se removido do mapa do cliente");
            assertEquals(1L, dispatcher.samplesFailed(), "a escrita ainda no buffer deveria ter falhado");
            assertEquals(0L, dispatcher.samplesSent());
            assertTrue(rpc.calls().stream().noneMatch(c -> c.command().equals(Commands.WRITE_BATCH)),
                    "a escrita nunca deveria ter chegado a ser enviada — falhou direto no buffer");

            // flushAll de outras séries no mesmo dispatcher (mesmo nó) continua funcionando — a rota
            // descartada de "gone" não pode ter ficado presa em pendingRoutes nem no buffer do nó.
            rpc.respondDefault((cmd, body) -> {
                WriteBatchRequest request = (WriteBatchRequest) body;
                Map<String, SeriesStatus> status = new LinkedHashMap<>();
                for (SeriesWrite write : request.writes()) {
                    status.put(write.seriesKey(), SeriesStatus.OK);
                }
                return new WriteBatchResponse(status, Map.of(), Map.of());
            });
            dispatcher.enqueue(OWNER_A.value(), new SeriesWrite("healthy", "in_octets", 2L, 2.0));
            dispatcher.flushAllSync();

            assertEquals(1L, dispatcher.samplesSent());
        } finally {
            dispatcher.close();
        }
    }

    /** {@link PlacementLookup} fake: sempre devolve o dono atual configurado, sem RPC ao líder. */
    private static final class FakePlacementLookup implements PlacementLookup {
        private final String owner;

        FakePlacementLookup(String owner) {
            this.owner = owner;
        }

        @Override
        public SeriesPlacement resolve(String seriesKey, String definitionHashHex) {
            return SeriesPlacement.active(owner, 0L);
        }

        @Override
        public SeriesPlacement resolveExisting(String seriesKey, Duration maxWait) {
            return resolve(seriesKey, null);
        }

        @Override
        public Optional<SeriesPlacement> placementCached(String seriesKey) {
            return Optional.of(SeriesPlacement.active(owner, 0L));
        }

        @Override
        public void invalidate(String seriesKey) {
        }

        @Override
        public void noteOwner(String seriesKey, String ownerNodeId) {
        }
    }
}
