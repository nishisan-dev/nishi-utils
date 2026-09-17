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

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.blob.BlobVolume;
import dev.nishisan.utils.oss.blob.BlobVolumeRegistry;
import dev.nishisan.utils.oss.blob.NgrrdBlob;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import dev.nishisan.utils.oss.cluster.protocol.OpenRequest;
import dev.nishisan.utils.oss.cluster.protocol.ReadRequest;
import dev.nishisan.utils.oss.cluster.protocol.ReadResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesCommandRequest;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatus;
import dev.nishisan.utils.oss.cluster.protocol.SeriesStatusResponse;
import dev.nishisan.utils.oss.cluster.protocol.SeriesWrite;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchRequest;
import dev.nishisan.utils.oss.cluster.protocol.WriteBatchResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link StorageRequestHandler} sem rede real: {@link PlacementLookupFake}
 * substitui o {@code CatalogService}, e um {@link FakeTransport} mínimo (só
 * {@code local()}) satisfaz {@code RequestHandlerSupport} sem subir um
 * {@code NGridNode}. O {@link SeriesHandleRegistry} é real, sobre um
 * {@link BlobVolume} em {@code @TempDir} — é a peça que decide {@code NOT_OPEN}
 * vs. aberto, então fingi-la esconderia justamente o que se quer testar.
 */
class StorageRequestHandlerTest {

    private static final NodeId SELF = NodeId.of("node-self");
    private static final NodeId OTHER = NodeId.of("node-other");
    private static final NodeId SOURCE = NodeId.of("node-client");

    private String yaml;
    private BlobVolumeRegistry volumeRegistry;
    private SeriesHandleRegistry registry;
    private MutableClock clock;
    private PlacementLookupFake placementLookup;
    private StorageRequestHandler handler;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws IOException {
        yaml = Files.readString(Path.of("src/test/resources/iface-traffic-blob.yaml"), StandardCharsets.UTF_8);
        volumeRegistry = NgrrdBlob.registry().basePath(tempDir).volume("ngrrd").build();
        BlobVolume volume = volumeRegistry.require("ngrrd");
        clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        registry = new SeriesHandleRegistry(volume, "ngrrd", Duration.ofMinutes(15), 10_000, clock);
        placementLookup = new PlacementLookupFake();
        handler = new StorageRequestHandler(new FakeTransport(SELF), placementLookup, registry, SELF,
                Durability.FSYNC, OnGeometryChange.FAIL);
    }

    @AfterEach
    void tearDown() {
        registry.close();
        volumeRegistry.close();
    }

    private OpenRequest openRequest(String seriesKey, SeriesPlacement hint) {
        return new OpenRequest(seriesKey, yaml, Map.of(), null, null, hint);
    }

    @Test
    void donoConfirmadoNoCatalogoAbreEscreveFazCheckpointELe() {
        String seriesKey = "series-1";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));

        SeriesStatusResponse openResponse = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequest(seriesKey, null), SOURCE);
        assertEquals(SeriesStatus.OK, openResponse.status());

        long baseStepMs = 300_000L;
        long t0 = 1_700_000_000_000L - (1_700_000_000_000L % baseStepMs);
        WriteBatchRequest batch = new WriteBatchRequest(List.of(
                new SeriesWrite(seriesKey, "in_octets", t0, 1_000d),
                new SeriesWrite(seriesKey, "in_octets", t0 + baseStepMs, 1_500d)));
        WriteBatchResponse writeResponse = (WriteBatchResponse) handler.handle(Commands.WRITE_BATCH, batch, SOURCE);
        assertEquals(Map.of(seriesKey, SeriesStatus.OK), writeResponse.statusBySeries());

        SeriesStatusResponse checkpointResponse = (SeriesStatusResponse) handler.handle(Commands.CHECKPOINT,
                new SeriesCommandRequest(seriesKey), SOURCE);
        assertEquals(SeriesStatus.OK, checkpointResponse.status());

        ReadRequest readRequest = new ReadRequest(seriesKey, "in_bps", Duration.ofDays(1).toMillis(), 300,
                ConsolidationFunction.AVERAGE, 500, t0 + 2 * baseStepMs);
        ReadResponse readResponse = (ReadResponse) handler.handle(Commands.READ, readRequest, SOURCE);
        assertEquals(SeriesStatus.OK, readResponse.status());
        assertNotNull(readResponse.result());
    }

    @Test
    void semPlacementMasComHintDoDonoAtualAceitaComoOk() {
        String seriesKey = "series-hint";
        SeriesPlacement hint = SeriesPlacement.active(SELF.value(), 1_000L);

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequest(seriesKey, hint), SOURCE);

        assertEquals(SeriesStatus.OK, response.status());
    }

    @Test
    void placementDeOutroDonoRespondeWrongOwnerComODonoCorreto() {
        String seriesKey = "series-alheia";
        placementLookup.put(seriesKey, SeriesPlacement.active(OTHER.value(), 1_000L));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.CHECKPOINT,
                new SeriesCommandRequest(seriesKey), SOURCE);

        assertEquals(SeriesStatus.WRONG_OWNER, response.status());
        assertEquals(OTHER.value(), response.ownerNodeId());
    }

    @Test
    void semPlacementSemHintESemHandleAbertoRespondeWrongOwnerComDonoNulo() {
        String seriesKey = "series-desconhecida";

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.FLUSH,
                new SeriesCommandRequest(seriesKey), SOURCE);

        assertEquals(SeriesStatus.WRONG_OWNER, response.status());
        assertNull(response.ownerNodeId());
    }

    @Test
    void donoSemHandleAbertoRespondeNotOpen() {
        String seriesKey = "series-fechada";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.CHECKPOINT,
                new SeriesCommandRequest(seriesKey), SOURCE);

        assertEquals(SeriesStatus.NOT_OPEN, response.status());
    }

    @Test
    void serieMarcadaMigratingBloqueiaOperacaoMesmoSendoODono() {
        String seriesKey = "series-migrando";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        registry.open(seriesKey, yaml, Ngrrd.OpenOptions.defaults());
        registry.markMigrating(seriesKey);

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequest(seriesKey, null), SOURCE);

        assertEquals(SeriesStatus.MIGRATING, response.status());
    }

    @Test
    void placementEmMigracaoRespondeMigratingComODonoAtual() {
        String seriesKey = "series-em-migracao";
        SeriesPlacement migrating = SeriesPlacement.migrating(
                SeriesPlacement.active(SELF.value(), 1_000L), OTHER.value(), "migration-1", 2_000L);
        assertEquals(PlacementState.MIGRATING, migrating.state());
        placementLookup.put(seriesKey, migrating);

        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.FLUSH,
                new SeriesCommandRequest(seriesKey), SOURCE);

        assertEquals(SeriesStatus.MIGRATING, response.status());
        assertEquals(SELF.value(), response.ownerNodeId());
    }

    @Test
    void writeBatchComVariasSeriesDevolveStatusMistoPorSerie() {
        String owned = "series-dono-aberta";
        String ownedButClosed = "series-dono-fechada";
        String notOwned = "series-de-outro-no";

        placementLookup.put(owned, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(owned, null), SOURCE);
        placementLookup.put(ownedButClosed, SeriesPlacement.active(SELF.value(), 1_000L));
        placementLookup.put(notOwned, SeriesPlacement.active(OTHER.value(), 1_000L));

        long ts = 1_700_000_000_000L;
        WriteBatchRequest batch = new WriteBatchRequest(List.of(
                new SeriesWrite(owned, "in_octets", ts, 1.0),
                new SeriesWrite(ownedButClosed, "in_octets", ts, 1.0),
                new SeriesWrite(notOwned, "in_octets", ts, 1.0)));

        WriteBatchResponse response = (WriteBatchResponse) handler.handle(Commands.WRITE_BATCH, batch, SOURCE);

        assertEquals(SeriesStatus.OK, response.statusBySeries().get(owned));
        assertEquals(SeriesStatus.NOT_OPEN, response.statusBySeries().get(ownedButClosed));
        assertEquals(SeriesStatus.WRONG_OWNER, response.statusBySeries().get(notOwned));
        assertEquals(OTHER.value(), response.ownerBySeries().get(notOwned));
    }

    @Test
    void excecaoDuranteLeituraViraStatusErrorComMensagem() {
        String seriesKey = "series-cf-invalido";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);

        // "MIN" não é declarado por nenhuma RRA da definição (só AVERAGE/MAX) -> NgrrdQueryException
        // síncrona dentro de NgrrdReader.read, capturada pelo handler e convertida em ERROR.
        ReadRequest badRequest = new ReadRequest(seriesKey, "in_bps", Duration.ofDays(1).toMillis(), 300,
                ConsolidationFunction.MIN, 500, 1_700_000_000_000L);

        ReadResponse response = (ReadResponse) handler.handle(Commands.READ, badRequest, SOURCE);

        assertEquals(SeriesStatus.ERROR, response.status());
        assertNotNull(response.message());
        assertNull(response.result());
    }

    @Test
    void closeEhIdempotenteMesmoSemHandleAberto() {
        SeriesStatusResponse response = (SeriesStatusResponse) handler.handle(Commands.CLOSE,
                new SeriesCommandRequest("nunca-existiu"), SOURCE);

        assertEquals(SeriesStatus.OK, response.status());
    }

    @Test
    void readAposFechamentoPorOciosidadeSeAutoCuraEDevolveOsDados() {
        // A1: read/readPreset agora passam por withHandleSelfHealing — um fechamento por
        // ociosidade/LRU (nunca um CLOSE explícito) não deveria exigir um novo OPEN do cliente.
        String seriesKey = "series-ociosa";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);

        long baseStepMs = 300_000L;
        long t0 = 1_700_000_000_000L - (1_700_000_000_000L % baseStepMs);
        WriteBatchRequest batch = new WriteBatchRequest(List.of(
                new SeriesWrite(seriesKey, "in_octets", t0, 1_000d),
                new SeriesWrite(seriesKey, "in_octets", t0 + baseStepMs, 1_500d)));
        handler.handle(Commands.WRITE_BATCH, batch, SOURCE);
        handler.handle(Commands.CHECKPOINT, new SeriesCommandRequest(seriesKey), SOURCE);

        clock.advance(Duration.ofMinutes(16));
        assertEquals(1, registry.closeIdle());
        assertTrue(registry.existing(seriesKey).isEmpty(), "closeIdle deveria ter fechado o handle");

        ReadRequest readRequest = new ReadRequest(seriesKey, "in_bps", Duration.ofDays(1).toMillis(), 300,
                ConsolidationFunction.AVERAGE, 500, t0 + 2 * baseStepMs);
        ReadResponse response = (ReadResponse) handler.handle(Commands.READ, readRequest, SOURCE);

        assertEquals(SeriesStatus.OK, response.status());
        assertNotNull(response.result());
        assertFalse(response.result().points().isEmpty(), "esperava dados após a auto-cura do READ");
    }

    @Test
    void readAposCloseExplicitoRespondeNotOpen() {
        String seriesKey = "series-fechada-close";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);

        SeriesStatusResponse closeResponse = (SeriesStatusResponse) handler.handle(Commands.CLOSE,
                new SeriesCommandRequest(seriesKey), SOURCE);
        assertEquals(SeriesStatus.OK, closeResponse.status());

        ReadRequest readRequest = new ReadRequest(seriesKey, "in_bps", Duration.ofDays(1).toMillis(), 300,
                ConsolidationFunction.AVERAGE, 500, 1_700_000_000_000L);
        ReadResponse response = (ReadResponse) handler.handle(Commands.READ, readRequest, SOURCE);

        assertEquals(SeriesStatus.NOT_OPEN, response.status());
    }

    @Test
    void readAposCloseSeguidoDeNovoOpenVoltaAFuncionar() {
        String seriesKey = "series-close-depois-open";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);
        handler.handle(Commands.CLOSE, new SeriesCommandRequest(seriesKey), SOURCE);

        SeriesStatusResponse reopenResponse = (SeriesStatusResponse) handler.handle(Commands.OPEN,
                openRequest(seriesKey, null), SOURCE);
        assertEquals(SeriesStatus.OK, reopenResponse.status());

        ReadRequest readRequest = new ReadRequest(seriesKey, "in_bps", Duration.ofDays(1).toMillis(), 300,
                ConsolidationFunction.AVERAGE, 500, 1_700_000_000_000L);
        ReadResponse response = (ReadResponse) handler.handle(Commands.READ, readRequest, SOURCE);

        assertEquals(SeriesStatus.OK, response.status());
    }

    @Test
    void handleLocalDespachaComandoAtendidoEHandlesDistingueComandoDeOutroHandler() {
        String seriesKey = "series-local";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));

        assertTrue(handler.handles(Commands.OPEN), "OPEN é um OWNER_COMMAND");
        Object result = handler.handleLocal(Commands.OPEN, openRequest(seriesKey, null));
        assertEquals(SeriesStatus.OK, ((SeriesStatusResponse) result).status());

        assertFalse(handler.handles(Commands.PLACE), "PLACE não é um OWNER_COMMAND");
    }

    @Test
    void metricsSnapshotContabilizaOperacoesEErros() {
        String seriesKey = "series-metricas";
        placementLookup.put(seriesKey, SeriesPlacement.active(SELF.value(), 1_000L));
        handler.handle(Commands.OPEN, openRequest(seriesKey, null), SOURCE);

        long ts = 1_700_000_000_000L;
        handler.handle(Commands.WRITE_BATCH, new WriteBatchRequest(
                List.of(new SeriesWrite(seriesKey, "in_octets", ts, 1.0))), SOURCE);
        handler.handle(Commands.CHECKPOINT, new SeriesCommandRequest(seriesKey), SOURCE);
        handler.handle(Commands.CHECKPOINT, new SeriesCommandRequest("nunca-aberta"), SOURCE);

        StorageRequestHandler.StorageHandlerMetrics metrics = handler.metricsSnapshot();

        assertEquals(1, metrics.writeBatches());
        assertEquals(1, metrics.samplesWritten());
        assertEquals(1, metrics.checkpoints());
        assertEquals(1L, metrics.errorsByStatus().get(SeriesStatus.WRONG_OWNER));
    }

    /** {@link Clock} determinístico para forçar fechamento por ociosidade via {@link SeriesHandleRegistry#closeIdle()}. */
    private static final class MutableClock extends Clock {
        private Instant instant;

        MutableClock(Instant start) {
            this.instant = start;
        }

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            throw new UnsupportedOperationException("não usado nos testes");
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }

    /** Fake de {@link StorageRequestHandler.PlacementLookup}: mapa em memória, sem catálogo distribuído. */
    private static final class PlacementLookupFake implements StorageRequestHandler.PlacementLookup {
        private final Map<String, SeriesPlacement> placements = new HashMap<>();

        void put(String seriesKey, SeriesPlacement placement) {
            placements.put(seriesKey, placement);
        }

        @Override
        public Optional<SeriesPlacement> placementLocal(String seriesKey) {
            return Optional.ofNullable(placements.get(seriesKey));
        }
    }

    /** {@link Transport} mínimo: só {@code local()} é usado por {@code RequestHandlerSupport} neste teste. */
    private static final class FakeTransport implements Transport {
        private final NodeInfo local;

        FakeTransport(NodeId id) {
            this.local = new NodeInfo(id, "127.0.0.1", 0);
        }

        @Override
        public void start() {
        }

        @Override
        public NodeInfo local() {
            return local;
        }

        @Override
        public Collection<NodeInfo> peers() {
            return List.of();
        }

        @Override
        public void addListener(TransportListener listener) {
        }

        @Override
        public void removeListener(TransportListener listener) {
        }

        @Override
        public void broadcast(ClusterMessage message) {
        }

        @Override
        public void send(ClusterMessage message) {
        }

        @Override
        public CompletableFuture<ClusterMessage> sendAndAwait(ClusterMessage message) {
            return new CompletableFuture<>();
        }

        @Override
        public boolean isConnected(NodeId nodeId) {
            return false;
        }

        @Override
        public boolean isReachable(NodeId nodeId) {
            return false;
        }

        @Override
        public void addPeer(NodeInfo peer) {
        }

        @Override
        public void close() {
        }
    }
}
