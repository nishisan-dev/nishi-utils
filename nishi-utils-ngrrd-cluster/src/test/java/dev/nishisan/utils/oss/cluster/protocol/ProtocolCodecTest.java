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

package dev.nishisan.utils.oss.cluster.protocol;

import dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec;
import dev.nishisan.utils.ngrid.common.ClientRequestPayload;
import dev.nishisan.utils.ngrid.common.ClientResponsePayload;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataPoint;
import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.PlacementState;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.metrics.BlobVolumeSummary;
import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;
import dev.nishisan.utils.oss.cluster.metrics.NodeMetricsSnapshot;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifica que todo payload do protocolo do cluster ngrrd sobrevive a um
 * round-trip completo pelo {@link JacksonMessageCodec} do core — o mesmo codec
 * usado pelo transporte real — dentro de {@link ClientRequestPayload}/
 * {@link ClientResponsePayload}, exatamente como trafegam em produção.
 */
class ProtocolCodecTest {

    private final JacksonMessageCodec codec = new JacksonMessageCodec();

    private <T> T roundTripRequestBody(String command, T body) throws IOException {
        ClientRequestPayload payload = new ClientRequestPayload(UUID.randomUUID(), command, body);
        ClusterMessage message = ClusterMessage.request(MessageType.CLIENT_REQUEST, command,
                NodeId.of("node-a"), NodeId.of("node-b"), payload);

        byte[] encoded = codec.encode(message);
        ClusterMessage decoded = codec.decode(encoded);
        ClientRequestPayload decodedPayload = decoded.payload(ClientRequestPayload.class);

        @SuppressWarnings("unchecked")
        T result = (T) decodedPayload.body();
        return result;
    }

    private <T> T roundTripResponseBody(String command, T body) throws IOException {
        ClientRequestPayload requestPayload = new ClientRequestPayload(UUID.randomUUID(), command, "trigger");
        ClusterMessage request = ClusterMessage.request(MessageType.CLIENT_REQUEST, command,
                NodeId.of("node-a"), NodeId.of("node-b"), requestPayload);
        ClientResponsePayload responsePayload =
                new ClientResponsePayload(requestPayload.requestId(), true, body, null);
        ClusterMessage response = ClusterMessage.response(request, responsePayload);

        byte[] encoded = codec.encode(response);
        ClusterMessage decoded = codec.decode(encoded);
        ClientResponsePayload decodedPayload = decoded.payload(ClientResponsePayload.class);

        @SuppressWarnings("unchecked")
        T result = (T) decodedPayload.body();
        return result;
    }

    @Test
    void liveCopyNegotiationAndPatchesSurviveTheWire() throws IOException {
        var prepare = new MigratePrepareRequest("series", "move", "series/series.ngrr", 16384, true);
        assertEquals(prepare, roundTripRequestBody(Commands.MIGRATE_PREPARE, prepare));
        var ready = MigrateResponse.of(MigrateStatus.COPY_READY, null);
        assertEquals(ready, roundTripResponseBody(Commands.MIGRATE_PREPARE, ready));
        var patch = new MigratePatchRequest("series", "move", 2, 4096, new byte[]{1, 2, 3});
        assertEquals(patch, roundTripRequestBody(Commands.MIGRATE_PATCH, patch));
        var legacy = new com.fasterxml.jackson.databind.ObjectMapper().readValue(
                "{\"seriesKey\":\"s\",\"migrationId\":\"m\",\"storageKey\":\"series/s.ngrr\",\"totalBytes\":4096}",
                MigratePrepareRequest.class);
        assertEquals(false, legacy.liveCopy());
    }

    @Test
    void placeRequestComPreferredOwnerSobreviveAoRoundTrip() throws IOException {
        PlaceRequest original = new PlaceRequest("series-1", "abc123def456", "node-a");
        assertEquals(original, roundTripRequestBody(Commands.PLACE, original));
    }

    @Test
    void placeRequestSemPreferredOwnerSobreviveAoRoundTrip() throws IOException {
        PlaceRequest original = new PlaceRequest("series-1", "abc123def456", null);
        assertEquals(original, roundTripRequestBody(Commands.PLACE, original));
    }

    @Test
    void placeResponseComPlacementMigrandoSobreviveAoRoundTrip() throws IOException {
        SeriesPlacement placement = new SeriesPlacement("node-a", "node-b", PlacementState.MIGRATING,
                "migration-1", 1_000L, 2_000L);
        PlaceResponse original = new PlaceResponse(SeriesStatus.OK, placement, null, null);
        assertEquals(original, roundTripResponseBody(Commands.PLACE, original));
    }

    @Test
    void placeResponseDeErroSemPlacementSobreviveAoRoundTrip() throws IOException {
        PlaceResponse original = new PlaceResponse(SeriesStatus.NO_STORAGE_NODE_AVAILABLE, null,
                "sem nós disponíveis", null);
        PlaceResponse roundTripped = roundTripResponseBody(Commands.PLACE, original);
        assertEquals(original, roundTripped);
        assertNull(roundTripped.placement());
    }

    @Test
    void placeResponseNotLeaderComLeaderNodeIdSobreviveAoRoundTrip() throws IOException {
        PlaceResponse original = new PlaceResponse(SeriesStatus.NOT_LEADER, null,
                "este nó não é o líder atual", "storage-1");
        PlaceResponse roundTripped = roundTripResponseBody(Commands.PLACE, original);
        assertEquals(original, roundTripped);
        assertEquals("storage-1", roundTripped.leaderNodeId());
    }

    @Test
    void openRequestComTagsEPlacementHintSobreviveAoRoundTrip() throws IOException {
        SeriesPlacement placement = SeriesPlacement.active("node-a", 1_000L);
        OpenRequest original = new OpenRequest("series-1", "ds: [in_octets]", Map.of("iface", "eth0"),
                Durability.FSYNC, OnGeometryChange.MIGRATE, placement);
        assertEquals(original, roundTripRequestBody(Commands.OPEN, original));
    }

    @Test
    void openRequestComTagsNulasViramVaziasNaoNull() throws IOException {
        OpenRequest original = new OpenRequest("series-1", "ds: [in_octets]", null,
                Durability.OS_CACHE, OnGeometryChange.FAIL, null);
        OpenRequest roundTripped = roundTripRequestBody(Commands.OPEN, original);
        assertEquals(Map.of(), roundTripped.tags());
        assertEquals(original, roundTripped);
    }

    @Test
    void seriesStatusResponseComWrongOwnerSobreviveAoRoundTrip() throws IOException {
        SeriesStatusResponse original = new SeriesStatusResponse(SeriesStatus.WRONG_OWNER, "node-c", "dono mudou");
        assertEquals(original, roundTripResponseBody(Commands.OPEN, original));
    }

    @Test
    void writeBatchRequestComVariasSeriesSobreviveAoRoundTrip() throws IOException {
        WriteBatchRequest original = new WriteBatchRequest(List.of(
                new SeriesWrite("series-1", "in_octets", 1_700_000_000_000L, 42.5),
                new SeriesWrite("series-2", "out_octets", 1_700_000_000_500L, 7.0)));
        assertEquals(original, roundTripRequestBody(Commands.WRITE_BATCH, original));
    }

    @Test
    void writeBatchRequestComListaNulaVemVaziaNaoNull() throws IOException {
        WriteBatchRequest original = new WriteBatchRequest(null);
        WriteBatchRequest roundTripped = roundTripRequestBody(Commands.WRITE_BATCH, original);
        assertEquals(List.of(), roundTripped.writes());
    }

    @Test
    void writeBatchResponseComMapasNulosViramVazios() throws IOException {
        WriteBatchResponse original = new WriteBatchResponse(null, null, null);
        WriteBatchResponse roundTripped = roundTripResponseBody(Commands.WRITE_BATCH, original);
        assertEquals(Map.of(), roundTripped.statusBySeries());
        assertEquals(Map.of(), roundTripped.ownerBySeries());
        assertEquals(Map.of(), roundTripped.errorBySeries());
        assertEquals(original, roundTripped);
    }

    @Test
    void writeBatchResponseComStatusMistoPorSerieSobreviveAoRoundTrip() throws IOException {
        WriteBatchResponse original = new WriteBatchResponse(
                Map.of("series-1", SeriesStatus.OK, "series-2", SeriesStatus.MIGRATING),
                Map.of("series-2", "node-b"),
                Map.of());
        assertEquals(original, roundTripResponseBody(Commands.WRITE_BATCH, original));
    }

    @Test
    void seriesCommandRequestSobreviveAoRoundTrip() throws IOException {
        SeriesCommandRequest original = new SeriesCommandRequest("series-1");
        assertEquals(original, roundTripRequestBody(Commands.CHECKPOINT, original));
    }

    @Test
    void readRequestComEndExclusiveNuloSobreviveAoRoundTrip() throws IOException {
        ReadRequest original = new ReadRequest("series-1", "in_octets", Duration.ofDays(1).toMillis(),
                60, ConsolidationFunction.AVERAGE, 500, null);
        ReadRequest roundTripped = roundTripRequestBody(Commands.READ, original);
        assertEquals(original, roundTripped);
        assertNull(roundTripped.endExclusiveEpochMs());
    }

    @Test
    void readRequestConstruidoAPartirDeViewQueryPreservaOsCampos() throws IOException {
        ViewQuery query = new ViewQuery(Duration.ofHours(6), 30, ConsolidationFunction.MAX, 200);
        ReadRequest original = ReadRequest.of("series-1", "in_octets", query, 1_700_000_000_000L);
        ReadRequest roundTripped = roundTripRequestBody(Commands.READ, original);
        assertEquals(original, roundTripped);
        assertEquals(query, roundTripped.toViewQuery());
    }

    @Test
    void readResponseComSeriesResultDePontosSobreviveAoRoundTrip() throws IOException {
        SeriesResult result = new SeriesResult("in_octets", "rra-1min", ConsolidationFunction.AVERAGE, 60,
                List.of(new DataPoint(1_000L, 1.5), new DataPoint(1_060L, Double.NaN)));
        ReadResponse original = new ReadResponse(SeriesStatus.OK, "node-a", result, null);
        assertEquals(original, roundTripResponseBody(Commands.READ, original));
    }

    @Test
    void readPresetRequestSobreviveAoRoundTrip() throws IOException {
        ReadPresetRequest original = new ReadPresetRequest("series-1", "dashboard-24h", 1_700_000_000_000L);
        assertEquals(original, roundTripRequestBody(Commands.READ_PRESET, original));
    }

    @Test
    void readPresetResponseComResultadosNulosViramVazios() throws IOException {
        ReadPresetResponse original = new ReadPresetResponse(SeriesStatus.MIGRATING, "node-a", null, "em migração");
        ReadPresetResponse roundTripped = roundTripResponseBody(Commands.READ_PRESET, original);
        assertEquals(Map.of(), roundTripped.results());
        assertEquals(original, roundTripped);
    }

    @Test
    void readPresetResponseComMultiplosSeriesResultSobreviveAoRoundTrip() throws IOException {
        SeriesResult inOctets = new SeriesResult("in_octets", "rra-1min", ConsolidationFunction.AVERAGE, 60,
                List.of(new DataPoint(1_000L, 10.0)));
        SeriesResult outOctets = new SeriesResult("out_octets", "rra-1min", ConsolidationFunction.LAST, 60,
                List.of(new DataPoint(1_000L, 20.0)));
        ReadPresetResponse original = new ReadPresetResponse(SeriesStatus.OK, "node-a",
                Map.of("in_octets", inOctets, "out_octets", outOctets), null);
        assertEquals(original, roundTripResponseBody(Commands.READ_PRESET, original));
    }

    @Test
    void geometryAndReservationProtocolSurviveTheWire() throws IOException {
        String yaml = java.nio.file.Files.readString(java.nio.file.Path.of("src/test/resources/iface-traffic-blob.yaml"));
        var geometry = dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor.from(
                new dev.nishisan.utils.oss.format.SeriesGeometry(
                        dev.nishisan.utils.oss.config.NgrrdYamlLoader.parse(yaml, ignored -> null)));
        var place = new PlaceRequest("series", "definition", null, geometry);
        assertEquals(place, roundTripRequestBody(Commands.PLACE, place));
        var update = new GeometryUpdateRequest("series", "owner", geometry);
        assertEquals(update, roundTripRequestBody(Commands.GEOMETRY_UPDATE, update));
        var prepare = new MigratePrepareRequest("series", "move", "series/series.ngrr", geometry.objectBytes());
        assertEquals(prepare, roundTripRequestBody(Commands.MIGRATE_PREPARE, prepare));
    }

    @Test
    void migrateStartRequestSobreviveAoRoundTrip() throws IOException {
        MigrateStartRequest original = new MigrateStartRequest("series-1", "migration-1", "node-b");
        assertEquals(original, roundTripRequestBody(Commands.MIGRATE_START, original));
    }

    @Test
    void migrateChunkRequestComTrezentosKibDeBytesSobreviveAoRoundTrip() throws IOException {
        byte[] data = new byte[300 * 1024];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 251);
        }
        MigrateChunkRequest original = new MigrateChunkRequest("series-1", "migration-1", 3, 7, data);
        MigrateChunkRequest roundTripped = roundTripRequestBody(Commands.MIGRATE_CHUNK, original);
        assertEquals(original, roundTripped);
        assertEquals(data.length, roundTripped.data().length);
    }

    @Test
    void migrateCommitRequestSobreviveAoRoundTrip() throws IOException {
        MigrateCommitRequest original = new MigrateCommitRequest("series-1", "migration-1",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 1_677_721L,
                "series/series-1.ngrr");
        assertEquals(original, roundTripRequestBody(Commands.MIGRATE_COMMIT, original));
    }

    @Test
    void migrateControlRequestSobreviveAoRoundTrip() throws IOException {
        MigrateControlRequest original = new MigrateControlRequest("series-1", "migration-1");
        assertEquals(original, roundTripRequestBody(Commands.MIGRATE_ABORT, original));
    }

    @Test
    void migrateResponseDeHashMismatchSobreviveAoRoundTrip() throws IOException {
        MigrateResponse original = new MigrateResponse(MigrateStatus.HASH_MISMATCH, "sha256 não confere", 0L);
        assertEquals(original, roundTripResponseBody(Commands.MIGRATE_COMMIT, original));
    }

    @Test
    void seriesExistsRequestSobreviveAoRoundTrip() throws IOException {
        SeriesExistsRequest original = new SeriesExistsRequest("series-1");
        assertEquals(original, roundTripRequestBody(Commands.SERIES_EXISTS, original));
    }

    @Test
    void seriesExistsResponseComExistsTrueSobreviveAoRoundTrip() throws IOException {
        SeriesExistsResponse original = new SeriesExistsResponse(true, 1_677_721L);
        assertEquals(original, roundTripResponseBody(Commands.SERIES_EXISTS, original));
    }

    @Test
    void seriesExistsResponseComExistsFalseSobreviveAoRoundTrip() throws IOException {
        SeriesExistsResponse original = new SeriesExistsResponse(false, 0L);
        SeriesExistsResponse roundTripped = roundTripResponseBody(Commands.SERIES_EXISTS, original);
        assertEquals(original, roundTripped);
        assertEquals(0L, roundTripped.bytes());
    }

    @Test
    void adminNodeStatusResponseOkComNodeStatusSobreviveAoRoundTrip() throws IOException {
        StorageNodeStatus status = new StorageNodeStatus("node-a", NodeState.DRAINING, 5, 1_000, 10_000, 5_000L);
        AdminNodeStatusResponse original = new AdminNodeStatusResponse(SeriesStatus.OK, "node-a", status, null);
        assertEquals(original, roundTripResponseBody(Commands.ADMIN_DRAIN, original));
    }

    @Test
    void adminNodeStatusResponseComCapacidadesSobreviveAoRoundTrip() throws IOException {
        StorageNodeStatus status = new StorageNodeStatus("node-a", NodeState.ACTIVE, 5, 1_000, 10_000, 5_000L,
                DistributionMode.COUNT, 1, 0, StorageCapabilities.ALL);
        AdminNodeStatusResponse original = new AdminNodeStatusResponse(SeriesStatus.OK, "node-a", status, null);

        AdminNodeStatusResponse roundTripped = roundTripResponseBody(Commands.ADMIN_ACTIVATE, original);

        assertEquals(original, roundTripped);
        assertEquals(StorageCapabilities.ALL, roundTripped.nodeStatus().capabilities());
    }

    @Test
    void statusDeNoSemCapacidadesNoJsonDesserializaComCapacidadesVazias() throws IOException {
        StorageNodeStatus legacy = JacksonMessageCodec.createDefaultMapper().readValue(
                "{\"nodeId\":\"node-a\",\"state\":\"ACTIVE\",\"seriesCount\":5,\"usedBytes\":1000,"
                        + "\"capacityBytes\":10000,\"reportedAtEpochMs\":5000,\"distributionMode\":\"COUNT\","
                        + "\"weight\":1.0,\"reservedBytes\":0}", StorageNodeStatus.class);

        assertEquals("node-a", legacy.nodeId());
        assertEquals(Set.of(), legacy.capabilities());
    }

    @Test
    void adminNodeStatusResponseDeErroSemNodeStatusSobreviveAoRoundTrip() throws IOException {
        AdminNodeStatusResponse original = new AdminNodeStatusResponse(SeriesStatus.ERROR, "node-a", null,
                "nó desconhecido pelo catálogo: node-x");
        AdminNodeStatusResponse roundTripped = roundTripResponseBody(Commands.ADMIN_DRAIN, original);
        assertEquals(original, roundTripped);
        assertNull(roundTripped.nodeStatus());
    }

    @Test
    void adminNodeStatusResponseNotLeaderSobreviveAoRoundTrip() throws IOException {
        AdminNodeStatusResponse original = new AdminNodeStatusResponse(SeriesStatus.NOT_LEADER, "storage-1", null, null);
        assertEquals(original, roundTripResponseBody(Commands.ADMIN_ACTIVATE, original));
    }

    @Test
    void adminNodeRequestSobreviveAoRoundTrip() throws IOException {
        AdminNodeRequest original = new AdminNodeRequest("node-a", false);
        assertEquals(original, roundTripRequestBody(Commands.ADMIN_DRAIN, original));
    }

    @Test
    void adminNodeRequestEncaminhadoSobreviveAoRoundTrip() throws IOException {
        AdminNodeRequest original = new AdminNodeRequest("node-a", true);
        AdminNodeRequest roundTripped = roundTripRequestBody(Commands.ADMIN_METRICS, original);
        assertEquals(original, roundTripped);
        assertTrue(roundTripped.forwarded());
    }

    @Test
    void adminStatusResponseComNosEContagensSobreviveAoRoundTrip() throws IOException {
        StorageNodeStatus nodeA = new StorageNodeStatus("node-a", NodeState.ACTIVE, 120, 1_000_000, 10_000_000, 5_000L);
        StorageNodeStatus nodeB = new StorageNodeStatus("node-b", NodeState.DRAINING, 80, 500_000, 10_000_000, 5_000L);
        AdminStatusResponse original = new AdminStatusResponse(SeriesStatus.OK, "node-a",
                List.of(new NodeStatusView(nodeA, true), new NodeStatusView(nodeB, false)), 1,
                Map.of("node-a", 120L, "node-b", 80L));
        assertEquals(original, roundTripResponseBody(Commands.ADMIN_STATUS, original));
    }

    @Test
    void adminStatusResponseComListasEMapasNulosViramVazios() throws IOException {
        AdminStatusResponse original = new AdminStatusResponse(SeriesStatus.NOT_LEADER, null, null, 0, null);
        AdminStatusResponse roundTripped = roundTripResponseBody(Commands.ADMIN_STATUS, original);
        assertEquals(List.of(), roundTripped.nodes());
        assertEquals(Map.of(), roundTripped.seriesCountByNode());
        assertEquals(original, roundTripped);
    }

    @Test
    void nodeMetricsSnapshotComHistogramasEErrosPorStatusSobreviveAoRoundTrip() throws IOException {
        // M2 (achado do Refuter): cobre especificamente os campos que um round-trip vazio não
        // exercitaria — Map<SeriesStatus, Long> não vazio, os três LatencySnapshot com valores reais
        // (não LatencySnapshot.EMPTY) e o BlobVolumeSummary.
        LatencySnapshot writeBatchLatency = new LatencySnapshot(120L, 850L, 4_200L, 9_100L);
        LatencySnapshot checkpointLatency = new LatencySnapshot(30L, 1_500L, 6_000L, 12_000L);
        LatencySnapshot readLatency = new LatencySnapshot(75L, 300L, 1_100L, 2_500L);
        LatencySnapshot leaderConfirmationLatency = new LatencySnapshot(9L, 700L, 2_000L, 3_000L);
        BlobVolumeSummary blobStats = new BlobVolumeSummary(4, 10_485_760L, 104_857_600L, 0.42, 350, 8_192L);
        Map<SeriesStatus, Long> errorsByStatus = Map.of(
                SeriesStatus.WRONG_OWNER, 3L,
                SeriesStatus.NOT_OPEN, 1L,
                SeriesStatus.ERROR, 2L);
        NodeMetricsSnapshot original = new NodeMetricsSnapshot("storage-0", 1_700_000_000_000L, true, 350L,
                10_485_760L, 104_857_600L, 12, 120L, 4_800L, 7L, 30L, 5L, 75L, writeBatchLatency, checkpointLatency,
                readLatency, errorsByStatus, blobStats, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 9L, leaderConfirmationLatency);

        NodeMetricsSnapshot roundTripped = roundTripResponseBody(Commands.ADMIN_METRICS, original);

        assertEquals(original, roundTripped);
        assertEquals(errorsByStatus, roundTripped.errorsByStatus());
        assertEquals(writeBatchLatency, roundTripped.writeBatchLatency());
        assertEquals(checkpointLatency, roundTripped.checkpointLatency());
        assertEquals(readLatency, roundTripped.readLatency());
        assertEquals(leaderConfirmationLatency, roundTripped.leaderConfirmationLatency());
        assertEquals(blobStats, roundTripped.blobStats());
    }

    @Test
    void catalogLookupRequestSobreviveAoRoundTrip() throws IOException {
        CatalogLookupRequest original = new CatalogLookupRequest(List.of("series-1", "series-2"));
        assertEquals(original, roundTripRequestBody(Commands.CATALOG_LOOKUP, original));
    }

    @Test
    void catalogLookupRequestComListaNulaVemVazia() throws IOException {
        CatalogLookupRequest original = new CatalogLookupRequest(null);
        CatalogLookupRequest roundTripped = roundTripRequestBody(Commands.CATALOG_LOOKUP, original);
        assertEquals(List.of(), roundTripped.seriesKeys());
        assertEquals(original, roundTripped);
    }

    @Test
    void catalogLookupResponseComPlacementsAtivoEMigrandoSobreviveAoRoundTrip() throws IOException {
        SeriesPlacement active = SeriesPlacement.active("node-a", 1_000L);
        SeriesPlacement migrating = SeriesPlacement.migrating(
                SeriesPlacement.active("node-b", 1_000L), "node-c", "migration-1", 2_000L);
        CatalogLookupResponse original =
                CatalogLookupResponse.ok(Map.of("series-1", active, "series-2", migrating));
        assertEquals(original, roundTripResponseBody(Commands.CATALOG_LOOKUP, original));
    }

    @Test
    void catalogLookupResponseNotLeaderComHintSobreviveAoRoundTrip() throws IOException {
        CatalogLookupResponse original = CatalogLookupResponse.notLeader("storage-1");
        CatalogLookupResponse roundTripped = roundTripResponseBody(Commands.CATALOG_LOOKUP, original);
        assertEquals(original, roundTripped);
        assertEquals("storage-1", roundTripped.leaderNodeId());
    }

    @Test
    void seriesExistsBatchRequestEResponseSobrevivemAoRoundTrip() throws IOException {
        SeriesExistsBatchRequest request = new SeriesExistsBatchRequest(List.of("series-1", "series-2", "series-3"));
        assertEquals(request, roundTripRequestBody(Commands.SERIES_EXISTS_BATCH, request));

        SeriesExistsBatchResponse response = SeriesExistsBatchResponse.ok(Set.of("series-1", "series-3"));
        assertEquals(response, roundTripResponseBody(Commands.SERIES_EXISTS_BATCH, response));
    }

    @Test
    void seriesExistsBatchResponseOkSemPresentNoJsonDesserializaComPresentNulo() throws IOException {
        SeriesExistsBatchResponse response = JacksonMessageCodec.createDefaultMapper()
                .readValue("{\"status\":\"OK\",\"message\":null}", SeriesExistsBatchResponse.class);

        assertEquals(SeriesStatus.OK, response.status());
        assertNull(response.present(), "sem o campo present no JSON, present() precisa continuar null — "
                + "nunca virar Set.of() (que o cliente confundiria com \"nenhuma chave presente\")");
    }

    @Test
    void openRequestComCreateIfMissingFalseSobreviveAoRoundTrip() throws IOException {
        SeriesPlacement placement = SeriesPlacement.active("node-a", 1_000L);
        OpenRequest original = new OpenRequest("series-1", "ds: [in_octets]", Map.of(), Durability.FSYNC,
                OnGeometryChange.MIGRATE, placement, false);
        OpenRequest roundTripped = roundTripRequestBody(Commands.OPEN, original);
        assertEquals(original, roundTripped);
        assertEquals(false, roundTripped.createIfMissingOrDefault());
    }

    @Test
    void seriesStatusResponseComConfirmacaoDeCreateIfMissingSobreviveAoRoundTrip() throws IOException {
        SeriesStatusResponse original = new SeriesStatusResponse(SeriesStatus.OK, "node-a", null, Boolean.TRUE);

        SeriesStatusResponse roundTripped = roundTripResponseBody(Commands.OPEN, original);

        assertEquals(original, roundTripped);
        assertEquals(Boolean.TRUE, roundTripped.createIfMissingHonored());
    }

    @Test
    void seriesStatusResponseSemConfirmacaoDesserializaComoNula() throws IOException {
        SeriesStatusResponse legacy = JacksonMessageCodec.createDefaultMapper().readValue(
                "{\"status\":\"OK\",\"ownerNodeId\":\"node-a\"}", SeriesStatusResponse.class);

        assertEquals(SeriesStatus.OK, legacy.status());
        assertNull(legacy.createIfMissingHonored(), "resposta de storage anterior não traz a confirmação");
        assertNull(new SeriesStatusResponse(SeriesStatus.OK, "node-a", null).createIfMissingHonored());
    }

    @Test
    void openRequestSemCreateIfMissingDesserializaComoCriar() throws IOException {
        OpenRequest legacy = new ObjectMapper().readValue(
                "{\"seriesKey\":\"series-1\",\"yaml\":\"ds: [in_octets]\"}", OpenRequest.class);
        assertTrue(legacy.createIfMissingOrDefault());
    }
}
