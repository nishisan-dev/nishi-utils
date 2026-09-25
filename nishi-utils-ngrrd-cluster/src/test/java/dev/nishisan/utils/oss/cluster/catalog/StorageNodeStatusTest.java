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

package dev.nishisan.utils.oss.cluster.catalog;

import dev.nishisan.utils.ngrid.map.MapReplicationCodec;
import dev.nishisan.utils.ngrid.map.MapReplicationCommand;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageNodeStatusTest {

    private static final Duration STALE_AFTER = Duration.ofSeconds(10);

    /** B1 (achado do Debugger): ver o mesmo teste em {@code SeriesPlacementTest}. */
    @Test
    void sobrevivePeloObjectOutputStreamEObjectInputStream() throws IOException, ClassNotFoundException {
        StorageNodeStatus original = new StorageNodeStatus("storage-0", NodeState.ACTIVE, 42, 1_000, 10_000, 5_000L);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(original);
        }
        StorageNodeStatus roundTripped;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            roundTripped = (StorageNodeStatus) in.readObject();
        }

        assertEquals(original, roundTripped);
    }

    @Test
    void fillRatioEhZeroQuandoCapacidadeEhZero() {
        StorageNodeStatus status = new StorageNodeStatus("node-a", NodeState.ACTIVE, 10, 500, 0, 1_000L);

        assertEquals(0.0, status.fillRatio());
    }

    @Test
    void fillRatioEhZeroQuandoCapacidadeEhNegativa() {
        StorageNodeStatus status = new StorageNodeStatus("node-a", NodeState.ACTIVE, 10, 500, -1, 1_000L);

        assertEquals(0.0, status.fillRatio());
    }

    @Test
    void fillRatioEhARazaoEntreUsadoECapacidadeQuandoCapacidadeEhConhecida() {
        StorageNodeStatus status = new StorageNodeStatus("node-a", NodeState.ACTIVE, 10, 250, 1000, 1_000L);

        assertEquals(0.25, status.fillRatio());
    }

    @Test
    void isFreshNaFronteiraExataDoPrazoEhVerdadeiro() {
        // F2.1 (Debugger): isFresh agora compara DIRETO com staleAfter — o chamador já decide o prazo
        // completo (ex.: StorageNodeConfig.nodeStatusStaleAfter(), tipicamente 5x o intervalo de
        // relatório, não 2x); isFresh não multiplica nada por conta própria.
        long now = 100_000L;
        long reportedAt = now - STALE_AFTER.toMillis();
        StorageNodeStatus status = new StorageNodeStatus("node-a", NodeState.ACTIVE, 0, 0, 0, reportedAt);

        assertTrue(status.isFresh(now, STALE_AFTER), "exatamente no prazo ainda deve ser fresh (<=)");
    }

    @Test
    void isFreshUmMilissegundoAlemDaFronteiraEhFalso() {
        long now = 100_000L;
        long reportedAt = now - STALE_AFTER.toMillis() - 1;
        StorageNodeStatus status = new StorageNodeStatus("node-a", NodeState.ACTIVE, 0, 0, 0, reportedAt);

        assertFalse(status.isFresh(now, STALE_AFTER), "1ms além do prazo não deve mais ser fresh");
    }

    @Test
    void withLoadAtualizaCargaEReportedAtPreservandoNodeIdEState() {
        StorageNodeStatus original = StorageNodeStatus.active("node-a", 1_000L);

        StorageNodeStatus updated = original.withLoad(42, 12_345, 100_000, 2_000L);

        assertEquals("node-a", updated.nodeId());
        assertEquals(NodeState.ACTIVE, updated.state());
        assertEquals(42, updated.seriesCount());
        assertEquals(12_345, updated.usedBytes());
        assertEquals(100_000, updated.capacityBytes());
        assertEquals(2_000L, updated.reportedAtEpochMs());
    }

    @Test
    void withStateTransicionaEstadoPreservandoCargaReportada() {
        StorageNodeStatus original = new StorageNodeStatus("node-a", NodeState.ACTIVE, 42, 12_345, 100_000, 1_000L);

        StorageNodeStatus draining = original.withState(NodeState.DRAINING, 2_000L);

        assertEquals("node-a", draining.nodeId());
        assertEquals(NodeState.DRAINING, draining.state());
        assertEquals(42, draining.seriesCount());
        assertEquals(12_345, draining.usedBytes());
        assertEquals(100_000, draining.capacityBytes());
        assertEquals(2_000L, draining.reportedAtEpochMs());
    }

    @Test
    void statusSerializadoPelaVersao850SemCapacidadesLeCapacidadesVazias() throws Exception {
        try (ObjectInputStream in = new ObjectInputStream(
                getClass().getResourceAsStream("/legacy-catalog/node-8.5.0.ser"))) {
            StorageNodeStatus status = (StorageNodeStatus) in.readObject();

            assertEquals("legacy-850", status.nodeId());
            assertEquals(7, status.seriesCount());
            assertEquals(DistributionMode.WEIGHT, status.distributionMode());
            assertEquals(2.5, status.weight());
            assertEquals(100, status.reservedBytes());
            assertEquals(Set.of(), status.capabilities());
        }
    }

    @Test
    void statusSerializadoPelaVersao831LeCapacidadesVazias() throws Exception {
        try (ObjectInputStream in = new ObjectInputStream(
                getClass().getResourceAsStream("/legacy-catalog/node-8.3.1.ser"))) {
            StorageNodeStatus status = (StorageNodeStatus) in.readObject();

            assertEquals("legacy", status.nodeId());
            assertEquals(Set.of(), status.capabilities());
        }
    }

    @Test
    void statusReplicadoPelaVersao850SemCapacidadesLeCapacidadesVazias() {
        // Bytes produzidos pelo MapReplicationCodec com o record de 9 componentes da 8.5.0.
        String legacy = "{\"type\":\"PUT\",\"key\":\"legacy-850\",\"value\":{\"@class\":"
                + "\"dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus\",\"nodeId\":\"legacy-850\","
                + "\"state\":\"ACTIVE\",\"seriesCount\":7,\"usedBytes\":2048,\"capacityBytes\":50000,"
                + "\"reportedAtEpochMs\":4321,\"distributionMode\":\"WEIGHT\",\"weight\":2.5,\"reservedBytes\":100}}";

        MapReplicationCommand command = MapReplicationCodec.decode(legacy.getBytes(StandardCharsets.UTF_8));

        StorageNodeStatus status = (StorageNodeStatus) command.value();
        assertEquals("legacy-850", status.nodeId());
        assertEquals(100, status.reservedBytes());
        assertEquals(Set.of(), status.capabilities());
    }

    @Test
    void capacidadesSobrevivemAReplicacaoDoMapa() {
        StorageNodeStatus original = withCapabilities(Set.copyOf(StorageCapabilities.ALL));

        MapReplicationCommand decoded = MapReplicationCodec.decode(
                MapReplicationCodec.encode(MapReplicationCommand.put(original.nodeId(), original)));

        assertEquals(original, decoded.value());
        assertEquals(StorageCapabilities.ALL, ((StorageNodeStatus) decoded.value()).capabilities());
    }

    @Test
    void capacidadesSobrevivemAoObjectOutputStream() throws IOException, ClassNotFoundException {
        StorageNodeStatus original = withCapabilities(Set.of(StorageCapabilities.CATALOG_LOOKUP));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(original);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            assertEquals(original, in.readObject());
        }
    }

    @Test
    void capacidadesNulasViramVaziasEACopiaEhImutavel() {
        assertEquals(Set.of(), withCapabilities(null).capabilities());
        assertEquals(Set.of(), new StorageNodeStatus("node-a", NodeState.ACTIVE, 0, 0, 0, 1L).capabilities());

        Set<String> source = new HashSet<>(Set.of(StorageCapabilities.CATALOG_LOOKUP));
        StorageNodeStatus status = withCapabilities(source);
        source.add(StorageCapabilities.SERIES_EXISTS_BATCH);

        assertEquals(Set.of(StorageCapabilities.CATALOG_LOOKUP), status.capabilities());
        assertThrows(UnsupportedOperationException.class, () -> status.capabilities().add("x"));
    }

    @Test
    void transicoesPreservamAsCapacidades() {
        StorageNodeStatus status = withCapabilities(StorageCapabilities.ALL);

        assertEquals(StorageCapabilities.ALL, status.withLoad(1, 2, 3, 4L).capabilities());
        assertEquals(StorageCapabilities.ALL, status.withState(NodeState.DRAINING, 4L).capabilities());
    }

    @Test
    void capacidadesAnunciadasSaoAsTresDoProtocolo() {
        assertEquals("catalog.lookup", StorageCapabilities.CATALOG_LOOKUP);
        assertEquals("open.createIfMissing", StorageCapabilities.OPEN_CREATE_IF_MISSING);
        assertEquals("series.exists.batch", StorageCapabilities.SERIES_EXISTS_BATCH);
        assertEquals(Set.of("catalog.lookup", "open.createIfMissing", "series.exists.batch"), StorageCapabilities.ALL);
    }

    @Test
    void statusSerializadoPelaVersao860SemReplicaDoCatalogoLeReplicaNula() throws Exception {
        try (ObjectInputStream in = new ObjectInputStream(
                getClass().getResourceAsStream("/legacy-catalog/node-8.6.0.ser"))) {
            StorageNodeStatus status = (StorageNodeStatus) in.readObject();

            assertEquals("legacy-860", status.nodeId());
            assertEquals(11, status.seriesCount());
            assertEquals(StorageCapabilities.ALL, status.capabilities());
            assertNull(status.catalogReplica());
        }
    }

    @Test
    void statusReplicadoPelaVersao860SemReplicaDoCatalogoLeReplicaNula() {
        // Bytes produzidos pelo MapReplicationCodec com o record de 10 componentes da 8.6.0.
        String legacy = "{\"type\":\"PUT\",\"key\":\"legacy-860\",\"value\":{\"@class\":"
                + "\"dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus\",\"nodeId\":\"legacy-860\","
                + "\"state\":\"ACTIVE\",\"seriesCount\":11,\"usedBytes\":3072,\"capacityBytes\":60000,"
                + "\"reportedAtEpochMs\":9876,\"distributionMode\":\"WEIGHT\",\"weight\":1.5,\"reservedBytes\":200,"
                + "\"capabilities\":[\"java.util.ImmutableCollections$Set12\",[\"catalog.lookup\"]]}}";

        MapReplicationCommand command = MapReplicationCodec.decode(legacy.getBytes(StandardCharsets.UTF_8));

        StorageNodeStatus status = (StorageNodeStatus) command.value();
        assertEquals("legacy-860", status.nodeId());
        assertEquals(Set.of(StorageCapabilities.CATALOG_LOOKUP), status.capabilities());
        assertNull(status.catalogReplica());
    }

    @Test
    void replicaDoCatalogoSobreviveAReplicacaoDoMapa() {
        StorageNodeStatus original = withCatalogReplica(
                new CatalogReplicaStatus(false, 12L, 5_000L, 4_989L, false, false, true));

        MapReplicationCommand decoded = MapReplicationCodec.decode(
                MapReplicationCodec.encode(MapReplicationCommand.put(original.nodeId(), original)));

        assertEquals(original, decoded.value());
        assertEquals(original.catalogReplica(), ((StorageNodeStatus) decoded.value()).catalogReplica());
    }

    @Test
    void replicaDoCatalogoSobreviveAoObjectOutputStream() throws IOException, ClassNotFoundException {
        StorageNodeStatus original = withCatalogReplica(CatalogReplicaStatus.ofLeader());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(original);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            assertEquals(original, in.readObject());
        }
    }

    @Test
    void construtoresAnterioresDeixamAReplicaDoCatalogoNula() {
        assertNull(new StorageNodeStatus("node-a", NodeState.ACTIVE, 0, 0, 0, 1L).catalogReplica());
        assertNull(withCapabilities(StorageCapabilities.ALL).catalogReplica());
    }

    @Test
    void transicoesPreservamAReplicaDoCatalogo() {
        CatalogReplicaStatus replica = new CatalogReplicaStatus(false, 3L, 100L, 98L, false, false, true);
        StorageNodeStatus status = withCatalogReplica(replica);

        assertEquals(replica, status.withLoad(1, 2, 3, 4L).catalogReplica());
        assertEquals(replica, status.withState(NodeState.DRAINING, 4L).catalogReplica());
    }

    private static StorageNodeStatus withCatalogReplica(CatalogReplicaStatus replica) {
        return new StorageNodeStatus("node-a", NodeState.ACTIVE, 1, 2, 3, 4L, DistributionMode.COUNT, 1, 0,
                StorageCapabilities.ALL, replica);
    }

    private static StorageNodeStatus withCapabilities(Set<String> capabilities) {
        return new StorageNodeStatus("node-a", NodeState.ACTIVE, 1, 2, 3, 4L, DistributionMode.COUNT, 1, 0,
                capabilities);
    }
}
