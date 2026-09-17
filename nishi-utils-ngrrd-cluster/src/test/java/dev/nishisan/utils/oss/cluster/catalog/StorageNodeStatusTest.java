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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
}
