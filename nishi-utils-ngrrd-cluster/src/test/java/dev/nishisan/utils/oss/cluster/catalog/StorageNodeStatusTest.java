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

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageNodeStatusTest {

    private static final Duration INTERVAL = Duration.ofSeconds(10);

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
    void isFreshNaFronteiraExataDeDuasVezesOIntervaloEhVerdadeiro() {
        long now = 100_000L;
        long reportedAt = now - 2 * INTERVAL.toMillis();
        StorageNodeStatus status = new StorageNodeStatus("node-a", NodeState.ACTIVE, 0, 0, 0, reportedAt);

        assertTrue(status.isFresh(now, INTERVAL), "exatamente 2x o intervalo ainda deve ser fresh (<=)");
    }

    @Test
    void isFreshUmMilissegundoAlemDaFronteiraEhFalso() {
        long now = 100_000L;
        long reportedAt = now - 2 * INTERVAL.toMillis() - 1;
        StorageNodeStatus status = new StorageNodeStatus("node-a", NodeState.ACTIVE, 0, 0, 0, reportedAt);

        assertFalse(status.isFresh(now, INTERVAL), "1ms além de 2x o intervalo não deve mais ser fresh");
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
