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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeriesPlacementTest {

    /**
     * B1 (achado do Debugger): {@code NMapPersistence} (core) grava o WAL via {@code ObjectOutputStream}
     * — sem {@link java.io.Serializable}, todo append falhava com {@code NotSerializableException} e o
     * catálogo persistente nunca persistia de fato.
     */
    @Test
    void sobrevivePeloObjectOutputStreamEObjectInputStream() throws IOException, ClassNotFoundException {
        SeriesPlacement original = SeriesPlacement.active("storage-0", 1_000L);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(original);
        }
        SeriesPlacement roundTripped;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            roundTripped = (SeriesPlacement) in.readObject();
        }

        assertEquals(original, roundTripped);
    }

    @Test
    void construtorFalhaQuandoOwnerNodeIdEhNulo() {
        assertThrows(NullPointerException.class,
                () -> new SeriesPlacement(null, null, PlacementState.ACTIVE, null, 1_000L, 1_000L));
    }

    @Test
    void construtorFalhaQuandoMigratingSemTargetNodeId() {
        assertThrows(IllegalArgumentException.class,
                () -> new SeriesPlacement("node-a", null, PlacementState.MIGRATING, "migration-1", 1_000L, 1_000L));
    }

    @Test
    void construtorFalhaQuandoMigratingSemMigrationId() {
        assertThrows(IllegalArgumentException.class,
                () -> new SeriesPlacement("node-a", "node-b", PlacementState.MIGRATING, null, 1_000L, 1_000L));
    }

    @Test
    void construtorFalhaQuandoMigratingSemTargetNemMigrationId() {
        assertThrows(IllegalArgumentException.class,
                () -> new SeriesPlacement("node-a", null, PlacementState.MIGRATING, null, 1_000L, 1_000L));
    }

    @Test
    void construtorFalhaQuandoActiveComTargetNodeId() {
        assertThrows(IllegalArgumentException.class,
                () -> new SeriesPlacement("node-a", "node-b", PlacementState.ACTIVE, null, 1_000L, 1_000L));
    }

    @Test
    void construtorFalhaQuandoActiveComMigrationId() {
        assertThrows(IllegalArgumentException.class,
                () -> new SeriesPlacement("node-a", null, PlacementState.ACTIVE, "migration-1", 1_000L, 1_000L));
    }

    @Test
    void construtorFalhaQuandoActiveComTargetEMigrationId() {
        assertThrows(IllegalArgumentException.class,
                () -> new SeriesPlacement("node-a", "node-b", PlacementState.ACTIVE, "migration-1", 1_000L, 1_000L));
    }

    @Test
    void activeCriaPlacementValidoComCreatedIgualUpdated() {
        SeriesPlacement placement = SeriesPlacement.active("node-a", 1_000L);

        assertEquals("node-a", placement.ownerNodeId());
        assertNull(placement.targetNodeId());
        assertEquals(PlacementState.ACTIVE, placement.state());
        assertNull(placement.migrationId());
        assertEquals(1_000L, placement.createdAtEpochMs());
        assertEquals(1_000L, placement.updatedAtEpochMs());
    }

    @Test
    void migratingPreservaOwnerECreatedAtEAtualizaUpdatedAt() {
        SeriesPlacement current = SeriesPlacement.active("node-a", 1_000L);

        SeriesPlacement migrating = SeriesPlacement.migrating(current, "node-b", "migration-1", 2_000L);

        assertEquals("node-a", migrating.ownerNodeId());
        assertEquals("node-b", migrating.targetNodeId());
        assertEquals(PlacementState.MIGRATING, migrating.state());
        assertEquals("migration-1", migrating.migrationId());
        assertEquals(1_000L, migrating.createdAtEpochMs(), "createdAt não muda numa transição");
        assertEquals(2_000L, migrating.updatedAtEpochMs());
    }

    @Test
    void migratingFalhaQuandoCurrentNaoEstaActive() {
        SeriesPlacement current = SeriesPlacement.active("node-a", 1_000L);
        SeriesPlacement jaMigrando = SeriesPlacement.migrating(current, "node-b", "migration-1", 2_000L);

        assertThrows(IllegalStateException.class,
                () -> SeriesPlacement.migrating(jaMigrando, "node-c", "migration-2", 3_000L));
    }

    @Test
    void completedTransfereOwnershipParaOTargetEVoltaParaActive() {
        SeriesPlacement current = SeriesPlacement.active("node-a", 1_000L);
        SeriesPlacement migrating = SeriesPlacement.migrating(current, "node-b", "migration-1", 2_000L);

        SeriesPlacement completed = SeriesPlacement.completed(migrating, 3_000L);

        assertEquals("node-b", completed.ownerNodeId(), "o dono passa a ser o antigo alvo");
        assertNull(completed.targetNodeId());
        assertEquals(PlacementState.ACTIVE, completed.state());
        assertNull(completed.migrationId());
        assertEquals(1_000L, completed.createdAtEpochMs(), "createdAt não muda numa transição");
        assertEquals(3_000L, completed.updatedAtEpochMs());
    }

    @Test
    void completedFalhaQuandoPlacementNaoEstaMigrating() {
        SeriesPlacement active = SeriesPlacement.active("node-a", 1_000L);

        assertThrows(IllegalStateException.class, () -> SeriesPlacement.completed(active, 2_000L));
    }

    @Test
    void abortedVoltaParaActiveNoOwnerOriginal() {
        SeriesPlacement current = SeriesPlacement.active("node-a", 1_000L);
        SeriesPlacement migrating = SeriesPlacement.migrating(current, "node-b", "migration-1", 2_000L);

        SeriesPlacement aborted = SeriesPlacement.aborted(migrating, 3_000L);

        assertEquals("node-a", aborted.ownerNodeId(), "o dono volta a ser o original, não o alvo");
        assertNull(aborted.targetNodeId());
        assertEquals(PlacementState.ACTIVE, aborted.state());
        assertNull(aborted.migrationId());
        assertEquals(1_000L, aborted.createdAtEpochMs(), "createdAt não muda numa transição");
        assertEquals(3_000L, aborted.updatedAtEpochMs());
    }

    @Test
    void abortedFalhaQuandoPlacementNaoEstaMigrating() {
        SeriesPlacement active = SeriesPlacement.active("node-a", 1_000L);

        assertThrows(IllegalStateException.class, () -> SeriesPlacement.aborted(active, 2_000L));
    }

    @Test
    void placementSerializadoPelaVersao870LeTodosOsCamposDaquelaVersao() throws Exception {
        try (ObjectInputStream in = new ObjectInputStream(
                getClass().getResourceAsStream("/legacy-catalog/placement-8.7.0.ser"))) {
            SeriesPlacement placement = (SeriesPlacement) in.readObject();

            assertEquals("legacy-870", placement.ownerNodeId());
            assertNull(placement.targetNodeId());
            assertEquals(PlacementState.ACTIVE, placement.state());
            assertNull(placement.migrationId());
            assertEquals(8765L, placement.createdAtEpochMs());
            assertEquals(8766L, placement.updatedAtEpochMs());
            assertEquals("geometry-870", placement.geometryId());
            assertTrue(placement.geometryConfirmed());
        }
    }

    @Test
    void isOwnedByDuranteMigracaoRespondePeloOwnerNaoPeloTarget() {
        SeriesPlacement current = SeriesPlacement.active("node-a", 1_000L);
        SeriesPlacement migrating = SeriesPlacement.migrating(current, "node-b", "migration-1", 2_000L);

        assertTrue(migrating.isOwnedBy("node-a"));
        assertFalse(migrating.isOwnedBy("node-b"));
    }
}
