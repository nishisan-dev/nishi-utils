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

import dev.nishisan.utils.ngrid.replication.ReplicationManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CatalogReplicaStatusTest {

    @Test
    void liderEstaSempreEmDiaEComLagConhecido() {
        CatalogReplicaStatus leader = CatalogReplicaStatus.ofLeader();

        assertTrue(leader.leader());
        assertTrue(leader.lagKnown());
        assertTrue(leader.caughtUp(0));
        assertEquals(0L, leader.lag());
    }

    @Test
    void fromComLiderIgnoraOStatusDoTopico() {
        ReplicationManager.TopicReplicationStatus stale = topicStatus(0L, 0L, true, true);

        assertEquals(CatalogReplicaStatus.ofLeader(), CatalogReplicaStatus.from(true, stale));
    }

    @Test
    void fromSemStatusDoTopicoDevolveLagDesconhecido() {
        CatalogReplicaStatus unknown = CatalogReplicaStatus.from(false, null);

        assertFalse(unknown.leader());
        assertEquals(0L, unknown.leaderHighWatermark());
        assertFalse(unknown.lagKnown());
        assertFalse(unknown.caughtUp(Long.MAX_VALUE));
    }

    @Test
    void fromCopiaOsCamposDoTopicoNoSeguidor() {
        CatalogReplicaStatus status = CatalogReplicaStatus.from(false, topicStatus(5_000L, 12L, false, false));

        assertFalse(status.leader());
        assertEquals(12L, status.lag());
        assertEquals(5_000L, status.leaderHighWatermark());
        assertEquals(4_989L, status.nextExpectedSequence());
        assertFalse(status.syncing());
        assertFalse(status.pendingBootstrap());
        assertTrue(status.streaming());
    }

    @Test
    void lagZeroComHighWatermarkDesconhecidoNaoContaComoEmDia() {
        // ReplicationManager.getReplicationLag devolve 0 quando o HWM do líder ainda é desconhecido.
        CatalogReplicaStatus status = CatalogReplicaStatus.from(false, topicStatus(0L, 0L, false, false));

        assertFalse(status.lagKnown());
        assertFalse(status.caughtUp(1_000L));
    }

    @Test
    void caughtUpRespeitaOLimiteESincronizacao() {
        assertTrue(new CatalogReplicaStatus(false, 1_000L, 10L, 1L, false, false, true).caughtUp(1_000L));
        assertFalse(new CatalogReplicaStatus(false, 1_001L, 10L, 1L, false, false, true).caughtUp(1_000L));
        assertFalse(new CatalogReplicaStatus(false, 0L, 10L, 11L, true, false, false).caughtUp(1_000L));
        assertFalse(new CatalogReplicaStatus(false, 0L, 10L, 11L, false, true, false).caughtUp(1_000L));
        assertTrue(new CatalogReplicaStatus(false, 0L, 10L, 11L, false, false, true).caughtUp(0L));
    }

    private static ReplicationManager.TopicReplicationStatus topicStatus(long hwm, long lag, boolean syncing,
            boolean pendingBootstrap) {
        return new ReplicationManager.TopicReplicationStatus("map:ngrrd.catalog", hwm - lag + 1, 0L, 0L, syncing,
                pendingBootstrap, false, 0L, hwm, -1L, lag, 0L, true);
    }
}
