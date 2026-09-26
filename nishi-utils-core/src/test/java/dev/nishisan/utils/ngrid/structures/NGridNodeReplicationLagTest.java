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

package dev.nishisan.utils.ngrid.structures;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Lag de replicação do {@link NGridNode#operationalSnapshot()}: o líder é a referência e nunca
 * atrasa, mesmo com o HWM rastreado obsoleto (ele não recebe o próprio heartbeat).
 */
class NGridNodeReplicationLagTest {

    @Test
    void leaderWithStaleTrackedWatermarkReportsZeroLag() {
        // Production (8.6.0): HIGH_REPLICATION_LAG CRITICAL 4761359 on the leader.
        assertEquals(0L, NGridNode.replicationLag(true, 4_761_359L, 0L));
        assertEquals(0L, NGridNode.replicationLag(true, 5_000_000L, 238_641L));
    }

    @Test
    void followerBehindTheLeaderReportsPositiveLag() {
        assertEquals(40L, NGridNode.replicationLag(false, 1_040L, 1_000L));
    }

    @Test
    void followerAheadOfATrackedWatermarkNeverReportsNegativeLag() {
        assertEquals(0L, NGridNode.replicationLag(false, -1L, 1_000L));
        assertEquals(0L, NGridNode.replicationLag(false, 1_000L, 1_000L));
    }
}
