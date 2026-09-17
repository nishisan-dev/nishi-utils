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

import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M0 — {@link NGridNodeBuilder#roles(String...)} must propagate the configured roles to the node's
 * local {@link NodeInfo} (and, consequently, to gossip/handshake).
 */
class NGridNodeBuilderRolesTest {

    @Test
    void withoutCallingRolesTheNodeCarriesNoRole(@TempDir Path dataDir) throws Exception {
        NGridNode node = NGrid.node("127.0.0.1", 0)
                .dataDir(dataDir)
                .start();
        try {
            NodeInfo local = node.transport().local();
            assertTrue(local.roles().isEmpty(), "without roles() on the builder, the node should carry no roles");
            assertTrue(local.isLeaderEligible(), "without roles, the node should be eligible for leadership");
        } finally {
            node.close();
        }
    }

    @Test
    void configuredRolesPropagateToTheLocalNodeInfo(@TempDir Path dataDir) throws Exception {
        NGridNode node = NGrid.node("127.0.0.1", 0)
                .dataDir(dataDir)
                .roles(NodeInfo.ROLE_LEADER_INELIGIBLE, "client")
                .start();
        try {
            NodeInfo local = node.transport().local();
            assertEquals(Set.of(NodeInfo.ROLE_LEADER_INELIGIBLE, "client"), local.roles());
            assertFalse(local.isLeaderEligible(),
                    "the leader-ineligible role configured via the builder should make the node ineligible");
        } finally {
            node.close();
        }
    }
}
