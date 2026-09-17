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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link NGridNodeBuilder#affinityHandbackMode(boolean)} must pass the flag through to
 * {@link NGridConfig.Builder#affinityHandbackMode(boolean)}: a downstream module that wants the
 * lossless, snapshot-orchestrated leadership handover (issue tems#9, D11) for its storage nodes
 * can only reach it through the simplified builder.
 */
class NGridNodeBuilderAffinityHandbackModeTest {

    @Test
    void affinityHandbackModeDefaultsToFalse(@TempDir Path dataDir) throws Exception {
        NGridNode node = NGrid.node("127.0.0.1", 0)
                .dataDir(dataDir)
                .start();
        try {
            assertFalse(node.config().affinityHandbackMode());
        } finally {
            node.close();
        }
    }

    @Test
    void affinityHandbackModeEnabledCarriesThroughToConfig(@TempDir Path dataDir) throws Exception {
        NGridNode node = NGrid.node("127.0.0.1", 0)
                .dataDir(dataDir)
                .affinityHandbackMode(true)
                .start();
        try {
            assertTrue(node.config().affinityHandbackMode());
        } finally {
            node.close();
        }
    }
}
