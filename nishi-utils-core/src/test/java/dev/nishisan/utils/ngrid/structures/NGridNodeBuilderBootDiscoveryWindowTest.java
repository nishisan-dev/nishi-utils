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
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * {@link NGridNodeBuilder#bootDiscoveryWindow(Duration)} must pass the deferral window through to
 * {@link NGridConfig.Builder#bootDiscoveryWindow(Duration)} — before this test, the builder had no
 * such setter, so callers (e.g. a downstream module tuning boot-discovery for its own storage nodes)
 * could only reach the window via {@link NGridConfig.Builder} directly, bypassing the simplified
 * {@link NGridNodeBuilder} API entirely.
 */
class NGridNodeBuilderBootDiscoveryWindowTest {

    @Test
    void bootDiscoveryWindowUnsetLeavesItNullOnConfig(@TempDir Path dataDir) throws Exception {
        NGridNode node = NGrid.node("127.0.0.1", 0)
                .dataDir(dataDir)
                .start();
        try {
            assertNull(node.config().bootDiscoveryWindow());
        } finally {
            node.close();
        }
    }

    @Test
    void bootDiscoveryWindowExplicitValueCarriesThroughToConfig(@TempDir Path dataDir) throws Exception {
        NGridNode node = NGrid.node("127.0.0.1", 0)
                .dataDir(dataDir)
                .bootDiscoveryWindow(Duration.ofSeconds(3))
                .start();
        try {
            assertEquals(Duration.ofSeconds(3), node.config().bootDiscoveryWindow());
        } finally {
            node.close();
        }
    }
}
