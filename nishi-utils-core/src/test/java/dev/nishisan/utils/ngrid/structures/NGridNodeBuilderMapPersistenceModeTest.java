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

import dev.nishisan.utils.map.NMapPersistenceMode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * M1c — {@link NGridNodeBuilder#map(String, NMapPersistenceMode)} must let a caller opt a specific
 * map into disk persistence, independently of {@link NGridNodeBuilder#map(String)} (which always
 * defaults to {@link NMapPersistenceMode#DISABLED}).
 */
class NGridNodeBuilderMapPersistenceModeTest {

    @Test
    void mapWithoutExplicitModeDefaultsToDisabled(@TempDir Path dataDir) throws Exception {
        NGridNode node = NGrid.node("127.0.0.1", 0)
                .dataDir(dataDir)
                .map("plain-map")
                .start();
        try {
            MapConfig configured = onlyMap(node);
            assertEquals("plain-map", configured.name());
            assertEquals(NMapPersistenceMode.DISABLED, configured.persistenceMode());
        } finally {
            node.close();
        }
    }

    @Test
    void mapWithExplicitPersistenceModeCarriesItThrough(@TempDir Path dataDir) throws Exception {
        NGridNode node = NGrid.node("127.0.0.1", 0)
                .dataDir(dataDir)
                .map("catalog-map", NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .start();
        try {
            MapConfig configured = onlyMap(node);
            assertEquals("catalog-map", configured.name());
            assertEquals(NMapPersistenceMode.ASYNC_WITH_FSYNC, configured.persistenceMode());
        } finally {
            node.close();
        }
    }

    /**
     * B1 (M1c Debugger finding, root cause of the ngrrd-cluster catalog never persisting): a map
     * configured with a non-DISABLED persistence mode must actually reload its data from disk when a
     * new node starts against the SAME dataDir — not just carry the configuration through.
     */
    @Test
    void mapWithPersistenceModeSurvivesRestartWithTheSameDataDir(@TempDir Path dataDir) throws Exception {
        NGridNode first = NGrid.node("127.0.0.1", 0)
                .dataDir(dataDir)
                .map("m", NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .start();
        try {
            first.getMap("m", String.class, String.class).put("key", "value");
        } finally {
            first.close();
        }

        NGridNode second = NGrid.node("127.0.0.1", 0)
                .dataDir(dataDir)
                .map("m", NMapPersistenceMode.ASYNC_WITH_FSYNC)
                .start();
        try {
            Optional<String> recovered = second.getMap("m", String.class, String.class)
                    .getOptional("key", Consistency.EVENTUAL);
            assertEquals(Optional.of("value"), recovered);
        } finally {
            second.close();
        }
    }

    private static MapConfig onlyMap(NGridNode node) {
        assertEquals(1, node.config().configuredMaps().size());
        return node.config().configuredMaps().get(0);
    }
}
