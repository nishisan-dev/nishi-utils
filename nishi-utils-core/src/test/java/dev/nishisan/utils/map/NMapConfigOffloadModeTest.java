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

package dev.nishisan.utils.map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Cobre a configuração declarativa de offloading via {@link OffloadMode} no
 * {@link NMapConfig}, a precedência do {@code offloadStrategyFactory} e as
 * validações dos novos knobs.
 */
class NMapConfigOffloadModeTest {

    @TempDir
    Path tempDir;

    @Test
    void inMemoryModeBuildsInMemoryStrategy() throws Exception {
        NMapConfig cfg = NMapConfig.builder().offloadMode(OffloadMode.IN_MEMORY).build();
        try (NMap<String, String> map = NMap.open(tempDir, "mode-mem", cfg)) {
            assertInstanceOf(InMemoryStrategy.class, map.strategy());
        }
    }

    @Test
    void diskModeBuildsDiskStrategy() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadMode(OffloadMode.DISK)
                .build();
        try (NMap<String, String> map = NMap.open(tempDir, "mode-disk", cfg)) {
            assertInstanceOf(DiskOffloadStrategy.class, map.strategy());
            map.put("k", "v");
            assertEquals(java.util.Optional.of("v"), map.get("k"));
        }
    }

    @Test
    void hybridModeBuildsHybridStrategy() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadMode(OffloadMode.HYBRID)
                .hotCacheMaxEntries(1_000)
                .evictionPolicy(EvictionPolicy.LRU)
                .build();
        try (NMap<String, String> map = NMap.open(tempDir, "mode-hybrid", cfg)) {
            assertInstanceOf(HybridOffloadStrategy.class, map.strategy());
        }
    }

    @Test
    void factoryTakesPrecedenceOverOffloadMode() throws Exception {
        // Mode says IN_MEMORY, but a custom factory must win.
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadMode(OffloadMode.IN_MEMORY)
                .offloadStrategyFactory(DiskOffloadStrategy::new)
                .build();
        try (NMap<String, String> map = NMap.open(tempDir, "precedence", cfg)) {
            assertInstanceOf(DiskOffloadStrategy.class, map.strategy());
        }
    }

    @Test
    void diskModeHonorsShardFanOut() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadMode(OffloadMode.DISK)
                .shardFanOut(1, 3)
                .build();
        String mapName = "mode-disk-fanout";
        try (NMap<String, String> map = NMap.open(tempDir, mapName, cfg)) {
            map.put("k", "v");
        }
        Path offloadDir = tempDir.resolve(mapName).resolve("offload");
        String keyHash = OffloadLayout.keyHash("k");
        Path expected = OffloadLayout.of(1, 3).shardedPath(offloadDir, keyHash, ".dat");
        org.junit.jupiter.api.Assertions.assertTrue(java.nio.file.Files.exists(expected));
    }

    @Test
    void validationRejectsBadKnobs() {
        assertThrows(IllegalArgumentException.class,
                () -> NMapConfig.builder().hotCacheMaxEntries(-1).build());
        assertThrows(IllegalArgumentException.class,
                () -> NMapConfig.builder().shardFanOut(1, 0).build());
        assertThrows(IllegalArgumentException.class,
                () -> NMapConfig.builder().shardFanOut(3, 20).build());
    }
}
