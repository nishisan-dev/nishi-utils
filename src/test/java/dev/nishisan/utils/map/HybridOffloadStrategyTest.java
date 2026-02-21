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

import dev.nishisan.utils.stats.StatsUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link HybridOffloadStrategy} covering eviction policies,
 * warm-up, persistence across restarts, and {@link StatsUtils} observability.
 */
class HybridOffloadStrategyTest {

    @TempDir
    Path tempDir;

    private HybridOffloadStrategy<String, String> strategy;
    private StatsUtils stats;

    @BeforeEach
    void setUp() {
        stats = new StatsUtils();
    }

    @AfterEach
    void tearDown() {
        if (strategy != null) {
            strategy.close();
        }
        stats.shutdown();
    }

    // ── Eviction behaviour ──────────────────────────────────────────────

    @Test
    void shouldEvictEldestWhenThresholdExceeded() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "eviction-test")
                .evictionPolicy(EvictionPolicy.SIZE_THRESHOLD)
                .maxInMemoryEntries(3)
                .build();

        strategy.put("a", "1");
        strategy.put("b", "2");
        strategy.put("c", "3");
        assertEquals(3, strategy.hotSize());
        assertEquals(0, strategy.coldSize());

        // Adding a 4th should evict the oldest (a)
        strategy.put("d", "4");
        assertEquals(3, strategy.hotSize());
        assertEquals(1, strategy.coldSize());

        // All values should still be readable (warm-up for cold entries)
        assertEquals("1", strategy.get("a"));
        assertEquals("4", strategy.get("d"));
        assertEquals(4, strategy.size());
    }

    @Test
    void lruShouldEvictLeastRecentlyUsed() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "lru-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(3)
                .build();

        strategy.put("a", "1");
        strategy.put("b", "2");
        strategy.put("c", "3");

        // Touch "a" so it becomes most-recently-used
        strategy.get("a");

        // Adding "d" should evict "b" (least recently used), not "a"
        strategy.put("d", "4");
        assertEquals(3, strategy.hotSize());
        assertEquals(1, strategy.coldSize());

        // "b" should be cold (but still readable via warm-up)
        assertEquals("2", strategy.get("b"));
        // "a" should still be hot (was touched)
        assertTrue(strategy.containsKey("a"));
    }

    // ── Warm-up ─────────────────────────────────────────────────────────

    @Test
    void getShouldPromoteColdEntryToHot() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "warmup-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(2)
                .build();

        strategy.put("a", "1");
        strategy.put("b", "2");
        strategy.put("c", "3"); // evicts "a" to cold

        assertEquals(2, strategy.hotSize());
        assertEquals(1, strategy.coldSize());

        // Get "a" → should warm-up (promote to hot), evicting someone else
        assertEquals("1", strategy.get("a"));
        assertEquals(2, strategy.hotSize());

        // Total should remain 3
        assertEquals(3, strategy.size());
    }

    // ── Persistence (close/reopen) ──────────────────────────────────────

    @Test
    void dataShouldSurviveCloseAndReopen() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "persist-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(2)
                .build();

        strategy.put("x", "10");
        strategy.put("y", "20");
        strategy.put("z", "30"); // "x" evicted to cold

        // Close flushes hot to cold
        strategy.close();

        // Reopen
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "persist-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(2)
                .build();

        assertEquals(3, strategy.size());
        assertEquals("10", strategy.get("x"));
        assertEquals("20", strategy.get("y"));
        assertEquals("30", strategy.get("z"));
    }

    // ── CRUD operations ─────────────────────────────────────────────────

    @Test
    void removeShouldWorkForHotAndColdEntries() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "remove-test")
                .evictionPolicy(EvictionPolicy.SIZE_THRESHOLD)
                .maxInMemoryEntries(2)
                .build();

        strategy.put("a", "1");
        strategy.put("b", "2");
        strategy.put("c", "3"); // "a" evicted to cold

        // Remove hot entry
        assertEquals("2", strategy.remove("b"));
        assertEquals(2, strategy.size());

        // Remove cold entry
        assertEquals("1", strategy.remove("a"));
        assertEquals(1, strategy.size());

        assertNull(strategy.get("a"));
        assertNull(strategy.get("b"));
    }

    @Test
    void clearShouldRemoveAllHotAndColdEntries() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "clear-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(2)
                .build();

        strategy.put("a", "1");
        strategy.put("b", "2");
        strategy.put("c", "3");

        strategy.clear();
        assertEquals(0, strategy.size());
        assertEquals(0, strategy.hotSize());
        assertEquals(0, strategy.coldSize());
        assertTrue(strategy.isEmpty());
    }

    @Test
    void updateExistingKeyShouldReplace() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "update-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(5)
                .build();

        strategy.put("key", "old");
        assertEquals("old", strategy.get("key"));

        strategy.put("key", "new");
        assertEquals("new", strategy.get("key"));
        assertEquals(1, strategy.size());
    }

    // ── Iteration ───────────────────────────────────────────────────────

    @Test
    void keySetShouldSpanHotAndCold() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "keyset-test")
                .evictionPolicy(EvictionPolicy.SIZE_THRESHOLD)
                .maxInMemoryEntries(2)
                .build();

        strategy.put("a", "1");
        strategy.put("b", "2");
        strategy.put("c", "3"); // "a" evicted

        Set<String> keys = strategy.keySet();
        assertEquals(Set.of("a", "b", "c"), keys);
    }

    @Test
    void forEachShouldVisitAllEntries() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "foreach-test")
                .evictionPolicy(EvictionPolicy.SIZE_THRESHOLD)
                .maxInMemoryEntries(2)
                .build();

        strategy.put("a", "1");
        strategy.put("b", "2");
        strategy.put("c", "3");

        ConcurrentHashMap<String, String> collected = new ConcurrentHashMap<>();
        strategy.forEach(collected::put);

        assertEquals(3, collected.size());
        assertEquals("1", collected.get("a"));
        assertEquals("2", collected.get("b"));
        assertEquals("3", collected.get("c"));
    }

    // ── StatsUtils observability ────────────────────────────────────────

    @Test
    void shouldEmitMetricsWhenStatsIsConfigured() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "stats-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(2)
                .stats(stats)
                .build();

        // Put 3 entries → 1 eviction
        strategy.put("a", "1");
        strategy.put("b", "2");
        strategy.put("c", "3");

        // Verify PUT counter
        Long putCount = stats.getCounterValueOrNull(NMapMetrics.PUT);
        assertNotNull(putCount, "PUT counter should be set");
        assertEquals(3L, putCount);

        // Verify EVICTION counter
        Long evictionCount = stats.getCounterValueOrNull(NMapMetrics.EVICTION);
        assertNotNull(evictionCount, "EVICTION counter should be set");
        assertEquals(1L, evictionCount);

        // Get hot entry → CACHE_HIT
        strategy.get("b");
        Long cacheHit = stats.getCounterValueOrNull(NMapMetrics.CACHE_HIT);
        assertNotNull(cacheHit, "CACHE_HIT counter should be set");
        assertTrue(cacheHit >= 1L);

        // Get cold entry → CACHE_MISS + WARM_UP
        strategy.get("a");
        Long warmUp = stats.getCounterValueOrNull(NMapMetrics.WARM_UP);
        assertNotNull(warmUp, "WARM_UP counter should be set");
        assertTrue(warmUp >= 1L);

        // Remove
        strategy.remove("c");
        Long removeCount = stats.getCounterValueOrNull(NMapMetrics.REMOVE);
        assertNotNull(removeCount, "REMOVE counter should be set");
        assertEquals(1L, removeCount);
    }

    @Test
    void shouldWorkWithoutStatsConfigured() {
        // No stats() call → should not throw
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "no-stats-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(2)
                .build();

        strategy.put("a", "1");
        strategy.put("b", "2");
        strategy.put("c", "3"); // eviction happens with no stats → no NPE
        assertEquals("1", strategy.get("a")); // warm-up with no stats → no NPE
        assertEquals(3, strategy.size());
    }

    // ── NMap integration ────────────────────────────────────────────────

    @Test
    void shouldWorkThroughNMapApi() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadStrategyFactory(new NMapOffloadStrategyFactory() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <K1 extends java.io.Serializable, V1 extends java.io.Serializable> NMapOffloadStrategy<K1, V1> create(
                            Path bd, String n) {
                        return (NMapOffloadStrategy<K1, V1>) HybridOffloadStrategy.<String, String>builder(bd, n)
                                .evictionPolicy(EvictionPolicy.LRU)
                                .maxInMemoryEntries(3)
                                .build();
                    }
                })
                .build();

        try (NMap<String, String> map = NMap.open(tempDir, "nmap-hybrid", cfg)) {
            for (int i = 0; i < 10; i++) {
                map.put("k" + i, "v" + i);
            }
            assertEquals(10, map.size());

            // All entries should be readable
            for (int i = 0; i < 10; i++) {
                assertEquals("v" + i, map.get("k" + i).orElse(null));
            }
        }
    }

    @Test
    void nMapWithHybridShouldSurviveRestart() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadStrategyFactory(new NMapOffloadStrategyFactory() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <K1 extends java.io.Serializable, V1 extends java.io.Serializable> NMapOffloadStrategy<K1, V1> create(
                            Path bd, String n) {
                        return (NMapOffloadStrategy<K1, V1>) HybridOffloadStrategy.<String, String>builder(bd, n)
                                .evictionPolicy(EvictionPolicy.LRU)
                                .maxInMemoryEntries(5)
                                .build();
                    }
                })
                .build();

        // Write data
        try (NMap<String, String> map = NMap.open(tempDir, "nmap-persist", cfg)) {
            map.put("session-1", "alice");
            map.put("session-2", "bob");
        }

        // Reopen and verify
        try (NMap<String, String> map = NMap.open(tempDir, "nmap-persist", cfg)) {
            assertEquals("alice", map.get("session-1").orElse(null));
            assertEquals("bob", map.get("session-2").orElse(null));
        }
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    @Test
    void shouldHandleSingleEntryThreshold() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "single-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(1)
                .build();

        strategy.put("a", "1");
        assertEquals(1, strategy.hotSize());

        strategy.put("b", "2");
        assertEquals(1, strategy.hotSize());
        assertEquals(1, strategy.coldSize());

        // Both should be readable
        assertEquals("1", strategy.get("a"));
        assertEquals("2", strategy.get("b"));
    }

    @Test
    void shouldRejectInvalidMaxInMemoryEntries() {
        assertThrows(IllegalArgumentException.class,
                () -> HybridOffloadStrategy.<String, String>builder(tempDir, "invalid")
                        .maxInMemoryEntries(0));

        assertThrows(IllegalArgumentException.class,
                () -> HybridOffloadStrategy.<String, String>builder(tempDir, "invalid")
                        .maxInMemoryEntries(-1));
    }
}
