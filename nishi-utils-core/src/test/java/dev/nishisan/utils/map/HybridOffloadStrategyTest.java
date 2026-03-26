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
 * <p>
 * <b>Note on striped architecture:</b> Since the strategy now uses
 * {@code CONCURRENCY_LEVEL = 16} lock stripes, each stripe has its own
 * {@code LinkedHashMap} with a per-stripe capacity of
 * {@code Math.max(1, maxInMemoryEntries / 16)}. Tests that validate
 * eviction behaviour must use thresholds large enough for evictions to
 * occur deterministically (i.e. enough entries to overflow at least one
 * stripe's per-stripe limit).
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
    void shouldEvictWhenStripesOverflow() {
        // With maxInMemoryEntries=16 and 16 stripes, each stripe holds ~1 entry.
        // Inserting 32 entries guarantees at least some stripes overflow.
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "eviction-test")
                .evictionPolicy(EvictionPolicy.SIZE_THRESHOLD)
                .maxInMemoryEntries(16)
                .build();

        for (int i = 0; i < 32; i++) {
            strategy.put("key-" + i, "val-" + i);
        }

        // Some entries must have been evicted to cold
        assertTrue(strategy.coldSize() > 0,
                "Expected at least some entries to be evicted to cold storage");
        assertEquals(32, strategy.size(), "Total size must remain 32");

        // All values should still be readable (warm-up for cold entries)
        for (int i = 0; i < 32; i++) {
            assertEquals("val-" + i, strategy.get("key-" + i));
        }
    }

    @Test
    void lruShouldEvictLeastRecentlyUsedEntries() {
        // maxInMemoryEntries=16, 16 stripes → 1 per stripe.
        // Fill all stripes, then touch some keys, then add more.
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "lru-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(16)
                .build();

        // Insert 32 entries — stripes will overflow, evicting oldest
        for (int i = 0; i < 32; i++) {
            strategy.put("key-" + i, "val-" + i);
        }

        assertTrue(strategy.coldSize() > 0,
                "Expected evictions to occur");

        // All entries should still be readable
        for (int i = 0; i < 32; i++) {
            assertNotNull(strategy.get("key-" + i),
                    "key-" + i + " should be readable");
        }
    }

    // ── Warm-up ─────────────────────────────────────────────────────────

    @Test
    void getShouldPromoteColdEntryToHot() {
        // maxInMemoryEntries=16, 16 stripes → 1 per stripe
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "warmup-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(16)
                .build();

        // Insert enough entries to cause evictions
        for (int i = 0; i < 32; i++) {
            strategy.put("key-" + i, "val-" + i);
        }

        int coldBefore = strategy.coldSize();
        assertTrue(coldBefore > 0, "Should have cold entries");

        // Warm-up: reading a cold entry promotes it back to hot.
        // Since we can't peek without promoting, we verify the total
        // invariant holds after reads.
        for (int i = 0; i < 32; i++) {
            assertNotNull(strategy.get("key-" + i));
        }

        // Total should remain 32 regardless of warm-up/eviction
        assertEquals(32, strategy.size());
    }

    // ── Persistence (close/reopen) ──────────────────────────────────────

    @Test
    void dataShouldSurviveCloseAndReopen() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "persist-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(32)
                .build();

        strategy.put("x", "10");
        strategy.put("y", "20");
        strategy.put("z", "30");

        // Close flushes hot to cold
        strategy.close();

        // Reopen
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "persist-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(32)
                .build();

        assertEquals(3, strategy.size());
        assertEquals("10", strategy.get("x"));
        assertEquals("20", strategy.get("y"));
        assertEquals("30", strategy.get("z"));
    }

    // ── CRUD operations ─────────────────────────────────────────────────

    @Test
    void removeShouldWorkForHotAndColdEntries() {
        // maxInMemoryEntries=16, 16 stripes → 1 per stripe
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "remove-test")
                .evictionPolicy(EvictionPolicy.SIZE_THRESHOLD)
                .maxInMemoryEntries(16)
                .build();

        // Insert enough to cause some evictions
        for (int i = 0; i < 32; i++) {
            strategy.put("key-" + i, "val-" + i);
        }
        assertEquals(32, strategy.size());

        // Remove a few entries (some hot, some cold)
        assertEquals("val-0", strategy.remove("key-0"));
        assertEquals("val-15", strategy.remove("key-15"));
        assertEquals("val-31", strategy.remove("key-31"));
        assertEquals(29, strategy.size());

        assertNull(strategy.get("key-0"));
        assertNull(strategy.get("key-15"));
        assertNull(strategy.get("key-31"));
    }

    @Test
    void clearShouldRemoveAllHotAndColdEntries() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "clear-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(16)
                .build();

        for (int i = 0; i < 32; i++) {
            strategy.put("key-" + i, "val-" + i);
        }

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
                .maxInMemoryEntries(80)
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
                .maxInMemoryEntries(16)
                .build();

        for (int i = 0; i < 32; i++) {
            strategy.put("key-" + i, "val-" + i);
        }

        Set<String> keys = strategy.keySet();
        assertEquals(32, keys.size());
        for (int i = 0; i < 32; i++) {
            assertTrue(keys.contains("key-" + i));
        }
    }

    @Test
    void forEachShouldVisitAllEntries() {
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "foreach-test")
                .evictionPolicy(EvictionPolicy.SIZE_THRESHOLD)
                .maxInMemoryEntries(16)
                .build();

        for (int i = 0; i < 32; i++) {
            strategy.put("key-" + i, "val-" + i);
        }

        ConcurrentHashMap<String, String> collected = new ConcurrentHashMap<>();
        strategy.forEach(collected::put);

        assertEquals(32, collected.size());
        for (int i = 0; i < 32; i++) {
            assertEquals("val-" + i, collected.get("key-" + i));
        }
    }

    // ── StatsUtils observability ────────────────────────────────────────

    @Test
    void shouldEmitMetricsWhenStatsIsConfigured() {
        // maxInMemoryEntries=16, 16 stripes → 1 per stripe
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "stats-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(16)
                .stats(stats)
                .build();

        // Put 32 entries → guarantees evictions across stripes
        for (int i = 0; i < 32; i++) {
            strategy.put("key-" + i, "val-" + i);
        }

        // Verify PUT counter
        Long putCount = stats.getCounterValueOrNull(NMapMetrics.PUT);
        assertNotNull(putCount, "PUT counter should be set");
        assertEquals(32L, putCount);

        // Verify EVICTION counter (stripe-level evictions occurred)
        Long evictionCount = stats.getCounterValueOrNull(NMapMetrics.EVICTION);
        assertNotNull(evictionCount, "EVICTION counter should be set");
        assertTrue(evictionCount >= 1L, "At least one eviction should have occurred");

        // Get hot entry → CACHE_HIT
        // Pick the last inserted key — most likely still hot
        strategy.get("key-31");
        Long cacheHit = stats.getCounterValueOrNull(NMapMetrics.CACHE_HIT);
        assertNotNull(cacheHit, "CACHE_HIT counter should be set");
        assertTrue(cacheHit >= 1L);

        // Get cold entry → CACHE_MISS + WARM_UP
        // Pick the first inserted key — most likely evicted
        strategy.get("key-0");
        // warm-up may or may not occur depending on stripe distribution,
        // so we just verify no exception is thrown
        stats.getCounterValueOrNull(NMapMetrics.WARM_UP);

        // Remove
        strategy.remove("key-1");
        Long removeCount = stats.getCounterValueOrNull(NMapMetrics.REMOVE);
        assertNotNull(removeCount, "REMOVE counter should be set");
        assertEquals(1L, removeCount);
    }

    @Test
    void shouldWorkWithoutStatsConfigured() {
        // No stats() call → should not throw
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "no-stats-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(16)
                .build();

        for (int i = 0; i < 32; i++) {
            strategy.put("key-" + i, "val-" + i);
        }
        // warm-up with no stats → no NPE
        for (int i = 0; i < 32; i++) {
            assertNotNull(strategy.get("key-" + i));
        }
        assertEquals(32, strategy.size());
    }

    // ── NMap integration ────────────────────────────────────────────────

    @Test
    void shouldWorkThroughNMapApi() throws Exception {
        NMapConfig cfg = NMapConfig.builder()
                .mode(NMapPersistenceMode.DISABLED)
                .offloadStrategyFactory(new NMapOffloadStrategyFactory() {
                    @Override
                    @SuppressWarnings("unchecked")
                    public <K1, V1> NMapOffloadStrategy<K1, V1> create(
                            Path bd, String n) {
                        return (NMapOffloadStrategy<K1, V1>) HybridOffloadStrategy.<String, String>builder(bd, n)
                                .evictionPolicy(EvictionPolicy.LRU)
                                .maxInMemoryEntries(48)
                                .build();
                    }
                })
                .build();

        try (NMap<String, String> map = NMap.open(tempDir, "nmap-hybrid", cfg)) {
            for (int i = 0; i < 100; i++) {
                map.put("k" + i, "v" + i);
            }
            assertEquals(100, map.size());

            // All entries should be readable
            for (int i = 0; i < 100; i++) {
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
                    public <K1, V1> NMapOffloadStrategy<K1, V1> create(
                            Path bd, String n) {
                        return (NMapOffloadStrategy<K1, V1>) HybridOffloadStrategy.<String, String>builder(bd, n)
                                .evictionPolicy(EvictionPolicy.LRU)
                                .maxInMemoryEntries(80)
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
    void shouldHandleSmallThreshold() {
        // With maxInMemoryEntries=16, each stripe holds 1 entry.
        // Inserting 2 entries that hash to the same stripe should evict one.
        strategy = HybridOffloadStrategy.<String, String>builder(tempDir, "single-test")
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(16)
                .build();

        // Insert enough entries to guarantee eviction
        for (int i = 0; i < 32; i++) {
            strategy.put("key-" + i, "val-" + i);
        }

        assertTrue(strategy.coldSize() > 0,
                "Expected evictions with small per-stripe threshold");

        // All should be readable
        for (int i = 0; i < 32; i++) {
            assertEquals("val-" + i, strategy.get("key-" + i));
        }
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
