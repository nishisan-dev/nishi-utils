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

/**
 * Metric keys emitted by {@link NMap} offloading operations.
 * <p>
 * These keys are designed to be used with
 * {@link dev.nishisan.utils.stats.StatsUtils}:
 * <ul>
 * <li><b>Hit counters</b> ({@code StatsUtils.notifyHitCounter}): track
 * operation frequency and rate</li>
 * <li><b>Current values</b> ({@code StatsUtils.notifyCurrentValue}): track
 * gauge-style metrics</li>
 * <li><b>Average counters</b> ({@code StatsUtils.notifyAverageCounter}): track
 * latency distributions</li>
 * </ul>
 *
 * Metric names are prefixed with {@code NMAP.} and include the map name for
 * multi-map disambiguation.
 */
public final class NMapMetrics {

    private NMapMetrics() {
    }

    // ── Hit Counters (frequency/rate) ───────────────────────────────────

    /** Incremented on every {@code get()} call. */
    public static final String GET_HIT = "NMAP.GET_HIT";

    /** Incremented when a {@code get()} finds the entry in memory (hot cache). */
    public static final String CACHE_HIT = "NMAP.CACHE_HIT";

    /** Incremented when a {@code get()} has to read from disk (cold). */
    public static final String CACHE_MISS = "NMAP.CACHE_MISS";

    /** Incremented on every {@code put()} call. */
    public static final String PUT = "NMAP.PUT";

    /** Incremented on every {@code remove()} call. */
    public static final String REMOVE = "NMAP.REMOVE";

    /** Incremented each time an entry is evicted from memory to disk. */
    public static final String EVICTION = "NMAP.EVICTION";

    /** Incremented each time a cold entry is promoted back to memory (warm-up). */
    public static final String WARM_UP = "NMAP.WARM_UP";

    /** Incremented when a disk I/O operation fails. */
    public static final String DISK_IO_ERROR = "NMAP.DISK_IO_ERROR";

    // ── Current Values (gauges) ─────────────────────────────────────────

    /** Current number of entries in the hot (in-memory) cache. */
    public static final String HOT_SIZE = "NMAP.HOT_SIZE";

    /** Current number of entries offloaded to disk (cold). */
    public static final String COLD_SIZE = "NMAP.COLD_SIZE";

    /** Total number of entries (hot + cold). */
    public static final String TOTAL_SIZE = "NMAP.TOTAL_SIZE";

    // ── Average Counters (latency) ──────────────────────────────────────

    /** Average time (ms) for disk read operations (deserialization). */
    public static final String DISK_READ_LATENCY = "NMAP.DISK_READ_LATENCY";

    /** Average time (ms) for disk write operations (serialization). */
    public static final String DISK_WRITE_LATENCY = "NMAP.DISK_WRITE_LATENCY";
}
