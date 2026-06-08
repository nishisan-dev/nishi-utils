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

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The leader op-log (binlog) count cap is exposed on the config builder so it can be tuned; default
 * stays unset (the replication layer applies 10,000,000).
 */
class NGridConfigOpLogKnobTest {

    private static NGridConfig.Builder builder() {
        return NGridConfig.builder(new NodeInfo(NodeId.of("n1"), "127.0.0.1", 0))
                .dataDirectory(java.nio.file.Path.of("target", "oplog-knob-test"));
    }

    @Test
    void unsetByDefault() {
        assertNull(builder().build().resendLogMaxEntries(),
                "unset must stay null so the replication-layer default (10M) applies");
    }

    @Test
    void tunable() {
        assertEquals(5_000_000L, builder().resendLogMaxEntries(5_000_000L).build().resendLogMaxEntries());
        assertEquals(50_000_000L, builder().resendLogMaxEntries(50_000_000L).build().resendLogMaxEntries());
    }

    @Test
    void rejectsNonPositive() {
        assertThrows(IllegalArgumentException.class, () -> builder().resendLogMaxEntries(0));
        assertThrows(IllegalArgumentException.class, () -> builder().resendLogMaxEntries(-1));
    }

    @Test
    void segmentRetentionKnobsUnsetByDefault() {
        NGridConfig config = builder().build();
        assertNull(config.resendLogSegmentMaxBytes(),
                "byte-based rolling stays unset so the replication-layer default (disabled) applies");
        assertNull(config.resendLogMaxSegments(),
                "segment-count retention stays unset so the replication-layer default (disabled) applies");
    }

    @Test
    void segmentRetentionKnobsTunable() {
        NGridConfig config = builder()
                .resendLogSegmentMaxBytes(10L * 1024 * 1024 * 1024) // 10GB per file
                .resendLogMaxSegments(10)                            // keep 10 files
                .build();
        assertEquals(10L * 1024 * 1024 * 1024, config.resendLogSegmentMaxBytes());
        assertEquals(10, config.resendLogMaxSegments());
    }

    @Test
    void segmentRetentionKnobsRejectNegative() {
        assertThrows(IllegalArgumentException.class, () -> builder().resendLogSegmentMaxBytes(-1));
        assertThrows(IllegalArgumentException.class, () -> builder().resendLogMaxSegments(-1));
    }
}
