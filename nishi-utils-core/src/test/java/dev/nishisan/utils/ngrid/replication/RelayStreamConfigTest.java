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

package dev.nishisan.utils.ngrid.replication;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Fase 2 — modo {@link FollowerIngestMode#RELAY_STREAM} e os knobs do stream em {@link ReplicationConfig}.
 */
class RelayStreamConfigTest {

    private static ReplicationConfig.Builder builder() {
        return ReplicationConfig.builder(1).dataDirectory(Path.of("target", "relay-stream-config-test"));
    }

    @Test
    void defaults() {
        ReplicationConfig cfg = builder().build();
        assertEquals(512, cfg.relayStreamFetchBatch());
        assertEquals(Duration.ofMillis(50), cfg.relayStreamPollInterval());
        assertEquals(Duration.ofSeconds(2), cfg.relayStreamFetchTimeout());
        assertEquals(200_000, cfg.relayMaxBacklog());
    }

    @Test
    void overrides() {
        ReplicationConfig cfg = builder()
                .followerIngestMode(FollowerIngestMode.RELAY_STREAM)
                .relayStreamFetchBatch(1000)
                .relayStreamPollInterval(Duration.ofMillis(20))
                .relayStreamFetchTimeout(Duration.ofSeconds(5))
                .relayMaxBacklog(50_000)
                .build();
        assertEquals(FollowerIngestMode.RELAY_STREAM, cfg.followerIngestMode());
        assertEquals(1000, cfg.relayStreamFetchBatch());
        assertEquals(Duration.ofMillis(20), cfg.relayStreamPollInterval());
        assertEquals(Duration.ofSeconds(5), cfg.relayStreamFetchTimeout());
        assertEquals(50_000, cfg.relayMaxBacklog());
    }

    @Test
    void validation() {
        assertThrows(IllegalArgumentException.class, () -> builder().relayStreamFetchBatch(0));
        assertThrows(IllegalArgumentException.class, () -> builder().relayMaxBacklog(0));
        assertThrows(NullPointerException.class, () -> builder().relayStreamPollInterval(null));
        assertThrows(NullPointerException.class, () -> builder().relayStreamFetchTimeout(null));
    }

    @Test
    void relayStreamModeSelectableByName() {
        // O loader YAML usa FollowerIngestMode.valueOf(...), então o nome deve resolver.
        assertEquals(FollowerIngestMode.RELAY_STREAM, FollowerIngestMode.valueOf("RELAY_STREAM"));
    }
}
