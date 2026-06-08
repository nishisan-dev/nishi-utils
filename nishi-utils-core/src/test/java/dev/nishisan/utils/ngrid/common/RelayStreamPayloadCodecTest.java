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

package dev.nishisan.utils.ngrid.common;

import dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fase 1 — os payloads do RELAY_STREAM trafegam pelo {@link ClusterMessage} via o type-info por FQCN
 * ({@code @JsonTypeInfo Id.CLASS}), sem registro no codec. Garante round-trip, inclusive do
 * {@code List<byte[]>} de frames.
 */
class RelayStreamPayloadCodecTest {

    private final JacksonMessageCodec codec = new JacksonMessageCodec();

    @Test
    void fetchPayloadRoundTrips() throws Exception {
        RelayStreamFetchPayload fetch = new RelayStreamFetchPayload("cardinal-state", 4242L, 256);
        ClusterMessage msg = ClusterMessage.request(MessageType.RELAY_STREAM_FETCH, "stream",
                NodeId.of("follower"), NodeId.of("leader"), fetch);

        ClusterMessage decoded = codec.decode(codec.encode(msg));

        assertEquals(MessageType.RELAY_STREAM_FETCH, decoded.type());
        RelayStreamFetchPayload out = decoded.payload(RelayStreamFetchPayload.class);
        assertEquals(fetch, out);
    }

    @Test
    void batchPayloadRoundTripsIncludingFrames() throws Exception {
        List<byte[]> frames = List.of(
                new byte[] {1, 2, 3, 4},
                new byte[] {(byte) 0xFF, 0, (byte) 0x80, 7, 7});
        RelayStreamBatchPayload batch = new RelayStreamBatchPayload("cardinal-state", 100L, frames,
                12345L, 50L, false);
        ClusterMessage msg = ClusterMessage.request(MessageType.RELAY_STREAM_BATCH, "stream",
                NodeId.of("leader"), NodeId.of("follower"), batch);

        RelayStreamBatchPayload out = codec.decode(codec.encode(msg)).payload(RelayStreamBatchPayload.class);

        assertEquals("cardinal-state", out.topic());
        assertEquals(100L, out.fromSequence());
        assertEquals(12345L, out.leaderHighWatermark());
        assertEquals(50L, out.oldestSequence());
        assertFalse(out.needSnapshot());
        assertEquals(2, out.frames().size());
        assertArrayEquals(frames.get(0), out.frames().get(0));
        assertArrayEquals(frames.get(1), out.frames().get(1));
    }

    @Test
    void needSnapshotBatchRoundTripsWithEmptyFrames() throws Exception {
        RelayStreamBatchPayload batch = new RelayStreamBatchPayload("t", 5L, List.of(), 999L, 800L, true);
        RelayStreamBatchPayload out = codec.decode(codec.encode(
                ClusterMessage.request(MessageType.RELAY_STREAM_BATCH, "stream",
                        NodeId.of("leader"), NodeId.of("follower"), batch)))
                .payload(RelayStreamBatchPayload.class);

        assertTrue(out.needSnapshot());
        assertTrue(out.frames().isEmpty());
        assertEquals(800L, out.oldestSequence());
    }
}
