/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura@gmail.com>
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

package dev.nishisan.utils.queue;

/**
 * Represents a single record in the NQueue system, combining metadata and payload data.
 *
 * An NQueueRecord encapsulates the metadata (header information) and the payload that
 * constitutes the actual data content of the record. The metadata is managed using
 * the {@link NQueueRecordMetaData} class.
 */
public class NQueueRecord {
    private final NQueueRecordMetaData meta;
    private final byte[] payload;

    public NQueueRecord(NQueueRecordMetaData meta, byte[] payload) {
        this.meta = meta;
        this.payload = payload;
    }

    public NQueueRecordMetaData meta() { return meta; }
    public byte[] payload() { return payload; }

    /** Tamanho total em bytes deste registro (header completo + payload). */
    public int totalSize() {
        return meta.totalHeaderSize() + payload.length;
    }
}
