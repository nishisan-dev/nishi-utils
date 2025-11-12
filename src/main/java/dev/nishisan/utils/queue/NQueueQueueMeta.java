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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

final class NQueueQueueMeta {
    private static final int MAGIC = 0x4E_51_4D_54; // 'NQMT'
    private static final short VERSION = 1;

    private final long consumerOffset;
    private final long producerOffset;
    private final long recordCount;
    private final long lastIndex;

    NQueueQueueMeta(long consumerOffset, long producerOffset, long recordCount, long lastIndex) {
        this.consumerOffset = consumerOffset;
        this.producerOffset = producerOffset;
        this.recordCount = recordCount;
        this.lastIndex = lastIndex;
    }

    long getConsumerOffset() {
        return consumerOffset;
    }

    long getProducerOffset() {
        return producerOffset;
    }

    long getRecordCount() {
        return recordCount;
    }

    long getLastIndex() {
        return lastIndex;
    }

    static NQueueQueueMeta read(Path path) throws IOException {
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + Short.BYTES + Long.BYTES * 4);
            int read = ch.read(buf);
            if (read < buf.capacity()) {
                throw new EOFException("queue.meta incompleto");
            }
            buf.flip();
            int magic = buf.getInt();
            if (magic != MAGIC) {
                throw new IOException("MAGIC inválido em queue.meta: " + Integer.toHexString(magic));
            }
            short version = buf.getShort();
            if (version != VERSION) {
                throw new IOException("Versão de queue.meta não suportada: " + version);
            }
            long consumerOffset = buf.getLong();
            long producerOffset = buf.getLong();
            long recordCount = buf.getLong();
            long lastIndex = buf.getLong();
            return new NQueueQueueMeta(consumerOffset, producerOffset, recordCount, lastIndex);
        }
    }

    static void write(Path path, long consumerOffset, long producerOffset, long recordCount, long lastIndex) throws IOException {
        Path tmp = path.resolveSibling(path.getFileName().toString() + ".tmp");
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES + Short.BYTES + Long.BYTES * 4);
        buf.putInt(MAGIC);
        buf.putShort(VERSION);
        buf.putLong(consumerOffset);
        buf.putLong(producerOffset);
        buf.putLong(recordCount);
        buf.putLong(lastIndex);
        buf.flip();

        Files.createDirectories(path.getParent());
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true);
        }
        Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }
}
