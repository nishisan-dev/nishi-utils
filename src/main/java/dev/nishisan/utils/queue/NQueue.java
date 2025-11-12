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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class NQueue<T extends Serializable> implements Closeable {
    private final Path path;
    private final RandomAccessFile raf;
    private final FileChannel ch;
    private AtomicLong index = new AtomicLong(0L);

    public NQueue(Path path) throws IOException {
        this.path = path;
        Files.createDirectories(path.getParent());
        this.raf = new RandomAccessFile(path.toFile(), "rw");
        this.ch = raf.getChannel();
    }


    public synchronized long offer(T object) throws IOException {
        byte[] payload = this.toBytes(object);
        int payloadLen = payload.length;
        NQueueMetaData meta = new NQueueMetaData(index.getAndIncrement(), payloadLen, object.getClass().getCanonicalName());
        long writePos = ch.size();

        // Header
        ByteBuffer hb = meta.toByteBuffer();
        while (hb.hasRemaining()) {
            ch.write(hb, writePos);
        }
        writePos += meta.totalHeaderSize();

        // Payload
        ByteBuffer pb = ByteBuffer.wrap(payload);
        while (pb.hasRemaining()) {
            ch.write(pb, writePos);
        }

        ch.force(true); // durabilidade
        return (writePos - meta.totalHeaderSize()); // offset do início do registro
    }

    /**
     * Lê 1 registro a partir de 'offset'. Retorna Optional.empty() se não houver bytes suficientes.
     * Também retorna o próximo offset útil (fim deste registro), para iteração.
     */
    public synchronized Optional<NQueueReadResult> readAt(long offset) throws IOException {
        long size = ch.size();
        if (offset + NQueueMetaData.fixedPrefixSize() > size) {
            return Optional.empty();
        }
        // Lê prefixo e pega headerLen
        NQueueMetaData.HeaderPrefix pref = NQueueMetaData.readPrefix(ch, offset);

        long headerEnd = offset + NQueueMetaData.fixedPrefixSize() + pref.headerLen;
        if (headerEnd > size) return Optional.empty();

        // Lê header completo
        NQueueMetaData meta = NQueueMetaData.fromBuffer(ch, offset, pref.headerLen);

        long payloadStart = headerEnd;
        long payloadEnd = payloadStart + meta.getPayloadLen();
        if (payloadEnd > size) return Optional.empty();

        byte[] payload = new byte[meta.getPayloadLen()];
        ByteBuffer pb = ByteBuffer.wrap(payload);
        int r = ch.read(pb, payloadStart);
        if (r < payload.length) throw new EOFException("Payload incompleto.");

        long nextOffset = payloadEnd;
        return Optional.of(new NQueueReadResult(new NQueueRecord(meta, payload), nextOffset));
    }

    public synchronized long size() throws IOException {
        return ch.size();
    }

    @Override
    public synchronized void close() throws IOException {
        ch.close();
        raf.close();
    }




    private byte[] toBytes(T obj) throws IOException {
        if (obj == null) throw new IllegalArgumentException("obj não pode ser null");
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos))) {
            oos.writeObject(obj);
            oos.flush(); // garante que os bytes foram empurrados para o BOS
            return bos.toByteArray();
        }
    }

}
