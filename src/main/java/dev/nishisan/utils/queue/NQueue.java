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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * NQueue is a persistent, thread-safe queue implementation that stores
 * objects in a file-backed structure. It is capable of handling serialized objects
 * and provides methods for offering, peeking, polling, and reading records based on offsets.
 * It also supports querying the current size and other metadata of the queue.
 *
 * @param <T> the type of elements in the queue, which must be serializable
 */
public class NQueue<T extends Serializable> implements Closeable {
    private static final String DATA_FILE = "data.log";
    private static final String META_FILE = "queue.meta";

    private final Path metaPath;
    private final RandomAccessFile raf;
    private final FileChannel dataChannel;
    private final ReentrantLock lock;
    private final Condition notEmpty;

    private long consumerOffset;
    private long producerOffset;
    private long recordCount;
    private long lastIndex;

    private NQueue(Path queueDir, RandomAccessFile raf, FileChannel dataChannel, QueueState state) {
        this.metaPath = queueDir.resolve(META_FILE);
        this.raf = raf;
        this.dataChannel = dataChannel;
        this.lock = new ReentrantLock();
        this.notEmpty = this.lock.newCondition();
        this.consumerOffset = state.consumerOffset;
        this.producerOffset = state.producerOffset;
        this.recordCount = state.recordCount;
        this.lastIndex = state.lastIndex;
    }

    public static <T extends Serializable> NQueue<T> open(Path baseDir, String queueName) throws IOException {
        Objects.requireNonNull(baseDir, "baseDir");
        Objects.requireNonNull(queueName, "queueName");

        Path queueDir = baseDir.resolve(queueName);
        Files.createDirectories(queueDir);

        Path dataPath = queueDir.resolve(DATA_FILE);
        Path metaPath = queueDir.resolve(META_FILE);

        RandomAccessFile raf = new RandomAccessFile(dataPath.toFile(), "rw");
        FileChannel ch = raf.getChannel();

        QueueState state;
        if (Files.exists(metaPath)) {
            NQueueQueueMeta meta = NQueueQueueMeta.read(metaPath);
            state = new QueueState(meta.getConsumerOffset(), meta.getProducerOffset(), meta.getRecordCount(), meta.getLastIndex());

            long fileSize = ch.size();
            boolean inconsistent = state.consumerOffset < 0
                    || state.producerOffset < 0
                    || state.consumerOffset > state.producerOffset
                    || fileSize < state.producerOffset;

            if (inconsistent) {
                state = rebuildState(ch);
                persistMeta(metaPath, state);
            } else if (fileSize > state.producerOffset) {
                ch.truncate(state.producerOffset);
                ch.force(true);
            }
        } else {
            state = rebuildState(ch);
            persistMeta(metaPath, state);
        }

        return new NQueue<>(queueDir, raf, ch, state);
    }

    public long offer(T object) throws IOException {
        Objects.requireNonNull(object, "object");
        byte[] payload = toBytes(object);
        int payloadLen = payload.length;

        lock.lock();
        try {
            long nextIndex = lastIndex + 1;
            if (nextIndex < 0) {
                nextIndex = 0;
            }
            NQueueRecordMetaData meta = new NQueueRecordMetaData(nextIndex, payloadLen, object.getClass().getCanonicalName());
            long writePos = producerOffset;
            long recordStart = writePos;

            ByteBuffer hb = meta.toByteBuffer();
            while (hb.hasRemaining()) {
                int written = dataChannel.write(hb, writePos);
                writePos += written;
            }

            ByteBuffer pb = ByteBuffer.wrap(payload);
            while (pb.hasRemaining()) {
                int written = dataChannel.write(pb, writePos);
                writePos += written;
            }

            producerOffset = writePos;
            recordCount++;
            lastIndex = nextIndex;
            if (recordCount == 1) {
                consumerOffset = recordStart;
            }

            dataChannel.force(true);
            persistCurrentStateLocked();
            notEmpty.signalAll();
            return recordStart;
        } finally {
            lock.unlock();
        }
    }

    public Optional<T> readAt(long offset) throws IOException {
        lock.lock();
        try {
            Optional<NQueueReadResult> result = readAtInternal(offset);
            if (result.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(deserializeRecord(result.get().getRecord()));
        } finally {
            lock.unlock();
        }
    }

    public Optional<NQueueReadResult> readRecordAt(long offset) throws IOException {
        lock.lock();
        try {
            return readAtInternal(offset);
        } finally {
            lock.unlock();
        }
    }

    public Optional<NQueueRecord> peekRecord() throws IOException {
        lock.lock();
        try {
            awaitRecords();
            return readAtInternal(consumerOffset).map(NQueueReadResult::getRecord);
        } finally {
            lock.unlock();
        }
    }

    public Optional<T> decodeRecord(NQueueRecord record) throws IOException {
        lock.lock();
        try {
            return Optional.of(deserializeRecord(record));
        } finally {
            lock.unlock();
        }
    }

    public Optional<T> peek() throws IOException {
        return this.decodeRecord(this.peekRecord().orElse(null));
    }

    public Optional<T> poll() throws IOException {
        lock.lock();
        try {
            awaitRecords();
            Optional<NQueueRecord> record = consumeNextRecordLocked();
            if (record.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(deserializeRecord(record.get()));
        } finally {
            lock.unlock();
        }
    }

    public Optional<T> poll(long timeout, TimeUnit unit) throws IOException {
        Objects.requireNonNull(unit, "unit");
        long nanos = unit.toNanos(timeout);

        lock.lock();
        try {
            while (recordCount == 0) {
                if (nanos <= 0L) {
                    return Optional.empty();
                }
                try {
                    nanos = notEmpty.awaitNanos(nanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                }
            }
            Optional<NQueueRecord> record = consumeNextRecordLocked();
            if (record.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(deserializeRecord(record.get()));
        } finally {
            lock.unlock();
        }
    }

    public long size() throws IOException {
        lock.lock();
        try {
            return recordCount;
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        lock.lock();
        try {
            return recordCount == 0;
        } finally {
            lock.unlock();
        }
    }

    public long getRecordCount() {
        lock.lock();
        try {
            return recordCount;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            dataChannel.close();
            raf.close();
        } finally {
            lock.unlock();
        }
    }

    private Optional<NQueueReadResult> readAtInternal(long offset) throws IOException {
        long size = dataChannel.size();
        if (offset + NQueueRecordMetaData.fixedPrefixSize() > size) {
            return Optional.empty();
        }

        NQueueRecordMetaData.HeaderPrefix pref = NQueueRecordMetaData.readPrefix(dataChannel, offset);
        long headerEnd = offset + NQueueRecordMetaData.fixedPrefixSize() + pref.headerLen;
        if (headerEnd > size) {
            return Optional.empty();
        }

        NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(dataChannel, offset, pref.headerLen);

        long payloadStart = headerEnd;
        long payloadEnd = payloadStart + meta.getPayloadLen();
        if (payloadEnd > size) {
            return Optional.empty();
        }

        byte[] payload = new byte[meta.getPayloadLen()];
        ByteBuffer pb = ByteBuffer.wrap(payload);
        int r = dataChannel.read(pb, payloadStart);
        if (r < payload.length) {
            throw new EOFException("Payload incompleto.");
        }

        long nextOffset = payloadEnd;
        return Optional.of(new NQueueReadResult(new NQueueRecord(meta, payload), nextOffset));
    }

    private static QueueState rebuildState(FileChannel ch) throws IOException {
        long size = ch.size();
        long offset = 0L;
        long count = 0L;
        long lastIndex = -1L;

        while (offset < size) {
            if (offset + NQueueRecordMetaData.fixedPrefixSize() > size) {
                ch.truncate(offset);
                size = offset;
                break;
            }

            NQueueRecordMetaData.HeaderPrefix prefix;
            try {
                prefix = NQueueRecordMetaData.readPrefix(ch, offset);
            } catch (EOFException e) {
                ch.truncate(offset);
                size = offset;
                break;
            }

            long headerEnd = offset + NQueueRecordMetaData.fixedPrefixSize() + prefix.headerLen;
            if (headerEnd > size) {
                ch.truncate(offset);
                size = offset;
                break;
            }

            NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(ch, offset, prefix.headerLen);
            long payloadEnd = headerEnd + meta.getPayloadLen();
            if (payloadEnd > size) {
                ch.truncate(offset);
                size = offset;
                break;
            }

            offset = payloadEnd;
            count++;
            lastIndex = meta.getIndex();
        }

        if (offset != size) {
            size = offset;
        }
        ch.force(true);

        long consumerOffset = count > 0 ? 0L : size;
        long producerOffset = size;
        if (lastIndex < 0 && count > 0) {
            lastIndex = count - 1;
        }
        if (count == 0) {
            lastIndex = -1L;
        }

        return new QueueState(consumerOffset, producerOffset, count, lastIndex);
    }

    private static void persistMeta(Path metaPath, QueueState state) throws IOException {
        NQueueQueueMeta.write(metaPath, state.consumerOffset, state.producerOffset, state.recordCount, state.lastIndex);
    }

    private QueueState currentState() {
        return new QueueState(consumerOffset, producerOffset, recordCount, lastIndex);
    }

    private void persistCurrentStateLocked() throws IOException {
        persistMeta(metaPath, currentState());
    }

    private void awaitRecords() {
        while (recordCount == 0) {
            notEmpty.awaitUninterruptibly();
        }
    }

    private Optional<NQueueRecord> consumeNextRecordLocked() throws IOException {
        Optional<NQueueReadResult> result = readAtInternal(consumerOffset);
        if (result.isEmpty()) {
            return Optional.empty();
        }

        NQueueReadResult rr = result.get();
        consumerOffset = rr.getNextOffset();
        recordCount--;
        if (recordCount == 0) {
            consumerOffset = producerOffset;
        }
        persistCurrentStateLocked();
        return Optional.of(rr.getRecord());
    }

    private byte[] toBytes(T obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos))) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        }
    }

    private T deserializeRecord(NQueueRecord record) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(record.payload());
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            @SuppressWarnings("unchecked")
            T obj = (T) ois.readObject();
            return obj;
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize record payload", e);
        }
    }

    private static final class QueueState {
        final long consumerOffset;
        final long producerOffset;
        final long recordCount;
        final long lastIndex;

        QueueState(long consumerOffset, long producerOffset, long recordCount, long lastIndex) {
            this.consumerOffset = consumerOffset;
            this.producerOffset = producerOffset;
            this.recordCount = recordCount;
            this.lastIndex = lastIndex;
        }
    }
}
