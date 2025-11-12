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
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectOutputStream;
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
 * NQueue is a persistent queue implementation designed to handle
 * Serializable objects and supports concurrent access with thread safety.
 * The queue operates by utilizing a RandomAccessFile and FileChannel
 * to store metadata and serialized objects on disk, ensuring data integrity
 * even after application restarts or crashes.
 *
 * <ul>
 * Key functionalities:
 * 1. Persistent storage: Objects are written to a file with metadata for later retrieval.
 * 2. Thread safety: A ReentrantLock and Condition are used to handle concurrent access.
 * 3. Serialization support: Objects stored in the queue must implement the Serializable interface.
 * 4. Durable queue state: Ensures consistency by rebuilding or truncating files in case of corruption or inconsistencies.
 * 5. Blocking operations: Blocking methods such as poll with timeout and wait for elements if the queue is empty.
 * </ul>
 *
 * Limitations:
 * 1. Objects to be stored must implement Serializable.
 * 2. Requires sufficient disk space for storing objects and metadata.
 * 3. Disk I/O operations could introduce latency compared to in-memory queues.
 *
 * @param <T> The type of objects this queue can hold. Must extend Serializable.
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

    /**
     * Constructs a private instance of the NQueue class.
     * This constructor initializes the metadata, file channels, locks, and queue state.
     * It is meant to be used internally by the NQueue implementation.
     *
     * @param queueDir    the directory where the queue files are stored
     * @param raf         the RandomAccessFile instance for accessing persistent queue data
     * @param dataChannel the FileChannel instance used for reading and writing queue data
     * @param state       the current state of the queue including offsets and record counts
     */
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

    /**
     * Opens an existing {@code NQueue} or initializes a new one if it does not exist.
     * The queue is backed by files in the specified base directory.
     *
     * @param baseDir   the path to the base directory where the queue files are stored; must not be null
     * @param queueName the name of the queue directory within the base directory; must not be null
     * @return a new instance of {@code NQueue} of type {@code T} for managing the queue
     * @throws IOException if there is an error reading from or writing to the file system
     * @param <T> the type of elements stored in the queue, which must implement {@code Serializable}
     */
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

    /**
     * Adds the given object to the queue as a new record. This method serializes
     * the object, writes its metadata and payload to the underlying queue storage,
     * updates queue state, and notifies any waiting consumers that a new record is
     * available.
     *
     * @param object the object to be added to the queue; must not be null
     * @return the offset (position) in the queue where the record was written
     * @throws IOException if an I/O error occurs during the serialization or
     *                     writing process
     */
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

    /**
     * Reads a record from the queue at the specified offset.
     * This method locks the queue, delegates the reading operation to an internal method,
     * and ensures that the lock is released after the operation completes.
     *
     * @param offset the position in the queue from which to read the record
     * @return an {@link Optional} containing an {@link NQueueReadResult} object
     *         if a record is found at the specified offset, or an empty {@link Optional}
     *         if no record exists at that position
     * @throws IOException if an I/O error occurs while reading the queue
     */
    public Optional<NQueueReadResult> readAt(long offset) throws IOException {
        lock.lock();
        try {
            return readAtInternal(offset);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves, but does not remove, the next record in the queue.
     * This method blocks until a record becomes available in the queue.
     *
     * @return an {@code Optional} containing the next {@code NQueueRecord} if available,
     *         or an empty {@code Optional} if the queue contains no records.
     * @throws IOException if an I/O error occurs while reading the queue.
     */
    public Optional<NQueueRecord> peek() throws IOException {
        lock.lock();
        try {
            awaitRecords();
            return readAtInternal(consumerOffset).map(NQueueReadResult::getRecord);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves and removes the next record from the queue, blocking until one is available.
     * This method locks the queue, waits for records to become available if the queue is empty,
     * consumes the next record in a thread-safe manner, and updates the queue state.
     *
     * @return an {@code Optional} containing the next {@code NQueueRecord} if available,
     *         or an empty {@code Optional} if no records exist in the queue.
     * @throws IOException if an I/O error occurs while consuming the record.
     */
    public Optional<NQueueRecord> poll() throws IOException {
        lock.lock();
        try {
            awaitRecords();
            return consumeNextRecordLocked();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves and removes the next record from the queue, blocking for up to the
     * specified timeout if the queue is empty. If a record is available, it is returned
     * as an {@link Optional}. If the timeout elapses before a record becomes available,
     * an empty {@link Optional} is returned.
     *
     * @param timeout the maximum time to wait for a record to become available, in the
     *                specified time unit
     * @param unit the {@link TimeUnit} of the timeout parameter; must not be null
     * @return an {@link Optional} containing the next {@link NQueueRecord} if available,
     *         or an empty {@link Optional} if no record is available within the timeout
     * @throws IOException if an I/O error occurs while consuming the next record
     */
    public Optional<NQueueRecord> poll(long timeout, TimeUnit unit) throws IOException {
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
            return consumeNextRecordLocked();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the current number of records stored in the queue.
     * This method provides a thread-safe way to retrieve the record count
     * by using a lock to ensure consistency.
     *
     * @return the number of records currently in the queue
     * @throws IOException if an I/O error occurs while accessing the queue
     */
    public long size() throws IOException {
        lock.lock();
        try {
            return recordCount;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Checks if the queue is empty.
     * This method acquires a lock to ensure a thread-safe check
     * of the current record count in the queue.
     *
     * @return {@code true} if the queue contains no records, {@code false} otherwise
     */
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

    /**
     * Closes the {@code NQueue} instance and releases any system resources associated with it.
     * This method ensures thread safety by acquiring a lock before closing the associated resources.
     * Specifically, it closes the {@code dataChannel} and {@code RandomAccessFile} instances
     * used for managing the persistent state of the queue. After the operation, the lock is
     * released to allow other threads to proceed.
     *
     * @throws IOException if an I/O error occurs while closing the queue resources
     */
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

    /**
     * Reads a record at the specified offset within the queue's data channel.
     * This method validates the offset, header, and payload boundaries to ensure
     * the integrity of the record being read. If the offset or any part of the record
     * exceeds the available size of the data channel or cannot be fully read, an
     * empty {@code Optional} is returned. Otherwise, the method returns the
     * record metadata, its payload, and the next offset.
     *
     * @param offset the position within the data channel to start reading the record; must be non-negative
     * @return an {@code Optional} containing an {@link NQueueReadResult} with the record's metadata, payload,
     *         and the next offset, or an empty {@code Optional} if no valid record exists at the specified offset
     * @throws IOException if an I/O error occurs while accessing the data channel
     */
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

    /**
     * Rebuilds the state of a queue by analyzing the content of a given file channel.
     * The method processes the file channel to determine valid records, truncates any
     * corrupt or partial data, and computes the key offsets and state of the queue.
     *
     * @param ch the file channel that contains the data to analyze and rebuild. This
     *           should represent the storage for the queue.
     * @return a {@code QueueState} object that reflects the current state of the queue
     *         including consumer offset, producer offset, record count, and last record index.
     * @throws IOException if an I/O error occurs while reading or truncating the file channel.
     */
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

    /**
     * Persists the metadata to the specified file path.
     *
     * @param metaPath the file path where the metadata will be written
     * @param state the state object containing metadata information including consumer offset, producer offset, record count, and last index
     * @throws IOException if an I/O error occurs during the write operation
     */
    private static void persistMeta(Path metaPath, QueueState state) throws IOException {
        NQueueQueueMeta.write(metaPath, state.consumerOffset, state.producerOffset, state.recordCount, state.lastIndex);
    }

    /**
     * Retrieves the current state of the queue, encapsulating details such as consumer offset,
     * producer offset, record count, and the last index.
     *
     * @return an instance of QueueState representing the current state of the queue.
     */
    private QueueState currentState() {
        return new QueueState(consumerOffset, producerOffset, recordCount, lastIndex);
    }

    /**
     * Persists the current state of the system to a metadata file. This method
     * ensures the operation is performed safely within a locked context to
     * prevent concurrent modifications.
     *
     * @throws IOException if there is an error while persisting the state to the file system.
     */
    private void persistCurrentStateLocked() throws IOException {
        persistMeta(metaPath, currentState());
    }

    /**
     * Waits until there are records available by blocking the current thread.
     * This method will repeatedly check the condition `recordCount == 0` and
     * will block the thread using the `notEmpty` condition's await method
     * if no records are available. The thread will remain in a waiting state
     * until it is signaled, ensuring that it proceeds only when records are added.
     *
     * This method is intended to manage concurrent access and synchronizes
     * threads by relying on the proper use of a condition variable. It assumes
     * that the condition `notEmpty` is associated with a lock which must be
     * properly acquired and released outside of this method.
     *
     * This method ignores interrupt signals as it uses `awaitUninterruptibly`,
     * ensuring that interruptions do not prevent the thread from being woken up
     * and processed once records become available.
     */
    private void awaitRecords() {
        while (recordCount == 0) {
            notEmpty.awaitUninterruptibly();
        }
    }

    /**
     * Consumes the next record in the queue in a thread-safe manner.
     * This method operates on a locked state to ensure thread safety.
     * It reads the record at the current consumer offset, updates the consumer offset
     * and other state parameters, and persists the updated state.
     *
     * @return an {@code Optional} containing the next {@code NQueueRecord} if available;
     *         otherwise, an empty {@code Optional}.
     * @throws IOException if an I/O error occurs while reading the record or persisting the state.
     */
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

    /**
     * Converts the given object into a byte array representation.
     *
     * @param obj the object to be converted into a byte array
     * @return a byte array representing the serialized form of the input object
     * @throws IOException if an I/O error occurs during the conversion process
     */
    private byte[] toBytes(T obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos))) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        }
    }

    /**
     * A class representing the state of a queue, encapsulating various offsets
     * and counts related to the queue's producer and consumer operations.
     *
     * The class is designed to be immutable and provides a snapshot of the current
     * state of the queue. It consists of information such as the consumer's offset,
     * producer's offset, the total number of records in the queue, and the index
     * of the last record.
     */
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
