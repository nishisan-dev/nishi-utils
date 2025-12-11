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

import dev.nishisan.utils.stats.StatsUtils;

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
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * NQueue is a class that implements a file-based persistent queue with
 * support for concurrent producers and consumers. This class is designed
 * to handle serialization and deserialization of objects while preserving
 * their order and supporting operations such as offer, poll, and peek.
 * <p>
 * This queue utilizes a backing data file for storage and a metadata file
 * for state persistence, ensuring data integrity and consistency across
 * application runs. Records are stored in a sequential manner, and the
 * internal state is periodically persisted to maintain the integrity of
 * the queue in case of application restarts or failures.
 * <p>
 * Note that objects stored in this queue must implement the Serializable interface.
 */
public class NQueue<T extends Serializable> implements Closeable {
    private static final String DATA_FILE = "data.log";
    private static final String META_FILE = "queue.meta";
    private final StatsUtils statsUtils = new StatsUtils();
    private final Path queueDir;
    private final Path dataPath;
    private final Path metaPath;
    private final Path tempDataPath;
    private volatile RandomAccessFile raf;
    private volatile FileChannel dataChannel;
    private final ReentrantLock lock;
    private final Condition notEmpty;
    private final double compactionWasteThreshold;
    private final long compactionIntervalNanos;
    private final int compactionBufferSize;
    private final Options options;
    private final ExecutorService compactionExecutor;
    private volatile Future<?> compactionFuture;
    private long consumerOffset;
    private long producerOffset;
    private long recordCount;
    private long lastIndex;
    private long lastCompactionTimeNanos;
    private volatile boolean closed;
    private volatile boolean shutdownRequested;
    private volatile CompactionState compactionState;

    /**
     * Constructs a new instance of the NQueue class with the specified parameters.
     *
     * @param queueDir    The directory where the queue files are located.
     * @param raf         The {@link RandomAccessFile} used for accessing the queue's data file.
     * @param dataChannel The {@link FileChannel} associated with the queue's data file for efficient file operations.
     * @param state       The initial state of the queue, encapsulating offsets and record count.
     */
    private NQueue(Path queueDir,
                   RandomAccessFile raf,
                   FileChannel dataChannel,
                   QueueState state,
                   Options optsions,
                   double compactionWasteThreshold,
                   long compactionIntervalNanos,
                   int compactionBufferSize) {
        this.metaPath = queueDir.resolve(META_FILE);
        this.queueDir = queueDir;
        this.dataPath = queueDir.resolve(DATA_FILE);
        this.raf = raf;
        this.dataChannel = dataChannel;
        this.lock = new ReentrantLock();
        this.notEmpty = this.lock.newCondition();
        this.compactionWasteThreshold = compactionWasteThreshold;
        this.compactionIntervalNanos = compactionIntervalNanos;
        this.compactionBufferSize = compactionBufferSize;
        this.consumerOffset = state.consumerOffset;
        this.producerOffset = state.producerOffset;
        this.recordCount = state.recordCount;
        this.lastIndex = state.lastIndex;
        this.options = optsions;
        this.lastCompactionTimeNanos = System.nanoTime();
        this.compactionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "nqueue-compaction-worker");
            t.setDaemon(true);
            return t;
        });
        this.compactionState = CompactionState.IDLE;
        this.tempDataPath = queueDir.resolve(DATA_FILE + ".compacting");
    }

    /**
     * Opens an existing NQueue or initializes a new one in the specified directory with the given queue name.
     * This method ensures that the queue's metadata and data files are properly initialized and consistent.
     *
     * @param baseDir   the base directory where the queue will be located; must not be null.
     * @param queueName the name of the queue to open or initialize; must not be null.
     * @param <T>       the type of objects to be stored in the queue, which must implement {@link Serializable}.
     * @return an instance of {@link NQueue} configured with the specified directory and queue name.
     * @throws IOException if an I/O error occurs during the initialization or access of the queue.
     */
    public static <T extends Serializable> NQueue<T> open(Path baseDir, String queueName) throws IOException {
        return open(baseDir, queueName, Options.defaults());
    }

    /**
     * Opens an existing NQueue or initializes a new one in the specified directory with the given queue name.
     * This overload allows callers to configure compaction behaviour via {@link Options}.
     *
     * @param baseDir   the base directory where the queue will be located; must not be null.
     * @param queueName the name of the queue to open or initialize; must not be null.
     * @param options   the options configuring queue behaviour; must not be null.
     * @param <T>       the type of objects to be stored in the queue, which must implement {@link Serializable}.
     * @return an instance of {@link NQueue} configured with the specified directory, queue name and options.
     * @throws IOException if an I/O error occurs during the initialization or access of the queue.
     */
    public static <T extends Serializable> NQueue<T> open(Path baseDir, String queueName, Options options) throws IOException {
        Objects.requireNonNull(baseDir, "baseDir cannot be null");
        Objects.requireNonNull(queueName, "queueName cannot be null");
        Objects.requireNonNull(options, "options cannot be null");

        Path queueDir = baseDir.resolve(queueName);
        Files.createDirectories(queueDir);

        Path dataPath = queueDir.resolve(DATA_FILE);
        Path metaPath = queueDir.resolve(META_FILE);

        RandomAccessFile raf = new RandomAccessFile(dataPath.toFile(), "rw");
        FileChannel ch = raf.getChannel();

        QueueState state;
        if (Files.exists(metaPath)) {
            NQueueQueueMeta meta = NQueueQueueMeta.read(metaPath);
            state = new QueueState(meta.getConsumerOffset(), meta.getProducerOffset(), meta.getRecordCount(), meta.getLastIndex(), ch);

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
                ch.force(options.withFsync);
            }
        } else {
            state = rebuildState(ch);
            persistMeta(metaPath, state);
        }

        Options.Snapshot snapshot = options.snapshot();
        return new NQueue<>(queueDir, raf, ch, state,
                options,
                snapshot.compactionWasteThreshold,
                snapshot.compactionIntervalNanos,
                snapshot.compactionBufferSize);
    }


    /**
     * Adds the specified object to the queue and returns the file offset at which the object
     * has been stored. This method ensures thread-safety, persists the state of the queue
     * after writing the object, and signals any waiting consumers.
     *
     * @param object the object to be added to the queue; must not be null.
     * @return the file offset at which the object has been stored.
     * @throws IOException if an I/O error occurs during the operation.
     */
    public long offer(T object) throws IOException {
        // Passo 1: Valida que o objeto não seja nulo
        Objects.requireNonNull(object, "object");

        // Passo 2: Serializa o objeto em um array de bytes para armazenamento
        byte[] payload = toBytes(object);
        int payloadLen = payload.length;

        // Passo 3: Adquire o lock para garantir thread-safety durante toda a operação
        lock.lock();
        try {
            // Passo 4: Calcula o próximo índice sequencial do registro
            // Se houver overflow (nextIndex < 0), reseta para 0
            long nextIndex = lastIndex + 1;
            if (nextIndex < 0) {
                nextIndex = 0;
            }

            // Passo 5: Cria os metadados do registro contendo:
            // - índice sequencial
            // - tamanho do payload
            // - nome canônico da classe do objeto
            NQueueRecordMetaData meta = new NQueueRecordMetaData(nextIndex, payloadLen, object.getClass().getCanonicalName());

            // Passo 6: Determina a posição de escrita no arquivo (offset do produtor)
            // e salva essa posição como início do registro para retorno
            long writePos = producerOffset;
            long recordStart = writePos;

            // Passo 7: Serializa os metadados em um ByteBuffer e escreve no canal
            // Garante que todos os bytes do header sejam escritos, mesmo em múltiplas operações
            ByteBuffer hb = meta.toByteBuffer();
            while (hb.hasRemaining()) {
                int written = dataChannel.write(hb, writePos);
                writePos += written;
            }

            // Passo 8: Escreve o payload (objeto serializado) no canal após o header
            // Garante que todos os bytes do payload sejam escritos
            ByteBuffer pb = ByteBuffer.wrap(payload);
            while (pb.hasRemaining()) {
                int written = dataChannel.write(pb, writePos);
                writePos += written;
            }

            // Passo 9: Atualiza o estado interno da fila:
            // - producerOffset: nova posição para próxima escrita
            // - recordCount:    incrementa contador de registros
            // - lastIndex:      atualiza último índice usado
            producerOffset = writePos;
            recordCount++;
            lastIndex = nextIndex;

            // Passo 10: Se este for o primeiro registro da fila,
            // ajusta o consumerOffset para apontar para o início deste registro
            if (recordCount == 1) {
                consumerOffset = recordStart;
            }

            // Passo 11: Se fsync for true faz a sincronização dos dados com o disco
            // para garantir durabilidade em caso de falha
            dataChannel.force(options.withFsync);

            // Passo 12: Persiste os metadados da fila (offsets, contadores) no arquivo .meta
            persistCurrentStateLocked();

            // Passo 13: Verifica se é necessário compactar o arquivo
            // (remover espaço desperdiçado por registros já consumidos)
            maybeCompactLocked();

            // Passo 14: Sinaliza threads consumidoras que estão aguardando
            // que novos dados estão disponíveis
            notEmpty.signalAll();

            // Passo 15: Retorna o offset onde o registro foi armazenado
            return recordStart;
        } finally {
            // Passo 16: Incrementa contador de métricas para monitoramento
            statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);

            // Passo 17: Libera o lock para permitir outras operações
            lock.unlock();
        }
    }

    /**
     * Reads and deserializes a stored object located at the specified file offset
     * in the queue, if available, and returns it as an {@link Optional}.
     *
     * @param offset the file offset where the object is expected to be located; must be non-negative.
     * @return an {@link Optional} containing the deserialized object of type {@code T},
     * or {@link Optional#empty()} if no valid object exists at the specified offset.
     * @throws IOException if an I/O error occurs while reading or deserializing the object.
     */
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

    /**
     * Reads a record located at the specified file offset in the queue and retrieves
     * it, along with the offset for the next record, encapsulated in an {@link NQueueReadResult}.
     * This method ensures thread safety by locking during the operation.
     *
     * @param offset the file offset where the record is expected to be located; must be non-negative.
     * @return an {@link Optional} containing the {@link NQueueReadResult} with the record and next offset,
     * or {@link Optional#empty()} if no valid record exists at the specified offset.
     * @throws IOException if an I/O error occurs while reading the record.
     */
    public Optional<NQueueReadResult> readRecordAt(long offset) throws IOException {
        lock.lock();
        try {
            return readAtInternal(offset);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves the next available record in the queue without removing it.
     * If the queue is empty, an empty {@code Optional} is returned.
     * <p>
     * This method ensures thread safety by employing a lock during its operation.
     *
     * @return an {@code Optional<NQueueRecord>} containing the next available record if the queue is not empty,
     * or {@code Optional.empty()} if the queue is empty.
     * @throws IOException if an I/O error occurs during the operation.
     */
    public Optional<NQueueRecord> peekRecord() throws IOException {
        lock.lock();
        try {
            if (recordCount == 0) {
                return Optional.empty();
            }
            return readAtInternal(consumerOffset).map(NQueueReadResult::getRecord);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves the next object from the queue without removing it.
     * If the queue is empty, an empty {@code Optional} is returned.
     * This method ensures thread safety by employing a lock during operation.
     *
     * @return an {@code Optional<T>} containing the next object if the queue is not empty,
     * or {@code Optional.empty()} if the queue is empty.
     * @throws IOException if an I/O error occurs during the operation.
     */
    public Optional<T> peek() throws IOException {
        lock.lock();
        try {
            if (recordCount == 0) {
                return Optional.empty();
            }
            Optional<NQueueReadResult> result = readAtInternal(consumerOffset);
            if (result.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(deserializeRecord(result.get().getRecord()));
        } finally {
            statsUtils.notifyHitCounter(NQueueMetrics.PEEK_EVENT);
            lock.unlock();
        }
    }

    /**
     * Retrieves and removes the next available object from the queue, if one exists.
     * This operation blocks until a record is available or the thread is interrupted.
     * <p>
     * The method ensures thread safety by locking during the operation and deserializes
     * the retrieved record into an object of type {@code T}.
     *
     * @return an {@code Optional<T>} containing the next object if the queue is not empty,
     * or {@code Optional.empty()} if the queue is empty.
     * @throws IOException if an I/O error occurs during the operation.
     */
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
            statsUtils.notifyHitCounter(NQueueMetrics.POLL_EVENT);
            lock.unlock();
        }
    }

    /**
     * Retrieves and removes the next available object from the queue, if one exists, within the specified timeout.
     * This operation blocks until a record is available, the timeout expires, or the thread is interrupted.
     * <p>
     * Thread safety is ensured by employing a lock during the operation. If the queue remains empty
     * within the given timeout or the thread is interrupted, an empty {@code Optional} is returned.
     *
     * @param timeout the maximum time to wait for a record to become available; must be non-negative.
     * @param unit    the time unit of the {@code timeout} argument; must not be {@code null}.
     * @return an {@code Optional<T>} containing the next object if the queue is not empty within
     * the timeout, or {@code Optional.empty()} if no object is available or the thread is interrupted.
     * @throws IOException if an I/O error occurs during the operation.
     */
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
            statsUtils.notifyHitCounter(NQueueMetrics.POLL_EVENT);
            lock.unlock();
        }
    }

    /**
     * Retrieves the current size of the queue, represented by the total number
     * of records present in the queue.
     * <p>
     * This method ensures thread safety by employing a lock during the operation.
     *
     * @return the total number of records currently in the queue.
     * @throws IOException if an I/O error occurs during the operation.
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
     * This method is thread-safe, as it locks the internal state during the operation.
     *
     * @return {@code true} if the queue is empty, {@code false} otherwise.
     */
    public boolean isEmpty() {
        lock.lock();
        try {
            return recordCount == 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves the number of records currently in the queue.
     * This method ensures thread safety by locking the internal state during the operation.
     *
     * @return the total number of records present in the queue.
     */
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
        Future<?> future;
        lock.lock();
        try {
            shutdownRequested = true;
            maybeCompactLocked();
            future = compactionFuture;
        } finally {
            lock.unlock();
        }

        if (future != null) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException ignored) {
                // Exceptions during compaction are ignored on close.
            }
        }

        lock.lock();
        try {
            dataChannel.close();
            raf.close();
            closed = true;
        } finally {
            lock.unlock();
        }
        compactionExecutor.shutdownNow();
    }

    /**
     * Reads data from the specified offset in the internal data channel and attempts to construct
     * an {@link Optional} containing an {@link NQueueReadResult} if the read operation is successful.
     * The method ensures that all required parts of the record (header and payload) are complete
     * based on the metadata and file size.
     *
     * @param offset the offset position in the data channel from which to start reading
     * @return an {@link Optional} containing {@link NQueueReadResult} if a valid record is read,
     * or an empty {@link Optional} if the offset does not contain a complete record
     * @throws IOException if there is an error reading from the data channel
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
     * Rebuilds the state of a queue by analyzing and repairing the content of a file channel.
     * Truncates the file channel if corrupted or incomplete data segments are detected.
     *
     * @param ch the FileChannel to read and sanitize the queue data from
     * @return a QueueState object that contains the reconstructed state of the queue,
     * including consumer offset, producer offset, element count, and last record index
     * @throws IOException if an I/O error occurs while reading or writing to the file channel
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

        return new QueueState(consumerOffset, producerOffset, count, lastIndex, ch);
    }

    /**
     * Persists the metadata of the queue state to the specified file path.
     *
     * @param metaPath the file path where the metadata will be stored
     * @param state    the current state of the queue containing consumer offset,
     *                 producer offset, record count, and last index
     * @throws IOException if an I/O error occurs while writing the metadata
     */
    private static void persistMeta(Path metaPath, QueueState state) throws IOException {
        NQueueQueueMeta.write(metaPath, state.consumerOffset, state.producerOffset, state.recordCount, state.lastIndex);
    }

    /**
     * Retrieves the current state of the queue, encapsulating details such as the
     * consumer offset, producer offset, record count, and the last index.
     *
     * @return an instance of QueueState representing the current state of the queue.
     */
    private QueueState currentState() {
        return new QueueState(consumerOffset, producerOffset, recordCount, lastIndex, dataChannel);
    }

    /**
     * Persists the current state to a specified location in a thread-safe manner.
     * <p>
     * This method ensures that the current application's state is serialized
     * and stored persistently using the defined metaPath. It is expected to be
     * used in scenarios where the state information needs to be saved securely
     * to prevent data loss.
     * <p>
     * The method is designed to be invoked within a synchronized context to
     * enforce thread-safety when saving state data.
     *
     * @throws IOException if an I/O error occurs during the persisting process
     */
    private void persistCurrentStateLocked() throws IOException {
        persistMeta(metaPath, currentState());
    }

    /**
     * Waits for records to become available in a thread-safe manner.
     * This method blocks the current thread until the condition that records are available
     * is met. The condition is evaluated based on the value of the recordCount field.
     * <p>
     * Utilizes the {@code notEmpty} condition to suspend the thread execution until signaled
     * that the records have been added (i.e., when {@code recordCount > 0}).
     * The awaiting is uninterruptible.
     * <p>
     * Note:
     * - Ensure the lock associated with the condition is held before invoking this method.
     * - Use caution when using uninterruptible waits, as they prevent the thread from responding to interruptions.
     */
    private void awaitRecords() {
        while (recordCount == 0) {
            notEmpty.awaitUninterruptibly();
        }
    }

    /**
     * Consumes the next record in the queue while the current state is locked,
     * updates the consumer offset, and decrements the record count. If the record
     * count reaches zero, the consumer offset is reset to the producer offset.
     * The state is persisted after processing.
     *
     * @return an {@code Optional} containing the next {@code NQueueRecord}
     * if available, or an empty {@code Optional} if no record is present.
     * @throws IOException if an I/O error occurs while reading the next record.
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
        maybeCompactLocked();
        return Optional.of(rr.getRecord());
    }

    /**
     * Determines if compaction should be performed based on specific thresholds and conditions,
     * and executes the compaction if necessary. This method is invoked while holding a lock to ensure
     * thread safety during the evaluation and potential modification of shared state.
     * <p>
     * Compaction is triggered under the following conditions:
     * - The total bytes and wasted bytes are greater than zero.
     * - The record count is not zero, or the elapsed time since the last compaction exceeds a specified interval.
     * - The waste ratio (calculated as the ratio of wasted bytes to total bytes) exceeds a predefined threshold,
     * or the compaction interval has been exceeded and there are wasted bytes.
     * <p>
     * If conditions are satisfied, the compaction logic is executed, and the timestamp of the last
     * compaction is updated.
     *
     * @throws IOException if an I/O error occurs during the compaction process.
     */
    private void maybeCompactLocked() throws IOException {
        if (shutdownRequested || compactionState == CompactionState.RUNNING) {
            return;
        }
        long totalBytes = producerOffset;
        long wastedBytes = consumerOffset;
        if (totalBytes <= 0 || wastedBytes <= 0) {
            return;
        }

        long now = System.nanoTime();
        boolean intervalExceeded = compactionIntervalNanos > 0 && (now - lastCompactionTimeNanos) >= compactionIntervalNanos;
        if (recordCount == 0 && !intervalExceeded) {
            return;
        }

        double wasteRatio = (double) wastedBytes / (double) totalBytes;
        boolean wasteExceeded = wasteRatio >= compactionWasteThreshold;
        if (!wasteExceeded && !(intervalExceeded && wastedBytes > 0)) {
            return;
        }

        startCompactionIfIdleLocked(now);
    }

    /**
     * Compacts the underlying data storage by removing all consumed data
     * and shifting the remaining data to the beginning of the storage.
     * This operation ensures that the storage utilizes space efficiently
     * by discarding unnecessary data and optimizing future access.
     * <p>
     * The heavy lifting is now delegated to a background compaction worker.
     * This method simply schedules the asynchronous compaction when invoked
     * while holding the main queue lock.
     *
     * @throws IOException if an I/O error occurs during the compaction process.
     *                     <p>
     *                     Key operations performed:
     *                     1. Calculates the length of data to be preserved.
     *                     2. Clears storage if no data is to be preserved.
     *                     3. Copies remaining data to the beginning of the storage.
     *                     4. Updates the producer and consumer offsets accordingly.
     *                     5. Truncates excess space and persists the updated state.
     */
    private void compactLocked() throws IOException {
        startCompactionIfIdleLocked(System.nanoTime());
    }

    private void startCompactionIfIdleLocked(long now) {
        // Only one compaction worker is allowed at a time. The lock protects the
        // state snapshot that is handed off to the worker and avoids scheduling
        // multiple background jobs concurrently.
        if (shutdownRequested || compactionState != CompactionState.IDLE) {
            return;
        }

        QueueState snapshot = currentState();
        compactionState = CompactionState.RUNNING;
        try {
            compactionFuture = compactionExecutor.submit(() -> runCompaction(snapshot));
            lastCompactionTimeNanos = now;
        } catch (RuntimeException e) {
            compactionState = CompactionState.IDLE;
            throw e;
        }
    }

    private void runCompaction(QueueState snapshot) {
        try {
            Files.deleteIfExists(tempDataPath);
            try (RandomAccessFile tmpRaf = new RandomAccessFile(tempDataPath.toFile(), "rw");
                 FileChannel tmpChannel = tmpRaf.getChannel()) {
                // Copies the active window [consumerOffset, producerOffset) into a temporary file
                // without holding the main queue lock, allowing offer/poll/peek to proceed. The
                // only synchronization points happen when switching the live file to the compacted one.
                copyActiveRegion(snapshot, tmpChannel);

                if (options.withFsync) {
                    tmpChannel.force(true);
                }

                finalizeCompaction(snapshot, tempDataPath, tmpChannel.size());
            }
        } catch (IOException ignored) {
            lock.lock();
            try {
                compactionState = CompactionState.IDLE;
            } finally {
                lock.unlock();
            }
        } finally {
            lock.lock();
            try {
                if (compactionState == CompactionState.IDLE) {
                    Files.deleteIfExists(tempDataPath);
                }
            } catch (IOException ignored) {
                // best-effort cleanup
            } finally {
                lock.unlock();
            }
        }
    }

    private void copyActiveRegion(QueueState snapshot, FileChannel target) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(compactionBufferSize);
        long readOffset = snapshot.consumerOffset;
        long limit = snapshot.producerOffset;
        FileChannel source = snapshot.dataChannel;

        while (readOffset < limit) {
            buffer.clear();
            int bytesRead = source.read(buffer, readOffset);
            if (bytesRead <= 0) {
                break;
            }
            buffer.flip();
            while (buffer.hasRemaining()) {
                target.write(buffer);
            }
            readOffset += bytesRead;
        }
    }

    private void finalizeCompaction(QueueState snapshot, Path tempPath, long newProducerOffset) throws IOException {
        lock.lock();
        try {
            boolean stateChanged = consumerOffset != snapshot.consumerOffset
                    || producerOffset != snapshot.producerOffset
                    || recordCount != snapshot.recordCount
                    || lastIndex != snapshot.lastIndex;

            if (stateChanged) {
                // State changed while we copied the active window. Abort this attempt.
                // Instead of immediately rescheduling compaction, update lastCompactionTimeNanos
                // to enforce a minimum interval before the next compaction attempt.
                compactionState = CompactionState.IDLE;
                lastCompactionTimeNanos = System.nanoTime();
                return;
            }

            // Minimal critical section: close the old channel, atomically switch the file,
            // reopen the channel, and update in-memory offsets/state before releasing the lock.
            // Use local variables to avoid exposing closed channels to other threads via volatile fields.
            FileChannel oldDataChannel = dataChannel;
            RandomAccessFile oldRaf = raf;
            
            oldDataChannel.close();
            oldRaf.close();

            try {
                Files.move(tempPath, dataPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tempPath, dataPath, StandardCopyOption.REPLACE_EXISTING);
            }

            RandomAccessFile newRaf = new RandomAccessFile(dataPath.toFile(), "rw");
            FileChannel newDataChannel = newRaf.getChannel();
            
            this.raf = newRaf;
            this.dataChannel = newDataChannel;

            consumerOffset = 0L;
            producerOffset = newProducerOffset;

            persistCurrentStateLocked();
            if (options.withFsync) {
                dataChannel.force(true);
            }

            lastCompactionTimeNanos = System.nanoTime();
        } finally {
            compactionState = CompactionState.IDLE;
            lock.unlock();
        }
    }

    /**
     * Converts the given object to its byte array representation.
     *
     * @param obj the object to be converted into a byte array. It should be serializable.
     * @return a byte array representing the serialized form of the given object.
     * @throws IOException if an I/O error occurs during the serialization process.
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
     * Deserializes a record from its payload into an object of type T.
     *
     * @param record the NQueueRecord containing the serialized payload to be deserialized
     * @return an object of type T obtained by deserializing the provided record's payload
     * @throws IOException if an I/O error occurs during deserialization or the class of the object cannot be found
     */
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

    private enum CompactionState {
        IDLE,
        RUNNING
    }

    /**
     * Represents the current state of a queue including offsets, record count,
     * and the last index.
     */
    private static final class QueueState {
        final long consumerOffset;
        final long producerOffset;
        final long recordCount;
        final long lastIndex;
        final FileChannel dataChannel;

        QueueState(long consumerOffset, long producerOffset, long recordCount, long lastIndex, FileChannel dataChannel) {
            this.consumerOffset = consumerOffset;
            this.producerOffset = producerOffset;
            this.recordCount = recordCount;
            this.lastIndex = lastIndex;
            this.dataChannel = dataChannel;
        }
    }

    /**
     * Options used to configure queue behaviour.
     */
    public static final class Options {
        private double compactionWasteThreshold = 0.5d;
        private Duration compactionInterval = Duration.ofMinutes(5);
        private int compactionBufferSize = 128 * 1024;
        private boolean withFsync = true;

        private Options() {
        }

        public static Options defaults() {
            return new Options();
        }

        /**
         * Sets the compaction waste threshold, which defines the proportion of wasted space
         * in data structures that can trigger a compaction process. The threshold must be
         * a value between 0.0 and 1.0 inclusive, representing percentages in decimal form.
         *
         * @param threshold the compaction waste threshold as a decimal value between 0.0 and 1.0
         * @return the current {@code Options} instance for method chaining
         * @throws IllegalArgumentException if {@code threshold} is less than 0.0 or greater than 1.0
         */
        public Options withCompactionWasteThreshold(double threshold) {
            if (threshold < 0.0d || threshold > 1.0d) {
                throw new IllegalArgumentException("threshold deve estar no intervalo [0.0, 1.0]");
            }
            this.compactionWasteThreshold = threshold;
            return this;
        }

        /**
         * Sets the compaction interval, which determines the frequency of compaction operations.
         * A valid, non-negative interval must be provided.
         *
         * @param interval the {@code Duration} specifying the compaction interval; must not be null or negative
         * @return the current {@code Options} instance for method chaining
         * @throws NullPointerException if {@code interval} is null
         * @throws IllegalArgumentException if {@code interval} is negative
         */
        public Options withCompactionInterval(Duration interval) {
            Objects.requireNonNull(interval, "interval");
            if (interval.isNegative()) {
                throw new IllegalArgumentException("interval não pode ser negativo");
            }
            this.compactionInterval = interval;
            return this;
        }

        /**
         * Sets the buffer size for compaction processes.
         * This size determines the allocation limit for buffer data during the compaction operation.
         *
         * @param bufferSize the size of the buffer in bytes; must be a positive value
         * @return the current {@code Options} instance for method chaining
         * @throws IllegalArgumentException if {@code bufferSize} is less than or equal to 0
         */
        public Options withCompactionBufferSize(int bufferSize) {
            if (bufferSize <= 0) {
                throw new IllegalArgumentException("bufferSize deve ser positivo");
            }
            this.compactionBufferSize = bufferSize;
            return this;
        }

        /**
         * Configures whether file system synchronization (fsync) should be performed.
         * When enabled, fsync ensures that data is physically written to disk, improving
         * data durability at the cost of performance. Disabling fsync may improve performance
         * but increases the risk of data loss in the event of a crash.
         *
         * @param fsync a boolean value indicating whether fsync should be enabled (true) or disabled (false)
         * @return the current {@code Options} instance for method chaining
         */
        public Options withFsync(boolean fsync) {
            this.withFsync = fsync;
            return this;
        }

        /**
         * Creates a snapshot of the current configuration by encapsulating the values
         * of compaction waste threshold, compaction interval, and compaction buffer size.
         * A snapshot is immutable and stores these values for future reference or processing.
         *
         * @return a new {@code Snapshot} instance containing the current configuration values
         */
        private Snapshot snapshot() {
            Duration interval = compactionInterval;
            long intervalNanos = interval != null ? interval.toNanos() : 0L;
            return new Snapshot(compactionWasteThreshold, intervalNanos, compactionBufferSize);
        }

        private static final class Snapshot {
            final double compactionWasteThreshold;
            final long compactionIntervalNanos;
            final int compactionBufferSize;

            Snapshot(double compactionWasteThreshold, long compactionIntervalNanos, int compactionBufferSize) {
                this.compactionWasteThreshold = compactionWasteThreshold;
                this.compactionIntervalNanos = compactionIntervalNanos;
                this.compactionBufferSize = compactionBufferSize;
            }
        }
    }


    public StatsUtils getStatsUtils() {
        return statsUtils;
    }


}
