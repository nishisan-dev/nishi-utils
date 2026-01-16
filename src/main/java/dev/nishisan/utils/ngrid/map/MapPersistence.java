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

package dev.nishisan.utils.ngrid.map;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Local disk persistence for the distributed map. Uses an append-only WAL and periodic full snapshots.
 * <p>
 * This component is intentionally best-effort: write failures are logged but do not fail map operations.
 */
public final class MapPersistence<K extends Serializable, V extends Serializable> implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(MapPersistence.class.getName());

    private static final String WAL_FILE = "wal.log";
    private static final String SNAPSHOT_FILE = "snapshot.dat";
    private static final String META_FILE = "map.meta";

    // Entry framing + compatibility
    private static final int ENTRY_MAGIC = 0x4E475741; // "NGWA"
    private static final int ENTRY_VERSION = 1;

    private final MapPersistenceConfig config;
    private final Map<K, V> data;
    private final Path mapDir;
    private final Path walPath;
    private final Path snapshotPath;
    private final Path metaPath;
    private final Path tempSnapshotPath;
    private final Path oldWalPath;

    private final LinkedBlockingQueue<WALEntry> queue = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean();

    private volatile RandomAccessFile walRaf;
    private volatile FileChannel walChannel;
    private volatile Thread writerThread;

    private long opsSinceSnapshot;
    private long lastSnapshotTimeMillis;

    public MapPersistence(MapPersistenceConfig config, Map<K, V> data) {
        this.config = Objects.requireNonNull(config, "config");
        this.data = Objects.requireNonNull(data, "data");
        this.mapDir = config.mapDirectory().resolve(config.mapName());
        this.walPath = mapDir.resolve(WAL_FILE);
        this.snapshotPath = mapDir.resolve(SNAPSHOT_FILE);
        this.metaPath = mapDir.resolve(META_FILE);
        this.tempSnapshotPath = mapDir.resolve(SNAPSHOT_FILE + ".tmp");
        this.oldWalPath = mapDir.resolve(WAL_FILE + ".old");
    }

    /**
     * Loads the map persistence state from disk if persistence mode is enabled.
     *
     * This method first checks the persistence mode using the configuration. If the mode is
     * {@code DISABLED}, the method exits without performing any action.
     *
     * It attempts to prepare the necessary directories, load snapshot data, and rebuild the
     * state from the write-ahead log (WAL). Additionally, it tries to read metadata, which
     * includes the last snapshot timestamp, but does not require it to proceed, as reading
     * metadata is a best-effort operation. If the metadata is read successfully, the timestamp
     * of the last snapshot is updated.
     *
     * Any {@link IOException} encountered during the process is logged for debugging or error
     * analysis, and the method continues without propagating the exception further.
     */
    public void load() {
        if (config.mode() == MapPersistenceMode.DISABLED) {
            return;
        }
        try {
            Files.createDirectories(mapDir);
            loadSnapshot();
            if (Files.exists(oldWalPath)) {
                LOGGER.info("Detected incomplete rotation. Replaying old WAL.");
                loadWal(oldWalPath);
            }
            loadWal(walPath);
            // Meta is best-effort; if missing/corrupt it's ignored.
            readMeta().ifPresent(meta -> lastSnapshotTimeMillis = meta.lastSnapshotTimestamp());
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to load map persistence state", e);
        }
    }

    /**
     * Starts the map persistence process based on the defined configuration mode.
     *
     * Checks if the persistence mode is enabled. If persistence is disabled, this method exits
     * without performing any actions. If the persistence process is already running, it does not
     * start another instance.
     *
     * If persistence is enabled and not running, this method:
     * - Creates necessary directories for storing persistence files.
     * - Opens the write-ahead log (WAL) file for appending data.
     * - Sets the initial snapshot time if it was not restored from metadata.
     * - Creates and starts a dedicated background writer thread for handling persistence tasks.
     *
     * If an {@link IOException} occurs during initialization (such as directory creation or WAL opening),
     * the persistence process is not started, the running state is reset, and a warning is logged.
     */
    public void start() {
        if (config.mode() == MapPersistenceMode.DISABLED) {
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            Files.createDirectories(mapDir);
            openWalForAppend();
            // Set initial snapshot time if not restored from meta.
            if (lastSnapshotTimeMillis <= 0) {
                lastSnapshotTimeMillis = System.currentTimeMillis();
            }
        } catch (IOException e) {
            running.set(false);
            LOGGER.log(Level.WARNING, "Failed to start map persistence (WAL open)", e);
            return;
        }

        writerThread = new Thread(this::runWriterLoop, "ngrid-map-persistence");
        writerThread.setDaemon(true);
        writerThread.start();
    }

    /**
     * Appends a write-ahead log (WAL) entry asynchronously for a given replication command.
     *
     * This method creates a new WAL entry, capturing the provided replication command type,
     * key, and value, then queues it for asynchronous processing. If the persistence mode
     * is disabled, the method exits without performing any operation.
     *
     * @param type the type of the replication command, such as PUT or REMOVE. Must not be null.
     * @param key the key associated with the command. Must not be null.
     * @param value the value associated with the command. Can be null depending on the command type.
     */
    public void appendAsync(MapReplicationCommandType type, Serializable key, Serializable value) {
        if (config.mode() == MapPersistenceMode.DISABLED) {
            return;
        }
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(key, "key");
        queue.offer(new WALEntry(System.currentTimeMillis(), type, key, value));
    }

    /**
     * Closes the map persistence process and releases associated resources.
     *
     * This method performs the following operations:
     * - If the persistence mode is {@code DISABLED}, it exits immediately without making any changes.
     * - Updates the state to indicate that the persistence process is no longer running.
     * - Waits for the associated writer thread to finish execution, with a maximum timeout of 10 seconds.
     *   If interrupted during the wait, the thread is re-interrupted.
     * - Cleans up and safely closes any resources associated with the write-ahead log (WAL) by invoking
     *   {@code closeWalQuietly()}.
     *
     * @throws IOException if an error occurs during thread handling or resource cleanup operations.
     */
    @Override
    public void close() throws IOException {
        if (config.mode() == MapPersistenceMode.DISABLED) {
            return;
        }
        running.set(false);
        Thread t = writerThread;
        if (t != null) {
            try {
                t.join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        closeWalQuietly();
    }

    /**
     * Executes the main persistence loop in a dedicated thread. This method continuously processes
     * write-ahead log (WAL) entries queued for persistence until the processing is terminated.
     *
     * The method operates as follows:
     * - Polls entries from the queue with a timeout defined by the `batchTimeout` configuration.
     * - If a timeout occurs without entries, attempts to create a snapshot if criteria are met
     *   using the `maybeSnapshot` method.
     * - If entries are available, forms a batch of entries up to the size specified by the
     *   `batchSize` configuration and writes the batch to persistent storage using the
     *   `writeBatch` method, while tracking the number of operations processed since the
     *   last snapshot.
     * - Continuously checks for graceful shutdown to stop processing once all queued entries
     *   are handled.
     *
     * Handles shutdown by:
     * - Responding to an `InterruptedException` to clear interruption status and gracefully
     *   stop processing.
     * - Attempting a final best-effort flush of remaining entries in the queue during shutdown.
     *   If any errors occur while processing this final flush, logs the exception but does
     *   not propagate it further.
     *
     * This method is intended to be called on a long-running background thread as part of
     * the map persistence lifecycle.
     */
    private void runWriterLoop() {
        Duration batchTimeout = config.batchTimeout();
        long batchTimeoutMs = Math.max(1L, batchTimeout.toMillis());

        while (running.get() || !queue.isEmpty()) {
            try {
                WALEntry first = queue.poll(batchTimeoutMs, TimeUnit.MILLISECONDS);
                if (first == null) {
                    maybeSnapshot();
                    continue;
                }
                List<WALEntry> batch = new ArrayList<>(config.batchSize());
                batch.add(first);
                queue.drainTo(batch, Math.max(0, config.batchSize() - 1));

                writeBatch(batch);
                opsSinceSnapshot += batch.size();
                maybeSnapshot();
            } catch (InterruptedException e) {
                // Graceful shutdown: do not keep the interrupted flag set while using FileChannel,
                // otherwise writes can fail with ClosedByInterruptException.
                running.set(false);
                Thread.interrupted(); // clear interruption status
                continue;
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Unexpected error in persistence writer loop", e);
            }
        }

        // Final best-effort flush on shutdown
        try {
            List<WALEntry> remaining = new ArrayList<>();
            queue.drainTo(remaining);
            if (!remaining.isEmpty()) {
                writeBatch(remaining);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to flush remaining WAL entries on shutdown", e);
        }
    }

    private void writeBatch(List<WALEntry> batch) {
        if (batch.isEmpty()) {
            return;
        }
        FileChannel ch = walChannel;
        if (ch == null) {
            return;
        }
        try {
            for (WALEntry entry : batch) {
                ByteBuffer buffer = encode(entry);
                while (buffer.hasRemaining()) {
                    ch.write(buffer);
                }
            }
            if (config.mode() == MapPersistenceMode.ASYNC_WITH_FSYNC) {
                ch.force(true);
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to append WAL batch", e);
        }
    }

    public void maybeSnapshot() {
        boolean byOps = config.snapshotIntervalOperations() > 0 && opsSinceSnapshot >= config.snapshotIntervalOperations();
        boolean byTime = config.snapshotIntervalTime() != null
                && !config.snapshotIntervalTime().isZero()
                && (System.currentTimeMillis() - lastSnapshotTimeMillis) >= config.snapshotIntervalTime().toMillis();
        if (!byOps && !byTime) {
            return;
        }
        try {
            createSnapshotAndRotateWal();
            opsSinceSnapshot = 0;
            lastSnapshotTimeMillis = System.currentTimeMillis();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to create map snapshot", e);
        }
    }

    /**
     * Rotates WAL so the new WAL contains only operations after the rotation, writes a snapshot,
     * then deletes the old WAL (compact).
     */
    private void createSnapshotAndRotateWal() throws IOException {
        // Best-effort: flush whatever is currently in queue to the current WAL before rotation.
        List<WALEntry> preRotate = new ArrayList<>();
        queue.drainTo(preRotate, config.batchSize());
        if (!preRotate.isEmpty()) {
            writeBatch(preRotate);
        }

        // Rotate WAL: wal.log -> wal.log.old; create new wal.log for subsequent entries.
        closeWalQuietly();
        try {
            Files.deleteIfExists(oldWalPath);
            if (Files.exists(walPath)) {
                Files.move(walPath, oldWalPath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            // If rotation fails, keep going (snapshot can still be written).
            LOGGER.log(Level.WARNING, "Failed to rotate WAL before snapshot", e);
        }
        openWalForAppend();

        // Snapshot of the current in-memory state.
        // If the backing map is concurrent (typical usage), the copy is weakly consistent but safe.
        // Otherwise, synchronize on the map instance to avoid ConcurrentModificationException and
        // ensure a consistent view for snapshotting.
        Map<K, V> snapshot;
        if (data instanceof ConcurrentMap<?, ?>) {
            snapshot = new HashMap<>(data);
        } else {
            synchronized (data) {
                snapshot = new HashMap<>(data);
            }
        }
        writeSnapshot(snapshot);

        // Compact: delete old WAL now that snapshot exists.
        try {
            Files.deleteIfExists(oldWalPath);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to delete rotated WAL after snapshot", e);
        }

        // Persist meta best-effort.
        writeMeta(new MapMetadata(0L, System.currentTimeMillis(), ENTRY_VERSION));
    }

    private void writeSnapshot(Map<K, V> snapshot) throws IOException {
        Files.deleteIfExists(tempSnapshotPath);
        try (BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(tempSnapshotPath));
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(snapshot);
            oos.flush();
        }
        try {
            Files.move(tempSnapshotPath, snapshotPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tempSnapshotPath, snapshotPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void loadSnapshot() throws IOException {
        if (!Files.exists(snapshotPath)) {
            return;
        }
        try (BufferedInputStream bis = new BufferedInputStream(Files.newInputStream(snapshotPath));
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            @SuppressWarnings("unchecked")
            Map<K, V> snapshot = (Map<K, V>) ois.readObject();
            data.clear();
            data.putAll(snapshot);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize snapshot", e);
        }
    }

    private void loadWal(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw");
             FileChannel ch = raf.getChannel()) {
            long size = ch.size();
            long offset = 0L;
            while (offset < size) {
                ByteBuffer lenBuf = ByteBuffer.allocate(4);
                int r = readFully(ch, lenBuf, offset);
                if (r <= 0) {
                    break;
                }
                if (r < 4) {
                    // Partial header: truncate.
                    ch.truncate(offset);
                    break;
                }
                lenBuf.flip();
                int entryLen = lenBuf.getInt();
                if (entryLen <= 0 || entryLen > (64 * 1024 * 1024)) {
                    ch.truncate(offset);
                    break;
                }
                long entryStart = offset + 4L;
                long entryEnd = entryStart + entryLen;
                if (entryEnd > size) {
                    ch.truncate(offset);
                    break;
                }
                byte[] payload = new byte[entryLen];
                ByteBuffer pb = ByteBuffer.wrap(payload);
                int read = readFully(ch, pb, entryStart);
                if (read < entryLen) {
                    ch.truncate(offset);
                    break;
                }
                try {
                    applyDecoded(payload);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Invalid WAL entry detected, truncating", e);
                    ch.truncate(offset);
                    break;
                }
                offset = entryEnd;
            }
        }
    }

    private void applyDecoded(byte[] payload) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            int magic = in.readInt();
            int version = in.readInt();
            if (magic != ENTRY_MAGIC || version != ENTRY_VERSION) {
                throw new IOException("Unsupported WAL entry format");
            }
            int typeOrdinal = in.readUnsignedByte();
            MapReplicationCommandType[] values = MapReplicationCommandType.values();
            if (typeOrdinal < 0 || typeOrdinal >= values.length) {
                throw new IOException("Invalid WAL entry type");
            }
            MapReplicationCommandType type = values[typeOrdinal];
            in.readLong(); // timestamp (reserved for future use)
            int keyLen = in.readInt();
            if (keyLen <= 0 || keyLen > (16 * 1024 * 1024)) {
                throw new IOException("Invalid key length");
            }
            byte[] keyBytes = in.readNBytes(keyLen);
            Serializable key = deserialize(keyBytes);
            int valueLen = in.readInt();
            Serializable value = null;
            if (valueLen > 0) {
                if (valueLen > (64 * 1024 * 1024)) {
                    throw new IOException("Invalid value length");
                }
                byte[] valueBytes = in.readNBytes(valueLen);
                value = deserialize(valueBytes);
            }
            @SuppressWarnings("unchecked")
            K k = (K) key;
            @SuppressWarnings("unchecked")
            V v = (V) value;
            switch (type) {
                case PUT -> data.put(k, v);
                case REMOVE -> data.remove(k);
            }
        }
    }

    private ByteBuffer encode(WALEntry entry) throws IOException {
        byte[] keyBytes = serialize(entry.key());
        byte[] valueBytes = entry.value() != null ? serialize(entry.value()) : new byte[0];

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(ENTRY_MAGIC);
            out.writeInt(ENTRY_VERSION);
            out.writeByte(entry.type().ordinal());
            out.writeLong(entry.timestamp());
            out.writeInt(keyBytes.length);
            out.write(keyBytes);
            out.writeInt(valueBytes.length);
            if (valueBytes.length > 0) {
                out.write(valueBytes);
            }
            out.flush();
        }

        byte[] body = bos.toByteArray();
        ByteBuffer framed = ByteBuffer.allocate(4 + body.length);
        framed.putInt(body.length);
        framed.put(body);
        framed.flip();
        return framed;
    }

    private static byte[] serialize(Serializable obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos))) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        }
    }

    private static Serializable deserialize(byte[] bytes) throws IOException {
        try (ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new ByteArrayInputStream(bytes)))) {
            return (Serializable) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize WAL field", e);
        }
    }

    private void openWalForAppend() throws IOException {
        Files.createDirectories(mapDir);
        walRaf = new RandomAccessFile(walPath.toFile(), "rw");
        walChannel = walRaf.getChannel();
        walChannel.position(walChannel.size());
    }

    private int readFully(FileChannel channel, ByteBuffer buffer, long offset) throws IOException {
        int total = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, offset + total);
            if (read <= 0) {
                break;
            }
            total += read;
        }
        return total;
    }

    private void closeWalQuietly() {
        FileChannel ch = walChannel;
        RandomAccessFile raf = walRaf;
        walChannel = null;
        walRaf = null;
        if (ch != null) {
            try {
                ch.close();
            } catch (IOException ignored) {
            }
        }
        if (raf != null) {
            try {
                raf.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void writeMeta(MapMetadata meta) {
        try {
            Files.deleteIfExists(metaPath);
            try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(metaPath)))) {
                out.writeInt(meta.version());
                out.writeLong(meta.lastSnapshotOffset());
                out.writeLong(meta.lastSnapshotTimestamp());
                out.flush();
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to write map meta", e);
        }
    }

    private java.util.Optional<MapMetadata> readMeta() {
        if (!Files.exists(metaPath)) {
            return java.util.Optional.empty();
        }
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(metaPath)))) {
            int version = in.readInt();
            long offset = in.readLong();
            long ts = in.readLong();
            return java.util.Optional.of(new MapMetadata(offset, ts, version));
        } catch (IOException e) {
            return java.util.Optional.empty();
        }
    }
}

