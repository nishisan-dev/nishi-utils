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

package dev.nishisan.utils.map;

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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Local disk persistence engine for {@link NMap}. Uses an append-only WAL and
 * periodic full snapshots.
 * <p>
 * This component is intentionally best-effort: write failures are logged but do
 * not fail map operations.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public final class NMapPersistence<K extends Serializable, V extends Serializable> implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(NMapPersistence.class.getName());

    private static final String WAL_FILE = "wal.log";
    private static final String SNAPSHOT_FILE = "snapshot.dat";
    private static final String META_FILE = "map.meta";

    // Entry framing + compatibility
    private static final int ENTRY_MAGIC = 0x4E4D5741; // "NMWA"
    private static final int ENTRY_VERSION = 1;

    private final NMapConfig config;
    private final Map<K, V> data;
    private final NMapHealthListener healthListener;
    private final AtomicLong failureCount = new AtomicLong();
    private final Path mapDir;
    private final Path walPath;
    private final Path snapshotPath;
    private final Path metaPath;
    private final Path tempSnapshotPath;
    private final Path oldWalPath;

    private final LinkedBlockingQueue<NMapWALEntry> queue = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final Object walLock = new Object();

    private volatile RandomAccessFile walRaf;
    private volatile FileChannel walChannel;
    private volatile Thread writerThread;

    private long opsSinceSnapshot;
    private long lastSnapshotTimeMillis;

    /**
     * Creates a new persistence instance.
     *
     * @param config  the persistence configuration
     * @param data    the in-memory map to persist
     * @param baseDir the base directory for persistence files
     * @param mapName the map name (used as subdirectory)
     */
    public NMapPersistence(NMapConfig config, Map<K, V> data, Path baseDir, String mapName) {
        this.config = Objects.requireNonNull(config, "config");
        this.data = Objects.requireNonNull(data, "data");
        this.healthListener = config.healthListener() != null
                ? config.healthListener()
                : (name, type, cause) -> {
                };
        Objects.requireNonNull(baseDir, "baseDir");
        Objects.requireNonNull(mapName, "mapName");
        this.mapDir = baseDir.resolve(mapName);
        this.walPath = mapDir.resolve(WAL_FILE);
        this.snapshotPath = mapDir.resolve(SNAPSHOT_FILE);
        this.metaPath = mapDir.resolve(META_FILE);
        this.tempSnapshotPath = mapDir.resolve(SNAPSHOT_FILE + ".tmp");
        this.oldWalPath = mapDir.resolve(WAL_FILE + ".old");
    }

    /**
     * Returns the number of persistence failures since this instance was created.
     *
     * @return the failure count
     */
    public long failureCount() {
        return failureCount.get();
    }

    /**
     * Loads state from disk (snapshot + WAL replay). No-op when persistence is
     * disabled.
     */
    public void load() {
        if (config.mode() == NMapPersistenceMode.DISABLED) {
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
            readMeta().ifPresent(meta -> lastSnapshotTimeMillis = meta.lastSnapshotTimestamp());
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to load map persistence state", e);
        }
    }

    /**
     * Starts the background writer thread. No-op when persistence is disabled.
     */
    public void start() {
        if (config.mode() == NMapPersistenceMode.DISABLED) {
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            Files.createDirectories(mapDir);
            openWalForAppend();
            if (lastSnapshotTimeMillis <= 0) {
                lastSnapshotTimeMillis = System.currentTimeMillis();
            }
        } catch (IOException e) {
            running.set(false);
            LOGGER.log(Level.WARNING, "Failed to start map persistence (WAL open)", e);
            failureCount.incrementAndGet();
            healthListener.onPersistenceFailure(mapDir.getFileName().toString(),
                    NMapHealthListener.PersistenceFailureType.WAL_OPEN, e);
            return;
        }

        writerThread = new Thread(this::runWriterLoop, "nmap-persistence");
        writerThread.setDaemon(true);
        writerThread.start();
    }

    /**
     * Appends a WAL entry asynchronously.
     *
     * @param type  the operation type
     * @param key   the key
     * @param value the value (may be null for REMOVE)
     */
    public void appendAsync(NMapOperationType type, Serializable key, Serializable value) {
        if (config.mode() == NMapPersistenceMode.DISABLED) {
            return;
        }
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(key, "key");
        queue.offer(new NMapWALEntry(System.currentTimeMillis(), type, key, value));
    }

    /**
     * Appends a WAL entry synchronously. Used for critical maps that must survive
     * hard crashes.
     *
     * @param type  the operation type
     * @param key   the key
     * @param value the value (may be null for REMOVE)
     */
    public void appendSync(NMapOperationType type, Serializable key, Serializable value) {
        if (config.mode() == NMapPersistenceMode.DISABLED) {
            return;
        }
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(key, "key");
        FileChannel ch = walChannel;
        if (ch == null) {
            return;
        }
        NMapWALEntry entry = new NMapWALEntry(System.currentTimeMillis(), type, key, value);
        synchronized (walLock) {
            try {
                ByteBuffer buffer = encode(entry);
                while (buffer.hasRemaining()) {
                    ch.write(buffer);
                }
                if (config.mode() == NMapPersistenceMode.ASYNC_WITH_FSYNC) {
                    ch.force(true);
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to append WAL entry synchronously", e);
                failureCount.incrementAndGet();
                healthListener.onPersistenceFailure(mapDir.getFileName().toString(),
                        NMapHealthListener.PersistenceFailureType.WAL_WRITE, e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (config.mode() == NMapPersistenceMode.DISABLED) {
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
     * Triggers a snapshot if the configured interval (by operations or time) has
     * been exceeded.
     */
    public void maybeSnapshot() {
        boolean byOps = config.snapshotIntervalOperations() > 0
                && opsSinceSnapshot >= config.snapshotIntervalOperations();
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
            failureCount.incrementAndGet();
            healthListener.onPersistenceFailure(mapDir.getFileName().toString(),
                    NMapHealthListener.PersistenceFailureType.SNAPSHOT_WRITE, e);
        }
    }

    // ── Writer Loop ──────────────────────────────────────────────────────

    private void runWriterLoop() {
        Duration batchTimeout = config.batchTimeout();
        long batchTimeoutMs = Math.max(1L, batchTimeout.toMillis());

        while (running.get() || !queue.isEmpty()) {
            try {
                NMapWALEntry first = queue.poll(batchTimeoutMs, TimeUnit.MILLISECONDS);
                if (first == null) {
                    maybeSnapshot();
                    continue;
                }
                List<NMapWALEntry> batch = new ArrayList<>(config.batchSize());
                batch.add(first);
                queue.drainTo(batch, Math.max(0, config.batchSize() - 1));

                writeBatch(batch);
                opsSinceSnapshot += batch.size();
                maybeSnapshot();
            } catch (InterruptedException e) {
                running.set(false);
                Thread.interrupted();
                continue;
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Unexpected error in persistence writer loop", e);
            }
        }

        // Final best-effort flush on shutdown
        try {
            List<NMapWALEntry> remaining = new ArrayList<>();
            queue.drainTo(remaining);
            if (!remaining.isEmpty()) {
                writeBatch(remaining);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to flush remaining WAL entries on shutdown", e);
        }
    }

    private void writeBatch(List<NMapWALEntry> batch) {
        if (batch.isEmpty()) {
            return;
        }
        FileChannel ch = walChannel;
        if (ch == null) {
            return;
        }
        synchronized (walLock) {
            try {
                for (NMapWALEntry entry : batch) {
                    ByteBuffer buffer = encode(entry);
                    while (buffer.hasRemaining()) {
                        ch.write(buffer);
                    }
                }
                if (config.mode() == NMapPersistenceMode.ASYNC_WITH_FSYNC) {
                    ch.force(true);
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to append WAL batch", e);
                failureCount.incrementAndGet();
                healthListener.onPersistenceFailure(mapDir.getFileName().toString(),
                        NMapHealthListener.PersistenceFailureType.WAL_WRITE, e);
            }
        }
    }

    // ── Snapshot ─────────────────────────────────────────────────────────

    private void createSnapshotAndRotateWal() throws IOException {
        List<NMapWALEntry> preRotate = new ArrayList<>();
        queue.drainTo(preRotate, config.batchSize());
        if (!preRotate.isEmpty()) {
            writeBatch(preRotate);
        }

        closeWalQuietly();
        try {
            Files.deleteIfExists(oldWalPath);
            if (Files.exists(walPath)) {
                Files.move(walPath, oldWalPath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to rotate WAL before snapshot", e);
        }
        openWalForAppend();

        Map<K, V> snapshot;
        if (data instanceof ConcurrentMap<?, ?>) {
            snapshot = new HashMap<>(data);
        } else {
            synchronized (data) {
                snapshot = new HashMap<>(data);
            }
        }
        writeSnapshot(snapshot);

        try {
            Files.deleteIfExists(oldWalPath);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to delete rotated WAL after snapshot", e);
        }

        writeMeta(new NMapMetadata(0L, System.currentTimeMillis(), ENTRY_VERSION));
    }

    private void writeSnapshot(Map<K, V> snapshot) throws IOException {
        Files.deleteIfExists(tempSnapshotPath);
        try (BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(tempSnapshotPath));
                ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(snapshot);
            oos.flush();
        }
        try {
            Files.move(tempSnapshotPath, snapshotPath, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
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

    // ── WAL Replay ──────────────────────────────────────────────────────

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

    @SuppressWarnings("unchecked")
    private void applyDecoded(byte[] payload) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
            int magic = in.readInt();
            int version = in.readInt();
            if (magic != ENTRY_MAGIC || version != ENTRY_VERSION) {
                throw new IOException("Unsupported WAL entry format");
            }
            int typeOrdinal = in.readUnsignedByte();
            NMapOperationType[] values = NMapOperationType.values();
            if (typeOrdinal < 0 || typeOrdinal >= values.length) {
                throw new IOException("Invalid WAL entry type");
            }
            NMapOperationType type = values[typeOrdinal];
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
            K k = (K) key;
            V v = (V) value;
            switch (type) {
                case PUT -> data.put(k, v);
                case REMOVE -> data.remove(k);
            }
        }
    }

    // ── Encoding / Decoding ─────────────────────────────────────────────

    private ByteBuffer encode(NMapWALEntry entry) throws IOException {
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
        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(new ByteArrayInputStream(bytes)))) {
            return (Serializable) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize WAL field", e);
        }
    }

    // ── WAL I/O ─────────────────────────────────────────────────────────

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

    // ── Metadata ────────────────────────────────────────────────────────

    private void writeMeta(NMapMetadata meta) {
        try {
            Files.deleteIfExists(metaPath);
            try (DataOutputStream out = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(metaPath)))) {
                out.writeInt(meta.version());
                out.writeLong(meta.lastSnapshotOffset());
                out.writeLong(meta.lastSnapshotTimestamp());
                out.flush();
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to write map meta", e);
        }
    }

    private java.util.Optional<NMapMetadata> readMeta() {
        if (!Files.exists(metaPath)) {
            return java.util.Optional.empty();
        }
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(Files.newInputStream(metaPath)))) {
            int version = in.readInt();
            long offset = in.readLong();
            long ts = in.readLong();
            return java.util.Optional.of(new NMapMetadata(offset, ts, version));
        } catch (IOException e) {
            return java.util.Optional.empty();
        }
    }
}
