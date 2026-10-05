package dev.nishisan.utils.oss.cluster.node;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

/** Durable local fences and hourly receipt upper bounds. Mutations are grouped before one fsync. */
public final class SeriesLifecycleJournal implements AutoCloseable {
    public enum Phase { ACTIVE, PREPARED, COMMITTED, DELETED, FINISHED, QUARANTINED }
    public record Entry(String generationId, long receivedThrough, Phase phase, SeriesPlacement placement) { }
    private static final int MAX_RECORD = 16 * 1024 * 1024;
    private static final long COMPACT_BYTES = 16L * 1024 * 1024;
    private final ObjectMapper mapper = new ObjectMapper();
    // Reads on the write path (gate) never wait behind an fsync; mutations publish only after durability.
    private final Map<String, Entry> entries = new ConcurrentHashMap<>();
    // Disk I/O happens under this lock; a monitor would pin virtual-thread carriers on JDK 21.
    private final ReentrantLock lock = new ReentrantLock();
    private final Path path;
    private final long minCompactBytes;
    private FileChannel channel;
    private volatile long forces;
    private volatile long compactions;
    private long compactThreshold;
    private boolean failed;
    public record Update(String key, Entry entry) { }

    public SeriesLifecycleJournal(Path directory) throws IOException {
        this(directory, COMPACT_BYTES);
    }

    /**
     * Compaction is amortized: it runs only when the file doubles relative to the last compacted
     * size (never below {@code minCompactBytes}). A fixed threshold below the live size rewrote the
     * whole journal on every mutation once a node held tens of thousands of series.
     */
    SeriesLifecycleJournal(Path directory, long minCompactBytes) throws IOException {
        if (minCompactBytes <= 0) throw new IllegalArgumentException("minCompactBytes must be positive");
        this.minCompactBytes = minCompactBytes;
        Files.createDirectories(directory);
        path = directory.resolve("series-lifecycle.wal");
        // Snapshot replacement is atomic; an uninstalled temp file is not part of recovery.
        channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            replay();
            compactThreshold = Math.max(minCompactBytes, 2 * channel.size());
            channel.force(true);
            try (FileChannel parent = FileChannel.open(directory, StandardOpenOption.READ)) { parent.force(true); }
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
    }

    private void replay() throws IOException {
        long valid = 0;
        while (valid < channel.size()) {
            ByteBuffer head = ByteBuffer.allocate(8);
            if (!readFully(head)) break;
            head.flip();
            int size = head.getInt(), expected = head.getInt();
            if (size <= 0 || size > MAX_RECORD) throw new IOException("invalid lifecycle record length at " + valid);
            ByteBuffer payload = ByteBuffer.allocate(size);
            if (!readFully(payload)) break; // hard crash while appending a tail
            CRC32 crc = new CRC32(); crc.update(payload.array());
            if ((int) crc.getValue() != expected) throw new IOException("corrupt lifecycle journal at " + valid);
            Update update = mapper.readValue(payload.array(), Update.class);
            entries.put(update.key(), update.entry());
            valid = channel.position();
        }
        channel.truncate(valid);
        channel.position(valid);
    }

    private boolean readFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) if (channel.read(buffer) < 0) return false;
        return true;
    }

    public Entry get(String key) { return entries.get(key); }
    public Map<String, Entry> snapshot() { return Map.copyOf(entries); }
    public long fsyncCount() { return forces; }
    long compactionCount() { return compactions; }

    public void put(String key, Entry entry) { putAll(Map.of(key, entry)); }

    /** No-op updates require no disk I/O. After an I/O error, fail closed until restart. */
    public void putAll(Map<String, Entry> updates) {
        lock.lock();
        try {
            if (failed) throw new IllegalStateException("lifecycle journal unavailable; restart required");
            Map<String, Entry> changed = new LinkedHashMap<>();
            updates.forEach((key, entry) -> { if (!entry.equals(entries.get(key))) changed.put(key, entry); });
            if (changed.isEmpty()) return;
            try {
                for (var update : changed.entrySet()) append(channel, new Update(update.getKey(), update.getValue()));
                channel.force(true); forces++;
                entries.putAll(changed);
                if (channel.size() >= compactThreshold) compact();
            } catch (IOException e) {
                failed = true;
                throw new UncheckedIOException("cannot persist series lifecycle", e);
            }
        } finally {
            lock.unlock();
        }
    }

    private void append(FileChannel destination, Update update) throws IOException {
        byte[] payload = mapper.writeValueAsBytes(update);
        if (payload.length > MAX_RECORD) throw new IOException("lifecycle record too large");
        CRC32 crc = new CRC32(); crc.update(payload);
        ByteBuffer buffer = ByteBuffer.allocate(8 + payload.length).putInt(payload.length)
                .putInt((int) crc.getValue()).put(payload);
        buffer.flip();
        while (buffer.hasRemaining()) destination.write(buffer);
    }

    private void compact() throws IOException {
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        try (FileChannel output = FileChannel.open(tmp, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            for (var entry : entries.entrySet()) append(output, new Update(entry.getKey(), entry.getValue()));
            output.force(true);
        }
        Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        try (FileChannel directory = FileChannel.open(path.getParent(), StandardOpenOption.READ)) {
            directory.force(true);
        }
        channel.close();
        channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        channel.position(channel.size());
        compactThreshold = Math.max(minCompactBytes, 2 * channel.size());
        compactions++;
    }

    /** Ceiling, with saturation instead of overflow for malformed/future timestamps. */
    public static long upperBound(long timestamp, long interval) {
        if (interval <= 0 || timestamp < 0) throw new IllegalArgumentException("invalid receipt interval/timestamp");
        long remainder = timestamp % interval;
        if (remainder == 0) return timestamp;
        long increment = interval - remainder;
        return timestamp > Long.MAX_VALUE - increment ? Long.MAX_VALUE : timestamp + increment;
    }

    @Override public void close() throws IOException {
        lock.lock();
        try {
            channel.close();
        } finally {
            lock.unlock();
        }
    }
}
