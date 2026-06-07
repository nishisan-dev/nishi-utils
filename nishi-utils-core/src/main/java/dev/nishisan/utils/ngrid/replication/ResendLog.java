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

package dev.nishisan.utils.ngrid.replication;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Leader-side resend op-log for a single topic (#127): a purpose-built, segmented,
 * append-only log indexed by sequence. It backs the resend of committed operations to a
 * catching-up follower without keeping the (potentially large) payloads on the JVM heap.
 *
 * <p>Why a bespoke store instead of {@code NQueue}/{@code NMap}: the access pattern is exactly
 * <ul>
 *   <li><b>append</b> by ascending sequence (commit order; sequences are monotonic but may have
 *       gaps when an operation fails before commit), and</li>
 *   <li><b>random read</b> by sequence — {@link #get(long)} and {@link #read(long, long)} — which
 *       {@code NQueue} (FIFO, no random access) cannot serve and {@code NMap} would serve only with
 *       hand-rolled compound (count + time) eviction.</li>
 * </ul>
 * A segmented log gives O(log n) random read via a compact primitive index and <b>auto-compaction
 * by whole-segment drop</b> (no rewrite): old segments are deleted when the total count exceeds the
 * cap or when the segment's newest record is older than the temporal retention window.
 *
 * <p>On-disk record layout (big-endian), within a {@code seg-<baseSequence>.dat} file:
 * <pre>
 *   recordLen(4) · timestamp(8) · frame(recordLen-8)
 * </pre>
 * where {@code frame} is a {@link RelayEntryCodec}-encoded {@link RelayEntry} (the same compact
 * frame the follower relay uses — reused, not reinvented). The per-record timestamp drives temporal
 * retention and survives a restart (the in-memory index is rebuilt by scanning on {@link #open}).
 *
 * <p>Durability mirrors the relay: {@code fsync} is off by default — the small not-yet-flushed tail
 * is recovered by the follower falling into the existing snapshot path, never silent divergence.
 *
 * <p>Thread-safety: all public methods are synchronized on the instance. Appends run on the
 * commit path (sub-millisecond: a buffered positional write plus two primitive-array appends) and
 * reads run on the rare resend path, so a single monitor is both correct and cheap.
 */
final class ResendLog implements Closeable {

    private static final Logger LOGGER = Logger.getLogger(ResendLog.class.getName());

    private static final String SEGMENT_PREFIX = "seg-";
    private static final String SEGMENT_SUFFIX = ".dat";
    /** recordLen(4). The length prefix covers timestamp(8) + frame. */
    private static final int LEN_PREFIX = 4;
    private static final int TIMESTAMP_BYTES = 8;

    private final Path dir;
    private final int segmentMaxEntries;
    private final long segmentMaxAgeMillis;
    private final long retentionMillis;
    private final long maxEntries;
    private final boolean fsync;

    /** Sealed + active segments keyed by their base (lowest) sequence. */
    private final NavigableMap<Long, Segment> segments = new TreeMap<>();
    private Segment active;
    private long totalEntries;
    private boolean closed;

    ResendLog(Path dir, int segmentMaxEntries, Duration segmentMaxAge, Duration retentionTime,
            long maxEntries, boolean fsync) {
        this.dir = dir;
        this.segmentMaxEntries = Math.max(1, segmentMaxEntries);
        this.segmentMaxAgeMillis = segmentMaxAge == null ? 0L : Math.max(0L, segmentMaxAge.toMillis());
        this.retentionMillis = retentionTime == null ? 0L : Math.max(0L, retentionTime.toMillis());
        this.maxEntries = Math.max(1L, maxEntries);
        this.fsync = fsync;
        open();
    }

    /** Scans existing segments on disk, rebuilding the in-memory index and truncating any torn tail. */
    private synchronized void open() {
        try {
            Files.createDirectories(dir);
            try (var stream = Files.newDirectoryStream(dir, SEGMENT_PREFIX + "*" + SEGMENT_SUFFIX)) {
                List<Path> files = new ArrayList<>();
                stream.forEach(files::add);
                files.sort((a, b) -> Long.compare(baseSequenceOf(a), baseSequenceOf(b)));
                for (Path file : files) {
                    Segment segment = Segment.recover(file, fsync);
                    if (segment.count == 0) {
                        segment.close();
                        Files.deleteIfExists(file);
                        continue;
                    }
                    segments.put(segment.baseSequence(), segment);
                    totalEntries += segment.count;
                }
            }
            active = segments.isEmpty() ? null : segments.lastEntry().getValue();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open resend log at " + dir, e);
        }
    }

    private static long baseSequenceOf(Path file) {
        String name = file.getFileName().toString();
        String digits = name.substring(SEGMENT_PREFIX.length(), name.length() - SEGMENT_SUFFIX.length());
        try {
            return Long.parseLong(digits);
        } catch (NumberFormatException e) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Appends a committed operation. Sequences must arrive non-decreasing (commit order); gaps are
     * permitted (an aborted operation consumes a sequence it never commits).
     *
     * @param sequence  the per-topic sequence number
     * @param timestamp the leader-local commit timestamp (epoch millis) driving temporal retention
     * @param frame     the {@link RelayEntryCodec}-encoded entry
     */
    synchronized void append(long sequence, long timestamp, byte[] frame) {
        if (closed) {
            return;
        }
        try {
            if (active == null || active.count >= segmentMaxEntries || active.shouldRollByAge(segmentMaxAgeMillis)) {
                rollSegment(sequence);
            }
            active.append(sequence, timestamp, frame);
            totalEntries++;
            enforceRetention();
        } catch (IOException e) {
            // An op-log append failure must never fail the commit; it only degrades a future resend
            // into the existing snapshot-fallback path. Surface and continue.
            LOGGER.log(Level.WARNING, "ResendLog append failed for sequence " + sequence + " at " + dir, e);
        }
    }

    private void rollSegment(long baseSequence) throws IOException {
        Path file = dir.resolve(SEGMENT_PREFIX + baseSequence + SEGMENT_SUFFIX);
        Segment segment = Segment.create(file, fsync);
        segments.put(baseSequence, segment);
        active = segment;
    }

    /** Drops whole sealed segments that exceed the count cap or fall entirely outside the time window. */
    private void enforceRetention() {
        long now = System.currentTimeMillis();
        // Never drop the active (tail) segment: it is still receiving appends.
        while (segments.size() > 1) {
            Map.Entry<Long, Segment> oldestEntry = segments.firstEntry();
            Segment oldest = oldestEntry.getValue();
            if (oldest == active) {
                break;
            }
            boolean overCount = totalEntries > maxEntries;
            boolean expired = retentionMillis > 0 && (now - oldest.maxTimestamp) > retentionMillis;
            if (!overCount && !expired) {
                break;
            }
            segments.pollFirstEntry();
            totalEntries -= oldest.count;
            oldest.delete();
        }
    }

    /** Returns the entry for {@code sequence}, or {@code null} if absent (never indexed or evicted). */
    synchronized RelayEntry get(long sequence) {
        if (closed) {
            return null;
        }
        Map.Entry<Long, Segment> floor = segments.floorEntry(sequence);
        if (floor == null) {
            return null;
        }
        Segment segment = floor.getValue();
        byte[] frame = segment.readFrame(sequence);
        return frame == null ? null : RelayEntryCodec.decode(frame);
    }

    /**
     * Reads all present entries with sequence in {@code [fromSequence, toSequence]}, in ascending
     * order. Sequences absent from the log (never indexed, or evicted by retention) are simply
     * omitted; the caller derives the missing set from the gap between the request and the result.
     */
    synchronized List<RelayEntry> read(long fromSequence, long toSequence) {
        List<RelayEntry> out = new ArrayList<>();
        if (closed || toSequence < fromSequence) {
            return out;
        }
        Long startKey = segments.floorKey(fromSequence);
        NavigableMap<Long, Segment> tail = segments.tailMap(startKey == null ? fromSequence : startKey, true);
        for (Segment segment : tail.values()) {
            if (segment.baseSequence() > toSequence) {
                break;
            }
            segment.readRange(fromSequence, toSequence, out);
        }
        return out;
    }

    /** Total number of entries currently retained across all segments. */
    synchronized long size() {
        return totalEntries;
    }

    /** Lowest sequence still retained, or {@code -1} when empty. */
    synchronized long oldestSequence() {
        return segments.isEmpty() ? -1L : segments.firstEntry().getValue().baseSequence();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (Segment segment : segments.values()) {
            segment.close();
        }
        segments.clear();
        active = null;
    }

    /** One append-only segment file plus its compact, gap-tolerant in-memory index. */
    private static final class Segment {

        private final Path file;
        private final FileChannel channel;
        private final boolean fsync;
        /** Sorted ascending (append order); supports binary search to tolerate sequence gaps. */
        private long[] sequences;
        /** Byte offset of each record's length prefix, parallel to {@link #sequences}. */
        private long[] offsets;
        private int count;
        private long writePosition;
        private long maxTimestamp;
        private long firstAppendMillis;

        private Segment(Path file, FileChannel channel, boolean fsync) {
            this.file = file;
            this.channel = channel;
            this.fsync = fsync;
            this.sequences = new long[16];
            this.offsets = new long[16];
            this.firstAppendMillis = System.currentTimeMillis();
        }

        static Segment create(Path file, boolean fsync) throws IOException {
            FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ,
                    StandardOpenOption.WRITE);
            channel.truncate(0);
            return new Segment(file, channel, fsync);
        }

        static Segment recover(Path file, boolean fsync) throws IOException {
            FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
            Segment segment = new Segment(file, channel, fsync);
            long size = channel.size();
            long pos = 0;
            ByteBuffer lenBuf = ByteBuffer.allocate(LEN_PREFIX);
            while (pos + LEN_PREFIX <= size) {
                lenBuf.clear();
                if (!readFully(channel, lenBuf, pos)) {
                    break;
                }
                lenBuf.flip();
                int recordLen = lenBuf.getInt();
                if (recordLen < TIMESTAMP_BYTES || pos + LEN_PREFIX + recordLen > size) {
                    break; // torn tail
                }
                ByteBuffer record = ByteBuffer.allocate(recordLen);
                if (!readFully(channel, record, pos + LEN_PREFIX)) {
                    break;
                }
                record.flip();
                long timestamp = record.getLong();
                byte[] frame = new byte[recordLen - TIMESTAMP_BYTES];
                record.get(frame);
                RelayEntry entry = RelayEntryCodec.decode(frame);
                segment.indexRecord(entry.sequence(), timestamp, pos);
                pos += LEN_PREFIX + recordLen;
            }
            if (pos < size) {
                channel.truncate(pos); // drop the torn trailing record
            }
            segment.writePosition = pos;
            return segment;
        }

        long baseSequence() {
            return count == 0 ? Long.MAX_VALUE : sequences[0];
        }

        boolean shouldRollByAge(long maxAgeMillis) {
            return maxAgeMillis > 0 && (System.currentTimeMillis() - firstAppendMillis) > maxAgeMillis;
        }

        void append(long sequence, long timestamp, byte[] frame) throws IOException {
            int recordLen = TIMESTAMP_BYTES + frame.length;
            ByteBuffer buf = ByteBuffer.allocate(LEN_PREFIX + recordLen);
            buf.putInt(recordLen);
            buf.putLong(timestamp);
            buf.put(frame);
            buf.flip();
            long startOffset = writePosition;
            while (buf.hasRemaining()) {
                writePosition += channel.write(buf, writePosition);
            }
            if (fsync) {
                channel.force(false);
            }
            indexRecord(sequence, timestamp, startOffset);
        }

        private void indexRecord(long sequence, long timestamp, long offset) {
            if (count == sequences.length) {
                sequences = Arrays.copyOf(sequences, sequences.length * 2);
                offsets = Arrays.copyOf(offsets, offsets.length * 2);
            }
            sequences[count] = sequence;
            offsets[count] = offset;
            count++;
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
            }
        }

        byte[] readFrame(long sequence) {
            int idx = Arrays.binarySearch(sequences, 0, count, sequence);
            if (idx < 0) {
                return null;
            }
            return readFrameAt(idx);
        }

        void readRange(long fromSequence, long toSequence, List<RelayEntry> out) {
            int idx = Arrays.binarySearch(sequences, 0, count, fromSequence);
            if (idx < 0) {
                idx = -(idx + 1); // first index with sequence > fromSequence
            }
            for (int i = idx; i < count && sequences[i] <= toSequence; i++) {
                byte[] frame = readFrameAt(i);
                if (frame != null) {
                    out.add(RelayEntryCodec.decode(frame));
                }
            }
        }

        private byte[] readFrameAt(int index) {
            try {
                long offset = offsets[index];
                ByteBuffer lenBuf = ByteBuffer.allocate(LEN_PREFIX);
                if (!readFully(channel, lenBuf, offset)) {
                    return null;
                }
                lenBuf.flip();
                int recordLen = lenBuf.getInt();
                ByteBuffer record = ByteBuffer.allocate(recordLen);
                if (!readFully(channel, record, offset + LEN_PREFIX)) {
                    return null;
                }
                record.flip();
                record.position(TIMESTAMP_BYTES); // skip the stored timestamp
                byte[] frame = new byte[recordLen - TIMESTAMP_BYTES];
                record.get(frame);
                return frame;
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "ResendLog read failed for index " + index + " in " + file, e);
                return null;
            }
        }

        void close() {
            try {
                channel.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to close resend-log segment " + file, e);
            }
        }

        void delete() {
            close();
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to delete resend-log segment " + file, e);
            }
        }
    }

    /** Reads exactly {@code buf.remaining()} bytes at {@code position}; false if EOF reached first. */
    private static boolean readFully(FileChannel channel, ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int read = channel.read(buf, pos);
            if (read < 0) {
                return false;
            }
            pos += read;
        }
        return true;
    }
}
