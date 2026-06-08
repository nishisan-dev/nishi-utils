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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Leader-side resend op-log for a single topic (#127): a purpose-built, segmented, append-only log
 * indexed by sequence. It backs the resend of committed operations to a catching-up follower without
 * keeping the (potentially large) payloads on the JVM heap.
 *
 * <p>Why a bespoke store instead of {@code NQueue}/{@code NMap}: the access pattern is exactly
 * <ul>
 *   <li><b>append</b> of committed operations, and</li>
 *   <li><b>random read</b> by sequence — {@link #get(long)} and {@link #read(long, long)} — which
 *       {@code NQueue} (FIFO, no random access) cannot serve and {@code NMap} would serve only with
 *       hand-rolled compound (count + time) eviction.</li>
 * </ul>
 * A segmented log gives random read via a compact primitive index and <b>auto-compaction by
 * whole-segment drop</b> (no rewrite): the oldest segment is deleted when the total count exceeds the
 * cap or when the segment's newest record is older than the temporal retention window.
 *
 * <p><b>Arrival order:</b> committed operations are indexed in commit order, which is NOT necessarily
 * sequence order (quorum can complete operation N+1 before N). The log therefore records data in the
 * file in arrival order but keeps each segment's in-memory {@code (sequence → offset)} index <b>sorted
 * by sequence</b> (binary insertion), and lookups scan the few segments whose {@code [minSeq, maxSeq]}
 * overlaps the query. With near-sorted arrival the per-segment insertions land at/near the tail, so the
 * cost stays close to a plain append.
 *
 * <p>On-disk record layout (big-endian), within a {@code seg-NNNNNNNNNN.dat} file (NNNN… = creation
 * order):
 * <pre>
 *   recordLen(4) · timestamp(8) · frame(recordLen-8)
 * </pre>
 * where {@code frame} is a {@link RelayEntryCodec}-encoded {@link RelayEntry} (the same compact frame
 * the follower relay uses — reused, not reinvented). The per-record timestamp drives temporal
 * retention and survives a restart (the in-memory index is rebuilt by scanning on {@link #open}).
 *
 * <p>Durability mirrors the relay: {@code fsync} is off by default — the small not-yet-flushed tail
 * is recovered by the follower falling into the existing snapshot path, never silent divergence.
 *
 * <p>Thread-safety: all public methods are synchronized on the instance.
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

    /** Segments in creation (arrival) order; head is the oldest, tail is the active one. */
    private final List<Segment> segments = new ArrayList<>();
    private Segment active;
    private long totalEntries;
    private long nextSegmentId;
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
                files.sort((a, b) -> Long.compare(segmentIdOf(a), segmentIdOf(b)));
                for (Path file : files) {
                    long id = segmentIdOf(file);
                    Segment segment = Segment.recover(file, fsync);
                    if (segment.count == 0) {
                        segment.close();
                        Files.deleteIfExists(file);
                        continue;
                    }
                    segments.add(segment);
                    totalEntries += segment.count;
                    nextSegmentId = Math.max(nextSegmentId, id + 1);
                }
            }
            active = segments.isEmpty() ? null : segments.get(segments.size() - 1);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open resend log at " + dir, e);
        }
    }

    private static long segmentIdOf(Path file) {
        String name = file.getFileName().toString();
        String digits = name.substring(SEGMENT_PREFIX.length(), name.length() - SEGMENT_SUFFIX.length());
        try {
            return Long.parseLong(digits);
        } catch (NumberFormatException e) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Appends a committed operation. Sequences may arrive in any order (commit order, not necessarily
     * sequence order); the in-memory index keeps them sorted.
     *
     * @param sequence  the per-topic sequence number
     * @param timestamp the leader-local commit timestamp (epoch millis) driving temporal retention
     * @param frame     the {@link RelayEntryCodec}-encoded entry
     */
    synchronized boolean append(long sequence, long timestamp, byte[] frame) {
        if (closed) {
            return false;
        }
        try {
            if (active == null || active.count >= segmentMaxEntries || active.shouldRollByAge(segmentMaxAgeMillis)) {
                rollSegment();
            }
            active.append(sequence, timestamp, frame);
            totalEntries++;
            enforceRetention();
            return true;
        } catch (IOException e) {
            // The caller decides what an append failure means: best-effort for the push resend op-log
            // (only degrades a future resend into snapshot fallback), but load-bearing for RELAY_STREAM
            // (the binlog is the only delivery path — see ReplicationManager.appendStreamOpLog).
            LOGGER.log(Level.WARNING, "ResendLog append failed for sequence " + sequence + " at " + dir, e);
            return false;
        }
    }

    private void rollSegment() throws IOException {
        long id = nextSegmentId++;
        Path file = dir.resolve(SEGMENT_PREFIX + String.format("%010d", id) + SEGMENT_SUFFIX);
        Segment segment = Segment.create(file, fsync);
        segments.add(segment);
        active = segment;
    }

    /** Drops the whole oldest segment when it exceeds the count cap or falls outside the time window. */
    private void enforceRetention() {
        long now = System.currentTimeMillis();
        // Never drop the active (tail) segment: it is still receiving appends.
        while (segments.size() > 1) {
            Segment oldest = segments.get(0);
            boolean overCount = totalEntries > maxEntries;
            boolean expired = retentionMillis > 0 && (now - oldest.maxTimestamp) > retentionMillis;
            if (!overCount && !expired) {
                break;
            }
            segments.remove(0);
            totalEntries -= oldest.count;
            oldest.delete();
        }
    }

    /** Returns the entry for {@code sequence}, or {@code null} if absent (never indexed or evicted). */
    synchronized RelayEntry get(long sequence) {
        if (closed) {
            return null;
        }
        for (Segment segment : segments) {
            if (segment.count == 0 || sequence < segment.minSequence || sequence > segment.maxSequence) {
                continue;
            }
            byte[] frame = segment.readFrame(sequence);
            if (frame != null) {
                return RelayEntryCodec.decode(frame);
            }
        }
        return null;
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
        for (Segment segment : segments) {
            if (segment.count == 0 || segment.maxSequence < fromSequence || segment.minSequence > toSequence) {
                continue;
            }
            segment.readRange(fromSequence, toSequence, out);
        }
        // Segments are arrival-ordered and may overlap slightly; sort the (small) result by sequence.
        out.sort((a, b) -> Long.compare(a.sequence(), b.sequence()));
        return out;
    }

    /** Total number of entries currently retained across all segments. */
    synchronized long size() {
        return totalEntries;
    }

    /** Lowest sequence still retained, or {@code -1} when empty. */
    synchronized long oldestSequence() {
        long min = Long.MAX_VALUE;
        for (Segment segment : segments) {
            if (segment.count > 0) {
                min = Math.min(min, segment.minSequence);
            }
        }
        return min == Long.MAX_VALUE ? -1L : min;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (Segment segment : segments) {
            segment.close();
        }
        segments.clear();
        active = null;
    }

    /** One append-only segment file plus its compact, sequence-sorted in-memory index. */
    private static final class Segment {

        private final Path file;
        private final FileChannel channel;
        private final boolean fsync;
        /** Sorted ascending by sequence (binary insertion on append). */
        private long[] sequences;
        /** Byte offset of each record's length prefix, parallel to {@link #sequences}. */
        private long[] offsets;
        private int count;
        private long writePosition;
        private long minSequence = Long.MAX_VALUE;
        private long maxSequence = Long.MIN_VALUE;
        private long maxTimestamp;
        private final long firstAppendMillis;

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

        /** Inserts (sequence, offset) keeping {@link #sequences} sorted ascending. */
        private void indexRecord(long sequence, long timestamp, long offset) {
            if (count == sequences.length) {
                sequences = Arrays.copyOf(sequences, sequences.length * 2);
                offsets = Arrays.copyOf(offsets, offsets.length * 2);
            }
            int pos = Arrays.binarySearch(sequences, 0, count, sequence);
            int insertAt = pos >= 0 ? pos : -(pos + 1);
            if (insertAt < count) {
                System.arraycopy(sequences, insertAt, sequences, insertAt + 1, count - insertAt);
                System.arraycopy(offsets, insertAt, offsets, insertAt + 1, count - insertAt);
            }
            sequences[insertAt] = sequence;
            offsets[insertAt] = offset;
            count++;
            if (sequence < minSequence) {
                minSequence = sequence;
            }
            if (sequence > maxSequence) {
                maxSequence = sequence;
            }
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
