package dev.nishisan.utils.oss.format;

import dev.nishisan.utils.oss.storage.SeriesChannel;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/** Reads last_up without a YAML definition, writer, checkpoint or full series image. */
public final class SeriesMetadataReader {
    private SeriesMetadataReader() { }

    /** Returns zero for a valid, empty series; malformed metadata is never interpreted as old data. */
    public static long lastUpdate(SeriesChannel channel) {
        SeriesHeader header = SeriesFileCodec.decodeFixedHeader(
                channel.readRegion(0, SeriesFileCodec.FIXED_HEADER_BYTES));
        long size = header.liveStateBytes();
        if (header.columnCount() <= 0 || header.archiveCount() <= 0 || header.baseStepSec() <= 0
                || header.fileTotalBytes() > channel.size() || header.fileTotalBytes() < SeriesFileCodec.FIXED_HEADER_BYTES || header.staticSectionBytes() < SeriesFileCodec.FIXED_HEADER_BYTES
                || header.staticSectionBytes() > header.liveStateOffset()
                || header.ringDataOffset() < header.liveStateOffset() || header.ringDataOffset() > header.fileTotalBytes()
                || size != SeriesGeometry.liveStateBytes(header.columnCount(), header.archiveCount())
                || size < 12 || size > Integer.MAX_VALUE || header.liveStateOffset() < SeriesFileCodec.FIXED_HEADER_BYTES
                || header.liveStateOffset() > header.fileTotalBytes() - size
                || header.ringDataOffset() - header.liveStateOffset() < size) {
            throw new NgrrdFormatException("invalid live-state bounds");
        }
        byte[] bytes = channel.readRegion(header.liveStateOffset(), (int) size);
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, bytes.length - Integer.BYTES);
        if ((int) crc.getValue() != ByteBuffer.wrap(bytes).getInt(bytes.length - Integer.BYTES)) {
            throw new NgrrdFormatException("invalid live-state CRC");
        }
        long lastUpdate = ByteBuffer.wrap(bytes).getLong();
        if (lastUpdate < 0) throw new NgrrdFormatException("negative last_up");
        return lastUpdate;
    }
}
