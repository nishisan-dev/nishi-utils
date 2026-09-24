package dev.nishisan.utils.oss.cluster.catalog;

import dev.nishisan.utils.oss.format.SeriesFileCodec;
import dev.nishisan.utils.oss.format.SeriesGeometry;
import dev.nishisan.utils.oss.storage.blob.BlobStorage;

import java.io.Serializable;
import java.util.HexFormat;
import java.util.List;

/** Immutable, validated physical geometry shared by every series with the same layout. */
public record GeometryDescriptor(String id, int formatVersion, int baseStepSec,
        List<SeriesGeometry.Column> columns, List<SeriesGeometry.ArchiveDef> archives,
        long objectBytes, long regionBytes) implements Serializable {
    private static final long serialVersionUID = 1L;

    public GeometryDescriptor {
        columns = List.copyOf(columns);
        archives = List.copyOf(archives);
        if (formatVersion != SeriesFileCodec.CURRENT_VERSION || columns.isEmpty() || archives.isEmpty()) {
            throw new IllegalArgumentException("unsupported or empty geometry");
        }
        for (var archive : archives) {
            if (archive.rows() <= 0 || archive.stepSec() <= 0 || baseStepSec <= 0
                    || archive.stepSec() % baseStepSec != 0 || !Double.isFinite(archive.xff())
                    || archive.xff() < 0 || archive.xff() > 1) {
                throw new IllegalArgumentException("invalid archive geometry");
            }
        }
        var geometry = SeriesGeometry.fromComponents(baseStepSec, columns, archives);
        if (!identifier(formatVersion, geometry).equals(id) || objectBytes != geometry.fileTotalBytes()
                || objectBytes > Integer.MAX_VALUE || regionBytes != BlobStorage.alignedRegionBytes(objectBytes)) {
            throw new IllegalArgumentException("geometry identity or size mismatch");
        }
    }

    /** Derives the descriptor using the OSS layout calculator. */
    public static GeometryDescriptor from(SeriesGeometry geometry) {
        var archives = geometry.archives().stream().map(a -> new SeriesGeometry.ArchiveDef(
                a.rraName(), a.cf(), a.stepSec(), a.rows(), a.xff())).toList();
        return new GeometryDescriptor(identifier(SeriesFileCodec.CURRENT_VERSION, geometry),
                SeriesFileCodec.CURRENT_VERSION, geometry.baseStepSec(), geometry.columns(), archives,
                geometry.fileTotalBytes(), BlobStorage.alignedRegionBytes(geometry.fileTotalBytes()));
    }

    /** Reconstructs geometry from a persisted static section, without reading rings. */
    public static GeometryDescriptor fromStaticSection(byte[] section) {
        var header = SeriesFileCodec.decodeFixedHeader(section);
        var descriptor = from(SeriesGeometry.fromPersisted(header, section));
        if (header.formatVersion() != descriptor.formatVersion() || header.fileTotalBytes() != descriptor.objectBytes()) {
            throw new IllegalArgumentException("persisted geometry size mismatch");
        }
        return descriptor;
    }

    private static String identifier(int version, SeriesGeometry geometry) {
        return version + ":" + HexFormat.of().formatHex(geometry.geometryHash());
    }
}
