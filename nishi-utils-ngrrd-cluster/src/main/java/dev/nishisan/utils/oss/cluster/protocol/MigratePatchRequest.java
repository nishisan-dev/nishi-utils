package dev.nishisan.utils.oss.cluster.protocol;

import java.util.Arrays;
import java.util.Objects;

/** Ordered replacement of a range in an already transferred image, before its final checksum/commit. */
public record MigratePatchRequest(String seriesKey, String migrationId, int sequence, int offset, byte[] data) {
    @Override public boolean equals(Object other) {
        return other instanceof MigratePatchRequest patch && sequence == patch.sequence && offset == patch.offset
                && Objects.equals(seriesKey, patch.seriesKey) && Objects.equals(migrationId, patch.migrationId)
                && Arrays.equals(data, patch.data);
    }
    @Override public int hashCode() {
        return 31 * Objects.hash(seriesKey, migrationId, sequence, offset) + Arrays.hashCode(data);
    }
}
