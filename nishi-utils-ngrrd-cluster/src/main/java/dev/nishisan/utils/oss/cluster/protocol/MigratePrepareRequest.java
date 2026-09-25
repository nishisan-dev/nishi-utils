package dev.nishisan.utils.oss.cluster.protocol;

/** Reserves the source image before transfer; liveCopy negotiates incremental patches. */
public record MigratePrepareRequest(String seriesKey, String migrationId, String storageKey, long totalBytes,
        boolean liveCopy) {
    /** Compatibility constructor for a source that freezes for the whole transfer. */
    public MigratePrepareRequest(String seriesKey, String migrationId, String storageKey, long totalBytes) {
        this(seriesKey, migrationId, storageKey, totalBytes, false);
    }
}
