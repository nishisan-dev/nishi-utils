package dev.nishisan.utils.oss.cluster.protocol;

/** Reserves the actual frozen source image at the destination before any chunks are sent. */
public record MigratePrepareRequest(String seriesKey, String migrationId, String storageKey, long totalBytes) { }
