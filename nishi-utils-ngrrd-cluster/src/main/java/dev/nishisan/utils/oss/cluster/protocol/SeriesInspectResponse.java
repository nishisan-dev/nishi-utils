package dev.nishisan.utils.oss.cluster.protocol;
/** Inventory probe also fences placement while crash recovery is pending. */
public record SeriesInspectResponse(boolean exists, boolean quarantined, String generationId, boolean deleting) { }
