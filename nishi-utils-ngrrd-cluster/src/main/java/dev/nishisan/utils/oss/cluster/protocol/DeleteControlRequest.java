package dev.nishisan.utils.oss.cluster.protocol;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
/** Internal phase request fenced by both generation and deletion operation. */
public record DeleteControlRequest(String seriesKey, SeriesPlacement placement) { }
