package dev.nishisan.utils.oss.cluster.protocol;

import dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor;

/** Owner publication of physical geometry; a null descriptor invalidates confirmation before OPEN. */
public record GeometryUpdateRequest(String seriesKey, String ownerNodeId, GeometryDescriptor geometry) { }
