package dev.nishisan.utils.oss.cluster.protocol;

import dev.nishisan.utils.oss.cluster.api.DeletePrecondition;
import java.util.Objects;

/** Conditional deletion; the operation id remains stable across transport retries. */
public record DeleteRequest(String seriesKey, DeletePrecondition precondition, String operationId, String generationId) {
    public DeleteRequest(String seriesKey, DeletePrecondition precondition, String operationId) {
        this(seriesKey, precondition, operationId, null);
    }
    public DeleteRequest {
        if (seriesKey == null || seriesKey.isBlank()) throw new IllegalArgumentException("seriesKey is required");
        Objects.requireNonNull(precondition, "precondition");
        Objects.requireNonNull(operationId, "operationId");
    }
}
