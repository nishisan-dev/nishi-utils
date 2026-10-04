package dev.nishisan.utils.oss.cluster.catalog;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** Replicated deletion reservation. COMMITTED is irreversible. */
public record SeriesDeletion(String operationId, long lastWriteBefore, List<String> participants,
                             boolean committed) implements Serializable {
    public SeriesDeletion {
        Objects.requireNonNull(operationId, "operationId");
        participants = List.copyOf(participants);
    }
    public SeriesDeletion commit() { return new SeriesDeletion(operationId, lastWriteBefore, participants, true); }
}
