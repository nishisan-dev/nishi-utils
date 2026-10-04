package dev.nishisan.utils.oss.cluster.api;

/** Delete only if every known write precedes this epoch-millisecond cutoff. */
public record DeletePrecondition(long lastWriteBefore) {
    public DeletePrecondition {
        if (lastWriteBefore < 0) throw new IllegalArgumentException("lastWriteBefore must be non-negative");
    }
}
