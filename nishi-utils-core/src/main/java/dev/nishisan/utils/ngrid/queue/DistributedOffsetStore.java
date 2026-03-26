package dev.nishisan.utils.ngrid.queue;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.structures.DistributedMap;

import java.util.Objects;
import java.util.Optional;

/**
 * Offset store backed by a {@link DistributedMap}, ensuring offsets survive
 * restarts and are replicated across the cluster.
 */
public final class DistributedOffsetStore implements OffsetStore {
    private final DistributedMap<String, Long> offsets;
    private final String queueName;

    /**
     * Creates a distributed offset store.
     *
     * @param offsets   the distributed map used for storage
     * @param queueName the queue name prefix
     */
    public DistributedOffsetStore(DistributedMap<String, Long> offsets, String queueName) {
        this.offsets = Objects.requireNonNull(offsets, "offsets");
        this.queueName = Objects.requireNonNull(queueName, "queueName");
    }

    @Override
    public long getOffset(NodeId nodeId) {
        String key = keyFor(nodeId);
        // Jackson may deserialize small numbers as Integer instead of Long.
        // Using Optional<Long>.map() would trigger an implicit checkcast Long
        // in bytecode before entering the lambda, causing ClassCastException.
        // We work with the raw Optional to bypass the generic checkcast.
        Optional<?> raw = (Optional<?>) offsets.get(key);
        if (raw.isPresent()) {
            Object v = raw.get();
            if (v instanceof Number) {
                return ((Number) v).longValue();
            }
        }
        return 0L;
    }

    @Override
    public void updateOffset(NodeId nodeId, long offset) {
        String key = keyFor(nodeId);
        long current = 0L;
        Optional<?> raw = (Optional<?>) offsets.get(key);
        if (raw.isPresent()) {
            Object v = raw.get();
            if (v instanceof Number) {
                current = ((Number) v).longValue();
            }
        }
        if (offset <= current) {
            // Ignore regression - monotonic offset guarantee
            return;
        }
        offsets.put(key, offset);
    }

    /**
     * Resets all consumer offsets for this queue. Should be called when the queue
     * state is completely replaced (e.g., after a snapshot install) so that
     * stale offsets from the previous epoch do not cause duplicate deliveries.
     *
     * <p>
     * Uses {@link DistributedMap#removeByPrefix(String)} which scans the live
     * in-memory state of the {@code MapClusterService} — so it works correctly
     * even after a JVM restart when {@code knownKeys} would otherwise be empty.
     */
    public void reset() {
        offsets.removeByPrefix(queueName + ":");
    }

    private String keyFor(NodeId nodeId) {
        return queueName + ":" + nodeId.value();
    }
}
