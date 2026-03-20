package dev.nishisan.utils.ngrid.queue;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.structures.DistributedMap;

import java.util.Objects;

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
        return offsets.get(key).orElse(0L);
    }

    @Override
    public void updateOffset(NodeId nodeId, long offset) {
        String key = keyFor(nodeId);
        Long current = offsets.get(key).orElse(0L);
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
