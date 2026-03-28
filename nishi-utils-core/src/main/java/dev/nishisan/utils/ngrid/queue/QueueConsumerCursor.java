package dev.nishisan.utils.ngrid.queue;

import dev.nishisan.utils.ngrid.common.NodeId;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * Logical consumer identity for stream-style queue reads.
 *
 * <p>Public callers should prefer {@code DistributedQueue.openConsumer(...)} so
 * consumption stays explicit and stable across nodes. The legacy node-bound
 * cursor is retained only for queue-style helper paths and internal bridging.
 */
public final class QueueConsumerCursor {
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();

    private final String groupId;
    private final String consumerId;
    private final String offsetStoreKey;
    private final boolean legacy;

    private QueueConsumerCursor(String groupId, String consumerId, String offsetStoreKey, boolean legacy) {
        this.groupId = groupId;
        this.consumerId = consumerId;
        this.offsetStoreKey = offsetStoreKey;
        this.legacy = legacy;
    }

    /**
     * Creates a logical consumer cursor.
     *
     * @param groupId logical consumer group
     * @param consumerId consumer identity inside the group
     * @return the cursor
     */
    public static QueueConsumerCursor of(String groupId, String consumerId) {
        String safeGroupId = requireNonBlank(groupId, "groupId");
        String safeConsumerId = requireNonBlank(consumerId, "consumerId");
        String encoded = "cg:" + encode(safeGroupId) + ":" + encode(safeConsumerId);
        return new QueueConsumerCursor(safeGroupId, safeConsumerId, encoded, false);
    }

    /**
     * Creates the legacy node-bound cursor used by queue-style helper paths.
     *
     * @param nodeId the node identifier
     * @return the cursor
     */
    public static QueueConsumerCursor legacy(NodeId nodeId) {
        Objects.requireNonNull(nodeId, "nodeId");
        return new QueueConsumerCursor("legacy", nodeId.value(), nodeId.value(), true);
    }

    /**
     * Rehydrates a cursor from an already-encoded offset-store key.
     *
     * @param consumerKey the persisted offset-store key
     * @return the cursor
     */
    public static QueueConsumerCursor fromOffsetStoreKey(String consumerKey) {
        String safeKey = requireNonBlank(consumerKey, "consumerKey");
        return new QueueConsumerCursor("opaque", safeKey, safeKey, false);
    }

    public String groupId() {
        return groupId;
    }

    public String consumerId() {
        return consumerId;
    }

    /**
     * Returns the opaque key used by offset stores.
     *
     * @return the offset-store key
     */
    public String offsetStoreKey() {
        return offsetStoreKey;
    }

    public boolean isLegacy() {
        return legacy;
    }

    private static String requireNonBlank(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " cannot be blank");
        }
        return value;
    }

    private static String encode(String raw) {
        return ENCODER.encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }
}
