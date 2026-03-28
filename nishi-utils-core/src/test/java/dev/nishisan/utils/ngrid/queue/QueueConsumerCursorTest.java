package dev.nishisan.utils.ngrid.queue;

import dev.nishisan.utils.ngrid.common.NodeId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueueConsumerCursorTest {

    @Test
    void logicalCursorShouldEncodeStableOffsetKey() {
        QueueConsumerCursor cursor = QueueConsumerCursor.of("orders-group", "consumer-1");

        assertEquals("orders-group", cursor.groupId());
        assertEquals("consumer-1", cursor.consumerId());
        assertTrue(cursor.offsetStoreKey().startsWith("cg:"));
    }

    @Test
    void logicalCursorShouldDifferentiateGroupsAndConsumers() {
        QueueConsumerCursor a = QueueConsumerCursor.of("group-a", "consumer-1");
        QueueConsumerCursor b = QueueConsumerCursor.of("group-a", "consumer-2");
        QueueConsumerCursor c = QueueConsumerCursor.of("group-b", "consumer-1");

        assertNotEquals(a.offsetStoreKey(), b.offsetStoreKey());
        assertNotEquals(a.offsetStoreKey(), c.offsetStoreKey());
    }

    @Test
    void legacyCursorShouldReuseNodeIdAsOffsetKey() {
        QueueConsumerCursor cursor = QueueConsumerCursor.legacy(NodeId.of("node-7"));

        assertTrue(cursor.isLegacy());
        assertEquals("node-7", cursor.offsetStoreKey());
        assertEquals("node-7", cursor.consumerId());
    }

    @Test
    void blankIdentifiersShouldBeRejected() {
        assertThrows(IllegalArgumentException.class, () -> QueueConsumerCursor.of(" ", "consumer"));
        assertThrows(IllegalArgumentException.class, () -> QueueConsumerCursor.of("group", " "));
        assertThrows(IllegalArgumentException.class, () -> QueueConsumerCursor.fromOffsetStoreKey(" "));
    }
}
