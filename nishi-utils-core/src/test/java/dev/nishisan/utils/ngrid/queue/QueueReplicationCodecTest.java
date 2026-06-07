package dev.nishisan.utils.ngrid.queue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.Objects;

import org.junit.jupiter.api.Test;

import dev.nishisan.utils.queue.NQueueHeaders;

class QueueReplicationCodecTest {

    /** A consumer POJO whose concrete type must survive the relay round-trip. */
    public static class Order {
        public String id;
        public int qty;

        public Order() {
        }

        public Order(String id, int qty) {
            this.id = id;
            this.qty = qty;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Order other)) {
                return false;
            }
            return qty == other.qty && Objects.equals(id, other.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, qty);
        }
    }

    @Test
    void roundTripPreservesConcretePojoValueType() {
        Order order = new Order("A-1", 7);
        QueueReplicationCommand decoded = QueueReplicationCodec.decode(
                QueueReplicationCodec.encode(QueueReplicationCommand.offer(order)));

        assertEquals(QueueReplicationCommandType.OFFER, decoded.type());
        assertInstanceOf(Order.class, decoded.value(),
                "value must keep its concrete type across the relay round-trip, not become a LinkedHashMap");
        assertEquals(order, decoded.value());
    }

    @Test
    void roundTripPreservesKeyHeadersAndScalarValue() {
        byte[] key = "k-1".getBytes();
        QueueReplicationCommand decoded = QueueReplicationCodec.decode(QueueReplicationCodec.encode(
                QueueReplicationCommand.offer(key, NQueueHeaders.empty(), "payload-string")));

        assertEquals(QueueReplicationCommandType.OFFER, decoded.type());
        assertArrayEquals(key, decoded.key());
        assertEquals("payload-string", decoded.value());
    }

    @Test
    void roundTripPollCommandWithLongValue() {
        QueueReplicationCommand decoded = QueueReplicationCodec.decode(
                QueueReplicationCodec.encode(QueueReplicationCommand.poll(42L)));

        assertEquals(QueueReplicationCommandType.POLL, decoded.type());
        assertEquals(42L, decoded.value());
    }
}
