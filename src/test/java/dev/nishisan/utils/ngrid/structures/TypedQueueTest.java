package dev.nishisan.utils.ngrid.structures;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TypedQueueTest {

    // Test classes
    static class Order implements Serializable {
        private final String orderId;

        public Order(String orderId) {
            this.orderId = orderId;
        }
    }

    static class Event implements Serializable {
        private final String eventType;

        public Event(String eventType) {
            this.eventType = eventType;
        }
    }

    @Test
    void shouldCreateTypedQueue() {
        TypedQueue<Order> orders = TypedQueue.of("orders", Order.class);

        assertEquals("orders", orders.name());
        assertEquals(Order.class, orders.type());
    }

    @Test
    void shouldRejectNullName() {
        assertThrows(NullPointerException.class, () -> TypedQueue.of(null, Order.class));
    }

    @Test
    void shouldRejectNullType() {
        assertThrows(NullPointerException.class, () -> TypedQueue.of("orders", null));
    }

    @Test
    void shouldRejectEmptyName() {
        assertThrows(IllegalArgumentException.class, () -> TypedQueue.of("", Order.class));
    }

    @Test
    void shouldRejectBlankName() {
        assertThrows(IllegalArgumentException.class, () -> TypedQueue.of("   ", Order.class));
    }

    @Test
    void shouldImplementEqualsCorrectly() {
        TypedQueue<Order> orders1 = TypedQueue.of("orders", Order.class);
        TypedQueue<Order> orders2 = TypedQueue.of("orders", Order.class);
        TypedQueue<Order> ordersDifferentName = TypedQueue.of("orders-v2", Order.class);
        TypedQueue<Event> events = TypedQueue.of("events", Event.class);

        // Same name and type should be equal
        assertEquals(orders1, orders2);
        assertEquals(orders2, orders1);

        // Different name should not be equal
        assertNotEquals(orders1, ordersDifferentName);

        // Different type should not be equal
        assertNotEquals(orders1, events);

        // Null should not be equal
        assertNotEquals(orders1, null);

        // Same instance should be equal
        assertEquals(orders1, orders1);
    }

    @Test
    void shouldImplementHashCodeCorrectly() {
        TypedQueue<Order> orders1 = TypedQueue.of("orders", Order.class);
        TypedQueue<Order> orders2 = TypedQueue.of("orders", Order.class);

        // Equal objects must have equal hash codes
        assertEquals(orders1.hashCode(), orders2.hashCode());
    }

    @Test
    void shouldWorkAsMapKey() {
        TypedQueue<Order> ordersKey = TypedQueue.of("orders", Order.class);
        TypedQueue<Event> eventsKey = TypedQueue.of("events", Event.class);

        Map<TypedQueue<?>, String> map = new HashMap<>();
        map.put(ordersKey, "Order Queue");
        map.put(eventsKey, "Event Queue");

        assertEquals("Order Queue", map.get(TypedQueue.of("orders", Order.class)));
        assertEquals("Event Queue", map.get(TypedQueue.of("events", Event.class)));
        assertNull(map.get(TypedQueue.of("logs", String.class)));
    }

    @Test
    void shouldWorkInSet() {
        TypedQueue<Order> orders = TypedQueue.of("orders", Order.class);
        TypedQueue<Event> events = TypedQueue.of("events", Event.class);

        Set<TypedQueue<?>> set = new HashSet<>();
        set.add(orders);
        set.add(events);
        set.add(TypedQueue.of("orders", Order.class)); // Duplicate

        assertEquals(2, set.size()); // Duplicate should not be added
        assertTrue(set.contains(orders));
        assertTrue(set.contains(events));
        assertTrue(set.contains(TypedQueue.of("orders", Order.class)));
    }

    @Test
    void shouldHaveReadableToString() {
        TypedQueue<Order> orders = TypedQueue.of("orders", Order.class);

        String toString = orders.toString();
        assertTrue(toString.contains("orders"));
        assertTrue(toString.contains("Order"));
    }

    @Test
    void shouldSupportStringType() {
        TypedQueue<String> messages = TypedQueue.of("messages", String.class);

        assertEquals("messages", messages.name());
        assertEquals(String.class, messages.type());
    }

    @Test
    void shouldSupportGenericSerializableType() {
        TypedQueue<Serializable> generic = TypedQueue.of("generic", Serializable.class);

        assertEquals("generic", generic.name());
        assertEquals(Serializable.class, generic.type());
    }
}
