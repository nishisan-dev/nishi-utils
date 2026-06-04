/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.ngrid.map;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the {@link MapReplicationCodec} ObjectMapper extension point (#110):
 * registering Jackson Modules / Mixins that compose with the default typing, with the
 * concrete use case of breaking self-referential graph cycles via {@code @JsonIdentityInfo}
 * without annotating the POJO globally.
 */
class MapReplicationCodecExtensionTest {

    @AfterEach
    void tearDown() {
        // Codec customizers are process-global; isolate tests.
        MapReplicationCodec.resetCustomizers();
    }

    /**
     * Self-referential DTO, mimicking the Cardinal {@code EventDto}
     * ({@code impacts}/{@code impactedBy} can form a reciprocal cycle).
     */
    public static class EventStub {
        public String identifier;
        public List<EventStub> impacts = new ArrayList<>();
        public List<EventStub> impactedBy = new ArrayList<>();

        public EventStub() {
        }

        public EventStub(String identifier) {
            this.identifier = identifier;
        }
    }

    /**
     * Mixin applying identity-based serialization only within the codec: dedups by
     * {@code identifier} and resolves repeated references by id, breaking the cycle —
     * without annotating {@link EventStub} globally.
     */
    @JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "identifier")
    abstract static class EventStubMixin {
    }

    /** Plain POJO with no annotations, to assert default typing still works (RF6). */
    public static class SimplePojo {
        public String name;
        public int count;

        public SimplePojo() {
        }

        public SimplePojo(String name, int count) {
            this.name = name;
            this.count = count;
        }
    }

    @Test
    void cyclicGraphRoundTripsWithIdentityMixin() {
        // Reciprocal cycle: A impacts B, B impactedBy A.
        EventStub a = new EventStub("A");
        EventStub b = new EventStub("B");
        a.impacts.add(b);
        b.impactedBy.add(a);

        // Mixin applied ONLY in the replication codec — POJO stays un-annotated globally.
        MapReplicationCodec.addMixIn(EventStub.class, EventStubMixin.class);

        // HashMap (non-final) mirrors how MapClusterService snapshots are built.
        Map<String, Object> snapshot = new HashMap<>();
        snapshot.put("k", a);
        byte[] encoded = MapReplicationCodec.encodeSnapshot(snapshot);
        Map<Object, Object> decoded = MapReplicationCodec.decodeSnapshot(encoded);

        EventStub roundTripped = (EventStub) decoded.get("k");
        assertEquals("A", roundTripped.identifier);
        assertEquals(1, roundTripped.impacts.size());

        EventStub bDecoded = roundTripped.impacts.get(0);
        assertEquals("B", bDecoded.identifier);
        assertEquals(1, bDecoded.impactedBy.size());

        // Identity preserved: the A referenced back by B is the same instance (no duplication).
        assertSame(roundTripped, bDecoded.impactedBy.get(0),
                "cycle must be resolved by id to the same instance");
    }

    @Test
    void cyclicGraphRoundTripsWithIdentityMixinRegisteredByModule() {
        // RF1: modules registered on the codec mapper can contribute mixins too.
        EventStub a = new EventStub("A");
        EventStub b = new EventStub("B");
        a.impacts.add(b);
        b.impactedBy.add(a);

        SimpleModule module = new SimpleModule("event-identity-module");
        module.setMixInAnnotation(EventStub.class, EventStubMixin.class);
        MapReplicationCodec.registerModule(module);

        byte[] encoded = MapReplicationCodec.encode(MapReplicationCommand.put("k", a));
        MapReplicationCommand decoded = MapReplicationCodec.decode(encoded);

        EventStub roundTripped = (EventStub) decoded.value();
        EventStub bDecoded = roundTripped.impacts.get(0);
        assertSame(roundTripped, bDecoded.impactedBy.get(0),
                "module-provided mixin must resolve repeated references by id");
    }

    @Test
    void cyclicGraphFailsWithoutCustomizer() {
        // Without the identity mixin the reciprocal cycle cannot be serialized: Jackson
        // aborts on nesting depth (wrapped by the codec as MapReplicationCodecException).
        // This is the motivation for the extension point (#110).
        EventStub a = new EventStub("A");
        EventStub b = new EventStub("B");
        a.impacts.add(b);
        b.impactedBy.add(a);

        Map<String, Object> snapshot = new HashMap<>();
        snapshot.put("k", a);
        assertThrows(RuntimeException.class,
                () -> MapReplicationCodec.encodeSnapshot(snapshot),
                "a reciprocal cycle must fail to serialize without an identity mixin");
    }

    @Test
    void defaultTypingPreservedWithoutCustomizer() {
        // RF6: with no customizer, an un-annotated POJO still round-trips via default typing.
        SimplePojo p = new SimplePojo("hello", 42);

        byte[] encoded = MapReplicationCodec.encode(MapReplicationCommand.put("k", p));
        MapReplicationCommand cmd = MapReplicationCodec.decode(encoded);

        assertInstanceOf(SimplePojo.class, cmd.value(), "default typing must restore the concrete type");
        SimplePojo decoded = (SimplePojo) cmd.value();
        assertEquals("hello", decoded.name);
        assertEquals(42, decoded.count);
    }

    @Test
    void defaultTypingPreservedWithCustomizerRegistered() {
        // A registered mixin for one type must not break default typing for others (RF3).
        MapReplicationCodec.addMixIn(EventStub.class, EventStubMixin.class);

        SimplePojo p = new SimplePojo("world", 7);
        MapReplicationCommand cmd = MapReplicationCodec.decode(
                MapReplicationCodec.encode(MapReplicationCommand.put("k", p)));

        assertInstanceOf(SimplePojo.class, cmd.value());
        assertEquals("world", ((SimplePojo) cmd.value()).name);
    }
}
