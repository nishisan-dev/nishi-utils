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

package dev.nishisan.utils.ngrid.common;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec;
import dev.nishisan.utils.ngrid.map.MapReplicationCodec;
import dev.nishisan.utils.ngrid.queue.QueueReplicationCodec;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class JsonFactoriesTest {

    /** A compile-time constant: the JVM interns it, so an interning parser hands back this very instance. */
    private static final String INTERNED_KEY = "device:olt-1/iface:pon-0/group:olt-auth-v1";

    @Test
    void fabricaDeChavesDinamicasDesligaCanonicalizacaoEIntern() {
        JsonFactory factory = JsonFactories.dynamicKeys();

        assertFalse(factory.isEnabled(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES));
        assertFalse(factory.isEnabled(JsonFactory.Feature.INTERN_FIELD_NAMES));
    }

    @Test
    void mapperPadraoDoCodecNaoInternaChavesDeMapa() throws Exception {
        ObjectMapper mapper = JacksonMessageCodec.createDefaultMapper();
        assertFalse(mapper.getFactory().isEnabled(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES));

        Map<String, String> decoded = mapper.readValue("{\"" + INTERNED_KEY + "\":\"OK\"}",
                new TypeReference<Map<String, String>>() { });

        String key = decoded.keySet().iterator().next();
        assertEquals(INTERNED_KEY, key);
        assertNotSame(INTERNED_KEY, key, "a chave decodificada não pode ter passado por String.intern");
    }

    @Test
    void mapperComJacksonPadraoInternariaAChave() throws Exception {
        // Contraprova do teste acima: com a fábrica padrão do Jackson a chave volta interna.
        Map<String, String> decoded = new ObjectMapper().readValue("{\"" + INTERNED_KEY + "\":\"OK\"}",
                new TypeReference<Map<String, String>>() { });

        assertSame(INTERNED_KEY, decoded.keySet().iterator().next());
    }

    @Test
    void codecsDeReplicacaoEOCodecLegadoDoTransporteUsamAFabricaSemCanonicalizacao() throws Exception {
        assertNoCanonicalization(staticMapper(MapReplicationCodec.class, "MAPPER"));
        assertNoCanonicalization(staticMapper(QueueReplicationCodec.class, "MAPPER"));
        assertNoCanonicalization(instanceMapper(
                new dev.nishisan.utils.ngrid.cluster.transport.JacksonMessageCodec(), "mapper"));
    }

    @Test
    void mapaComMilharesDeChavesDistintasFazIdaEVoltaIntacto() throws Exception {
        ObjectMapper mapper = JacksonMessageCodec.createDefaultMapper();
        Map<String, String> original = new LinkedHashMap<>();
        for (int i = 0; i < 10_000; i++) {
            original.put("device:d-" + i + "/iface:if-" + (i % 97) + "/group:g", i % 3 == 0 ? "ERROR" : "OK");
        }

        Map<String, String> decoded = mapper.readValue(mapper.writeValueAsBytes(original),
                new TypeReference<LinkedHashMap<String, String>>() { });

        assertEquals(original, decoded);
    }

    private static void assertNoCanonicalization(ObjectMapper mapper) {
        assertFalse(mapper.getFactory().isEnabled(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES),
                "mapper com canonicalização de nomes de campo ligada: " + mapper.getFactory());
    }

    private static ObjectMapper staticMapper(Class<?> owner, String field) throws ReflectiveOperationException {
        Field f = owner.getDeclaredField(field);
        f.setAccessible(true);
        return (ObjectMapper) f.get(null);
    }

    private static ObjectMapper instanceMapper(Object owner, String field) throws ReflectiveOperationException {
        Field f = owner.getClass().getDeclaredField(field);
        f.setAccessible(true);
        return (ObjectMapper) f.get(owner);
    }
}
