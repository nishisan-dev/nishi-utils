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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Fase 0.4a — a leadership priority (afinidade) viaja na {@link NodeInfo} (que é propagada via
 * handshake/gossip) e deve ser retrocompatível: o JSON de um nó antigo (sem o campo) desserializa
 * com priority 0, e a identidade ({@code equals}) ignora a priority.
 */
class NodeInfoPriorityTest {

    private final ObjectMapper mapper = JacksonMessageCodec.createDefaultMapper();

    @Test
    void defaultsToZeroAndIsNotPartOfIdentity() {
        NodeInfo a = new NodeInfo(NodeId.of("n1"), "127.0.0.1", 5000);
        assertEquals(0, a.priority(), "priority deveria default para 0");

        NodeInfo withPrio = a.withPriority(150);
        assertEquals(150, withPrio.priority());
        // priority é metadado de seleção, não de identidade: host/port/nodeId iguais => equals.
        assertEquals(a, withPrio, "priority não deveria entrar no equals/hashCode");
        assertEquals(a.hashCode(), withPrio.hashCode());
    }

    @Test
    void roundTripsPriority() throws Exception {
        NodeInfo node = new NodeInfo(NodeId.of("n1"), "127.0.0.1", 5000,
                java.util.Collections.emptySet(), 150);
        NodeInfo decoded = mapper.readValue(mapper.writeValueAsBytes(node), NodeInfo.class);
        assertEquals(150, decoded.priority(), "priority deveria sobreviver ao round-trip JSON");
        assertEquals(node, decoded);
    }

    @Test
    void legacyJsonWithoutPriorityDeserializesAsZero() throws Exception {
        // Simula o JSON de um nó antigo: serializa um real e remove o campo priority.
        ObjectNode node = (ObjectNode) mapper.readTree(
                mapper.writeValueAsBytes(new NodeInfo(NodeId.of("n1"), "127.0.0.1", 5000).withPriority(99)));
        node.remove("priority");
        assertFalse(node.has("priority"));

        NodeInfo decoded = mapper.treeToValue(node, NodeInfo.class);
        assertEquals(0, decoded.priority(), "campo ausente deve desserializar como 0 (retrocompat)");
        assertEquals(NodeId.of("n1"), decoded.nodeId());
    }
}
