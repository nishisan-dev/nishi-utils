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

package dev.nishisan.utils.oss.cluster.client;

import dev.nishisan.utils.oss.cluster.catalog.NodeState;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;
import dev.nishisan.utils.oss.cluster.placement.DistributionMode;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * {@link NodeCapabilities} prontos para os testes do cliente: todo nó anuncia as capacidades pedidas na
 * réplica local, sem leitura forte. Não é uma classe de teste.
 */
final class CapabilityFixtures {

    private CapabilityFixtures() {
    }

    /** Todo nó anuncia todas as capacidades do protocolo atual. */
    static NodeCapabilities advertisingAll() {
        return advertising(StorageCapabilities.ALL);
    }

    /** Todo nó anuncia exatamente {@code capabilities} na réplica local. */
    static NodeCapabilities advertising(Set<String> capabilities) {
        return new NodeCapabilities(nodeId -> Optional.of(status(nodeId, capabilities)), nodeId -> {
            throw new AssertionError("status local presente: não deveria haver leitura forte de " + nodeId);
        });
    }

    /** Cada nó anuncia o que {@code byNode} disser; nó ausente do mapa não tem status publicado. */
    static NodeCapabilities advertisingByNode(Map<String, Set<String>> byNode) {
        return new NodeCapabilities(
                nodeId -> Optional.ofNullable(byNode.get(nodeId)).map(caps -> status(nodeId, caps)),
                nodeId -> Optional.empty());
    }

    /** Falha se for consultado — para caminhos que nunca podem conferir capacidade. */
    static NodeCapabilities unused() {
        return new NodeCapabilities(nodeId -> {
            throw new AssertionError("capacidade não deveria ser conferida (" + nodeId + ")");
        }, nodeId -> {
            throw new AssertionError("capacidade não deveria ser conferida (" + nodeId + ")");
        });
    }

    /** Status mínimo de {@code nodeId} anunciando {@code capabilities}. */
    static StorageNodeStatus status(String nodeId, Set<String> capabilities) {
        return new StorageNodeStatus(nodeId, NodeState.ACTIVE, 0, 0, 0, 1L, DistributionMode.COUNT, 1, 0,
                capabilities);
    }
}
