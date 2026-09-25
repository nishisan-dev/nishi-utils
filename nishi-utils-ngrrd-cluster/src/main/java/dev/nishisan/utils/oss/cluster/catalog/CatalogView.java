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

package dev.nishisan.utils.oss.cluster.catalog;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * Visão do catálogo consumida pelos colaboradores do líder ({@code PlacementRequestHandler},
 * {@code MigrationCoordinator}, {@code Rebalancer}) — isola a dependência de {@link CatalogService}
 * (que por sua vez envolve {@code DistributedMap}, classe final do core) para permitir fakes nos
 * testes unitários desses colaboradores. {@link CatalogService} implementa esta interface
 * diretamente: nenhum adaptador é necessário em produção.
 */
public interface CatalogView {

    /** Leitura forte (round-trip ao líder) do placement da série. */
    Optional<SeriesPlacement> placementStrong(String seriesKey);

    /**
     * Leitura forte (round-trip ao líder) do status de um storage node — usada quando a réplica local
     * (eventual) não é confiável o bastante (ex.: decidir se {@code self} ainda está {@code ACTIVE}
     * antes de adotar uma série, ou preservar {@code DRAINING}/{@code DRAINED} ao republicar o próprio
     * status). Pode lançar se não houver líder alcançável — o chamador decide o que fazer.
     */
    Optional<StorageNodeStatus> nodeStatusStrong(String nodeId);

    /** Snapshot local (eventual) do status de todos os storage nodes conhecidos. */
    Collection<StorageNodeStatus> nodesLocal();

    /** Cópia imutável do catálogo na visão local (eventual) do nó. */
    Map<String, SeriesPlacement> placementsLocal();

    /**
     * Placement de uma série na visão local (eventual) do nó, lido na hora. O default copia o catálogo
     * inteiro via {@link #placementsLocal()} — implementações reais devem ler a chave direto.
     */
    default Optional<SeriesPlacement> placementLocal(String seriesKey) {
        return Optional.ofNullable(placementsLocal().get(seriesKey));
    }

    /** Grava o placement da série; roteado ao líder pelo próprio {@code DistributedMap}. */
    void putPlacement(String seriesKey, SeriesPlacement placement);

    /**
     * Publica/atualiza o status de um storage node; roteado ao líder pelo próprio {@code DistributedMap}
     * — usado por {@code NodeStatusReporter} (isolamento do {@code CatalogService}/{@code DistributedMap}
     * reais para testes com fake, mesmo padrão dos demais métodos desta interface).
     */
    void putNodeStatus(StorageNodeStatus status);
    /** Whether this catalog persists geometry references. */
    default boolean geometryTrackingEnabled() { return false; }

    /** Shared leader-side lock identity; acquire through {@code CoordinationLocks.acquire}. */
    default Object placementLock(String seriesKey) { return this; }

    /** Registers a validated, immutable geometry before its reference is published. */
    default void putGeometry(GeometryDescriptor geometry) { }

    /** Local replicated geometry lookup. */
    default Optional<GeometryDescriptor> geometryLocal(String id) { return Optional.empty(); }

    /** Strong geometry lookup used before acknowledging a reference. */
    default Optional<GeometryDescriptor> geometryStrong(String id) { return geometryLocal(id); }
    /** Rebuilds leader-local admission tracking after a leadership change. */
    default void resetAdmissionTracking() { }

    /** Pending allocated bytes not yet reflected in node reports. */
    default Map<String, Long> pendingBytesByNode() { return Map.of(); }

    /** Incoming migrations whose destination has not yet become the catalog owner. */
    default Map<String, Long> pendingMigrationSeriesByNode() { return Map.of(); }
}
