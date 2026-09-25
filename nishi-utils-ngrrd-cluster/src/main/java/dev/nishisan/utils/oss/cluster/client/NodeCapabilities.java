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

import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;
import dev.nishisan.utils.oss.cluster.catalog.CatalogService;
import dev.nishisan.utils.oss.cluster.catalog.StorageCapabilities;
import dev.nishisan.utils.oss.cluster.catalog.StorageNodeStatus;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Confere se um storage node anuncia uma capacidade de protocolo ({@link StorageCapabilities}) antes de
 * o cliente lhe enviar uma operação que depende dela.
 *
 * <p>Lê o status do nó na réplica local do catálogo {@code ngrrd.nodes}; só se o status estiver ausente
 * faz UMA leitura forte (no líder). Status presente sem a capacidade — inclusive o de um nó de versão
 * anterior, que publica o status sem capacidades — ou ausente também no líder falha na hora com
 * {@link ErrorCode#UNSUPPORTED_BY_NODE}, sem nenhum RPC ao nó. Uma falha da leitura forte não é
 * confundida com falta de capacidade: vira {@link NgrrdClusterException} com outro código.</p>
 *
 * <p>Usado por {@link CatalogLookupClient} ({@code catalog.lookup} no líder, antes de
 * {@code exists}/{@code find}/resolução de séries existentes) e por {@link RemoteSeriesHandle}
 * ({@code open.createIfMissing} no dono, antes de todo {@code OPEN} de handle somente leitura), e
 * reutilizável por qualquer outra operação que exija uma capacidade.</p>
 */
public final class NodeCapabilities {

    private final Function<String, Optional<StorageNodeStatus>> localStatus;
    private final Function<String, Optional<StorageNodeStatus>> strongStatus;

    /**
     * @param localStatus  status do nó na réplica local (leitura eventual, sem RPC)
     * @param strongStatus status do nó confirmado no líder — só consultado com o local ausente
     */
    public NodeCapabilities(Function<String, Optional<StorageNodeStatus>> localStatus,
            Function<String, Optional<StorageNodeStatus>> strongStatus) {
        this.localStatus = Objects.requireNonNull(localStatus, "localStatus");
        this.strongStatus = Objects.requireNonNull(strongStatus, "strongStatus");
    }

    /** Capacidades lidas do catálogo {@code ngrrd.nodes} de {@code catalog}. */
    public static NodeCapabilities from(CatalogService catalog) {
        Objects.requireNonNull(catalog, "catalog");
        return new NodeCapabilities(catalog::nodeStatusLocal, catalog::nodeStatusStrong);
    }

    /**
     * Exige que {@code nodeId} anuncie {@code capability}.
     *
     * @throws NgrrdClusterException com {@link ErrorCode#UNSUPPORTED_BY_NODE} se o nó não anuncia a
     *         capacidade (ou não tem status publicado nem no líder); com outro código se não foi possível
     *         ler o status no líder
     */
    public void require(String nodeId, String capability) {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(capability, "capability");
        StorageNodeStatus status = localStatus.apply(nodeId).orElse(null);
        if (status == null) {
            status = strongStatusOf(nodeId);
        }
        if (status == null) {
            throw new NgrrdClusterException(ErrorCode.UNSUPPORTED_BY_NODE,
                    nodeId + " não anuncia " + capability + " (nenhum status publicado)");
        }
        if (!status.advertises(capability)) {
            throw new NgrrdClusterException(ErrorCode.UNSUPPORTED_BY_NODE, nodeId + " não anuncia " + capability);
        }
    }

    private StorageNodeStatus strongStatusOf(String nodeId) {
        try {
            return strongStatus.apply(nodeId).orElse(null);
        } catch (NgrrdClusterException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                    "falha ao ler no líder o status de " + nodeId + " para conferir as capacidades", e);
        }
    }
}
