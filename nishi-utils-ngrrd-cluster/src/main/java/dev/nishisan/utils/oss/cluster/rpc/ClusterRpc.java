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

package dev.nishisan.utils.oss.cluster.rpc;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;

import java.util.Optional;

/**
 * Chamada RPC síncrona de um comando do protocolo do cluster ngrrd a um nó
 * específico (dono da série ou líder). Abstrai se o alvo é o próprio nó local
 * (despacho direto, sem rede) ou um nó remoto (via {@code Transport}).
 */
public interface ClusterRpc {

    /**
     * Executa {@code command} em {@code target} e devolve o corpo da resposta
     * já convertido para {@code responseType}.
     *
     * @throws NgrrdClusterException se o alvo responder com falha
     *                                ({@link dev.nishisan.utils.oss.cluster.api.ErrorCode#REMOTE_ERROR}),
     *                                a chamada expirar
     *                                ({@link dev.nishisan.utils.oss.cluster.api.ErrorCode#TIMEOUT}) ou o
     *                                corpo da resposta não for do tipo esperado
     */
    <R> R call(NodeId target, String command, Object body, Class<R> responseType);

    /** Identificador do nó local. */
    NodeId localId();

    /** Identificador do líder atual do cluster, ou vazio se nenhum foi eleito. */
    Optional<NodeId> leaderId();
}
