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

import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.cluster.transport.TransportListener;
import dev.nishisan.utils.ngrid.common.ClientRequestPayload;
import dev.nishisan.utils.ngrid.common.ClientResponsePayload;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;

import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base para os handlers de comando do módulo: filtra {@code CLIENT_REQUEST}
 * pelos comandos atendidos, extrai o {@link ClientRequestPayload} e chama
 * {@link #handle(String, Object, NodeId)} — igual ao padrão já usado por
 * {@code DistributedQueue}/{@code DistributedMap} no core.
 *
 * <p>Garante duas coisas exigidas pelo protocolo do cluster ngrrd: (1) toda
 * exceção de {@link #handle} é capturada dentro de {@link #onMessage} e vira
 * uma resposta {@code CLIENT_RESPONSE} com {@code success=false} — nunca
 * escapa para a worker thread do transporte; (2) o mesmo handler atende
 * chamadas locais via {@link LocalRequestHandler#handleLocal}, usado por
 * {@link TransportClusterRpc} quando o alvo é o próprio nó.</p>
 */
public abstract class RequestHandlerSupport implements TransportListener, LocalRequestHandler {

    private static final Logger LOGGER = Logger.getLogger(RequestHandlerSupport.class.getName());

    private final Transport transport;
    private final Set<String> commands;

    protected RequestHandlerSupport(Transport transport, Set<String> commands) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.commands = Set.copyOf(Objects.requireNonNull(commands, "commands"));
    }

    /**
     * Processa {@code command} e devolve o corpo da resposta em caso de
     * sucesso. Exceções lançadas aqui são convertidas em resposta de erro por
     * {@link #onMessage} (caminho de rede) ou em {@link NgrrdClusterException}
     * por {@link #handleLocal} (caminho local) — nunca precisam ser tratadas
     * pelo próprio {@code handle}.
     *
     * @param command comando do protocolo (sempre um de {@link #commands})
     * @param body    corpo da requisição
     * @param source  nó que originou a requisição
     * @return corpo da resposta de sucesso
     */
    protected abstract Object handle(String command, Object body, NodeId source);

    /** Comandos atendidos por este handler. */
    protected final Set<String> commands() {
        return commands;
    }

    /** Nó local do transporte associado a este handler. */
    protected final NodeId localId() {
        return transport.local().nodeId();
    }

    @Override
    public void onPeerConnected(NodeInfo peer) {
        // no-op por padrão; subclasses sobrescrevem se precisarem reagir a conexões.
    }

    @Override
    public void onPeerDisconnected(NodeId peerId) {
        // no-op por padrão; subclasses sobrescrevem se precisarem reagir a desconexões.
    }

    @Override
    public final void onMessage(ClusterMessage message) {
        if (message.type() != MessageType.CLIENT_REQUEST) {
            return;
        }
        ClientRequestPayload payload = message.payload(ClientRequestPayload.class);
        if (!commands.contains(payload.command())) {
            return;
        }
        ClientResponsePayload responsePayload;
        try {
            Object result = handle(payload.command(), payload.body(), message.source());
            responsePayload = new ClientResponsePayload(payload.requestId(), true, result, null);
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Falha ao processar " + payload.command() + " de " + message.source(), e);
            responsePayload = new ClientResponsePayload(payload.requestId(), false, null, describe(e));
        }
        transport.send(ClusterMessage.response(message, responsePayload));
    }

    @Override
    public final boolean handles(String command) {
        return commands.contains(command);
    }

    @Override
    public final Object handleLocal(String command, Object body) {
        try {
            return handle(command, body, localId());
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Falha ao processar " + command + " localmente", e);
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, describe(e), e);
        }
    }

    private static String describe(RuntimeException e) {
        String message = e.getMessage();
        return e.getClass().getName() + (message != null ? ": " + message : "");
    }
}
