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

import dev.nishisan.utils.ngrid.cluster.coordination.ClusterCoordinator;
import dev.nishisan.utils.ngrid.cluster.transport.Transport;
import dev.nishisan.utils.ngrid.common.ClientRequestPayload;
import dev.nishisan.utils.ngrid.common.ClientResponsePayload;
import dev.nishisan.utils.ngrid.common.ClusterMessage;
import dev.nishisan.utils.ngrid.common.MessageType;
import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.ngrid.common.NodeInfo;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;

import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link ClusterRpc} sobre o {@code Transport}/{@code ClusterCoordinator} do
 * NGrid, no mesmo estilo de {@code DistributedQueue}/{@code DistributedMap}
 * (ver {@code structures/DistributedQueue.java:332-362,475-503}).
 *
 * <h2>Chamada ao próprio nó (loopback)</h2>
 *
 * <p><strong>Verificado em {@code TcpTransport}:</strong> {@code ensureConnection}
 * devolve {@code null} quando o nó de destino é o próprio nó local
 * ({@code nodeId.equals(config.local().nodeId())}), então tanto
 * {@code send} quanto {@code sendAndAwait} falham ("No connection available")
 * para uma mensagem endereçada a si mesmo — não há loopback no transporte.
 * {@code DistributedQueue.invokeLeader} já contorna isso chamando
 * {@code executeLocal} diretamente quando o líder é o próprio nó; esta classe
 * generaliza o mesmo contorno via {@link LocalRequestHandler#handleLocal},
 * registrado pelos handlers do módulo em {@link #registerLocalHandler}.</p>
 */
public final class TransportClusterRpc implements ClusterRpc {

    private final Transport transport;
    private final ClusterCoordinator coordinator;
    private final Duration requestTimeout;
    private final List<LocalRequestHandler> localHandlers = new CopyOnWriteArrayList<>();

    public TransportClusterRpc(Transport transport, ClusterCoordinator coordinator, Duration requestTimeout) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        if (requestTimeout.isNegative() || requestTimeout.isZero()) {
            throw new IllegalArgumentException("requestTimeout deve ser > 0");
        }
    }

    /**
     * Registra um handler para despacho local — usado por {@link #call} quando
     * {@code target} é o próprio nó. Tipicamente o mesmo objeto já registrado
     * em {@code transport.addListener} para o caminho de rede.
     */
    public void registerLocalHandler(LocalRequestHandler handler) {
        localHandlers.add(Objects.requireNonNull(handler, "handler"));
    }

    /** Remove um handler local previamente registrado. */
    public void unregisterLocalHandler(LocalRequestHandler handler) {
        localHandlers.remove(handler);
    }

    @Override
    public NodeId localId() {
        return transport.local().nodeId();
    }

    @Override
    public Optional<NodeId> leaderId() {
        return coordinator.leaderInfo().map(NodeInfo::nodeId);
    }

    @Override
    public boolean isConnected(NodeId target) {
        return transport.isConnected(target);
    }

    @Override
    public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
        // B1 (achado do Refuter): a sobrecarga simples delega para a com teto explícito, usando o
        // requestTimeout configurado — um único caminho de execução, não dois mantidos em paralelo.
        return call(target, command, body, responseType, requestTimeout);
    }

    @Override
    public <R> R call(NodeId target, String command, Object body, Class<R> responseType, Duration timeout) {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(responseType, "responseType");
        Objects.requireNonNull(timeout, "timeout");
        if (target.equals(localId())) {
            // Despacho local: síncrono, sem espera de rede — o teto explícito não se aplica.
            return callLocal(command, body, responseType);
        }
        return callRemote(target, command, body, responseType, timeout);
    }

    private <R> R callLocal(String command, Object body, Class<R> responseType) {
        for (LocalRequestHandler handler : localHandlers) {
            if (!handler.handles(command)) {
                continue;
            }
            // handleLocal já converte falha de aplicação em NgrrdClusterException —
            // o mesmo contrato de erro do caminho remoto (ver callRemote). O retorno pode ser
            // null legitimamente (m3: handles() já decidiu que este é o handler certo).
            Object result = handler.handleLocal(command, body);
            return castBody(result, responseType, command);
        }
        throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                "nenhum handler local registrado para o comando " + command);
    }

    private <R> R callRemote(NodeId target, String command, Object body, Class<R> responseType, Duration timeout) {
        ClientRequestPayload payload = new ClientRequestPayload(UUID.randomUUID(), command, body);
        ClusterMessage request = ClusterMessage.request(MessageType.CLIENT_REQUEST, command,
                localId(), target, payload);

        // B1: nunca espera mais que o requestTimeout configurado neste rpc, mas pode esperar MENOS se
        // o chamador já está sob um orçamento total mais apertado (ver Javadoc de ClusterRpc#call(..., Duration)).
        long waitMillis = Math.min(requestTimeout.toMillis(), timeout.toMillis());
        ClusterMessage response;
        try {
            response = transport.sendAndAwait(request).get(waitMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new NgrrdClusterException(ErrorCode.TIMEOUT,
                    "tempo esgotado aguardando resposta de " + command + " em " + target, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                    "interrompido aguardando resposta de " + command + " em " + target, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            // B3: um TimeoutException pode chegar embrulhado (o future falha com ExecutionException
            // cuja causa, em qualquer nível, é um TimeoutException) em vez de estourar diretamente
            // como no `get(timeout)` abaixo — trata os dois casos como o mesmo ErrorCode.TIMEOUT.
            if (containsTimeoutException(cause)) {
                throw new NgrrdClusterException(ErrorCode.TIMEOUT,
                        "tempo esgotado aguardando resposta de " + command + " em " + target, cause);
            }
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                    "falha de transporte ao enviar " + command + " para " + target, cause);
        }

        ClientResponsePayload responsePayload;
        try {
            responsePayload = response.payload(ClientResponsePayload.class);
        } catch (ClassCastException e) {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                    "resposta de " + command + " com payload de tipo inesperado", e);
        }
        if (!responsePayload.success()) {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR, responsePayload.error());
        }
        return castBody(responsePayload.body(), responseType, command);
    }

    /** Percorre a cadeia de causas (com proteção contra ciclos) procurando um {@link TimeoutException}. */
    private static boolean containsTimeoutException(Throwable throwable) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = throwable;
        while (current != null && seen.add(current)) {
            if (current instanceof TimeoutException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static <R> R castBody(Object body, Class<R> responseType, String command) {
        if (body == null) {
            return null;
        }
        if (!responseType.isInstance(body)) {
            throw new NgrrdClusterException(ErrorCode.REMOTE_ERROR,
                    "resposta de " + command + " com tipo inesperado: esperado " + responseType.getName()
                            + ", recebido " + body.getClass().getName());
        }
        return responseType.cast(body);
    }
}
