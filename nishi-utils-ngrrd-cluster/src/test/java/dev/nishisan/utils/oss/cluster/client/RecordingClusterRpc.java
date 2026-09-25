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

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.rpc.ClusterRpc;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;

/**
 * {@link ClusterRpc} fake usado pelos testes unitários de {@code client}
 * ({@link WriteDispatcherTest}, {@link PlacementResolverTest},
 * {@link RemoteSeriesHandleTest}): grava toda chamada e devolve respostas
 * programadas por {@link #respondNext} (uma vez, na ordem) ou {@link #respondDefault}
 * (para qualquer chamada sem resposta programada específica).
 *
 * <p>Não é uma classe de teste — sem métodos {@code @Test}, sem
 * {@code assert}. Puro fake, thread-safe o suficiente para as chamadas
 * assíncronas do {@code WriteDispatcher}.</p>
 */
final class RecordingClusterRpc implements ClusterRpc {

    /** Uma chamada capturada: alvo, comando e corpo enviados. */
    record Recorded(NodeId target, String command, Object body) {
    }

    /** Resposta que depende também do nó de destino da chamada. */
    @FunctionalInterface
    interface TargetResponder {
        Object respond(NodeId target, String command, Object body);
    }

    private final NodeId localId;
    private final List<Recorded> calls = new CopyOnWriteArrayList<>();
    private final Queue<BiFunction<String, Object, Object>> queuedResponders = new ArrayDeque<>();

    private volatile Optional<NodeId> leaderId = Optional.empty();
    private volatile TargetResponder defaultResponder;
    /** B3 (achado do Refuter): estado de conexão simulado por alvo — ausente aqui = conectado. */
    private final ConcurrentMap<NodeId, Boolean> connected = new ConcurrentHashMap<>();

    RecordingClusterRpc(NodeId localId) {
        this.localId = localId;
    }

    /** Define o líder devolvido por {@link #leaderId()}. */
    void leader(NodeId id) {
        this.leaderId = Optional.ofNullable(id);
    }

    /** Programa a resposta (ou lançamento) da próxima chamada, uma única vez. */
    synchronized void respondNext(BiFunction<String, Object, Object> responder) {
        queuedResponders.add(responder);
    }

    /** Programa a resposta usada quando não há mais respostas enfileiradas por {@link #respondNext}. */
    void respondDefault(BiFunction<String, Object, Object> responder) {
        this.defaultResponder = responder == null ? null : (target, command, body) -> responder.apply(command, body);
    }

    /** Como {@link #respondDefault}, mas a resposta pode depender do nó de destino. */
    void respondByTarget(TargetResponder responder) {
        this.defaultResponder = responder;
    }

    /** Chamadas capturadas até agora, na ordem de chegada. */
    List<Recorded> calls() {
        return List.copyOf(calls);
    }

    @Override
    public <R> R call(NodeId target, String command, Object body, Class<R> responseType) {
        calls.add(new Recorded(target, command, body));
        BiFunction<String, Object, Object> queued = pollResponder();
        Object result;
        if (queued != null) {
            result = queued.apply(command, body);
        } else {
            TargetResponder responder = defaultResponder;
            if (responder == null) {
                throw new IllegalStateException("nenhuma resposta programada para " + command + " em " + target);
            }
            result = responder.respond(target, command, body);
        }
        return responseType.cast(result);
    }

    private synchronized BiFunction<String, Object, Object> pollResponder() {
        return queuedResponders.poll();
    }

    @Override
    public NodeId localId() {
        return localId;
    }

    @Override
    public Optional<NodeId> leaderId() {
        return leaderId;
    }

    /** B3 (achado do Refuter): marca {@code target} como (des)conectado — default: sempre conectado. */
    void setConnected(NodeId target, boolean isConnected) {
        connected.put(target, isConnected);
    }

    @Override
    public boolean isConnected(NodeId target) {
        return connected.getOrDefault(target, true);
    }
}
