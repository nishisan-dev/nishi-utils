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

import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;

/**
 * Despacho local de um comando do protocolo, usado por {@link TransportClusterRpc}
 * quando o alvo da chamada é o próprio nó — {@code TcpTransport} não entrega
 * mensagens endereçadas a si mesmo aos próprios listeners (ver
 * {@code TransportClusterRpc}), então esse caminho evita depender da rede
 * (loopback) para uma chamada local.
 *
 * <p>Implementado pelos mesmos handlers registrados em
 * {@code Transport.addListener} (ex.: {@code RequestHandlerSupport}), que assim
 * atendem tanto {@code CLIENT_REQUEST} vindos da rede quanto chamadas locais
 * com o mesmo código de aplicação.</p>
 *
 * <p>{@link #handles} e {@link #handleLocal} são dois métodos, não um só que
 * devolve {@code Optional} — de propósito: um {@code Optional.empty()} seria
 * ambíguo entre "este handler não atende {@code command}" e "atendeu, e o
 * resultado é {@code null}". Separar as perguntas remove a ambiguidade.</p>
 */
public interface LocalRequestHandler {

    /**
     * Indica se este handler atende {@code command}. Chamado antes de
     * {@link #handleLocal} para decidir se ele é o handler certo.
     */
    boolean handles(String command);

    /**
     * Processa {@code command} localmente. Só é chamado quando {@link #handles}
     * já devolveu {@code true} para o mesmo comando.
     *
     * @param command comando do protocolo (ex.: {@code Commands.OPEN})
     * @param body    corpo da requisição
     * @return o corpo da resposta — pode ser {@code null} legitimamente
     * @throws NgrrdClusterException se o processamento falhar — equivalente a
     *                                uma resposta remota com {@code success=false}
     */
    Object handleLocal(String command, Object body);
}
