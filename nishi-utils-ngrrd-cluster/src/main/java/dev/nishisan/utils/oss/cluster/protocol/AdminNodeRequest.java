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

package dev.nishisan.utils.oss.cluster.protocol;

/**
 * Pedido de administração dirigido a um nó específico: {@code ngrrd.admin.drain}
 * / {@code ngrrd.admin.activate} / {@code ngrrd.admin.metrics}.
 *
 * @param nodeId    storage node alvo; {@code null} em {@code ngrrd.admin.metrics} pede as
 *                  métricas do próprio nó que atende a requisição
 * @param forwarded {@code ngrrd.admin.metrics} apenas: {@code true} quando este pedido já é o
 *                  encaminhamento de um nó que não era o alvo — impede um segundo encaminhamento
 *                  (um salto no máximo) caso o catálogo local do nó intermediário estivesse errado
 */
public record AdminNodeRequest(String nodeId, boolean forwarded) {
}
