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

package dev.nishisan.utils.oss.cluster.api;

/**
 * Códigos de erro do cliente do cluster ngrrd, expostos em {@link NgrrdClusterException}.
 */
public enum ErrorCode {

    /** Nenhum líder eleito no momento da requisição. */
    NO_LEADER,

    /** Nenhum storage node candidato disponível para receber a série. */
    NO_STORAGE_NODE_AVAILABLE,

    /** A série ficou indisponível (dono inalcançável, buffer esgotado, retries exauridos). */
    SERIES_UNAVAILABLE,

    /** O nó contatado não é (mais) o dono da série segundo seu catálogo local. */
    WRONG_OWNER,

    /** A série está em migração e não aceita a operação pedida. */
    MIGRATING,

    /** O nó remoto respondeu com um erro de aplicação. */
    REMOTE_ERROR,

    /** A operação excedeu o tempo limite configurado. */
    TIMEOUT,

    /** O buffer de escrita local atingiu o limite configurado. */
    BUFFER_FULL,

    /** O cliente ou o handle já foi fechado. */
    CLOSED,

    /**
     * O storage node envolvido não suporta o que a operação exige — tipicamente um nó de versão anterior
     * durante uma atualização. Dois casos:
     * <ul>
     *   <li>antes do RPC: o nó não anuncia a capacidade de protocolo exigida (ver
     *       {@code StorageCapabilities}), ou não há status publicado dele dentro do prazo — a operação falha
     *       sem enviar nada a esse nó: {@code exists}/{@code find} nunca devolvem {@code false} e
     *       {@code open} sem criar nunca é enviado a um nó que poderia criar a série;</li>
     *   <li>depois do {@code OPEN}: o storage respondeu {@code OK} a um {@code OPEN} sem criar sem confirmar
     *       que honrou {@code createIfMissing=false} — detectado depois do fato, então o storage já pode ter
     *       criado a série; o handle somente leitura se fecha e relança esta falha.</li>
     * </ul>
     * Atualize os storages antes dos clientes.
     */
    UNSUPPORTED_BY_NODE
}
