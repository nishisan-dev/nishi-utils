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
 * Status de uma resposta do protocolo do cluster relacionada a uma série.
 */
public enum SeriesStatus {

    /** Operação concluída com sucesso. */
    OK,

    /** O nó contatado não é (mais) o dono da série segundo seu catálogo local. */
    WRONG_OWNER,

    /** A série está em migração e não aceita a operação pedida no momento. */
    MIGRATING,

    /**
     * O dono não tem handle/definição em memória para a série (ex.: após
     * reinício do processo). O cliente deve reenviar {@code ngrrd.open} e
     * repetir a operação.
     */
    NOT_OPEN,

    /** O nó contatado não é o líder atual do cluster. */
    NOT_LEADER,

    /** Nenhum storage node candidato disponível para receber a série. */
    NO_STORAGE_NODE_AVAILABLE,

    /** Série inexistente e abertura sem criar. */
    NOT_FOUND,

    /** Falha de aplicação não coberta pelos demais status. */
    ERROR
}
