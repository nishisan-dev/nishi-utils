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
 * Status de uma operação do protocolo de migração.
 */
public enum MigrateStatus {

    /** Operação concluída com sucesso. */
    OK,

    /** The destination reserved the image and supports live-copy patches. */
    COPY_READY,

    /** A migração foi confirmada e a cópia no destino está ativa. */
    COMMITTED,

    /** A migração está em andamento; nem todos os chunks foram recebidos. */
    PARTIAL,

    /** O destino não reconhece o {@code migrationId} consultado. */
    UNKNOWN,

    /** O hash informado no commit não confere com os bytes recebidos. */
    HASH_MISMATCH,

    /** Falha de aplicação não coberta pelos demais status. */
    ERROR
}
