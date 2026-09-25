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
 * Resultado da verificação física de uma série em {@link NgrrdClusterClient#verify(java.util.Collection)}
 * — diferente de {@link NgrrdClusterClient#exists(String)}, que só olha o catálogo, {@code verify}
 * confirma no próprio dono se o objeto existe fisicamente no volume.
 */
public enum SeriesVerification {

    /** O dono confirmou que o objeto físico da série existe no seu volume local. */
    PRESENT,

    /** Há placement no catálogo, mas o dono confirmou que o objeto físico não existe (inconsistência). */
    MISSING_ON_OWNER,

    /** Não há placement no catálogo para a série — ela não existe no cluster. */
    NOT_PLACED,

    /**
     * Não foi possível confirmar fisicamente com o dono (RPC/capacidade/status indisponível) — nunca
     * interpretado como {@link #MISSING_ON_OWNER} nem {@link #NOT_PLACED}.
     */
    UNVERIFIED
}
