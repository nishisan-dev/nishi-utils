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

package dev.nishisan.utils.oss.cluster.catalog;

import java.util.Set;

/**
 * Capacidades de protocolo que um storage node anuncia em {@link StorageNodeStatus#capabilities()}. O
 * cliente confere a capacidade no status do nó antes de usar uma operação que um nó de versão anterior
 * não entende ou trataria de outro jeito, e falha com {@code ErrorCode.UNSUPPORTED_BY_NODE} em vez de
 * enviar o pedido: um status sem a capacidade (inclusive o de uma versão que ainda não publicava
 * capacidades) significa que o nó não a suporta.
 */
public final class StorageCapabilities {

    /**
     * O líder responde {@code ngrrd.catalog.lookup} (consulta ao catálogo sem posicionar) — exigida por
     * {@code exists}/{@code find} e pela resolução de séries existentes.
     */
    public static final String CATALOG_LOOKUP = "catalog.lookup";

    /**
     * O dono honra {@code OpenRequest.createIfMissing=false} (nunca cria a série) — exigida antes de
     * enviar o {@code OPEN} de um handle somente leitura; um nó antigo ignoraria o campo e criaria a
     * série.
     */
    public static final String OPEN_CREATE_IF_MISSING = "open.createIfMissing";

    /** O dono responde {@code ngrrd.series.exists.batch} (existência de objetos em lote no volume). */
    public static final String SERIES_EXISTS_BATCH = "series.exists.batch";

    /** Todas as capacidades anunciadas por um storage node desta versão. */
    public static final Set<String> ALL = Set.of(CATALOG_LOOKUP, OPEN_CREATE_IF_MISSING, SERIES_EXISTS_BATCH);

    private StorageCapabilities() {
    }
}
