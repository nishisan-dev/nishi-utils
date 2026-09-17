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

package dev.nishisan.utils.oss.cluster;

import dev.nishisan.utils.oss.cluster.api.NgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterConfig;
import dev.nishisan.utils.oss.cluster.client.DefaultNgrrdClusterClient;
import dev.nishisan.utils.oss.cluster.node.NgrrdStorageNode;
import dev.nishisan.utils.oss.cluster.node.StorageNodeConfig;

import java.io.IOException;

/**
 * Ponto de entrada público do cluster ngrrd: conectar como cliente transparente
 * ou subir um storage node. Ambos delegam à implementação concreta do módulo —
 * esta classe existe só para dar uma fachada estável e curta ao chamador.
 */
public final class NgrrdCluster {

    private NgrrdCluster() {
    }

    /** Conecta ao cluster ngrrd como cliente transparente, pronto para {@link NgrrdClusterClient#open}. */
    public static NgrrdClusterClient connect(NgrrdClusterConfig cfg) {
        return DefaultNgrrdClusterClient.connect(cfg);
    }

    /** Sobe um storage node completo a partir de {@code cfg}. */
    public static NgrrdStorageNode startStorageNode(StorageNodeConfig cfg) throws IOException {
        return NgrrdStorageNode.start(cfg);
    }
}
