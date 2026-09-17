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

import java.util.Arrays;
import java.util.Objects;

/**
 * Um chunk de bytes da série em migração, enviado da origem para o destino.
 * Apenas o tipo — o handler é implementado num marco futuro.
 *
 * <p>{@code equals}/{@code hashCode} são sobrescritos porque o {@code record}
 * padrão compararia {@code data} por referência (arrays não têm
 * {@code equals}/{@code hashCode} de conteúdo em Java) — sem isso, duas
 * instâncias com os mesmos bytes, vindas de serializações distintas (ex.: um
 * round-trip de rede), seriam consideradas diferentes.
 *
 * @param seriesKey   chave lógica da série
 * @param migrationId identificador único da migração
 * @param seq         índice do chunk, 0-based
 * @param total       quantidade total de chunks da migração
 * @param data        bytes do chunk
 */
public record MigrateChunkRequest(String seriesKey, String migrationId, int seq, int total, byte[] data) {

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MigrateChunkRequest other)) {
            return false;
        }
        return seq == other.seq
                && total == other.total
                && Objects.equals(seriesKey, other.seriesKey)
                && Objects.equals(migrationId, other.migrationId)
                && Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(seriesKey, migrationId, seq, total);
        return 31 * result + Arrays.hashCode(data);
    }
}
