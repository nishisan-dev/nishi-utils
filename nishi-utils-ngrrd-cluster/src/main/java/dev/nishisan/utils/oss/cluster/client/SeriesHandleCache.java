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

import dev.nishisan.utils.oss.cluster.rpc.CoordinationLocks;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Handles abertos pelo cliente, um por chave de série, com as regras de reaproveitamento de
 * {@code DefaultNgrrdClusterClient.open}:
 * <ul>
 *   <li>{@code open} sem criar devolve o handle aberto em cache, seja somente leitura ou gravável;</li>
 *   <li>{@code open} com criação devolve o handle gravável em cache ou PROMOVE o somente leitura aberto
 *       ({@link RemoteSeriesHandle#tryPromoteToWritable()}) — nunca rebaixa um gravável;</li>
 *   <li>um handle fechado (ou cuja promoção falhou por estar fechando) nunca é devolvido: um novo é
 *       aberto no lugar.</li>
 * </ul>
 *
 * <p>Nunca faz RPC dentro de {@code computeIfAbsent} — isso manteria o bin lock interno do
 * {@link ConcurrentHashMap} preso durante a chamada de rede do {@code OPEN}, bloqueando qualquer outra
 * série do mapa. Um lock por chave ({@code openLocks}, construído sem I/O) serializa só as aberturas
 * concorrentes DA MESMA série. A remoção é sempre condicional à instância ({@link #remove}).</p>
 */
final class SeriesHandleCache {

    private final ConcurrentMap<String, RemoteSeriesHandle> handles = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Object> openLocks = new ConcurrentHashMap<>();

    /**
     * Devolve um handle reaproveitável de {@code seriesKey} ou abre um novo com {@code opener} (que faz o
     * {@code OPEN} remoto), publicando-o só se a abertura der certo.
     *
     * @param createIfMissing se o chamador pediu criação — decide entre promover ou só reaproveitar
     * @param opener          cria e abre um handle novo; o que ele lançar sobe ao chamador
     */
    RemoteSeriesHandle open(String seriesKey, boolean createIfMissing, Supplier<RemoteSeriesHandle> opener) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        Objects.requireNonNull(opener, "opener");
        RemoteSeriesHandle reusable = reusable(handles.get(seriesKey), createIfMissing);
        if (reusable != null) {
            return reusable;
        }
        Object lock = openLocks.computeIfAbsent(seriesKey, key -> new Object());
        try {
            try (var guard = CoordinationLocks.acquire(lock)) {
                reusable = reusable(handles.get(seriesKey), createIfMissing);
                if (reusable != null) {
                    return reusable;
                }
                RemoteSeriesHandle handle = opener.get();
                handles.put(seriesKey, handle);
                return handle;
            }
        } finally {
            openLocks.remove(seriesKey, lock);
        }
    }

    /**
     * {@code cached} se puder ser devolvido a um {@code open} com (ou sem) criação, promovendo-o quando
     * preciso; {@code null} se não houver handle ou se ele estiver fechado.
     */
    private static RemoteSeriesHandle reusable(RemoteSeriesHandle cached, boolean createIfMissing) {
        if (cached == null) {
            return null;
        }
        if (createIfMissing) {
            return cached.tryPromoteToWritable() ? cached : null;
        }
        return cached.isOpen() ? cached : null;
    }

    /** Remove {@code handle} de {@code seriesKey} só se ainda for ele o registrado (nunca um handle mais novo). */
    void remove(String seriesKey, RemoteSeriesHandle handle) {
        handles.remove(seriesKey, handle);
    }

    /** Handle registrado para {@code seriesKey}, aberto ou não; {@code null} se não houver. */
    RemoteSeriesHandle get(String seriesKey) {
        return handles.get(seriesKey);
    }

    /** Quantidade de handles registrados. */
    int size() {
        return handles.size();
    }

    /** Cópia dos handles registrados neste instante. */
    List<RemoteSeriesHandle> snapshot() {
        return List.copyOf(handles.values());
    }
}
