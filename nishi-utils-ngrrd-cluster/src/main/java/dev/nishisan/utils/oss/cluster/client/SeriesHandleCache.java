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

import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.cluster.rpc.CoordinationLocks;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * Handles abertos pelo cliente — no máximo UM handle principal por chave de série — com as regras de
 * reaproveitamento de {@code DefaultNgrrdClusterClient.open}:
 * <ul>
 *   <li>{@code open} com criação: principal gravável aberto → devolve-o; principal somente leitura
 *       aberto → abre um gravável NOVO (com as opções de quem pediu criar) e o publica no lugar do
 *       somente leitura, que fica destacado do mapa (segue válido para quem o tem, com close local);
 *       sem principal ou principal fechado → abre um gravável e o publica;</li>
 *   <li>{@code open} sem criar: principal gravável aberto → devolve uma {@link ReadOnlyHandleView} sobre
 *       ele (o close da vista nunca fecha o gravável; a vista não entra no mapa); principal somente
 *       leitura aberto → devolve-o (compartilhado); sem principal ou principal fechado → abre um somente
 *       leitura e o publica.</li>
 * </ul>
 *
 * <p>Toda publicação e remoção é condicional à instância ({@code putIfAbsent}, {@code replace(key, antigo,
 * novo)}, {@code remove(key, handle)}): se o principal mudou entre a decisão e a publicação, a decisão é
 * refeita com o handle já aberto — ele é publicado sobre o principal novo ou, se este já servir ao
 * chamador, descartado localmente ({@link RemoteSeriesHandle#discard()}), sem nenhum RPC a mais.</p>
 *
 * <p>Nunca faz RPC dentro de {@code computeIfAbsent} — isso manteria o bin lock interno do
 * {@link ConcurrentHashMap} preso durante a chamada de rede do {@code OPEN}, bloqueando qualquer outra
 * série do mapa. O lock por chave ({@code openLocks}, construído sem I/O, adquirido via
 * {@link CoordinationLocks}) serializa as aberturas DA MESMA série, e o {@code OPEN} remoto do handle novo
 * roda sob ele, como na 8.5.0 — é um {@code ReentrantLock}, que libera o carrier de uma virtual thread
 * durante a espera, e só bloqueia quem abre a mesma chave. Ele evita que aberturas concorrentes abram
 * vários handles; a correção não depende dele — depende da publicação condicional.</p>
 */
final class SeriesHandleCache {

    private final ConcurrentMap<String, RemoteSeriesHandle> handles = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Object> openLocks = new ConcurrentHashMap<>();

    /**
     * Devolve o handle de {@code seriesKey} segundo as regras da classe, abrindo um novo com
     * {@code opener} (que faz o {@code OPEN} remoto) quando nada no mapa serve — o handle novo só é
     * publicado se a abertura der certo.
     *
     * @param createIfMissing se o chamador pediu criação
     * @param opener          cria e abre um handle novo com as opções do chamador; o que ele lançar sobe
     *                        ao chamador
     */
    NgrrdHandle open(String seriesKey, boolean createIfMissing, Supplier<RemoteSeriesHandle> opener) {
        Objects.requireNonNull(seriesKey, "seriesKey");
        Objects.requireNonNull(opener, "opener");
        NgrrdHandle reusable = reusable(handles.get(seriesKey), createIfMissing);
        if (reusable != null) {
            return reusable;
        }
        Object lock = openLocks.computeIfAbsent(seriesKey, key -> new Object());
        try {
            try (var guard = CoordinationLocks.acquire(lock)) {
                RemoteSeriesHandle opened = null;
                for (;;) {
                    RemoteSeriesHandle current = handles.get(seriesKey);
                    reusable = reusable(current, createIfMissing);
                    if (reusable != null) {
                        if (opened != null) {
                            opened.discard();
                        }
                        return reusable;
                    }
                    if (opened == null) {
                        opened = opener.get();
                    }
                    if (publish(seriesKey, current, opened)) {
                        return opened;
                    }
                }
            }
        } finally {
            openLocks.remove(seriesKey, lock);
        }
    }

    /**
     * O que um {@code open} com (ou sem) criação recebe do principal {@code current}: o próprio handle,
     * uma vista somente leitura sobre ele, ou {@code null} se for preciso abrir um novo (sem principal,
     * principal fechado, ou somente leitura diante de um pedido de criação).
     */
    private static NgrrdHandle reusable(RemoteSeriesHandle current, boolean createIfMissing) {
        if (current == null || !current.isOpen()) {
            return null;
        }
        if (current.isWritable()) {
            return createIfMissing ? current : new ReadOnlyHandleView(current);
        }
        return createIfMissing ? null : current;
    }

    /** Publica {@code opened} no lugar de {@code current} só se o principal ainda for {@code current}. */
    private boolean publish(String seriesKey, RemoteSeriesHandle current, RemoteSeriesHandle opened) {
        if (current == null) {
            return handles.putIfAbsent(seriesKey, opened) == null;
        }
        return handles.replace(seriesKey, current, opened);
    }

    /** Remove {@code handle} de {@code seriesKey} só se ainda for ele o registrado (nunca um handle mais novo). */
    void remove(String seriesKey, RemoteSeriesHandle handle) {
        handles.remove(seriesKey, handle);
    }

    /** Handle principal registrado para {@code seriesKey}, aberto ou não; {@code null} se não houver. */
    RemoteSeriesHandle get(String seriesKey) {
        return handles.get(seriesKey);
    }

    /** Quantidade de handles principais registrados. */
    int size() {
        return handles.size();
    }

    /** Cópia dos handles principais registrados neste instante. */
    List<RemoteSeriesHandle> snapshot() {
        return List.copyOf(handles.values());
    }
}
