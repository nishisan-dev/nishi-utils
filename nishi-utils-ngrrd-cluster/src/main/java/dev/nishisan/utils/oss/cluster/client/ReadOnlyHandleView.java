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
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.cluster.api.ErrorCode;
import dev.nishisan.utils.oss.cluster.api.NgrrdClusterException;

import java.util.Map;
import java.util.Objects;

/**
 * Vista somente leitura sobre um {@link RemoteSeriesHandle} gravável — o que um {@code open} sem criar
 * recebe quando a chave já tem um handle gravável aberto no cliente (ex.: a ingestão).
 *
 * <p>As leituras delegam ao gravável; {@link #write}, {@link #flush} e {@link #checkpoint} lançam
 * {@link IllegalStateException} com a mesma mensagem do handle somente leitura. O {@link #close()} só
 * fecha a vista — operações posteriores nela lançam {@link NgrrdClusterException} com
 * {@link ErrorCode#CLOSED} — e NUNCA fecha o gravável, que continua escrevendo para quem o abriu com
 * criação. A vista não entra no mapa de handles do cliente; se o gravável for fechado, as leituras pela
 * vista falham como falhariam nele.</p>
 */
final class ReadOnlyHandleView implements NgrrdHandle {

    private final RemoteSeriesHandle delegate;
    private volatile boolean closed;

    ReadOnlyHandleView(RemoteSeriesHandle delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public String seriesKey() {
        return delegate.seriesKey();
    }

    @Override
    public void write(String dsName, Sample sample) {
        throw RemoteSeriesHandle.readOnlyViolation(delegate.seriesKey());
    }

    @Override
    public void flush() {
        throw RemoteSeriesHandle.readOnlyViolation(delegate.seriesKey());
    }

    @Override
    public void checkpoint() {
        throw RemoteSeriesHandle.readOnlyViolation(delegate.seriesKey());
    }

    @Override
    public SeriesResult read(String dsName, ViewQuery query) {
        ensureOpen();
        return delegate.read(dsName, query);
    }

    @Override
    public SeriesResult read(String dsName, ViewQuery query, long endExclusiveEpochMs) {
        ensureOpen();
        return delegate.read(dsName, query, endExclusiveEpochMs);
    }

    @Override
    public Map<String, SeriesResult> read(String presetName) {
        ensureOpen();
        return delegate.read(presetName);
    }

    @Override
    public Map<String, SeriesResult> read(String presetName, long endExclusiveEpochMs) {
        ensureOpen();
        return delegate.read(presetName, endExclusiveEpochMs);
    }

    /** Fecha só a vista: local, sem RPC, sem tocar o handle gravável. Idempotente. */
    @Override
    public void close() {
        closed = true;
    }

    private void ensureOpen() {
        if (closed) {
            throw new NgrrdClusterException(ErrorCode.CLOSED, "handle fechado: " + delegate.seriesKey());
        }
    }
}
