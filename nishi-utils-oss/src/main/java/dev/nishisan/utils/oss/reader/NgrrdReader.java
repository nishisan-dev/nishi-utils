package dev.nishisan.utils.oss.reader;

import dev.nishisan.utils.oss.api.DataPoint;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.definition.RraDef;
import dev.nishisan.utils.oss.engine.BestFitSelector;
import dev.nishisan.utils.oss.format.NgrrdFormatException;
import dev.nishisan.utils.oss.format.SeriesFileCodec;
import dev.nishisan.utils.oss.format.SeriesGeometry;
import dev.nishisan.utils.oss.format.SeriesHeader;
import dev.nishisan.utils.oss.format.SeriesLiveState;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.SeriesChannel;
import dev.nishisan.utils.oss.storage.SeriesChannelProvider;
import dev.nishisan.utils.oss.storage.StorageKey;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Materializa séries temporais a partir do objeto único NGRR. Lê os ring buffers
 * diretamente do arquivo (sem manifesto): escolhe o RRA via
 * {@link BestFitSelector}, mapeia {@code (rra, cf)} para o archive, reconstrói os
 * timestamps pelo ponteiro do ring e recorta para a janela solicitada.
 *
 * <p>Quando construído com o {@link ReadWriteLock} compartilhado com o writer da
 * série (caso do {@code NgrrdHandle}), a sequência live-state → ring é lida sob
 * o read-lock — atômica em relação às mutações do writer e em paralelo com
 * outros leitores. A abertura do canal (GET no S3) ocorre fora do lock.</p>
 */
public final class NgrrdReader {

    private final NgrrdDefinition definition;
    private final SeriesChannelProvider provider;
    private final String seriesKey;
    private final SeriesGeometry geo;
    private final byte[] geometryHash;
    private final String storageKey;
    private final Lock readLock;

    public NgrrdReader(NgrrdDefinition definition, NgrrdStorage storage, String seriesKey) {
        this(definition, storage, seriesKey, new ReentrantReadWriteLock());
    }

    /**
     * Variante com {@link ReadWriteLock} compartilhado com o writer da mesma
     * série (mesmo processo), garantindo leituras consistentes concorrentes.
     */
    public NgrrdReader(NgrrdDefinition definition, NgrrdStorage storage, String seriesKey,
                       ReadWriteLock seriesLock) {
        this.definition = Objects.requireNonNull(definition, "definition é obrigatório");
        Objects.requireNonNull(storage, "storage é obrigatório");
        this.seriesKey = Objects.requireNonNull(seriesKey, "seriesKey é obrigatório");
        if (!(storage instanceof SeriesChannelProvider p)) {
            throw new IllegalArgumentException(
                    "storage não suporta SeriesChannel: " + storage.getClass().getName());
        }
        this.provider = p;
        this.geo = new SeriesGeometry(definition);
        this.geometryHash = geo.geometryHash();
        this.storageKey = StorageKey.series(definition.spec().storage().objectNaming(), seriesKey);
        this.readLock = Objects.requireNonNull(seriesLock, "seriesLock é obrigatório").readLock();
    }

    /** Variante de conveniência que usa {@link System#currentTimeMillis()} como fim do range. */
    public SeriesResult read(String dsName, ViewQuery query) {
        return read(dsName, query, System.currentTimeMillis());
    }

    /**
     * Lê o DS derivado {@code dsName} no range
     * {@code [endExclusiveEpochMs - window, endExclusiveEpochMs)}.
     */
    public SeriesResult read(String dsName, ViewQuery query, long endExclusiveEpochMs) {
        Objects.requireNonNull(dsName, "dsName é obrigatório");
        Objects.requireNonNull(query, "query é obrigatório");

        Optional<RraDef> rraOpt = BestFitSelector.pick(query, definition.spec().archives().rras());
        if (rraOpt.isEmpty()) {
            return new SeriesResult(dsName, null, query.cf(), query.targetStepSec(), List.of());
        }
        RraDef rra = rraOpt.get();
        int col = geo.columnIndex(dsName);
        int arch = geo.archiveIndex(rra.name(), query.cf());
        if (col < 0 || arch < 0 || !provider.seriesExists(storageKey)) {
            return new SeriesResult(dsName, rra.name(), query.cf(), rra.stepSec(), List.of());
        }

        try (SeriesChannel channel = provider.openSeries(storageKey)) {
            List<DataPoint> points;
            readLock.lock();
            try {
                points = readPoints(channel, arch, col, query, endExclusiveEpochMs);
            } finally {
                readLock.unlock();
            }
            List<DataPoint> downsampled = downsample(points, query.maxPoints());
            return new SeriesResult(dsName, rra.name(), query.cf(), rra.stepSec(), downsampled);
        } catch (NgrrdFormatException e) {
            return new SeriesResult(dsName, rra.name(), query.cf(), rra.stepSec(), List.of());
        }
    }

    private List<DataPoint> readPoints(SeriesChannel channel, int arch, int col,
                                       ViewQuery query, long endExclusiveEpochMs) {
        if (channel.size() < SeriesFileCodec.FIXED_HEADER_BYTES) {
            return List.of();
        }
        SeriesHeader header = SeriesFileCodec.decodeFixedHeader(
                channel.readRegion(0, SeriesFileCodec.FIXED_HEADER_BYTES));
        boolean compatible = header.formatVersion() == SeriesFileCodec.CURRENT_VERSION
                && header.fileTotalBytes() == geo.fileTotalBytes()
                && Arrays.equals(header.definitionHash(), geometryHash);
        if (!compatible) {
            return List.of(); // geometria divergente (em recriação) — sem dados ainda.
        }
        SeriesLiveState live = SeriesFileCodec.decodeLiveState(geo,
                channel.readRegion(geo.liveStateOffset(), (int) geo.liveStateBytes()));

        SeriesGeometry.Archive archive = geo.archives().get(arch);
        int rows = archive.rows();
        int s = archive.stepSec();
        int curRow = live.curRow[arch];
        long anchor = live.curRowEpochSec[arch];
        if (curRow < 0) {
            return List.of(); // ring nunca escrito.
        }

        long endSec = endExclusiveEpochMs / 1000L;
        long startSec = endSec - query.window().getSeconds();

        // Lê a região inteira do ring deste archive de uma vez (uma syscall).
        int d = geo.columnCount();
        byte[] ring = channel.readRegion(archive.ringBaseOffset(), rows * d * Double.BYTES);
        ByteBuffer buf = ByteBuffer.wrap(ring).order(ByteOrder.BIG_ENDIAN);

        List<DataPoint> result = new ArrayList<>();
        // j: 0 = linha mais antiga do ring; rows-1 = mais nova (anchor).
        for (int j = 0; j < rows; j++) {
            int distanceFromNewest = rows - 1 - j;
            long tsSec = anchor - (long) distanceFromNewest * s;
            if (tsSec < startSec || tsSec >= endSec) {
                continue;
            }
            int row = ((curRow - distanceFromNewest) % rows + rows) % rows;
            double v = buf.getDouble((row * d + col) * Double.BYTES);
            result.add(new DataPoint(tsSec * 1000L, v));
        }
        return result;
    }

    private List<DataPoint> downsample(List<DataPoint> points, int maxPoints) {
        if (points.size() <= maxPoints) {
            return points;
        }
        List<DataPoint> out = new ArrayList<>(maxPoints);
        double step = (double) points.size() / maxPoints;
        for (int i = 0; i < maxPoints; i++) {
            int idx = (int) Math.floor(i * step);
            if (idx >= points.size()) {
                idx = points.size() - 1;
            }
            out.add(points.get(idx));
        }
        return out;
    }
}
