package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.ViewQuery;
import dev.nishisan.utils.oss.config.NgrrdDefinitionValidator;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.reader.NgrrdReader;
import dev.nishisan.utils.oss.storage.LocalDiskStorage;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.SeriesChannel;
import dev.nishisan.utils.oss.storage.SeriesChannelProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre o gating do {@code fsync} por {@link Durability}: em {@link Durability#FSYNC}
 * cada checkpoint força o canal; em {@link Durability#OS_CACHE} o checkpoint
 * materializa os bytes (legíveis via page cache) mas não força — só um
 * {@code close()} limpo descarrega o pendente.
 *
 * <p>O storage de teste delega a um {@link LocalDiskStorage} real (escrita
 * in-place num arquivo, page cache compartilhado entre writer e reader) e conta
 * as chamadas a {@link SeriesChannel#force()}.</p>
 */
class NgrrdWriterDurabilityTest {

    private static final long START_SEC = 1_747_339_200L; // alinhado a 6h
    private static final long START_MS = START_SEC * 1000L;
    private static final int STEP_MS = 300_000;
    private static final double EXPECTED_BPS = 8 * 100_000.0 / 300.0; // 2666.67 bit/s
    private static final String SERIES_KEY = "device:r1/iface:eth0";

    private NgrrdDefinition definition() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-local-disk.yaml")) {
            NgrrdDefinition def = NgrrdYamlLoader.parse(
                    new String(in.readAllBytes(), StandardCharsets.UTF_8), k -> null);
            NgrrdDefinitionValidator.validate(def);
            return def;
        }
    }

    private NgrrdWriter writer(NgrrdDefinition def, NgrrdStorage storage, Durability durability) {
        return new NgrrdWriter(def, storage, SERIES_KEY, null,
                new ReentrantReadWriteLock(), durability);
    }

    private List<Double> readInBps(NgrrdDefinition def, NgrrdStorage storage, long endMs) {
        NgrrdReader reader = new NgrrdReader(def, storage, SERIES_KEY);
        ViewQuery query = new ViewQuery(Duration.ofHours(1), 300, ConsolidationFunction.AVERAGE, 100);
        SeriesResult result = reader.read("in_bps", query, endMs);
        return result.points().stream().map(p -> p.value())
                .filter(v -> !Double.isNaN(v)).toList();
    }

    @Test
    void fsyncForcaPorCheckpoint(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = definition();
        ForceCountingStorage storage = new ForceCountingStorage(tempDir);

        try (NgrrdWriter w = writer(def, storage, Durability.FSYNC)) {
            int afterCreate = storage.forceCount.get(); // createFresh já forçou (=1)
            w.write("in_octets", new Sample(START_MS, 1_000_000L));
            w.write("in_octets", new Sample(START_MS + STEP_MS, 1_100_000L));
            w.checkpoint();
            assertEquals(afterCreate + 1, storage.forceCount.get(),
                    "FSYNC deveria forçar uma vez por checkpoint");
            w.checkpoint();
            assertEquals(afterCreate + 2, storage.forceCount.get(),
                    "cada checkpoint adicional em FSYNC deveria forçar de novo");
        }
    }

    @Test
    void osCacheNaoForcaPorCheckpoint(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = definition();
        ForceCountingStorage storage = new ForceCountingStorage(tempDir);

        try (NgrrdWriter w = writer(def, storage, Durability.OS_CACHE)) {
            int afterCreate = storage.forceCount.get();
            w.write("in_octets", new Sample(START_MS, 1_000_000L));
            w.write("in_octets", new Sample(START_MS + STEP_MS, 1_100_000L));
            w.checkpoint();
            w.checkpoint();
            assertEquals(afterCreate, storage.forceCount.get(),
                    "OS_CACHE não deveria forçar por checkpoint");

            // Visibilidade preservada: o CDP parcial é legível via page cache
            // (mesmo arquivo), independentemente do fsync.
            List<Double> values = readInBps(def, storage, START_MS + 2L * STEP_MS);
            assertTrue(values.contains(EXPECTED_BPS),
                    "OS_CACHE deveria manter o CDP parcial legível sem fsync: " + values);
        }
    }

    @Test
    void closeDescarregaMesmoEmOsCache(@TempDir Path tempDir) throws Exception {
        NgrrdDefinition def = definition();
        ForceCountingStorage storage = new ForceCountingStorage(tempDir);

        NgrrdWriter w = writer(def, storage, Durability.OS_CACHE);
        w.write("in_octets", new Sample(START_MS, 1_000_000L));
        w.write("in_octets", new Sample(START_MS + STEP_MS, 1_100_000L));
        w.checkpoint();
        int beforeClose = storage.forceCount.get();

        w.close(); // shutdown limpo deve descarregar o pendente

        assertTrue(storage.forceCount.get() > beforeClose,
                "um close() limpo deveria forçar mesmo em OS_CACHE");
    }

    /** Storage que delega ao {@link LocalDiskStorage} e conta chamadas a {@code force()}. */
    private static final class ForceCountingStorage implements NgrrdStorage, SeriesChannelProvider {

        private final LocalDiskStorage delegate;
        final AtomicInteger forceCount = new AtomicInteger();

        ForceCountingStorage(Path root) {
            this.delegate = new LocalDiskStorage(root);
        }

        @Override
        public void put(String key, byte[] data) {
            delegate.put(key, data);
        }

        @Override
        public Optional<byte[]> get(String key) {
            return delegate.get(key);
        }

        @Override
        public boolean exists(String key) {
            return delegate.exists(key);
        }

        @Override
        public void delete(String key) {
            delegate.delete(key);
        }

        @Override
        public List<String> list(String prefix) {
            return delegate.list(prefix);
        }

        @Override
        public void atomicReplace(String key, byte[] data) {
            delegate.atomicReplace(key, data);
        }

        @Override
        public boolean seriesExists(String key) {
            return delegate.seriesExists(key);
        }

        @Override
        public SeriesChannel openSeries(String key) {
            return new CountingChannel(delegate.openSeries(key));
        }

        private final class CountingChannel implements SeriesChannel {
            private final SeriesChannel inner;

            CountingChannel(SeriesChannel inner) {
                this.inner = inner;
            }

            @Override
            public long size() {
                return inner.size();
            }

            @Override
            public void allocate(long totalBytes) {
                inner.allocate(totalBytes);
            }

            @Override
            public byte[] readRegion(long offset, int len) {
                return inner.readRegion(offset, len);
            }

            @Override
            public void writeRegion(long offset, byte[] data) {
                inner.writeRegion(offset, data);
            }

            @Override
            public void force() {
                forceCount.incrementAndGet();
                inner.force();
            }

            @Override
            public void close() {
                // O caminho de fechamento dispara a durabilidade: força (contado)
                // e fecha o canal interno (cujo force() interno é no-op se já limpo).
                force();
                inner.close();
            }
        }
    }
}
