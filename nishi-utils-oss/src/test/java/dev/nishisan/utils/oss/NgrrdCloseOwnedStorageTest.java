package dev.nishisan.utils.oss;

import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Cobre {@code Ngrrd.closeOwnedStorage}: helper interno compartilhado entre
 * {@code buildHandle} (fecha um storage próprio quando o construtor do
 * {@code NgrrdWriter} falha — ex.: {@link SeriesNotFoundException} com
 * {@code createIfMissing=false}) e {@code existsInStorage} (fecha após a
 * consulta de existência).
 *
 * <p>Testado no nível mais baixo possível: os call-sites reais em que o
 * storage é próprio (não compartilhado) usam backend {@code OBJECT_STORAGE}
 * ({@code S3Storage}, cujo {@code seriesExists} faz um HEAD de rede real) — um
 * teste fim-a-fim exigiria um endpoint S3 alcançável, o que pertence às IT's
 * de {@code ngrrd-integration}, não a um teste unitário rápido. Aqui, o
 * fechamento/supressão de exceção é isolado com um {@link NgrrdStorage}
 * falso que também implementa {@link AutoCloseable} (como {@code S3Storage}
 * faz de verdade).</p>
 */
class NgrrdCloseOwnedStorageTest {

    private static final class CountingStorage implements NgrrdStorage, AutoCloseable {
        final AtomicInteger closeCount = new AtomicInteger();
        RuntimeException failOnClose;

        @Override
        public void put(String key, byte[] data) {
        }

        @Override
        public Optional<byte[]> get(String key) {
            return Optional.empty();
        }

        @Override
        public boolean exists(String key) {
            return false;
        }

        @Override
        public void delete(String key) {
        }

        @Override
        public List<String> list(String prefix) {
            return List.of();
        }

        @Override
        public void atomicReplace(String key, byte[] data) {
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
            if (failOnClose != null) {
                throw failOnClose;
            }
        }
    }

    @Test
    void fechaStorageProprioUmaVezAposSeriesNotFoundException() {
        CountingStorage storage = new CountingStorage();
        SeriesNotFoundException primary = new SeriesNotFoundException("device:r1/iface:eth0");

        Ngrrd.closeOwnedStorage(storage, true, primary);

        assertEquals(1, storage.closeCount.get());
        assertEquals(0, primary.getSuppressed().length);
    }

    @Test
    void naoFechaStorageCompartilhado() {
        CountingStorage storage = new CountingStorage();

        // ownsStorage=false: caso do backend SHARDED_BLOB, gerido pelo
        // BlobVolumeRegistry — nunca deve ser fechado por este helper.
        Ngrrd.closeOwnedStorage(storage, false, new SeriesNotFoundException("k"));

        assertEquals(0, storage.closeCount.get());
    }

    @Test
    void falhaAoFecharVaiComoSuprimidaNaFalhaPrimaria() {
        CountingStorage storage = new CountingStorage();
        storage.failOnClose = new IllegalStateException("close falhou");
        SeriesNotFoundException primary = new SeriesNotFoundException("k");

        Ngrrd.closeOwnedStorage(storage, true, primary);

        assertEquals(1, storage.closeCount.get());
        assertEquals(1, primary.getSuppressed().length);
        assertSame(storage.failOnClose, primary.getSuppressed()[0]);
    }

    @Test
    void semFalhaPrimariaFalhaAoFecharNaoPropaga() {
        CountingStorage storage = new CountingStorage();
        storage.failOnClose = new IllegalStateException("close falhou");

        // existsInStorage no caminho feliz: nenhuma exceção em curso para
        // anexar a falha de close — não deve propagar (apenas registrada).
        Ngrrd.closeOwnedStorage(storage, true, null);

        assertEquals(1, storage.closeCount.get());
    }
}
