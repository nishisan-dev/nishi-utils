package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.NgrrdStorageException;
import dev.nishisan.utils.oss.storage.SeriesChannel;
import dev.nishisan.utils.oss.storage.SeriesChannelProvider;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Garante que uma falha de persistência durante o {@code checkpoint()} (ex.: PUT
 * no S3 / fsync) é <strong>propagada</strong> ao chamador em vez de travá-lo
 * indefinidamente em {@code latch.await()}.
 */
class NgrrdWriterFailureTest {

    private static final long START_MS = 1_747_339_200L * 1000L;

    private NgrrdDefinition definition() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-local-disk.yaml")) {
            return NgrrdYamlLoader.parse(new String(in.readAllBytes(), StandardCharsets.UTF_8), k -> null);
        }
    }

    @Test
    void checkpointPropagaFalhaDeForceSemTravar() throws Exception {
        NgrrdDefinition def = definition();
        FailingStorage storage = new FailingStorage();

        NgrrdWriter writer = new NgrrdWriter(def, storage, "device:r1/iface:eth0");
        try {
            writer.write("in_octets", new Sample(START_MS, 1_000_000L));
            storage.failForce = true; // o create inicial já foi forçado com sucesso

            // Deve lançar (rapidamente), não bloquear para sempre.
            assertTimeoutPreemptively(Duration.ofSeconds(5),
                    () -> assertThrows(RuntimeException.class, writer::checkpoint));
        } finally {
            writer.close(); // shutdown best-effort não deve travar nem lançar
        }
    }

    /** Storage em memória cujo {@code force()} pode ser configurado para falhar. */
    private static final class FailingStorage implements NgrrdStorage, SeriesChannelProvider {

        private final Map<String, byte[]> objects = new HashMap<>();
        volatile boolean failForce = false;

        @Override
        public void put(String key, byte[] data) {
            objects.put(key, data.clone());
        }

        @Override
        public Optional<byte[]> get(String key) {
            byte[] b = objects.get(key);
            return b == null ? Optional.empty() : Optional.of(b.clone());
        }

        @Override
        public boolean exists(String key) {
            return objects.containsKey(key);
        }

        @Override
        public void delete(String key) {
            objects.remove(key);
        }

        @Override
        public List<String> list(String prefix) {
            List<String> out = new ArrayList<>();
            for (String k : objects.keySet()) {
                if (k.startsWith(prefix)) {
                    out.add(k);
                }
            }
            return out;
        }

        @Override
        public void atomicReplace(String key, byte[] data) {
            put(key, data);
        }

        @Override
        public boolean seriesExists(String key) {
            return objects.containsKey(key);
        }

        @Override
        public SeriesChannel openSeries(String key) {
            return new MemChannel(key);
        }

        private final class MemChannel implements SeriesChannel {
            private final String key;
            private byte[] image;

            MemChannel(String key) {
                this.key = key;
                byte[] existing = objects.get(key);
                this.image = existing == null ? new byte[0] : existing.clone();
            }

            @Override
            public long size() {
                return image.length;
            }

            @Override
            public void allocate(long totalBytes) {
                int n = (int) totalBytes;
                if (image.length != n) {
                    image = Arrays.copyOf(image, n);
                }
            }

            @Override
            public byte[] readRegion(long offset, int len) {
                int off = (int) offset;
                return Arrays.copyOfRange(image, off, off + len);
            }

            @Override
            public void writeRegion(long offset, byte[] data) {
                int off = (int) offset;
                int end = off + data.length;
                if (end > image.length) {
                    image = Arrays.copyOf(image, end);
                }
                System.arraycopy(data, 0, image, off, data.length);
            }

            @Override
            public void force() {
                if (failForce) {
                    throw new NgrrdStorageException("falha simulada de force()");
                }
                objects.put(key, image.clone());
            }

            @Override
            public void close() {
                force();
            }
        }
    }
}
