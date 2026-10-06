package dev.nishisan.utils.oss.blob;

import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.storage.blob.BlobStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Equivalência byte a byte entre os modos de escrita: a mesma sequência determinística de
 * escritas, aplicada a um volume {@code MMAP} e a outro {@code PWRITE}, deve produzir o mesmo
 * objeto de série. O superblock de cada shard tem UUID aleatório, então a comparação é feita
 * sobre o objeto lido pela API de storage, e não sobre o arquivo de shard inteiro.
 */
class VolumeWriteModeEquivalenceTest {

    private static final long BLOCK_START_MS = 1_747_339_200L * 1000L;
    private static final int STEP_MS = 300_000;
    private static final long SEG = 1L << 20;
    private static final List<String> SERIES = List.of("r1/iface:eth0", "r2/iface:eth1", "r3/iface:eth2");

    private String loadYaml() throws Exception {
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-blob.yaml")) {
            return new String(in.readAllBytes());
        }
    }

    private BlobVolumeRegistry registry(Path base, VolumeWriteMode mode) {
        return NgrrdBlob.registry()
                .basePath(base).shardCount(4).segmentBytes(SEG)
                .writeMode(mode)
                .volume("ifaceStats")
                .build();
    }

    /** Sequência determinística: rampa, lacuna de passos, mais amostras, checkpoint e close do handle. */
    private void applyWrites(BlobVolumeRegistry registry, String series, boolean secondPhase) throws Exception {
        NgrrdUri locator = NgrrdUri.parse("ngrrd://ifaceStats/device:" + series);
        try (NgrrdHandle h = Ngrrd.open(registry, locator, loadYaml())) {
            long octets = secondPhase ? 10_000_000L : 0L;
            int firstStep = secondPhase ? 40 : 0;
            for (int i = 0; i < 8; i++) {
                octets += 50_000L;
                long ts = BLOCK_START_MS + (firstStep + i) * (long) STEP_MS;
                h.write("in_octets", new Sample(ts, octets));
                h.write("out_octets", new Sample(ts, octets / 2));
            }
            // lacuna de 30 passos
            int gapStep = firstStep + 8 + 30;
            for (int i = 0; i < 4; i++) {
                octets += 70_000L;
                long ts = BLOCK_START_MS + (gapStep + i) * (long) STEP_MS;
                h.write("in_octets", new Sample(ts, octets));
                h.write("out_octets", new Sample(ts, octets / 3));
            }
            h.flush();
        }
        registry.require("ifaceStats").checkpoint();
    }

    private static String key(String series) {
        return "series/device:" + series + ".ngrr";
    }

    @Test
    void mmapEPwriteProduzemOMesmoObjetoDeSerie(@TempDir Path tmp) throws Exception {
        Path mmapBase = tmp.resolve("mmap");
        Path pwriteBase = tmp.resolve("pwrite");
        try (BlobVolumeRegistry mm = registry(mmapBase, VolumeWriteMode.MMAP);
             BlobVolumeRegistry pw = registry(pwriteBase, VolumeWriteMode.PWRITE)) {
            for (String s : SERIES) {
                applyWrites(mm, s, false);
                applyWrites(pw, s, false);
            }
            BlobStorage a = mm.require("ifaceStats").storage();
            BlobStorage b = pw.require("ifaceStats").storage();
            assertEquals(VolumeWriteMode.MMAP, a.writeMode());
            assertEquals(VolumeWriteMode.PWRITE, b.writeMode());
            for (String s : SERIES) {
                byte[] fromMmap = a.get(key(s)).orElseThrow();
                byte[] fromPwrite = b.get(key(s)).orElseThrow();
                assertArrayEquals(fromMmap, fromPwrite, "objeto divergente para " + s);
            }
            assertTrue(a.bytesWritten() > 0 && b.bytesWritten() > 0);
            // todo byte do volume (inclusive superblocks) passa pelo caminho do modo: nada vaza para o outro
            assertEquals(0L, a.positionalBytesWritten(), "MMAP não pode usar o caminho posicional");
            assertEquals(a.bytesWritten(), a.mappedBytesWritten());
            assertEquals(0L, b.mappedBytesWritten(), "PWRITE não pode escrever pelo mmap");
            assertEquals(b.bytesWritten(), b.positionalBytesWritten());
            assertEquals(a.bytesWritten(), b.bytesWritten(), "mesmos bytes lógicos nos dois modos");
        }
    }

    @Test
    void mmapEPwriteProduzemOMesmoObjetoAposReabrir(@TempDir Path tmp) throws Exception {
        Path mmapBase = tmp.resolve("mmap");
        Path pwriteBase = tmp.resolve("pwrite");
        try (BlobVolumeRegistry mm = registry(mmapBase, VolumeWriteMode.MMAP);
             BlobVolumeRegistry pw = registry(pwriteBase, VolumeWriteMode.PWRITE)) {
            for (String s : SERIES) {
                applyWrites(mm, s, false);
                applyWrites(pw, s, false);
            }
        }
        // reabre cada volume no modo OPOSTO ao da escrita e aplica uma segunda fase de escritas
        try (BlobVolumeRegistry mm = registry(mmapBase, VolumeWriteMode.PWRITE);
             BlobVolumeRegistry pw = registry(pwriteBase, VolumeWriteMode.MMAP)) {
            for (String s : SERIES) {
                applyWrites(mm, s, true);
                applyWrites(pw, s, true);
            }
            BlobStorage a = mm.require("ifaceStats").storage();
            BlobStorage b = pw.require("ifaceStats").storage();
            for (String s : SERIES) {
                assertArrayEquals(a.get(key(s)).orElseThrow(), b.get(key(s)).orElseThrow(),
                        "objeto divergente após reabrir para " + s);
            }
        }
        // e uma última abertura, sem escrita, no modo padrão
        try (BlobVolumeRegistry mm = registry(mmapBase, VolumeWriteMode.MMAP);
             BlobVolumeRegistry pw = registry(pwriteBase, VolumeWriteMode.MMAP)) {
            for (String s : SERIES) {
                assertArrayEquals(mm.require("ifaceStats").storage().get(key(s)).orElseThrow(),
                        pw.require("ifaceStats").storage().get(key(s)).orElseThrow(),
                        "objeto divergente na releitura para " + s);
            }
        }
    }

    @Test
    void statsDoVolumeExpoeModoEBytesEscritos(@TempDir Path tmp) throws Exception {
        try (BlobVolumeRegistry pw = registry(tmp.resolve("pwrite"), VolumeWriteMode.PWRITE)) {
            long before = pw.require("ifaceStats").storage().stats().bytesWritten();
            applyWrites(pw, SERIES.get(0), false);
            var stats = pw.require("ifaceStats").storage().stats();
            assertEquals(VolumeWriteMode.PWRITE, stats.writeMode());
            assertTrue(stats.bytesWritten() > before, "bytesWritten deve crescer com as escritas");
        }
    }
}
