package dev.nishisan.utils.oss.cluster.node;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.nishisan.utils.oss.cluster.catalog.SeriesDeletion;
import dev.nishisan.utils.oss.cluster.catalog.SeriesPlacement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.io.TempDir;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import static dev.nishisan.utils.oss.cluster.node.SeriesLifecycleJournal.Phase.*;
import static org.junit.jupiter.api.Assertions.*;

class SeriesLifecycleJournalCompatibilityTest {
    private final ObjectMapper mapper = new ObjectMapper();
    @TempDir Path base;

    // Exact field shape read by 8.10.2; no normalization in this reader.
    record LegacyEntry(String generationId, long receivedThrough,
                       SeriesLifecycleJournal.Phase phase, SeriesPlacement placement) { }
    record LegacyUpdate(String key, LegacyEntry entry) { }

    @Test void legacyReplayAndCompactionDropOnlyActivePlacementAndRemainReadable() throws Exception {
        var placement = SeriesPlacement.active("storage-209", 1_790_000_000_000L)
                .withDeletion(new SeriesDeletion("delete-op", 3_600_000L, List.of("storage-209"), true), 42);
        var active = new LegacyEntry(placement.generationId(), 3_600_000L, ACTIVE, placement);
        Map<String, LegacyEntry> expected = new LinkedHashMap<>();
        expected.put("active", new LegacyEntry(active.generationId(), active.receivedThrough(), ACTIVE, null));
        try (var out = new DataOutputStream(Files.newOutputStream(base.resolve("series-lifecycle.wal")))) {
            writeLegacy(out, new LegacyUpdate("active", active));
            for (var phase : List.of(PREPARED, COMMITTED, DELETED, FINISHED, QUARANTINED)) {
                var entry = new LegacyEntry(placement.generationId(), 3_600_000L, phase, placement);
                expected.put(phase.name(), entry);
                writeLegacy(out, new LegacyUpdate(phase.name(), entry));
            }
        }
        try (var journal = new SeriesLifecycleJournal(base, 1)) {
            assertNull(journal.get("active").placement());
            long forces = journal.fsyncCount();
            journal.put("active", new SeriesLifecycleJournal.Entry(active.generationId(), active.receivedThrough(), ACTIVE, placement));
            assertEquals(forces, journal.fsyncCount(), "legacy ACTIVE is normalized before no-op comparison");
            journal.put("new", new SeriesLifecycleJournal.Entry("new-generation", 7_200_000L, ACTIVE, placement));
            expected.put("new", new LegacyEntry("new-generation", 7_200_000L, ACTIVE, null));
            // A single larger update crosses the threshold based on the legacy file size.
            String largeKey = "key-" + "x".repeat(20_000);
            journal.put(largeKey, new SeriesLifecycleJournal.Entry("new-generation", 7_200_000L, ACTIVE, null));
            expected.put(largeKey, new LegacyEntry("new-generation", 7_200_000L, ACTIVE, null));
            assertTrue(journal.compactionCount() > 0);
        }
        assertEquals(expected, readLegacy(base.resolve("series-lifecycle.wal")));
        try (var reopened = new SeriesLifecycleJournal(base)) {
            assertEquals(expected.size(), reopened.snapshot().size());
            assertEquals(placement, reopened.get(COMMITTED.name()).placement());
        }
    }

    @Test
    @EnabledIf("legacyReaderConfigured")
    void actual8102ReaderAcceptsNewActiveEntriesAndAdvancesReceipts() throws Exception {
        try (var journal = new SeriesLifecycleJournal(base, 1)) {
            journal.put("active", new SeriesLifecycleJournal.Entry("generation", 3_600_000L, ACTIVE, null));
        }
        String name = SeriesLifecycleJournal.class.getName();
        try (var loader = new URLClassLoader(new java.net.URL[]{legacyReaderClasses().toUri().toURL()}, getClass().getClassLoader()) {
            @Override protected synchronized Class<?> loadClass(String className, boolean resolve) throws ClassNotFoundException {
                if (!className.equals(name) && !className.startsWith(name + "$")) return super.loadClass(className, resolve);
                Class<?> loaded = findLoadedClass(className);
                if (loaded == null) loaded = findClass(className);
                if (resolve) resolveClass(loaded);
                return loaded;
            }
        }) {
            Class<?> journalClass = loader.loadClass(name);
            Object oldJournal = journalClass.getConstructor(Path.class).newInstance(base);
            try {
                Object entry = journalClass.getMethod("get", String.class).invoke(oldJournal, "active");
                assertNull(entry.getClass().getMethod("placement").invoke(entry));
                Class<?> phase = loader.loadClass(name + "$Phase");
                @SuppressWarnings({"unchecked", "rawtypes"})
                Object active = Enum.valueOf((Class) phase, "ACTIVE");
                Class<?> entryClass = loader.loadClass(name + "$Entry");
                Object next = entryClass.getConstructor(String.class, long.class, phase, SeriesPlacement.class)
                        .newInstance("generation", 7_200_000L, active, null);
                journalClass.getMethod("put", String.class, entryClass).invoke(oldJournal, "active", next);
            } finally { ((AutoCloseable) oldJournal).close(); }
        }
        try (var journal = new SeriesLifecycleJournal(base)) {
            assertEquals(7_200_000L, journal.get("active").receivedThrough());
            assertNull(journal.get("active").placement());
        }
    }

    private static boolean legacyReaderConfigured() {
        return System.getProperty("ngrrd.journal.legacyClasses") != null
                || System.getProperty("ngrrd.journal.legacySource") != null;
    }

    private Path legacyReaderClasses() throws Exception {
        String source = System.getProperty("ngrrd.journal.legacySource");
        if (source == null) return Path.of(System.getProperty("ngrrd.journal.legacyClasses"));
        Path classes = Files.createDirectories(base.resolve("legacy-reader-classes"));
        var compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "The real 8.10.2 reader gate requires a JDK");
        int result = compiler.run(null, null, null, "--release", "21", "-classpath",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                "-d", classes.toString(), source);
        assertEquals(0, result, "Compile the unmodified reader from the pinned 8.10.2 release");
        return classes;
    }

    private void writeLegacy(DataOutputStream out, LegacyUpdate update) throws Exception {
        byte[] payload = mapper.writeValueAsBytes(update);
        CRC32 crc = new CRC32(); crc.update(payload);
        out.writeInt(payload.length); out.writeInt((int) crc.getValue()); out.write(payload);
    }

    private Map<String, LegacyEntry> readLegacy(Path path) throws Exception {
        Map<String, LegacyEntry> entries = new LinkedHashMap<>();
        try (var input = new DataInputStream(Files.newInputStream(path))) {
            while (input.available() > 0) {
                int length = input.readInt(), expectedCrc = input.readInt();
                byte[] bytes = input.readNBytes(length);
                assertEquals(length, bytes.length);
                CRC32 crc = new CRC32(); crc.update(bytes);
                assertEquals(expectedCrc, (int) crc.getValue());
                var update = mapper.readValue(bytes, LegacyUpdate.class);
                entries.put(update.key(), update.entry());
            }
        }
        return entries;
    }
}
