package dev.nishisan.utils.oss.cli;

import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.api.StorageBackendType;
import dev.nishisan.utils.oss.config.NgrrdDefinitionValidator;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.format.NgrrdFormatException;
import dev.nishisan.utils.oss.format.SeriesFileCodec;
import dev.nishisan.utils.oss.format.SeriesGeometry;
import dev.nishisan.utils.oss.format.SeriesHeader;
import dev.nishisan.utils.oss.migration.GeometryChangeReport;
import dev.nishisan.utils.oss.migration.GeometryDiff;
import dev.nishisan.utils.oss.migration.GeometryReconciler;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.S3Settings;
import dev.nishisan.utils.oss.storage.StorageFactory;
import dev.nishisan.utils.oss.storage.StorageFactory.StorageBindings;

import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CLI de migração de geometria do ngrrd: varre todas as séries de um dataset,
 * classifica a divergência em relação à nova definição e, em modo de execução,
 * reescreve cada série aplicando {@link OnGeometryChange} com paralelismo
 * limitado. {@code --dry-run} apenas relata (nenhuma escrita).
 *
 * <pre>
 * java -cp ngrrd.jar dev.nishisan.utils.oss.cli.NgrrdMigrateCli \
 *      &lt;definition.yaml&gt; [&lt;rootDir&gt;] [--dry-run] \
 *      [--parallelism N] [--on-geometry-change MIGRATE|RECREATE]
 * </pre>
 *
 * <p>Backend {@code localDisk}: informe {@code rootDir}. Backend
 * {@code objectStorage}: configure via variáveis de ambiente
 * {@code NGRRD_S3_BUCKET}, {@code NGRRD_S3_REGION} e (opcional)
 * {@code NGRRD_S3_ENDPOINT}.</p>
 */
public final class NgrrdMigrateCli {

    private static final String USAGE =
            "uso: ngrrd-migrate <definition.yaml> [<rootDir>] [--dry-run] "
                    + "[--parallelism N] [--on-geometry-change MIGRATE|RECREATE]";

    /** Classificação de uma série em relação à nova geometria. */
    enum Kind {
        COMPATIBLE, MIGRATABLE, NEEDS_RECREATE, BLOCKED, UNREADABLE
    }

    private record Entry(String key, Kind kind) {
    }

    public static void main(String[] args) {
        int exit = new NgrrdMigrateCli().run(args, System.out, System.err);
        if (exit != 0) {
            System.exit(exit);
        }
    }

    int run(String[] argv, PrintStream out, PrintStream err) {
        Args args;
        try {
            args = Args.parse(argv);
        } catch (IllegalArgumentException e) {
            err.println("erro: " + e.getMessage());
            err.println(USAGE);
            return 2;
        }

        NgrrdDefinition def;
        try {
            String yaml = Files.readString(args.yamlPath, StandardCharsets.UTF_8);
            def = NgrrdYamlLoader.parse(yaml, System::getenv);
            NgrrdDefinitionValidator.validate(def);
        } catch (RuntimeException | java.io.IOException e) {
            err.println("erro ao carregar definition: " + e.getMessage());
            return 2;
        }

        NgrrdStorage storage;
        try {
            storage = StorageFactory.from(def.spec().storage(), bindings(def, args));
        } catch (RuntimeException e) {
            err.println("erro ao instanciar storage: " + e.getMessage());
            return 2;
        }

        SeriesGeometry newGeo = new SeriesGeometry(def);
        byte[] newHash = newGeo.geometryHash();
        int newRevision = def.metadata().schemaRevisionOrDefault();
        String prefix = def.spec().storage().objectNaming().seriesPrefixOrDefault();

        List<String> keys = storage.list(prefix).stream()
                .filter(k -> k.endsWith(".ngrr"))
                .sorted()
                .toList();
        List<Entry> entries = classify(storage, keys, newGeo, newHash, newRevision);

        printSummary(out, def, newRevision, newGeo.fileTotalBytes(), entries, args.dryRun);
        if (args.dryRun) {
            for (Entry e : entries) {
                if (e.kind() != Kind.COMPATIBLE) {
                    out.println("  [" + label(e.kind()) + "] " + e.key());
                }
            }
            return 0;
        }

        return execute(storage, entries, newGeo, newHash, newRevision, args, out, err);
    }

    private List<Entry> classify(NgrrdStorage storage, List<String> keys, SeriesGeometry newGeo,
                                 byte[] newHash, int newRevision) {
        List<Entry> entries = new ArrayList<>(keys.size());
        for (String key : keys) {
            Optional<byte[]> img = storage.get(key);
            if (img.isEmpty() || img.get().length < SeriesFileCodec.FIXED_HEADER_BYTES) {
                continue;
            }
            SeriesHeader header;
            try {
                header = SeriesFileCodec.decodeFixedHeader(img.get());
            } catch (NgrrdFormatException e) {
                entries.add(new Entry(key, Kind.UNREADABLE));
                continue;
            }
            boolean same = header.fileTotalBytes() == newGeo.fileTotalBytes()
                    && Arrays.equals(header.definitionHash(), newHash);
            if (same) {
                entries.add(new Entry(key, Kind.COMPATIBLE));
                continue;
            }
            Kind kind;
            if (newRevision <= header.schemaRevision()) {
                kind = Kind.BLOCKED;
            } else {
                GeometryDiff diff = GeometryDiff.between(SeriesGeometry.fromPersisted(header, img.get()), newGeo);
                kind = diff.structurallyMigratable() ? Kind.MIGRATABLE : Kind.NEEDS_RECREATE;
            }
            entries.add(new Entry(key, kind));
        }
        return entries;
    }

    private void printSummary(PrintStream out, NgrrdDefinition def, int newRevision, long bytesEach,
                              List<Entry> entries, boolean dryRun) {
        long compatible = count(entries, Kind.COMPATIBLE);
        long migratable = count(entries, Kind.MIGRATABLE);
        long needsRecreate = count(entries, Kind.NEEDS_RECREATE);
        long blocked = count(entries, Kind.BLOCKED);
        long unreadable = count(entries, Kind.UNREADABLE);
        long divergent = entries.size() - compatible;

        out.println("ngrrd-migrate" + (dryRun ? " (dry-run)" : "") + ": definition '"
                + def.metadata().name() + "' schemaRevision=" + newRevision);
        out.println("  séries inspecionadas: " + entries.size());
        out.println("  compatíveis (sem mudança): " + compatible);
        out.println("  migráveis (estrutural): " + migratable
                + "  (~" + GeometryChangeReport.humanBytes(migratable * bytesEach) + ")");
        out.println("  requerem RECREATE (reamostragem): " + needsRecreate);
        out.println("  bloqueadas (sem bump de schemaRevision): " + blocked);
        if (unreadable > 0) {
            out.println("  ilegíveis: " + unreadable);
        }
        out.println("  total divergente: " + divergent
                + "  (~" + GeometryChangeReport.humanBytes(divergent * bytesEach) + " a reescrever)");
    }

    private int execute(NgrrdStorage storage, List<Entry> entries, SeriesGeometry newGeo,
                        byte[] newHash, int newRevision, Args args, PrintStream out, PrintStream err) {
        List<Entry> toProcess = entries.stream().filter(e -> e.kind() != Kind.COMPATIBLE).toList();
        if (toProcess.isEmpty()) {
            out.println("nada a fazer: todas as séries já estão na geometria atual.");
            return 0;
        }
        out.println("migrando " + toProcess.size() + " série(s) com paralelismo "
                + args.parallelism + " (política " + args.policy + ")...");

        ExecutorService pool = Executors.newFixedThreadPool(args.parallelism);
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger fail = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>(toProcess.size());
        for (Entry e : toProcess) {
            futures.add(pool.submit(() -> {
                try {
                    GeometryReconciler.reconcile(storage, e.key(), newGeo, newHash, newRevision, args.policy);
                    ok.incrementAndGet();
                    synchronized (out) {
                        out.println("  ok: " + e.key());
                    }
                } catch (RuntimeException ex) {
                    fail.incrementAndGet();
                    synchronized (err) {
                        err.println("  falha: " + e.key() + " — " + firstLine(ex.getMessage()));
                    }
                }
            }));
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (java.util.concurrent.ExecutionException ee) {
                fail.incrementAndGet();
            }
        }
        pool.shutdown();
        try {
            pool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        out.println("concluído: " + ok.get() + " migrada(s), " + fail.get() + " falha(s)");
        return fail.get() > 0 ? 1 : 0;
    }

    private static long count(List<Entry> entries, Kind kind) {
        return entries.stream().filter(e -> e.kind() == kind).count();
    }

    private static String label(Kind kind) {
        return switch (kind) {
            case MIGRATABLE -> "migrável";
            case NEEDS_RECREATE -> "recreate";
            case BLOCKED -> "bloqueada";
            case UNREADABLE -> "ilegível";
            case COMPATIBLE -> "compatível";
        };
    }

    private static String firstLine(String message) {
        if (message == null) {
            return "(sem mensagem)";
        }
        int nl = message.indexOf('\n');
        return nl < 0 ? message : message.substring(0, nl);
    }

    private static StorageBindings bindings(NgrrdDefinition def, Args args) {
        StorageBackendType backend = def.spec().storage().backend();
        if (backend == StorageBackendType.LOCAL_DISK) {
            if (args.rootDir == null) {
                throw new IllegalArgumentException("backend localDisk requer <rootDir> como argumento");
            }
            return StorageBindings.forLocalDisk(args.rootDir);
        }
        String bucket = requireEnv("NGRRD_S3_BUCKET");
        String region = requireEnv("NGRRD_S3_REGION");
        String endpoint = System.getenv("NGRRD_S3_ENDPOINT");
        if (endpoint == null || endpoint.isBlank()) {
            return StorageBindings.forS3(S3Settings.forAws(bucket, region));
        }
        return StorageBindings.forS3(S3Settings.forEndpoint(bucket, region, URI.create(endpoint),
                System.getenv("NGRRD_S3_ACCESS_KEY"), System.getenv("NGRRD_S3_SECRET_KEY")));
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("variável de ambiente obrigatória ausente: " + name);
        }
        return value;
    }

    /** Argumentos parseados da linha de comando. */
    record Args(Path yamlPath, Path rootDir, boolean dryRun, int parallelism, OnGeometryChange policy) {

        static Args parse(String[] argv) {
            Path yaml = null;
            Path root = null;
            boolean dryRun = false;
            int parallelism = Math.max(1, Runtime.getRuntime().availableProcessors());
            OnGeometryChange policy = OnGeometryChange.MIGRATE;
            List<String> positional = new ArrayList<>();
            for (int i = 0; i < argv.length; i++) {
                switch (argv[i]) {
                    case "--dry-run" -> dryRun = true;
                    case "--parallelism" -> parallelism = Integer.parseInt(requireValue(argv, ++i, "--parallelism"));
                    case "--on-geometry-change" ->
                            policy = OnGeometryChange.from(requireValue(argv, ++i, "--on-geometry-change"));
                    default -> {
                        if (argv[i].startsWith("--")) {
                            throw new IllegalArgumentException("flag desconhecida: " + argv[i]);
                        }
                        positional.add(argv[i]);
                    }
                }
            }
            if (positional.isEmpty()) {
                throw new IllegalArgumentException("informe o caminho da definition YAML");
            }
            yaml = Path.of(positional.get(0));
            if (positional.size() > 1) {
                root = Path.of(positional.get(1));
            }
            if (parallelism < 1) {
                throw new IllegalArgumentException("--parallelism deve ser >= 1");
            }
            if (policy != OnGeometryChange.MIGRATE && policy != OnGeometryChange.RECREATE) {
                throw new IllegalArgumentException("--on-geometry-change deve ser MIGRATE ou RECREATE");
            }
            return new Args(yaml, root, dryRun, parallelism, policy);
        }

        private static String requireValue(String[] argv, int index, String flag) {
            if (index >= argv.length) {
                throw new IllegalArgumentException("valor ausente para " + flag);
            }
            return argv[index];
        }
    }
}
