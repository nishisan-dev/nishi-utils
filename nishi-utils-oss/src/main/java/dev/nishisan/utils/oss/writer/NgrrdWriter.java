package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataSourceType;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.definition.AppliesTo;
import dev.nishisan.utils.oss.definition.ArchiveSpec;
import dev.nishisan.utils.oss.definition.DataSourceDef;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.definition.ObjectNaming;
import dev.nishisan.utils.oss.definition.RraDef;
import dev.nishisan.utils.oss.engine.CounterDeriver;
import dev.nishisan.utils.oss.engine.PrimaryDataPoint;
import dev.nishisan.utils.oss.engine.RraConsolidator;
import dev.nishisan.utils.oss.engine.TimeBucket;
import dev.nishisan.utils.oss.format.BlockCodec;
import dev.nishisan.utils.oss.format.BlockHeader;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.StorageKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Ingere {@link Sample}s e materializa blocos consolidados no
 * {@link NgrrdStorage} alinhados ao {@code blockSizeSec}.
 *
 * <p>Modelo de execução: produtor-consumidor com worker thread única
 * (LinkedBlockingQueue + ExecutorService de tamanho 1). Toda mutação de estado
 * ocorre na worker thread; clientes apenas enfileiram comandos via
 * {@link #write(String, Sample)} e {@link #flush()}.</p>
 *
 * <p>Para cada DS COUNTER com {@code derive.output} a engine aplica
 * {@link CounterDeriver}; o valor derivado é acumulado em
 * {@link PrimaryDataPoint}s indexados pelo slot do step base. Ao cruzar o
 * {@code blockEndEpochSec}, o bloco é fechado: para cada {@code (RRA, CF)}
 * declarado, os PDPs são consolidados em CDPs via {@link RraConsolidator} e o
 * resultado é persistido como {@code .ngrrd} sob o {@code aggPrefix} usando
 * {@link StorageKey}.</p>
 */
public final class NgrrdWriter implements AutoCloseable {

    private final NgrrdDefinition definition;
    private final NgrrdStorage storage;
    private final String seriesKey;

    private final int baseStepSec;
    private final int blockSizeSec;
    private final ObjectNaming naming;
    private final ArchiveSpec archives;

    private final Map<String, DataSourceDef> rawByName;
    private final Map<String, DataSourceDef> rawByDerivedName;
    private final Set<String> includedDerivedDsNames;

    private final ExecutorService worker;
    private final LinkedBlockingQueue<Command> queue = new LinkedBlockingQueue<>();

    private final List<PersistedBlock> persistedBlocks = new CopyOnWriteArrayList<>();
    private volatile boolean closed;

    private BlockWindow current;

    public NgrrdWriter(NgrrdDefinition definition, NgrrdStorage storage, String seriesKey) {
        this.definition = Objects.requireNonNull(definition, "definition é obrigatório");
        this.storage = Objects.requireNonNull(storage, "storage é obrigatório");
        this.seriesKey = Objects.requireNonNull(seriesKey, "seriesKey é obrigatório");

        this.baseStepSec = definition.spec().time().baseStepSec();
        this.blockSizeSec = definition.spec().time().blockSizeSec();
        this.naming = definition.spec().storage().objectNaming();
        this.archives = definition.spec().archives();

        this.rawByName = new HashMap<>();
        this.rawByDerivedName = new HashMap<>();
        for (DataSourceDef ds : definition.spec().dataSources()) {
            this.rawByName.put(ds.name(), ds);
            if (ds.derive() != null && ds.derive().output() != null) {
                this.rawByDerivedName.put(ds.derive().output().name(), ds);
            }
        }
        AppliesTo appliesTo = archives.appliesTo();
        this.includedDerivedDsNames = appliesTo == null || appliesTo.include() == null
                ? Set.of()
                : Set.copyOf(appliesTo.include());

        this.worker = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "ngrrd-writer-" + seriesKey);
            t.setDaemon(true);
            return t;
        });
        this.worker.submit(this::runLoop);
    }

    /** Enfileira uma amostra para o DS raw indicado. */
    public void write(String dsName, Sample sample) {
        Objects.requireNonNull(dsName, "dsName é obrigatório");
        Objects.requireNonNull(sample, "sample é obrigatório");
        if (closed) {
            throw new IllegalStateException("Writer já fechado");
        }
        queue.add(new Command.Write(dsName, sample));
    }

    /**
     * Bloqueia até a fila atual ser drenada e o bloco aberto ser fechado e
     * persistido (caso tenha conteúdo).
     */
    public void flush() {
        if (closed) {
            return;
        }
        CountDownLatch latch = new CountDownLatch(1);
        queue.add(new Command.Flush(latch));
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("flush interrompido", e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        CountDownLatch latch = new CountDownLatch(1);
        queue.add(new Command.Shutdown(latch));
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        worker.shutdown();
        try {
            worker.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Snapshot imutável da lista de blocos já persistidos por este writer. */
    public List<PersistedBlock> persistedBlocks() {
        return List.copyOf(persistedBlocks);
    }

    public String seriesKey() {
        return seriesKey;
    }

    public NgrrdDefinition definition() {
        return definition;
    }

    private void runLoop() {
        while (true) {
            try {
                Command cmd = queue.take();
                switch (cmd) {
                    case Command.Write w -> handleWrite(w);
                    case Command.Flush f -> {
                        closeCurrentBlock();
                        f.latch().countDown();
                    }
                    case Command.Shutdown s -> {
                        closeCurrentBlock();
                        s.latch().countDown();
                        return;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // erro de uma amostra individual não pode derrubar a worker thread.
                System.err.println("ngrrd-writer error: " + e);
            }
        }
    }

    private void handleWrite(Command.Write w) {
        DataSourceDef rawDs = rawByName.get(w.dsName());
        if (rawDs == null) {
            throw new IllegalArgumentException("DS desconhecido: " + w.dsName());
        }
        long tsEpochSec = w.sample().tsEpochMs() / 1000L;
        long alignedSec = TimeBucket.alignDown(tsEpochSec, baseStepSec);

        rolloverIfNeeded(alignedSec);
        if (!current.contains(alignedSec)) {
            // amostra anterior ao bloco corrente — drop silencioso no MVP.
            return;
        }

        double derivedValue = deriveValue(rawDs, w.sample());
        if (Double.isNaN(derivedValue)) {
            // sem valor derivado válido — mas ainda atualiza counterPrev acima.
            return;
        }
        String derivedDsName = rawDs.derive().output().name();
        current.addSampleDerived(derivedDsName, alignedSec, derivedValue);
    }

    private double deriveValue(DataSourceDef rawDs, Sample sample) {
        if (rawDs.type() != DataSourceType.COUNTER || rawDs.derive() == null
                || rawDs.derive().output() == null) {
            // GAUGE / DERIVE / ABSOLUTE: tratamento simplificado no MVP — entrega
            // o valor como gauge direto se houver derive declarado, senão pula.
            if (rawDs.derive() != null && rawDs.derive().output() != null) {
                return sample.value();
            }
            return Double.NaN;
        }
        BlockWindow.CounterPrev prev = current.counterPrev(rawDs.name());
        double prevValue = prev == null ? Double.NaN : prev.value();
        long prevTsMs = prev == null ? 0L : prev.tsEpochMs();
        CounterDeriver.CounterDeriverResult result = CounterDeriver.derive(
                rawDs, prevValue, prevTsMs, sample.value(), sample.tsEpochMs());
        current.setCounterPrev(rawDs.name(), new BlockWindow.CounterPrev(sample.value(), sample.tsEpochMs()));
        return result.value();
    }

    private void rolloverIfNeeded(long tsAlignedSec) {
        if (current == null) {
            long blockStart = TimeBucket.alignDown(tsAlignedSec, blockSizeSec);
            current = new BlockWindow(blockStart, baseStepSec, blockSizeSec);
            return;
        }
        while (tsAlignedSec >= current.blockEndExclusiveEpochSec()) {
            BlockWindow finished = current;
            long nextStart = finished.blockEndExclusiveEpochSec();
            // preserva counterPrev entre blocos consecutivos.
            current = new BlockWindow(nextStart, baseStepSec, blockSizeSec);
            for (Map.Entry<String, BlockWindow.CounterPrev> e : finished.counterPrevSnapshot().entrySet()) {
                current.setCounterPrev(e.getKey(), e.getValue());
            }
            persistBlock(finished);
        }
    }

    private void closeCurrentBlock() {
        if (current == null) {
            return;
        }
        BlockWindow finished = current;
        current = null;
        persistBlock(finished);
    }

    private void persistBlock(BlockWindow window) {
        Map<String, PrimaryDataPoint[]> buckets = window.bucketsSnapshot();
        if (buckets.isEmpty()) {
            return;
        }
        for (Map.Entry<String, PrimaryDataPoint[]> entry : buckets.entrySet()) {
            String derivedDsName = entry.getKey();
            if (!includedDerivedDsNames.isEmpty() && !includedDerivedDsNames.contains(derivedDsName)) {
                continue;
            }
            PrimaryDataPoint[] pdps = entry.getValue();
            for (RraDef rra : archives.rras()) {
                for (ConsolidationFunction cf : rra.cf()) {
                    persistRraBlock(window, derivedDsName, rra, cf, pdps);
                }
            }
        }
    }

    private void persistRraBlock(BlockWindow window, String derivedDsName, RraDef rra,
                                 ConsolidationFunction cf, PrimaryDataPoint[] pdps) {
        if (rra.stepSec() % baseStepSec != 0) {
            return; // protegido pelo validator, mas evita estado inconsistente.
        }
        int groupSize = rra.stepSec() / baseStepSec;
        int cdpCount = pdps.length / groupSize;
        if (cdpCount == 0) {
            return;
        }
        double[] payload = new double[cdpCount];
        boolean anyValue = false;
        for (int i = 0; i < cdpCount; i++) {
            List<Double> consolidated = new ArrayList<>(groupSize);
            int missing = 0;
            for (int j = 0; j < groupSize; j++) {
                PrimaryDataPoint pdp = pdps[i * groupSize + j];
                if (pdp.isEmpty()) {
                    missing++;
                } else {
                    consolidated.add(pdp.consolidate(cf));
                }
            }
            double cdp = RraConsolidator.consolidate(rra, cf, consolidated, missing);
            payload[i] = cdp;
            if (!Double.isNaN(cdp)) {
                anyValue = true;
            }
        }
        if (!anyValue) {
            return;
        }

        String storageKeyStr = StorageKey.aggBlock(naming,
                seriesKey,
                rra.name() + "__" + derivedDsName + "__" + cf,
                rra.stepSec(),
                window.blockStartEpochSec());

        BlockHeader header = new BlockHeader(
                BlockHeader.CURRENT_VERSION,
                BlockHeader.flags(false, false, true),
                rra.stepSec(),
                cf,
                DataSourceType.GAUGE,
                window.blockStartEpochSec(),
                cdpCount);
        byte[] encoded = BlockCodec.encode(header, payload);
        storage.atomicReplace(storageKeyStr, encoded);

        long crc = readCrcFromEncoded(encoded);
        persistedBlocks.add(new PersistedBlock(
                rra.name(),
                derivedDsName,
                cf,
                rra.stepSec(),
                window.blockStartEpochSec(),
                cdpCount,
                crc,
                storageKeyStr));
    }

    private static long readCrcFromEncoded(byte[] encoded) {
        int crc = java.nio.ByteBuffer.wrap(encoded)
                .order(java.nio.ByteOrder.BIG_ENDIAN)
                .getInt(BlockHeader.HEADER_BYTES_NO_CRC);
        return Integer.toUnsignedLong(crc);
    }

    private sealed interface Command {
        record Write(String dsName, Sample sample) implements Command {
        }

        record Flush(CountDownLatch latch) implements Command {
        }

        record Shutdown(CountDownLatch latch) implements Command {
        }
    }
}
