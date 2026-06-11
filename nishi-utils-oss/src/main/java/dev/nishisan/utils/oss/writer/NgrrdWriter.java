package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataSourceType;
import dev.nishisan.utils.oss.api.PersistenceMode;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.definition.AppliesTo;
import dev.nishisan.utils.oss.definition.ArchiveSpec;
import dev.nishisan.utils.oss.definition.DataSourceDef;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.definition.ObjectNaming;
import dev.nishisan.utils.oss.definition.RraDef;
import dev.nishisan.utils.oss.definition.WritePolicy;
import dev.nishisan.utils.oss.engine.CounterDeriver;
import dev.nishisan.utils.oss.engine.PrimaryDataPoint;
import dev.nishisan.utils.oss.engine.RraConsolidator;
import dev.nishisan.utils.oss.engine.TimeBucket;
import dev.nishisan.utils.oss.format.BlockCodec;
import dev.nishisan.utils.oss.format.BlockHeader;
import dev.nishisan.utils.oss.metrics.NgrrdMetricsListener;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.StorageKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
    private final NgrrdMetricsListener metrics;

    /** Persistência incremental (rrdtool-like) habilitada via writePolicy. */
    private final boolean incremental;
    /** Chave do estado durável da série (último valor + acumuladores da janela). */
    private final String stateKey;

    private volatile boolean closed;

    private BlockWindow current;

    public NgrrdWriter(NgrrdDefinition definition, NgrrdStorage storage, String seriesKey) {
        this(definition, storage, seriesKey, null);
    }

    public NgrrdWriter(NgrrdDefinition definition, NgrrdStorage storage, String seriesKey,
                       NgrrdMetricsListener metrics) {
        this.definition = Objects.requireNonNull(definition, "definition é obrigatório");
        this.storage = Objects.requireNonNull(storage, "storage é obrigatório");
        this.seriesKey = Objects.requireNonNull(seriesKey, "seriesKey é obrigatório");
        this.metrics = metrics;

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

        WritePolicy writePolicy = definition.spec().storage().writePolicy();
        this.incremental = writePolicy != null
                && writePolicy.persistenceModeOrDefault() == PersistenceMode.INCREMENTAL;
        this.stateKey = StorageKey.seriesState(naming, seriesKey);
        if (incremental) {
            // Reabertura stateless: reidrata a janela aberta + counterPrev do disco.
            this.current = loadState();
        }

        this.worker = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "ngrrd-writer-" + seriesKey);
            t.setDaemon(true);
            return t;
        });
        // submit() estabelece happens-before com o estado reidratado acima.
        this.worker.submit(this::runLoop);
    }

    /**
     * Reconstrói a janela aberta a partir do estado durável (modo incremental).
     * Retorna {@code null} se não houver estado, se a geometria mudou na
     * definição, ou se o estado estiver ilegível (recomeça vazio com segurança).
     */
    private BlockWindow loadState() {
        try {
            Optional<byte[]> bytes = storage.get(stateKey);
            if (bytes.isEmpty()) {
                return null;
            }
            SeriesWriterState st = SeriesStateCodec.decode(bytes.get());
            if (st.baseStepSec() != baseStepSec || st.blockSizeSec() != blockSizeSec) {
                return null;
            }
            BlockWindow w = new BlockWindow(st.blockStartEpochSec(), baseStepSec, blockSizeSec);
            for (Map.Entry<String, BlockWindow.CounterPrev> e : st.counterPrev().entrySet()) {
                w.setCounterPrev(e.getKey(), e.getValue());
            }
            for (Map.Entry<String, PrimaryDataPoint.Memento[]> e : st.buckets().entrySet()) {
                PrimaryDataPoint.Memento[] mem = e.getValue();
                if (mem.length != w.slots()) {
                    continue;
                }
                PrimaryDataPoint[] arr = new PrimaryDataPoint[mem.length];
                for (int i = 0; i < mem.length; i++) {
                    arr[i] = PrimaryDataPoint.restore(mem[i]);
                }
                w.restoreBucket(e.getKey(), arr);
            }
            return w;
        } catch (RuntimeException e) {
            System.err.println("ngrrd-writer: estado ilegível em " + stateKey + ", iniciando vazio: " + e);
            return null;
        }
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
     * Bloqueia até a fila atual ser drenada e o bloco aberto ser materializado.
     *
     * <p>No modo {@code BLOCK_ROLLOVER} fecha o bloco aberto (janela zerada). No
     * modo {@code INCREMENTAL} comporta-se como {@link #checkpoint()}: persiste o
     * bloco aberto como parcial + o estado durável, <em>sem</em> fechar a janela.</p>
     */
    public void flush() {
        awaitCommand(new Command.Flush(new CountDownLatch(1)));
    }

    /**
     * Persiste o bloco aberto como parcial ({@code FLAG_PARTIAL}) e o estado
     * durável da série, mantendo a janela viva (semântica rrdtool-like). No modo
     * {@code BLOCK_ROLLOVER} degrada para {@link #flush()} (fecha o bloco).
     */
    public void checkpoint() {
        awaitCommand(new Command.Checkpoint(new CountDownLatch(1)));
    }

    private void awaitCommand(Command cmd) {
        if (closed) {
            return;
        }
        CountDownLatch latch = latchOf(cmd);
        queue.add(cmd);
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("comando do writer interrompido", e);
        }
    }

    private static CountDownLatch latchOf(Command cmd) {
        return switch (cmd) {
            case Command.Flush f -> f.latch();
            case Command.Checkpoint c -> c.latch();
            case Command.Shutdown s -> s.latch();
            case Command.Write ignored -> throw new IllegalArgumentException("Write não tem latch");
        };
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
                        flushOrCheckpoint();
                        f.latch().countDown();
                    }
                    case Command.Checkpoint c -> {
                        flushOrCheckpoint();
                        c.latch().countDown();
                    }
                    case Command.Shutdown s -> {
                        flushOrCheckpoint();
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
            // amostra anterior ao bloco corrente — drop com métrica de late.
            if (metrics != null) {
                long latenessSec = current.blockStartEpochSec() - alignedSec;
                metrics.onLateSample(w.dsName(), latenessSec);
            }
            return;
        }

        double derivedValue = deriveValue(rawDs, w.sample());
        if (Double.isNaN(derivedValue)) {
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
        if (metrics != null) {
            switch (result.flag()) {
                case RESET -> metrics.onCounterReset(rawDs.name());
                case WRAP -> metrics.onWrapDetected(rawDs.name());
                default -> {
                }
            }
        }
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
            // Bloco anterior é finalizado (não-parcial), gravado exatamente uma vez.
            persistBlock(finished, false);
        }
    }

    /** Comportamento de {@code flush()}/{@code checkpoint()}/{@code shutdown}. */
    private void flushOrCheckpoint() {
        if (incremental) {
            checkpointOpenBlock();
        } else {
            closeCurrentBlock();
        }
    }

    /**
     * Persiste o bloco aberto como parcial e grava o estado durável, mantendo a
     * janela viva. Como a janela é a acumulação completa (reidratada + novas
     * amostras), o {@code atomicReplace} do bloco é lossless.
     */
    private void checkpointOpenBlock() {
        if (current == null) {
            return;
        }
        persistBlock(current, true);
        persistState();
    }

    private void closeCurrentBlock() {
        if (current == null) {
            return;
        }
        BlockWindow finished = current;
        current = null;
        persistBlock(finished, false);
    }

    private void persistState() {
        if (!incremental || current == null) {
            return;
        }
        try {
            storage.atomicReplace(stateKey, SeriesStateCodec.encode(snapshotState(current)));
        } catch (RuntimeException e) {
            System.err.println("ngrrd-writer: falha ao persistir estado " + stateKey + ": " + e);
        }
    }

    private SeriesWriterState snapshotState(BlockWindow window) {
        Map<String, PrimaryDataPoint.Memento[]> buckets = new LinkedHashMap<>();
        for (Map.Entry<String, PrimaryDataPoint[]> e : window.bucketsSnapshot().entrySet()) {
            PrimaryDataPoint[] arr = e.getValue();
            PrimaryDataPoint.Memento[] mem = new PrimaryDataPoint.Memento[arr.length];
            for (int i = 0; i < arr.length; i++) {
                mem[i] = arr[i].snapshot();
            }
            buckets.put(e.getKey(), mem);
        }
        Map<String, BlockWindow.CounterPrev> cp = new LinkedHashMap<>(window.counterPrevSnapshot());
        return new SeriesWriterState(window.blockStartEpochSec(), baseStepSec, blockSizeSec, cp, buckets);
    }

    private void persistBlock(BlockWindow window, boolean partial) {
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
                    persistRraBlock(window, derivedDsName, rra, cf, pdps, partial);
                }
            }
        }
        // Após gravar blocos novos, expira blocos que caíram fora da janela
        // rows × stepSec de cada RRA (semântica de retenção finita do RRD).
        for (RraDef rra : archives.rras()) {
            expireOutOfWindowBlocks(rra);
        }
    }

    /**
     * Remove do Storage e do registro interno blocos cujo final em segundos
     * caiu fora da janela {@code rows × stepSec} mais recente do RRA.
     *
     * <p>Sem esta lógica, o ngrrd cresceria indefinidamente e perderia a
     * paridade com a semântica RRD clássica (ring buffer com janela fixa).</p>
     */
    private void expireOutOfWindowBlocks(RraDef rra) {
        long retentionSec = (long) rra.rows() * rra.stepSec();
        long latestBlockEnd = 0L;
        for (PersistedBlock b : persistedBlocks) {
            if (!b.rraName().equals(rra.name())) {
                continue;
            }
            long blockEnd = b.blockStartEpoch() + (long) b.rows() * b.stepSec();
            if (blockEnd > latestBlockEnd) {
                latestBlockEnd = blockEnd;
            }
        }
        if (latestBlockEnd == 0L) {
            return;
        }
        long horizonStart = latestBlockEnd - retentionSec;
        List<PersistedBlock> toRemove = new ArrayList<>();
        for (PersistedBlock b : persistedBlocks) {
            if (!b.rraName().equals(rra.name())) {
                continue;
            }
            long blockEnd = b.blockStartEpoch() + (long) b.rows() * b.stepSec();
            if (blockEnd <= horizonStart) {
                toRemove.add(b);
            }
        }
        for (PersistedBlock b : toRemove) {
            try {
                storage.delete(b.storageKey());
            } catch (RuntimeException e) {
                System.err.println("ngrrd-writer: falha ao expirar bloco " + b.storageKey() + ": " + e);
            }
        }
        persistedBlocks.removeAll(toRemove);
    }

    private void persistRraBlock(BlockWindow window, String derivedDsName, RraDef rra,
                                 ConsolidationFunction cf, PrimaryDataPoint[] pdps, boolean partial) {
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
        int totalMissing = 0;
        int totalPdps = 0;
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
            totalMissing += missing;
            totalPdps += groupSize;
            double cdp = RraConsolidator.consolidate(rra, cf, consolidated, missing);
            payload[i] = cdp;
            if (!Double.isNaN(cdp)) {
                anyValue = true;
            }
        }
        if (!anyValue) {
            return;
        }
        if (metrics != null && totalPdps > 0) {
            double ratio = (double) totalMissing / totalPdps;
            metrics.onBlockClosed(rra.name(), derivedDsName, ratio);
        }

        String storageKeyStr = StorageKey.aggBlock(naming,
                seriesKey,
                rra.name() + "__" + derivedDsName + "__" + cf,
                rra.stepSec(),
                window.blockStartEpochSec());

        BlockHeader header = new BlockHeader(
                BlockHeader.CURRENT_VERSION,
                BlockHeader.flags(false, partial, true),
                rra.stepSec(),
                cf,
                DataSourceType.GAUGE,
                window.blockStartEpochSec(),
                cdpCount);
        byte[] encoded = BlockCodec.encode(header, payload);
        storage.atomicReplace(storageKeyStr, encoded);

        long crc = readCrcFromEncoded(encoded);
        // Idempotência: um mesmo bloco (mesma chave) pode ser reescrito a cada
        // checkpoint (parcial) e novamente ao finalizar — substitui a entrada
        // anterior para o manifesto não acumular duplicatas.
        persistedBlocks.removeIf(b -> b.storageKey().equals(storageKeyStr));
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

        record Checkpoint(CountDownLatch latch) implements Command {
        }

        record Shutdown(CountDownLatch latch) implements Command {
        }
    }
}
