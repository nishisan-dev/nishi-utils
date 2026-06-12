package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataSourceType;
import dev.nishisan.utils.oss.api.Durability;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.definition.DataSourceDef;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.engine.CounterDeriver;
import dev.nishisan.utils.oss.engine.PrimaryDataPoint;
import dev.nishisan.utils.oss.engine.TimeBucket;
import dev.nishisan.utils.oss.format.SeriesFileCodec;
import dev.nishisan.utils.oss.format.SeriesGeometry;
import dev.nishisan.utils.oss.format.SeriesHeader;
import dev.nishisan.utils.oss.format.SeriesLiveState;
import dev.nishisan.utils.oss.format.NgrrdFormatException;
import dev.nishisan.utils.oss.metrics.NgrrdMetricsListener;
import dev.nishisan.utils.oss.storage.NgrrdStorage;
import dev.nishisan.utils.oss.storage.SeriesChannel;
import dev.nishisan.utils.oss.storage.SeriesChannelProvider;
import dev.nishisan.utils.oss.storage.StorageKey;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Ingere {@link Sample}s e mantém um único arquivo de série NGRR por
 * {@code seriesKey}, em paridade com o RRDtool: ring buffers in-place de tamanho
 * fixo, sem rollover de blocos e sem manifesto.
 *
 * <p>Modelo de execução: produtor-consumidor com worker thread única. Toda
 * mutação de estado e todo acesso ao {@link SeriesChannel} ocorrem na worker
 * thread. Quando um {@link ReadWriteLock} é compartilhado com leitores (caso do
 * {@code NgrrdHandle}), a worker adquire o write-lock em volta de toda mutação
 * do objeto da série, e a região do live-state é regravada sempre que o ring
 * avança — o par (ponteiro do ring, células) persistido fica sempre coerente
 * para leituras concorrentes no mesmo processo. {@code force()} (fsync/PUT)
 * ocorre fora do lock para não bloquear leitores durante I/O de durabilidade.</p>
 *
 * <p>Consolidação contínua (estilo {@code cdp_prep}): cada amostra é derivada via
 * {@link CounterDeriver} e acumulada no PDP do step base da coluna; ao avançar o
 * slot, o PDP completo é consolidado e dobrado no CDP em progresso de cada
 * archive; quando o passo do archive fecha, o CDP é emitido no ring (avançando o
 * ponteiro, sobrescrevendo o mais antigo — retenção como ring buffer). O
 * {@link #checkpoint()} materializa também o CDP em progresso como parcial
 * (legível antes do fechamento) e torna o estado durável.</p>
 */
public final class NgrrdWriter implements AutoCloseable {

    private final NgrrdDefinition definition;
    private final String seriesKey;
    private final NgrrdMetricsListener metrics;

    private final SeriesGeometry geo;
    private final byte[] geometryHash;
    private final int schemaRevision;
    private final int baseStepSec;
    private final int d;
    private final int a;
    private final String storageKey;

    private final Map<String, DataSourceDef> rawDefByName = new HashMap<>();
    private final Map<String, Integer> rawNameToColIndex = new HashMap<>();
    private final DataSourceDef[] colRawDef; // [d] DataSourceDef do DS raw de cada coluna

    private final SeriesChannel channel;
    private final SeriesLiveState state;
    private final Lock writeLock;
    private final Durability durability;
    private boolean ringDirty;

    private final ExecutorService worker;
    private final LinkedBlockingQueue<Command> queue = new LinkedBlockingQueue<>();

    private volatile boolean closed;

    public NgrrdWriter(NgrrdDefinition definition, NgrrdStorage storage, String seriesKey) {
        this(definition, storage, seriesKey, null);
    }

    public NgrrdWriter(NgrrdDefinition definition, NgrrdStorage storage, String seriesKey,
                       NgrrdMetricsListener metrics) {
        this(definition, storage, seriesKey, metrics, new ReentrantReadWriteLock());
    }

    /**
     * Variante com {@link ReadWriteLock} compartilhado com leitores da mesma
     * série (mesmo processo): o write-lock é adquirido em volta de toda mutação
     * do objeto da série, permitindo leituras consistentes concorrentes.
     * Durabilidade {@link Durability#FSYNC} (padrão).
     */
    public NgrrdWriter(NgrrdDefinition definition, NgrrdStorage storage, String seriesKey,
                       NgrrdMetricsListener metrics, ReadWriteLock seriesLock) {
        this(definition, storage, seriesKey, metrics, seriesLock, Durability.FSYNC);
    }

    /**
     * Variante completa com política de {@link Durability}. Em
     * {@link Durability#OS_CACHE} o checkpoint materializa os bytes mas pula o
     * {@code fsync} por checkpoint (o SO descarrega no seu ritmo); um
     * {@code close()} limpo ainda descarrega o pendente.
     */
    public NgrrdWriter(NgrrdDefinition definition, NgrrdStorage storage, String seriesKey,
                       NgrrdMetricsListener metrics, ReadWriteLock seriesLock,
                       Durability durability) {
        this.definition = Objects.requireNonNull(definition, "definition é obrigatório");
        Objects.requireNonNull(storage, "storage é obrigatório");
        this.seriesKey = Objects.requireNonNull(seriesKey, "seriesKey é obrigatório");
        this.metrics = metrics;
        this.writeLock = Objects.requireNonNull(seriesLock, "seriesLock é obrigatório").writeLock();
        this.durability = Objects.requireNonNull(durability, "durability é obrigatório");

        if (!(storage instanceof SeriesChannelProvider provider)) {
            throw new IllegalArgumentException(
                    "storage não suporta SeriesChannel: " + storage.getClass().getName());
        }

        this.geo = new SeriesGeometry(definition);
        this.geometryHash = geo.geometryHash();
        this.schemaRevision = definition.metadata().schemaRevisionOrDefault();
        this.baseStepSec = geo.baseStepSec();
        this.d = geo.columnCount();
        this.a = geo.archiveCount();

        for (DataSourceDef ds : definition.spec().dataSources()) {
            rawDefByName.put(ds.name(), ds);
        }
        this.colRawDef = new DataSourceDef[d];
        for (int i = 0; i < d; i++) {
            SeriesGeometry.Column col = geo.columns().get(i);
            rawNameToColIndex.put(col.rawName(), i);
            colRawDef[i] = rawDefByName.get(col.rawName());
        }

        this.storageKey = StorageKey.series(definition.spec().storage().objectNaming(), seriesKey);
        this.channel = provider.openSeries(storageKey);
        this.state = openOrCreate();

        this.worker = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "ngrrd-writer-" + seriesKey);
            t.setDaemon(true);
            return t;
        });
        // submit() estabelece happens-before com o estado reidratado acima.
        this.worker.submit(this::runLoop);
    }

    /**
     * Abre o arquivo: cria+pré-aloca se ausente/incompatível; senão reidrata a
     * live-state. Geometria divergente (hash) ⇒ recriação (clean cut).
     */
    private SeriesLiveState openOrCreate() {
        if (channel.size() < SeriesFileCodec.FIXED_HEADER_BYTES) {
            return createFresh();
        }
        try {
            SeriesHeader header = SeriesFileCodec.decodeFixedHeader(
                    channel.readRegion(0, SeriesFileCodec.FIXED_HEADER_BYTES));
            boolean compatible = header.formatVersion() == SeriesFileCodec.CURRENT_VERSION
                    && header.fileTotalBytes() == geo.fileTotalBytes()
                    && Arrays.equals(header.definitionHash(), geometryHash);
            if (!compatible) {
                System.err.println("ngrrd-writer: geometria divergente em " + storageKey
                        + ", recriando (clean cut)");
                return createFresh();
            }
            byte[] live = channel.readRegion(geo.liveStateOffset(), (int) geo.liveStateBytes());
            return SeriesFileCodec.decodeLiveState(geo, live);
        } catch (NgrrdFormatException e) {
            System.err.println("ngrrd-writer: arquivo de série ilegível em " + storageKey
                    + ", recriando: " + e);
            return createFresh();
        }
    }

    private SeriesLiveState createFresh() {
        channel.allocate(geo.fileTotalBytes());
        channel.writeRegion(0, SeriesFileCodec.buildInitialImage(geo, geometryHash, schemaRevision));
        channel.force();
        return new SeriesLiveState(d, a);
    }

    /** Enfileira uma amostra para o DS raw indicado. */
    public void write(String dsName, Sample sample) {
        Objects.requireNonNull(dsName, "dsName é obrigatório");
        Objects.requireNonNull(sample, "sample é obrigatório");
        if (closed) {
            throw new IllegalStateException("Writer já fechado");
        }
        if (!rawDefByName.containsKey(dsName)) {
            throw new IllegalArgumentException("DS desconhecido: " + dsName);
        }
        queue.add(new Command.Write(dsName, sample));
    }

    /** Materializa o CDP em progresso como parcial e torna o arquivo durável. */
    public void flush() {
        sync();
    }

    /** Idêntico a {@link #flush()} no formato de série única (sempre incremental). */
    public void checkpoint() {
        sync();
    }

    /**
     * Enfileira um checkpoint síncrono e bloqueia até a worker thread concluí-lo.
     * Uma falha de persistência (ex.: PUT no S3 / fsync) é propagada ao chamador
     * em vez de travar indefinidamente em {@code await()}.
     */
    private void sync() {
        if (closed) {
            return;
        }
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<RuntimeException> error = new AtomicReference<>();
        queue.add(new Command.Sync(latch, error));
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("comando do writer interrompido", e);
        }
        RuntimeException failure = error.get();
        if (failure != null) {
            throw failure;
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
                    case Command.Write w -> {
                        writeLock.lock();
                        try {
                            handleWrite(w);
                            if (ringDirty) {
                                // ring avançou: mantém (ponteiro, células) coerente
                                // no objeto para leitores concorrentes; durabilidade
                                // continua sendo responsabilidade do checkpoint.
                                persistLiveState();
                            }
                        } finally {
                            writeLock.unlock();
                        }
                    }
                    case Command.Sync s -> {
                        // Sempre libera o latch; uma falha vai para o chamador via error ref.
                        try {
                            checkpointAndForce();
                        } catch (RuntimeException e) {
                            s.error().set(e);
                        } finally {
                            s.latch().countDown();
                        }
                    }
                    case Command.Shutdown s -> {
                        try {
                            checkpointAndForce();
                        } catch (RuntimeException e) {
                            System.err.println("ngrrd-writer: falha no checkpoint final: " + e);
                        } finally {
                            closeChannelQuietly();
                            s.latch().countDown();
                        }
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
        DataSourceDef rawDs = rawDefByName.get(w.dsName());
        long tsMs = w.sample().tsEpochMs();
        long tsSec = tsMs / 1000L;
        long slotSec = TimeBucket.alignDown(tsSec, baseStepSec);

        if (tsMs > state.lastUpEpochMs) {
            state.lastUpEpochMs = tsMs;
        }

        Integer colObj = rawNameToColIndex.get(w.dsName());
        if (colObj == null) {
            return; // DS conhecido mas não arquivado (fora do appliesTo).
        }
        int i = colObj;

        long cur = state.pdpSlotSec[i];
        if (cur != -1L && slotSec < cur) {
            if (metrics != null) {
                metrics.onLateSample(w.dsName(), cur - slotSec);
            }
            return; // amostra atrasada: descarta sem derivar (preserva continuidade do counter).
        }
        if (cur == -1L) {
            state.pdpSlotSec[i] = slotSec;
        } else if (slotSec > cur) {
            advanceColumn(i, cur, slotSec);
        }

        double derived = deriveValue(rawDs, i, w.sample());
        if (!Double.isNaN(derived)) {
            state.pdp[i].add(derived);
        }
    }

    private double deriveValue(DataSourceDef rawDs, int col, Sample sample) {
        if (rawDs.type() == DataSourceType.COUNTER && rawDs.derive() != null
                && rawDs.derive().output() != null) {
            double prevValue = state.counterPrevValue[col];
            long prevTs = state.counterPrevTsMs[col];
            CounterDeriver.CounterDeriverResult result = CounterDeriver.derive(
                    rawDs, prevValue, prevTs, sample.value(), sample.tsEpochMs());
            state.counterPrevValue[col] = sample.value();
            state.counterPrevTsMs[col] = sample.tsEpochMs();
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
        // GAUGE/DERIVE/ABSOLUTE com derive declarado: passthrough.
        if (rawDs.derive() != null && rawDs.derive().output() != null) {
            return sample.value();
        }
        return Double.NaN;
    }

    /**
     * Finaliza o PDP completo do slot {@code fromSlot} da coluna, dobra-o nos CDPs
     * de cada archive e emite os CDPs cujo passo fechou ao cruzar para
     * {@code toSlot}. Reseta o PDP para o novo slot.
     */
    private void advanceColumn(int col, long fromSlot, long toSlot) {
        for (int arch = 0; arch < a; arch++) {
            SeriesGeometry.Archive archive = geo.archives().get(arch);
            int s = archive.stepSec();
            foldPdp(arch, col, archive.cf());
            long wf = TimeBucket.alignDown(fromSlot, s);
            long wt = TimeBucket.alignDown(toSlot, s);
            if (wt > wf) {
                emit(arch, col, wf, true);
                resetCdp(arch, col);
            }
        }
        state.pdp[col].reset();
        state.pdpSlotSec[col] = toSlot;
    }

    /** Dobra o PDP corrente da coluna no acumulador de CDP de {@code (archive, col)}. */
    private void foldPdp(int arch, int col, ConsolidationFunction cf) {
        PrimaryDataPoint pdp = state.pdp[col];
        if (pdp.isEmpty()) {
            return; // slot base sem amostra: contribui como missing (implícito).
        }
        double pv = pdp.consolidate(cf);
        int idx = state.cdpIndex(arch, col);
        if (state.cdpFolded[idx] == 0) {
            state.cdpPartial[idx] = pv;
        } else {
            state.cdpPartial[idx] = combine(cf, state.cdpPartial[idx], pv);
        }
        state.cdpFolded[idx]++;
    }

    private void resetCdp(int arch, int col) {
        int idx = state.cdpIndex(arch, col);
        state.cdpPartial[idx] = Double.NaN;
        state.cdpFolded[idx] = 0;
        state.cdpMissing[idx] = 0;
    }

    private static double combine(ConsolidationFunction cf, double acc, double v) {
        return switch (cf) {
            case AVERAGE -> acc + v;
            case MAX -> Math.max(acc, v);
            case MIN -> Math.min(acc, v);
            case LAST -> v;
        };
    }

    /**
     * Calcula o valor do CDP de {@code (archive, col)}. Em {@code finalEmit}
     * aplica o XFF sobre o passo completo; parcial usa só o observado.
     * {@code includeCurrentPdp} dobra o PDP em progresso (usado no checkpoint).
     */
    private double cdpValue(int arch, int col, boolean finalEmit, boolean includeCurrentPdp) {
        SeriesGeometry.Archive archive = geo.archives().get(arch);
        ConsolidationFunction cf = archive.cf();
        int idx = state.cdpIndex(arch, col);
        double accum = state.cdpPartial[idx];
        int observed = state.cdpFolded[idx];
        if (includeCurrentPdp && !state.pdp[col].isEmpty()) {
            double pv = state.pdp[col].consolidate(cf);
            if (observed == 0) {
                accum = pv;
            } else {
                accum = combine(cf, accum, pv);
            }
            observed++;
        }
        if (observed == 0) {
            return Double.NaN;
        }
        if (finalEmit) {
            int g = archive.groupSize();
            int missing = Math.max(0, g - observed);
            if (g > 0 && (double) missing / g > archive.xff()) {
                return Double.NaN;
            }
        }
        return cf == ConsolidationFunction.AVERAGE ? accum / observed : accum;
    }

    private void emit(int arch, int col, long windowStart, boolean finalEmit) {
        double value = cdpValue(arch, col, finalEmit, !finalEmit);
        int row = placeRow(arch, windowStart);
        if (row < 0) {
            return; // janela mais antiga que o ring: descarta.
        }
        writeCell(arch, row, col, value);
        if (finalEmit && metrics != null) {
            SeriesGeometry.Archive archive = geo.archives().get(arch);
            int g = archive.groupSize();
            int observed = state.cdpFolded[state.cdpIndex(arch, col)];
            double ratio = g > 0 ? (double) Math.max(0, g - observed) / g : 0.0;
            metrics.onBlockClosed(archive.rraName(), geo.columns().get(col).derivedName(), ratio);
        }
    }

    /**
     * Posiciona a linha do ring para {@code windowStart}, avançando o ponteiro e
     * inicializando linhas novas com {@code NaN} (gap-fill). Retorna {@code -1} se
     * a janela for mais antiga que a capacidade do ring.
     */
    private int placeRow(int arch, long windowStart) {
        SeriesGeometry.Archive archive = geo.archives().get(arch);
        int s = archive.stepSec();
        int rows = archive.rows();
        long anchor = state.curRowEpochSec[arch];
        if (state.curRow[arch] < 0) {
            state.curRow[arch] = 0;
            state.curRowEpochSec[arch] = windowStart;
            nanInitRow(arch, 0);
            return 0;
        }
        if (windowStart == anchor) {
            return state.curRow[arch];
        }
        if (windowStart > anchor) {
            long steps = (windowStart - anchor) / s;
            if (steps > rows) {
                steps = rows;
            }
            int row = state.curRow[arch];
            for (long k = 0; k < steps; k++) {
                row = (row + 1) % rows;
                nanInitRow(arch, row);
            }
            state.curRow[arch] = row;
            state.curRowEpochSec[arch] = windowStart;
            return row;
        }
        long delta = (anchor - windowStart) / s;
        if (delta >= rows) {
            return -1;
        }
        return (int) (((state.curRow[arch] - delta) % rows + rows) % rows);
    }

    private void nanInitRow(int arch, int row) {
        for (int col = 0; col < d; col++) {
            writeCell(arch, row, col, Double.NaN);
        }
    }

    private void writeCell(int arch, int row, int col, double value) {
        channel.writeRegion(geo.cellOffset(arch, row, col), SeriesFileCodec.doubleBytes(value));
        ringDirty = true;
    }

    /** Regrava a região do live-state (sem force) e limpa a flag do ring. */
    private void persistLiveState() {
        channel.writeRegion(geo.liveStateOffset(), SeriesFileCodec.encodeLiveState(geo, state));
        ringDirty = false;
    }

    /** Emite os CDPs em progresso como parciais e torna o arquivo durável. */
    private void checkpointAndForce() {
        writeLock.lock();
        try {
            for (int col = 0; col < d; col++) {
                long slot = state.pdpSlotSec[col];
                if (slot == -1L) {
                    continue;
                }
                for (int arch = 0; arch < a; arch++) {
                    long windowStart = TimeBucket.alignDown(slot, geo.archives().get(arch).stepSec());
                    emit(arch, col, windowStart, false);
                }
            }
            persistLiveState();
        } finally {
            writeLock.unlock();
        }
        // fsync (disco) / PUT (S3) fora do lock: não muta a imagem e não deve
        // bloquear leitores durante o I/O de durabilidade.
        if (durability == Durability.FSYNC) {
            channel.force();
        }
        // OS_CACHE: pula o fsync por checkpoint; o SO descarrega no seu ritmo.
        // Os bytes já estão materializados (legíveis por leitores no mesmo
        // processo via page cache); a janela de perda é um crash abrupto, pois
        // um close() limpo ainda descarrega o pendente via channel.close().
    }

    private void closeChannelQuietly() {
        try {
            channel.close();
        } catch (RuntimeException e) {
            System.err.println("ngrrd-writer: falha ao fechar canal de " + storageKey + ": " + e);
        }
    }

    private sealed interface Command {
        record Write(String dsName, Sample sample) implements Command {
        }

        /** Checkpoint síncrono; {@code error} carrega uma eventual falha de volta ao chamador. */
        record Sync(CountDownLatch latch, AtomicReference<RuntimeException> error) implements Command {
        }

        record Shutdown(CountDownLatch latch) implements Command {
        }
    }
}
