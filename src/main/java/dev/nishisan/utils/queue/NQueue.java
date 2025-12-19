/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.queue;

import dev.nishisan.utils.stats.StatsUtils;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A classe NQueue representa uma implementação de fila persistente e escalável, projetada para
 * suportar operações simultâneas de produtores e consumidores. Essa classe gerencia dados que
 * podem ser armazenados tanto em memória quanto em disco, oferecendo alta resiliência e
 * consistência mesmo em condições de falhas ou reinicialização do sistema.
 *
 * A principal responsabilidade da NQueue é coordenar o armazenamento e a recuperação de
 * elementos de forma ordenada, garantindo controle sobre entrega única de elementos, suporte
 * a diferentes estratégias de armazenamento e funcionalidades como compactação de dados para
 * otimização do espaço em disco.
 *
 * No fluxo da aplicação, NQueue desempenha o papel de intermediário entre os sistemas de
 * produção e consumo, permitindo o gerenciamento eficiente de dados em transição. Suas
 * operações suportam tolerância a falhas, com mecanismos de persistência que registram o progresso
 * e o estado atual, assegurando que nenhum dado seja perdido durante a execução de tarefas críticas.
 *
 * Essa classe gerencia candidatos ao consumo de fila com bloqueios sofisticados e estratégias
 * de uso de buffers de memória para otimizações de desempenho. Sua integração com sistemas
 * externos permite leitura e gravação direta em arquivos e utiliza operações de compactação
 * para manter um controle eficiente sobre o armazenamento.
 *
 * Além disso, NQueue é extensível, com opções configuráveis para gerenciamento de estado,
 * revalidação e estratégias de compactação, tornando-a um componente adaptável para cenários
 * diversos de processamento de filas.
 */
public class NQueue<T extends Serializable> implements Closeable {
    private static final String DATA_FILE = "data.log";
    private static final String META_FILE = "queue.meta";
    private static final int MEMORY_DRAIN_BATCH_SIZE = 256;
    /**
     * Sentinel offset returned when an element is handed off directly to a waiting consumer, bypassing
     * persistence and staging buffers.
     */
    public static final long OFFSET_HANDOFF = -2;

    private final StatsUtils statsUtils = new StatsUtils();
    private final Path queueDir, dataPath, metaPath, tempDataPath;
    private volatile RandomAccessFile raf;
    private volatile FileChannel dataChannel;
    private volatile RandomAccessFile metaRaf;
    private volatile FileChannel metaChannel;
    private final ReentrantLock lock;
    private final Condition notEmpty;
    private final Object metaWriteLock = new Object();
    private final Options options;
    private final AtomicLong approximateSize = new AtomicLong(0);

    private long consumerOffset, producerOffset, recordCount, lastIndex, lastCompactionTimeNanos;
    private volatile boolean closed, shutdownRequested;
    private volatile CompactionState compactionState = CompactionState.IDLE;
    private T handoffItem;

    private final boolean enableMemoryBuffer;
    private final BlockingQueue<MemoryBufferEntry<T>> memoryBuffer;
    private final BlockingDeque<MemoryBufferEntry<T>> drainingQueue;
    private final AtomicLong memoryBufferModeUntil = new AtomicLong(0);
    private final AtomicBoolean drainingInProgress = new AtomicBoolean(false), switchBackRequested = new AtomicBoolean(false), revalidationScheduled = new AtomicBoolean(false);
    private volatile CountDownLatch drainCompletionLatch;

    private final ExecutorService drainExecutor, compactionExecutor;
    private final ScheduledExecutorService revalidationExecutor;
    private final ScheduledExecutorService maintenanceExecutor;

    private volatile long lastSizeReconciliationTimeNanos = System.nanoTime();


    /**
     * Constructs a queue instance bound to the supplied directory and I/O handles, restoring the
     * persisted state captured in the provided snapshot and applying the chosen options. The constructor
     * wires concurrency primitives and, when enabled, prepares in-memory staging and maintenance
     * services used to balance throughput during compaction or contention. Intended for internal use by
     * factory methods.
     *
     * @param queueDir base directory for persistent data and metadata
     * @param raf open random-access handle for the queue log
     * @param dataChannel file channel associated with the queue log
     * @param state recovered or rebuilt queue state to initialize cursors and counters
     * @param options operational configuration snapshot
     */
    private NQueue(Path queueDir, RandomAccessFile raf, FileChannel dataChannel, QueueState state, Options options) throws IOException {
        this.queueDir = queueDir;
        this.dataPath = queueDir.resolve(DATA_FILE);
        this.metaPath = queueDir.resolve(META_FILE);
        this.tempDataPath = queueDir.resolve(DATA_FILE + ".compacting");
        this.raf = raf;
        this.dataChannel = dataChannel;
        
        // Initialize metadata channel
        this.metaRaf = new RandomAccessFile(this.metaPath.toFile(), "rw");
        this.metaChannel = this.metaRaf.getChannel();

        this.lock = new ReentrantLock();
        this.notEmpty = this.lock.newCondition();
        this.consumerOffset = state.consumerOffset;
        this.producerOffset = state.producerOffset;
        this.recordCount = state.recordCount;
        this.lastIndex = state.lastIndex;
        this.approximateSize.set(state.recordCount);
        this.options = options;
        this.enableMemoryBuffer = options.enableMemoryBuffer;
        this.lastCompactionTimeNanos = System.nanoTime();
        if (enableMemoryBuffer) {
            this.memoryBuffer = new LinkedBlockingQueue<>(options.memoryBufferSize);
            this.drainingQueue = new LinkedBlockingDeque<>(options.memoryBufferSize);
            this.drainExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "nqueue-drain-worker");
                t.setDaemon(true);
                return t;
            });
            this.revalidationExecutor = new ScheduledThreadPoolExecutor(1, r -> {
                Thread t = new Thread(r, "nqueue-revalidation-worker");
                t.setDaemon(true);
                return t;
            });
        } else {
            this.memoryBuffer = null;
            this.drainingQueue = null;
            this.drainExecutor = null;
            this.revalidationExecutor = null;
        }
        this.compactionExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "nqueue-compaction-worker");
            t.setDaemon(true);
            return t;
        });
        this.maintenanceExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "nqueue-maintenance-worker");
            t.setDaemon(true);
            return t;
        });

        // Schedule periodic size reconciliation
        this.maintenanceExecutor.scheduleWithFixedDelay(this::reconcileSize, options.maintenanceIntervalNanos, options.maintenanceIntervalNanos, TimeUnit.NANOSECONDS);
    }


    /**
     * Realiza a reconciliação do tamanho aproximado com base no estado atual da aplicação.
     *
     * Este método é responsável por atualizar o valor do tamanho aproximado de registros,
     * sincronizando-o com o tamanho exato observado no momento da execução. Ele é projetado
     * para ser utilizado em cenários onde a precisão do tamanho deve ser mantida dentro de
     * um intervalo de tempo específico ou quando forçado por condições configuradas.
     *
     * O fluxo de execução do método segue as seguintes etapas:
     * 1. Verifica se a operação pode continuar, encerrando imediatamente se o estado
     *    indicar "fechado" ou "solicitação de desligamento" (early return).
     * 2. Calcula se a reconciliação deve ser forçada com base em um intervalo de tempo máximo
     *    definido para a reconciliação, comparado ao tempo desde a última atualização.
     * 3. Tenta adquirir um bloqueio no objeto `lock` para garantir que a operação seja segura
     *    em ambientes concorrentes. O bloqueio pode ser adquirido de forma condicional ou
     *    forçada, dependendo do contexto.
     * 4. Se o bloqueio for bem-sucedido:
     *    - Calcula o tamanho exato de registros, considerando os dados na memória e
     *      outras filas de armazenamento relacionadas, caso o buffer de memória esteja ativado.
     *    - Atualiza o valor do tamanho aproximado e armazena o timestamp da última reconciliação.
     *    - Libera o bloqueio após a atualização.
     * 5. Em caso de exceções durante a execução, o erro é registrado ou ignorado, permitindo
     *    que a aplicação continue operando sem interrupções.
     *
     * Este método interage com estruturas de dados internas e aproveita um mecanismo de
     * bloqueio para garantir consistência em ambientes multithread. É projetado para evitar
     * impactos significativos no desempenho, utilizando verificações condicionais e
     * reconciliação apenas quando necessário.
     */
    private void reconcileSize() {
        if (closed || shutdownRequested) return;

        long maxDelay = options.maxSizeReconciliationIntervalNanos;
        boolean forced = maxDelay > 0 && (System.nanoTime() - lastSizeReconciliationTimeNanos) > maxDelay;
        boolean locked = false;

        try {
            if (forced) {
                lock.lock();
                locked = true;
            } else {
                locked = lock.tryLock();
            }

            if (locked) {
                try {
                    long exactSize = recordCount;
                    if (enableMemoryBuffer) {
                        exactSize += (long) memoryBuffer.size() + drainingQueue.size();
                    }
                    approximateSize.set(exactSize);
                    lastSizeReconciliationTimeNanos = System.nanoTime();
                } finally {
                    lock.unlock();
                }
            }
        } catch (Exception e) {
            // Log or ignore, just keep running
        }
    }

    /**
     * Opens or creates a named queue under the given base directory using default options. On first use,
     * the directory structure is created. On subsequent runs, prior state is recovered so that ordering and
     * consumption progress are preserved. Any incomplete tail data is safely pruned before use.
     *
     * @param baseDir   base directory where the queue folder will reside
     * @param queueName logical queue name used as a directory under the base
     * @param <T>       serializable element type
     * @return an operational queue instance ready for concurrent producers and consumers
     * @throws IOException if the storage cannot be prepared or state cannot be recovered
     */
    public static <T extends Serializable> NQueue<T> open(Path baseDir, String queueName) throws IOException {
        return open(baseDir, queueName, Options.defaults());
    }

    /**
     * Opens or creates a named queue under the given base directory with explicit options. Startup
     * reconciles metadata with the durable log so the queue begins in a consistent state. When requested,
     * a rebuild pass is performed to reconstruct state solely from the log.
     *
     * @param baseDir   base directory where the queue folder will reside
     * @param queueName logical queue name used as a directory under the base
     * @param options   operational configuration controlling durability, compaction, and staging
     * @param <T>       serializable element type
     * @return an operational queue instance ready for concurrent producers and consumers
     * @throws IOException if the storage cannot be prepared or state cannot be recovered
     */
    public static <T extends Serializable> NQueue<T> open(Path baseDir, String queueName, Options options) throws IOException {
        Objects.requireNonNull(baseDir);
        Objects.requireNonNull(queueName);
        Objects.requireNonNull(options);
        Path qDir = baseDir.resolve(queueName);
        Files.createDirectories(qDir);
        RandomAccessFile raf = new RandomAccessFile(qDir.resolve(DATA_FILE).toFile(), "rw");
        FileChannel ch = raf.getChannel();
        QueueState state = loadOrRebuildState(ch, qDir.resolve(META_FILE), options);
        if (ch.size() > state.producerOffset) {
            ch.truncate(state.producerOffset);
            ch.force(options.withFsync);
        }
        return new NQueue<>(qDir, raf, ch, state, options);
    }

    /**
     * Oferece o objeto fornecido para inserção na fila, utilizando diferentes modos de operação
     * (memória ou disco) com base no estado atual e nas condições de uso. Este método gerencia
     * o fluxo de inserção, incluindo otimizações para entrega imediata a consumidores em espera
     * e controle de recursos, como buffers de memória e bloqueios.
     *
     * O fluxo do método inicia validando o objeto fornecido, em seguida verifica se o buffer de
     * memória está habilitado e se o tempo de operação em modo de memória ainda é válido. Caso
     * esteja, prioriza operações no buffer de memória. Quando o bloqueio é adquirido, o método
     * avalia se a fila (disco + memória) está vazia e, se possível, realiza a entrega direta do
     * item a um consumidor em espera.
     *
     * Se a operação de compactação está em andamento, o método ativa o modo de memória e executa
     * a inserção no buffer correspondente. Caso contrário, realiza a drenagem sincronizada do
     * buffer e procede com a inserção no disco. Dependendo do estado da aplicação, pode acionar
     * operações complementares para manter a consistência e desempenho da fila.
     *
     * Em situações onde o buffer de memória não está habilitado, a inserção ocorre diretamente na
     * fila principal com lógica semelhante, privilegiando consumidores em espera quando aplicável.
     *
     * Este método lida com exceções de I/O, que podem ocorrer caso haja falhas ao acessar recursos
     * externos como disco ou sistema de arquivos.
     *
     * @param object o objeto a ser oferecido na fila; não pode ser nulo. Um valor nulo resultará na
     *               interrupção imediata do fluxo com uma exceção {@link NullPointerException}.
     * @return um identificador exclusivo para a posição do objeto na fila, representado por um valor
     *         long. Pode retornar valores especiais, como {@code OFFSET_HANDOFF}, indicando que o
     *         item foi entregue diretamente para um consumidor em espera.
     * @throws IOException se ocorrer um erro de entrada/saída durante operações relacionadas à fila,
     *                     como acesso ao disco ou falha em buffers de memória.
     * @throws NullPointerException se o objeto fornecido for nulo.
     */
    public long offer(T object) throws IOException {
        Objects.requireNonNull(object);
        if (enableMemoryBuffer) {
            if (System.nanoTime() < memoryBufferModeUntil.get()) return offerToMemory(object, true);
            if (lock.tryLock()) {
                try {
                    // Short-circuit: if queue is empty (disk + memory) and a consumer is waiting, handoff directly.
                    if (options.allowShortCircuit && recordCount == 0 && handoffItem == null && drainingQueue.isEmpty() && memoryBuffer.isEmpty() && lock.hasWaiters(notEmpty)) {
                        handoffItem = object;
                        notEmpty.signal();
                        statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);
                        return OFFSET_HANDOFF;
                    }

                    if (compactionState == CompactionState.RUNNING) {
                        activateMemoryMode();
                        return offerToMemory(object, false);
                    }
                    drainMemoryBufferSync();
                    long offset = offerDirectLocked(object);
                    triggerDrainIfNeeded();
                    return offset;
                } finally {
                    lock.unlock();
                }
            } else {
                activateMemoryMode();
                return offerToMemory(object, false);
            }
        } else {
            lock.lock();
            try {
                // Short-circuit: if queue is empty and a consumer is waiting, handoff directly.
                if (options.allowShortCircuit && recordCount == 0 && handoffItem == null && lock.hasWaiters(notEmpty)) {
                    handoffItem = object;
                    notEmpty.signal();
                    statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);
                    return OFFSET_HANDOFF;
                }
                return offerDirectLocked(object);
            } finally {
                lock.unlock();
            }
        }
    }


    /**
     * Recupera o próximo elemento disponível da fila, se houver, garantindo que as operações
     * sejam realizadas com segurança em ambientes concorrentes e otimizadas para o uso de recursos.
     * O método utiliza um mecanismo de bloqueio para coordenar a leitura e escrita, garantindo que
     * dados em memória ou em disco sejam gerenciados de forma apropriada.
     *
     * O fluxo do método segue os seguintes passos:
     * 1. Obtém o bloqueio para início da operação.
     * 2. Se a contagem de registros for zero e não houver itens disponíveis em handoff, sincroniza
     *    com o buffer de memória para drenar dados persistidos.
     * 3. Verifica se há um item presente no atributo `handoffItem`:
     *    - Caso haja, o item é recuperado, removido do atributo e retornado encapsulado em um
     *      {@code Optional}.
     * 4. Caso contrário, aguarda até que um item esteja disponível na fila, utilizando
     *    {@code notEmpty.awaitUninterruptibly()}.
     * 5. Após liberar o bloqueio, o método consome o próximo registro disponível na fila usando
     *    {@code consumeNextRecordLocked()}, aplica a desserialização segura no item e o retorna.
     * 6. Durante o fluxo, realiza notificações de estatísticas de eventos relacionados ao consumo
     *    utilizando {@code statsUtils.notifyHitCounter}.
     *
     * Em caso de erro de entrada/saída, uma exceção {@link IOException} pode ser lançada.
     *
     * @return Um {@code Optional} contendo o próximo elemento da fila, se disponível. Retorna
     *         {@code Optional.empty()} se não houver registros ou se algo impedir a recuperação normal.
     * @throws IOException Se ocorrer algum problema durante o acesso aos dados persistidos.
     */
    public Optional<T> poll() throws IOException {
        lock.lock();
        try {
            // Optimization: Only force a sync drain if the disk queue is empty.
            // If we have records on disk, consume them first to avoid blocking readers on write I/O.
            if (recordCount == 0 && handoffItem == null) {
                 drainMemoryBufferSync();
            }

            if (handoffItem != null) {
                T item = handoffItem;
                handoffItem = null;
                return Optional.of(item);
            }

            while (recordCount == 0) {
                notEmpty.awaitUninterruptibly();
                if (handoffItem != null) {
                    T item = handoffItem;
                    handoffItem = null;
                    return Optional.of(item);
                }
            }
            return consumeNextRecordLocked().map(this::safeDeserialize);
        } finally {
            statsUtils.notifyHitCounter(NQueueMetrics.POLL_EVENT);
            lock.unlock();
        }
    }

    /**
     * Tenta recuperar o próximo item disponível na fila dentro do tempo limite especificado.
     * Esse método implementa uma lógica de polling que verifica a memória, realiza operações
     * de drenagem sincronizada e aguarda condições específicas para obter o próximo item.
     * Caso o tempo limite expire ou a operação seja interrompida, pode retornar um valor vazio.
     *
     * O fluxo do método consiste nas seguintes etapas:
     * 1. Converte o tempo limite fornecido para nanossegundos e adquire um bloqueio para garantir
     *    consistência em operações concorrentes.
     * 2. Caso a contagem de registros na fila esteja vazia, força uma drenagem sincronizada
     *    dos buffers de memória para tentar carregar dados disponíveis.
     * 3. Verifica se algum item ("handoffItem") já foi preparado para consumo. Caso exista,
     *    o item é retornado e removido do estado interno.
     * 4. Se nenhum item está disponível, entra em um loop onde:
     *    - Verifica novamente pela possibilidade de drenagem sincronizada de memória.
     *    - Aguarda uma notificação de que itens foram adicionados ou o tempo limite especificado expire.
     *    - Caso seja interrompido durante a espera, restaura o estado do thread, realiza uma verificação
     *      de dados e retorna vazio.
     * 5. Finalmente, consome e retorna o próximo registro da fila utilizando um método de consumo seguro
     *    e, quando presente, realiza a desserialização do item.
     *
     * Em sua conclusão, o método atualiza contadores relacionados a métricas de polling e libera o bloqueio.
     *
     * @param timeout o tempo máximo, na unidade especificada, em que o método deve aguardar para recuperar um item
     * @param unit a unidade de tempo do parâmetro {@code timeout}, como SEGUNDOS ou MILLISECONDS
     * @return um {@code Optional<T>} contendo o próximo item disponível na fila. Retorna {@code Optional.empty()}
     *         caso*/
    public Optional<T> poll(long timeout, TimeUnit unit) throws IOException {
        long nanos = unit.toNanos(timeout);
        lock.lock();
        try {
            // Optimization: Only force a sync drain if we absolutely need data and none is on disk
            if (recordCount == 0 && handoffItem == null) {
                 drainMemoryBufferSync();
            }

            if (handoffItem != null) {
                T item = handoffItem;
                handoffItem = null;
                return Optional.of(item);
            }

            while (recordCount == 0) {
                // If checking memory triggered a drain and resulted in records, break
                if (checkAndDrainMemorySync()) break;
                
                if (nanos <= 0L) return Optional.empty();
                try {
                    nanos = notEmpty.awaitNanos(nanos);
                    if (handoffItem != null) {
                        T item = handoffItem;
                        handoffItem = null;
                        return Optional.of(item);
                    }
                    if (checkAndDrainMemorySync()) break;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    checkAndDrainMemorySync();
                    return Optional.empty();
                }
            }
            return consumeNextRecordLocked().map(this::safeDeserialize);
        } finally {
            statsUtils.notifyHitCounter(NQueueMetrics.POLL_EVENT);
            lock.unlock();
        }
    }


    /**
     * Retorna opcionalmente o próximo item da fila para leitura, sem removê-lo.
     * Este método é utilizado para visualizar o elemento "head" da fila sem
     * alterá-la, verificando diversas fontes de armazenamento como disco
     * ou buffers de memória. O método é thread-safe e fornece garantias de
     * sincronização interna para operações concorrentes.
     *
     * O fluxo de processamento segue as seguintes etapas:
     * 1. Verifica se existe um item de transferência direta (handoffItem). Se existir, ele é retornado.
     * 2. Caso contrário, verifica a presença de registros armazenados no disco. Se há registros,
     *    busca o elemento do início da fila (FIFO) e o desserializa antes de retorná-lo encapsulado
     *    em um objeto Optional.
     * 3. Na ausência de registros em disco, e se o buffer de memória estiver habilitado, verifica os
     *    itens tanto na fila de drenagem quanto no buffer de memória. O elemento mais antigo é retornado,
     *    caso existente.
     * 4. Se não houver elementos disponíveis em nenhum dos recursos mencionados, retorna um Optional vazio.
     *
     * Este método registra uma métrica de estatísticas para cada execução e garante o desbloqueio do recurso
     * mesmo em caso de falhas ou exceções.
     *
     * @return Um {@code Optional} contendo o próximo item disponível na fila, caso exista, ou {@code Optional.empty()}
     *         se nenhum item estiver disponível em disco ou em memória.
     * @throws IOException Caso ocorra um erro ao tentar acessar ou ler do disco.
     */
    public Optional<T> peek() throws IOException {
        lock.lock();
        try {
            // 0. If we have a handoff item, it is effectively the head
            if (handoffItem != null) {
                return Optional.of(handoffItem);
            }

            // 1. If we have records on disk, peek from there (FIFO head)
            if (recordCount > 0) {
                return readAtInternal(consumerOffset).map(res -> safeDeserialize(res.getRecord()));
            }

            // 2. If disk is empty but we have memory buffer enabled, check memory buffers.
            // NOTE: This does NOT drain/flush to disk, it just looks at what is waiting in RAM.
            if (enableMemoryBuffer) {
                // Draining queue has older items than memoryBuffer, check it first.
                MemoryBufferEntry<T> fromDrain = drainingQueue.peekFirst();
                if (fromDrain != null) {
                    return Optional.of(fromDrain.item);
                }
                
                MemoryBufferEntry<T> fromMem = memoryBuffer.peek();
                if (fromMem != null) {
                    return Optional.of(fromMem.item);
                }
            }
            
            return Optional.empty();
        } finally {
            statsUtils.notifyHitCounter(NQueueMetrics.PEEK_EVENT);
            lock.unlock();
        }
    }

    /**
     * Calcula e retorna o tamanho total relacionado a elementos gerenciados por esta classe,
     * considerando diversos fatores e estados internos.
     *
     * O método utiliza um fluxo seguro com bloqueio para evitar condições de corrida durante
     * o cálculo. Ele contabiliza:
     * 1. O número de registros principais (recordCount).
     * 2. O item em processo de transferência (handoffItem), se existente.
     * 3. O conteúdo armazenado em um buffer de memória (memoryBuffer), caso este esteja ativado.
     * 4. A fila de drenagem (drainingQueue).
     *
     * Esse fluxo permite consolidar todos os elementos relevantes, mesmo em contextos de estados
     * transitórios ou operacionais da classe.
     *
     * @return O tamanho total de elementos, somando registros persistidos, itens em transferência,
     *         elementos em buffer de memória e elementos na fila de drenagem. O valor pode variar
     *         dependendo do estado interno dos recursos e das condições verificadas no momento.
     */
    public long size() {
        lock.lock();
        try {
            long s = recordCount;
            if (handoffItem != null) s++;
            if (enableMemoryBuffer) s += (long) memoryBuffer.size() + drainingQueue.size();
            return s;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retorna o tamanho atual de um elemento, podendo utilizar uma abordagem otimista ou não.
     * O método permite obter uma estimativa aproximada do tamanho ou calcular o valor exato,
     * dependendo do parâmetro fornecido.
     *
     * O fluxo de execução do método é:
     * - Se o parâmetro `optimistic` for verdadeiro, o método retorna um valor aproximado
     *   obtido pela chamada ao método `get()` de `approximateSize`.
     * - Caso contrário, o método executa a lógica interna do método `size()` para retornar
     *   o valor exato.
     *
     * Este método pode ser utilizado em cenários onde há uma necessidade de desempenho
     * superior e uma estimativa é suficiente, ou em casos que exijam precisão com o custo
     * de um cálculo mais detalhado.
     *
     * @param optimistic um valor booleano que indica se o tamanho será retornado de forma
     *                   otimista (utilizando uma estimativa aproximada) ou exata.
     *                   Se verdadeiro, uma estimativa é retornada; caso contrário, o valor
     *                   real é calculado.
     * @return o tamanho do elemento, seja uma estimativa aproximada (se `optimistic` for
     *         verdadeiro) ou o valor exato (se `optimistic` for falso).
     */
    public long size(boolean optimistic) {
        return optimistic ? approximateSize.get() : size();
    }

    /**
     * Indicates whether the queue is currently empty according to a precise size check.
     *
     * @return true if no records are pending delivery; false otherwise
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Returns the count of durable records strictly within the persistent log segment awaiting consumption.
     * This value does not include items that may be temporarily staged in memory.
     *
     * @return durable record count pending delivery
     */
    public long getRecordCount() {
        lock.lock();
        try {
            return recordCount;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Fecha e libera todos os recursos utilizados pela classe para assegurar
     * uma finalização ordenada e segura das operações.
     *
     * O método segue uma sequência lógica para garantir que todas as atividades
     * de manutenção, buffers em memória e estados de compactação sejam
     * adequadamente tratados antes do encerramento dos recursos externos, como
     * arquivos ou canais de dados. Este método implementa a interface {@code AutoCloseable}.
     * Caso ocorram erros durante o fechamento, uma exceção {@code IOException} pode ser lançada.
     *
     * Fluxo do método:
     * 1. Interrompe imediatamente o executor responsável por tarefas de manutenção utilizando o método {@code shutdownNow}.
     * 2. Se o buffer em memória estiver habilitado (`enableMemoryBuffer`), esvazia o buffer sincronamente.
     *    O esvaziamento é protegido por um bloqueio para evitar acessos concorrentes.
     * 3. Marca uma solicitação de desligamento por meio da variável `shutdownRequested`.
     *    Caso haja dados acumulados a serem processados e o estado de compactação seja `IDLE`,
     *    uma tarefa de compactação é submetida ao executor.
     * 4. Encerra o executor responsável pelas operações de compactação e aguarda sua finalização,
     *    com um timeout definido para evitar bloqueios indefinidos.
     * 5. Se o buffer em memória estiver habilitado, também interrompe imediata e coordenadamente
     *    os executores relacionados ao esvaziamento de buffers e revalidações.
     * 6. Fecha todos os canais e streams de dados associados (`dataChannel`, `raf`, `metaChannel`, `metaRaf`),
     *    assegurando que estejam em um estado apropriado antes do encerramento.
     *    Essa operação também é protegida por bloqueios para evitar problemas concorrentes.
     * 7. Marca a instância como encerrada definindo a variável `closed = true`.
     *
     * Em cenários de erro:
     * - Se ocorrerem exceções ao esvaziar o buffer em memória, elas serão ignoradas explicitamente
     *   para garantir a execução do fluxo de fechamento subsequente.
     * - Se o método for interrompido durante a espera pela finalização dos executores,
     *  */
    @Override
    public void close() throws IOException {
        // Stop maintenance first
        maintenanceExecutor.shutdownNow();

        if (enableMemoryBuffer) {
            lock.lock();
            try {
                drainMemoryBufferSync();
            } catch (IOException ignored) {
            } finally {
                lock.unlock();
            }
        }
        lock.lock();
        try {
            shutdownRequested = true;
            if (compactionState == CompactionState.IDLE && consumerOffset > 0) {
                QueueState snap = currentState();
                compactionState = CompactionState.RUNNING;
                compactionExecutor.submit(() -> runCompactionTask(snap));
            }
        } finally {
            lock.unlock();
        }
        compactionExecutor.shutdown();
        try {
            compactionExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (enableMemoryBuffer) {
            drainExecutor.shutdownNow();
            revalidationExecutor.shutdownNow();
        }
        lock.lock();
        try {
            if (dataChannel.isOpen()) dataChannel.close();
            if (raf != null) raf.close();
            if (metaChannel != null && metaChannel.isOpen()) metaChannel.close();
            if (metaRaf != null) metaRaf.close();
            closed = true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits briefly for an in-progress memory-buffer drain to complete, if any. Used to improve handoff
     * between maintenance phases and normal operation without blocking indefinitely.
     */
    private void awaitDrainCompletion() {
        if (!enableMemoryBuffer) return;
        if (!drainingInProgress.get() && (drainingQueue == null || drainingQueue.isEmpty())) return;
        CountDownLatch latch = drainCompletionLatch;
        if (latch != null) {
            try {
                latch.await(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Callback invoked after a compaction attempt finishes. It reconciles the operating mode with the
     * current workload, scheduling a revalidation step when necessary to decide whether to continue using
     * the in-memory staging path or revert to direct durable appends.
     *
     * @param success whether the compaction reached a consistent end state
     * @param error   an optional cause when a failure was detected
     */
    private void onCompactionFinished(boolean success, Throwable error) {
        if (!enableMemoryBuffer) return;
        boolean acquired = lock.tryLock();
        if (acquired) {
            try {
                if (compactionState == CompactionState.RUNNING) compactionState = CompactionState.IDLE;
                if (memoryBuffer.isEmpty() && drainingQueue.isEmpty()) {
                    memoryBufferModeUntil.set(0);
                    return;
                }
            } finally {
                lock.unlock();
            }
        }
        if (!switchBackRequested.compareAndSet(false, true)) return;
        revalidationExecutor.execute(() -> {
            try {
                if (lock.tryLock()) {
                    try {
                        if (compactionState != CompactionState.RUNNING && memoryBuffer.isEmpty() && drainingQueue.isEmpty()) {
                            memoryBufferModeUntil.set(0);
                            return;
                        }
                    } finally {
                        lock.unlock();
                    }
                }
                try {
                    if (lock.tryLock(options.lockTryTimeoutNanos, TimeUnit.NANOSECONDS)) {
                        try {
                            drainMemoryBufferSync();
                        } finally {
                            lock.unlock();
                        }
                    }
                } catch (Exception ignored) {
                }
                if (lock.tryLock()) {
                    try {
                        if (memoryBuffer.isEmpty() && drainingQueue.isEmpty() && compactionState != CompactionState.RUNNING) {
                            memoryBufferModeUntil.set(0);
                        } else {
                            activateMemoryMode();
                            triggerDrainIfNeeded();
                        }
                    } finally {
                        lock.unlock();
                    }
                } else {
                    activateMemoryMode();
                }
            } finally {
                switchBackRequested.set(false);
            }
        });
    }

    /**
     * Reads, without advancing the consumption cursor, the record stored at the provided durable offset and
     * returns the deserialized element if a complete record exists at that position. This is intended for
     * diagnostic and auditing scenarios.
     *
     * @param offset durable position to inspect
     * @return the element at the offset or empty if no complete record is present
     * @throws IOException if reading from storage fails
     */
    public Optional<T> readAt(long offset) throws IOException {
        lock.lock();
        try {
            return readAtInternal(offset).map(res -> safeDeserialize(res.getRecord()));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads, without advancing the consumption cursor, the record stored at the provided durable offset and
     * returns its structured representation together with the next record position. This variant exposes
     * raw metadata for advanced tooling.
     *
     * @param offset durable position to inspect
     * @return a structured result with the record and the next offset, or empty if none is available
     * @throws IOException if reading from storage fails
     */
    public Optional<NQueueReadResult> readRecordAt(long offset) throws IOException {
        lock.lock();
        try {
            return readAtInternal(offset);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns, without advancing the consumption cursor, the raw representation of the head record if one
     * exists. This provides visibility into metadata alongside the payload for diagnostic use.
     *
     * @return the next record or empty if the queue is logically empty
     * @throws IOException if reading from storage fails
     */
    public Optional<NQueueRecord> peekRecord() throws IOException {
        lock.lock();
        try {
            if (recordCount == 0) return Optional.empty();
            return readAtInternal(consumerOffset).map(NQueueReadResult::getRecord);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Appends a single element directly to durable storage. Coordination must already be in place to ensure
     * exclusive access to mutable state. Intended for internal paths that bypass the in-memory staging layer.
     *
     * @param object element to append
     * @return logical durable offset of the appended record
     * @throws IOException if the append cannot be acknowledged
     */
    private long offerDirectLocked(T object) throws IOException {
        return offerBatchLocked(List.of(object), true);
    }

    /**
     * Escreve em lote os itens fornecidos no canal de dados associado, atualizando os metadados,
     * persistindo o estado atual e notificando alterações relevantes na fila.
     *
     * Este método processa uma lista de itens, convertendo cada um para formato binário,
     * associando metadados e registrando-os no armazenamento subjacente. Ele também
     * gerencia ajustes em deslocamentos do produtor e consumidor, bem como
     * os contadores de registros e métricas associadas. A sincronização no sistema de arquivos
     * pode ser opcionalmente realizada dependendo do parâmetro fornecido.
     *
     * Fluxo do método:
     * 1. Valida se a lista de itens não está vazia; caso contrário, retorna imediatamente -1.
     * 2. Inicializa os deslocamentos e contadores iniciais.
     * 3. Itera sobre cada item da lista:
     *    - Converte o item para um array de bytes.
     *    - Calcula e associa os metadados relacionados ao item.
     *    - Escreve o cabeçalho de metadados e os dados binários do item no canal de dados.
     *    - Atualiza o deslocamento de escrita.
     * 4. Atualiza os deslocamentos do produtor e do consumidor, bem como contadores internos.
     * 5. Persistente o estado atual da fila.
     * 6. Condicionalmente realiza sincronização forçada no canal, caso solicitado.
     * 7. Avalia a necessidade de compactação e aciona os sinais e métricas apropriados.
     *
     * @param items a lista de itens que serão inseridos em lote na fila. Cada item será serializado,
     *              armazenado no canal de dados e terá metadados registrados. A lista não pode ser vazia.
     * @param fsync indica se um sincronismo forçado deve ser executado no canal de dados após
     *              o processamento do lote. Se verdadeiro, força a gravação no sistema de arquivos.
     * @return o deslocamento inicial (offset) do primeiro item persistido neste lote. Retorna -1
     *         se a lista fornecida estiver vazia.
     * @throws IOException se ocorrerem erros de entrada/saída durante a gravação no canal de dados
     *                     ou na persistência do estado atual.
     */
    private long offerBatchLocked(List<T> items, boolean fsync) throws IOException {
        if (items.isEmpty()) return -1;
        long writePos = producerOffset, firstStart = -1, initialCount = recordCount;
        for (T obj : items) {
            byte[] payload = toBytes(obj);
            lastIndex++;
            if (lastIndex < 0) lastIndex = 0;
            NQueueRecordMetaData meta = new NQueueRecordMetaData(lastIndex, payload.length, obj.getClass().getCanonicalName());
            ByteBuffer hb = meta.toByteBuffer();
            long rStart = writePos;
            while (hb.hasRemaining()) writePos += dataChannel.write(hb, writePos);
            ByteBuffer pb = ByteBuffer.wrap(payload);
            while (pb.hasRemaining()) writePos += dataChannel.write(pb, writePos);
            if (firstStart < 0) firstStart = rStart;
        }
        producerOffset = writePos;
        recordCount += items.size();
        approximateSize.addAndGet(items.size());
        if (initialCount == 0) consumerOffset = firstStart;
        persistCurrentStateLocked();
        if (fsync && options.withFsync) dataChannel.force(true);
        maybeCompactLocked();
        notEmpty.signalAll();
        statsUtils.notifyHitCounter(NQueueMetrics.OFFERED_EVENT);
        return firstStart;
    }

    /**
     * Lê e interpreta um registro em um canal de dados específico com base em um
     * deslocamento inicial, retornando as informações encapsuladas do registro se forem válidas.
     * Este método realiza múltiplas verificações de integridade e segue um fluxo rigoroso
     * para garantir a consistência dos dados lidos.
     *
     * O processo executa os seguintes passos:
     * 1. Calcula o tamanho total do canal de dados.
     * 2. Verifica se o deslocamento e o tamanho fixo do prefixo do registro estão dentro dos limites do canal.
     * 3. Lê o prefixo fixo do cabeçalho a partir do canal no deslocamento especificado.
     * 4. Calcula o final do cabeçalho e valida se este está dentro do tamanho do canal.
     * 5. Lê os metadados do registro baseados nos dados do prefixo.
     * 6. Calcula o final do payload, validando novamente se está dentro do tamanho do canal.
     * 7. Lê os dados do payload em um byte array, garantindo que tudo seja consumido adequadamente.
     * 8. Se todas as etapas forem concluídas com sucesso, retorna o resultado encapsulado contendo
     *    o registro lido e o próximo ponto de leitura no canal.
     *
     * Em caso de erro, o método pode retornar um Optional vazio ou lançar uma exceção, dependendo
     * da natureza do problema encontrado.
     *
     * @param offset o deslocamento no canal de dados a partir do qual o registro será lido.
     *               Deve estar dentro dos limites permitidos pelo tamanho do canal.
     * @return um <code>Optional</code> contendo um <code>NQueueReadResult</code> que encapsula o
     *         registro lido e a posição final da leitura no canal. Retorna um Optional vazio se os
     *         dados no deslocamento não forem válidos para um registro completo.
     * @throws IOException se ocorrer um erro de leitura no canal de dados, incluindo casos onde
     *                     EOF é alcançado de forma inesperada durante a leitura do registro.
     */
    private Optional<NQueueReadResult> readAtInternal(long offset) throws IOException {
        long size = dataChannel.size();
        if (offset + NQueueRecordMetaData.fixedPrefixSize() > size) return Optional.empty();
        NQueueRecordMetaData.HeaderPrefix pref = NQueueRecordMetaData.readPrefix(dataChannel, offset);
        long hEnd = offset + NQueueRecordMetaData.fixedPrefixSize() + pref.headerLen;
        if (hEnd > size) return Optional.empty();
        NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(dataChannel, offset, pref.headerLen);
        long pEnd = hEnd + meta.getPayloadLen();
        if (pEnd > size) return Optional.empty();
        byte[] payload = new byte[meta.getPayloadLen()];
        ByteBuffer pb = ByteBuffer.wrap(payload);
        while (pb.hasRemaining()) {
            if (dataChannel.read(pb, hEnd + (long) pb.position()) < 0) throw new EOFException();
        }
        return Optional.of(new NQueueReadResult(new NQueueRecord(meta, payload), pEnd));
    }

    /**
     * Consome o próximo registro disponível na fila de forma segura, garantindo que operações
     * concorrentes sejam evitadas através de bloqueio.
     *
     * O método realiza a leitura do registro no deslocamento atual do consumidor e atualiza
     * os estados internos relacionados, como o deslocamento do consumidor, o número de registros
     * restantes e o tamanho aproximado da estrutura. Caso não haja mais registros, o deslocamento
     * do consumidor será sincronizado com o do produtor.
     *
     * Após modificar o estado interno, o método persiste o estado atualizado e avalia a necessidade
     * de compactação da estrutura de dados subjacente. Caso ocorra falha durante essas operações,
     * como erros de I/O, será lançado um RuntimeException encapsulando a causa original.
     *
     * @return Um {@code Optional} contendo o próximo registro consumido, ou um {@code Optional.empty()}
     *         caso não existam registros disponíveis.
     * @throws IOException se ocorrer falha durante a leitura ou na tentativa de persistir o estado.
     */
    private Optional<NQueueRecord> consumeNextRecordLocked() throws IOException {
        return readAtInternal(consumerOffset).map(res -> {
            consumerOffset = res.getNextOffset();
            recordCount--;
            approximateSize.decrementAndGet();
            if (recordCount == 0) consumerOffset = producerOffset;
            try {
                persistCurrentStateLocked();
                maybeCompactLocked();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return res.getRecord();
        });
    }

    /**
     * Persists the current queue cursors and counters so that progress is preserved across restarts. This
     * method assumes exclusive access to the queue state.
     *
     * @throws IOException if the metadata cannot be written
     */
    private void persistCurrentStateLocked() throws IOException {
        synchronized (metaWriteLock) {
            NQueueQueueMeta.update(metaChannel, consumerOffset, producerOffset, recordCount, lastIndex, options.withFsync);
        }
    }

    /**
     * Restores a consistent queue state using existing metadata when possible, or by scanning the durable
     * log to rebuild cursors and counters. When the log contains incomplete data at the tail, the torn tail
     * is discarded to ensure a consistent starting point.
     *
     * @param ch       channel to the durable log
     * @param metaPath path to the metadata file
     * @param options  startup options controlling reset behavior
     * @return recovered state snapshot
     * @throws IOException if recovery cannot complete
     */
    private static QueueState loadOrRebuildState(FileChannel ch, Path metaPath, Options options) throws IOException {
        if (options.resetOnRestart) {
            QueueState s = rebuildStateFromLog(ch);
            NQueueQueueMeta.write(metaPath, s.consumerOffset, s.producerOffset, s.recordCount, s.lastIndex);
            return s;
        }
        if (Files.exists(metaPath)) {
            try {
                NQueueQueueMeta meta = NQueueQueueMeta.read(metaPath);
                QueueState s = new QueueState(meta.getConsumerOffset(), meta.getProducerOffset(), meta.getRecordCount(), meta.getLastIndex());
                if (s.consumerOffset >= 0 && s.producerOffset >= s.consumerOffset && ch.size() >= s.producerOffset)
                    return s;
            } catch (IOException ignored) {
            }
        }
        QueueState s = rebuildStateFromLog(ch);
        NQueueQueueMeta.write(metaPath, s.consumerOffset, s.producerOffset, s.recordCount, s.lastIndex);
        return s;
    }

    /**
     * Scans the durable log from the beginning to reconstruct a consistent state snapshot, stopping at the
     * last complete record. Any partial tail is removed before the state is returned. This operation is used
     * when metadata is absent or cannot be trusted.
     *
     * @param ch channel to the durable log
     * @return reconstructed state snapshot
     * @throws IOException if the log cannot be read or reconciled
     */
    private static QueueState rebuildStateFromLog(FileChannel ch) throws IOException {
        long offset = 0, count = 0, lastIdx = -1, size = ch.size();
        while (offset < size) {
            try {
                NQueueRecordMetaData.HeaderPrefix pref = NQueueRecordMetaData.readPrefix(ch, offset);
                NQueueRecordMetaData meta = NQueueRecordMetaData.fromBuffer(ch, offset, pref.headerLen);
                offset = offset + NQueueRecordMetaData.fixedPrefixSize() + pref.headerLen + meta.getPayloadLen();
                if (offset > size) throw new EOFException();
                count++;
                lastIdx = meta.getIndex();
            } catch (Exception e) {
                ch.truncate(offset);
                size = offset;
                break;
            }
        }
        ch.force(true);
        return new QueueState(count > 0 ? 0 : size, size, count, lastIdx);
    }

    /**
     * Transfers a contiguous region from one channel to another. Used by background maintenance to assemble
     * a compacted log while preserving the unconsumed segment and arrival order.
     *
     * @param src   source channel
     * @param start start position, inclusive
     * @param end   end position, exclusive
     * @param dst   destination channel
     * @throws IOException if the transfer fails
     */
    private void copyRegion(FileChannel src, long start, long end, FileChannel dst) throws IOException {
        long pos = start, count = end - start;
        while (count > 0) {
            long t = src.transferTo(pos, count, dst);
            if (t <= 0) break;
            pos += t;
            count -= t;
        }
    }

    /**
     * Serializes an element to a byte array using the platform’s object serialization mechanism. Used to
      * produce a durable payload for storage.
     *
     * @param obj element to serialize
     * @return binary representation suitable for persistence
     * @throws IOException if the element cannot be serialized
     */
    private byte[] toBytes(T obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(bos))) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        }
    }

    /**
     * Deserializes the payload of a raw record into the expected element type. Errors during deserialization
     * surface as unchecked failures to signal data that cannot be interpreted on the current classpath.
     *
     * @param record raw record containing a binary payload
     * @return deserialized element
     */
    private T safeDeserialize(NQueueRecord record) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(record.payload()); ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (T) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Activates a temporary staging mode that routes new enqueues through the in-memory buffer for a short
     * interval. This helps to smooth producer throughput during maintenance or brief contention bursts. The
     * mode revalidates itself periodically to decide when to revert to direct appends.
     */
    private void activateMemoryMode() {
        if (!enableMemoryBuffer) return;
        long until = System.nanoTime() + options.revalidationIntervalNanos;
        memoryBufferModeUntil.updateAndGet(c -> Math.max(c, until));
        scheduleRevalidation();
    }

    /**
     * Schedules a future revalidation of the temporary staging mode when not already pending. The scheduled
     * task verifies whether conditions still warrant staying in memory-buffer mode.
     */
    private void scheduleRevalidation() {
        if (revalidationExecutor != null && revalidationScheduled.compareAndSet(false, true)) {
            revalidationExecutor.schedule(() -> {
                try {
                    revalidateMemoryMode();
                } finally {
                    revalidationScheduled.set(false);
                    if (memoryBufferModeUntil.get() > System.nanoTime()) scheduleRevalidation();
                }
            }, options.revalidationIntervalNanos, TimeUnit.NANOSECONDS);
        }
    }

    /**
     * Re-evaluates whether the in-memory staging mode should continue. When conditions improve and no staged
     * work remains, the queue reverts to direct durable appends; otherwise, the staging window is extended.
     */
    private void revalidateMemoryMode() {
        if (System.nanoTime() >= memoryBufferModeUntil.get()) {
            if (lock.tryLock()) {
                try {
                    if (compactionState != CompactionState.RUNNING && (!enableMemoryBuffer || (memoryBuffer.isEmpty() && drainingQueue.isEmpty()))) {
                        memoryBufferModeUntil.set(0);
                        return;
                    }
                } finally {
                    lock.unlock();
                }
            }
            activateMemoryMode();
        }
    }

    /**
     * Attempts to place a new element into the bounded in-memory staging buffer. When the buffer is full and
     * conditions allow, the queue may opportunistically revert to a direct durable append for the current
     * element. Producers block only when the buffer is at capacity and no immediate fallback is possible.
     *
     * @param object             element to stage
     * @param revalidateIfFull   whether to reassess staging mode when the buffer is saturated
     * @return a sentinel indicating that durability will be achieved by a later drain
     * @throws IOException if a fallback durable append is attempted and fails
     */
    private long offerToMemory(T object, boolean revalidateIfFull) throws IOException {
        try {
            if (revalidateIfFull && memoryBuffer.remainingCapacity() == 0) {
                revalidateMemoryMode();
                if (lock.tryLock()) {
                    try {
                        if (compactionState != CompactionState.RUNNING) {
                            drainMemoryBufferSync();
                            return offerDirectLocked(object);
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            }
            MemoryBufferEntry<T> e = new MemoryBufferEntry<>(object);
            if (!memoryBuffer.offer(e)) memoryBuffer.put(e);
            triggerDrainIfNeeded();
            return -1;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException(ex);
        }
    }

    /**
     * Initiates an asynchronous drain from the staging buffer into durable storage when necessary. A single
     * drain worker is kept active at a time to preserve ordering.
     */
    private void triggerDrainIfNeeded() {
        if (enableMemoryBuffer && !memoryBuffer.isEmpty() && drainingInProgress.compareAndSet(false, true)) {
            drainCompletionLatch = new CountDownLatch(1);
            drainExecutor.submit(this::drainMemoryBufferAsync);
        }
    }

    /**
     * Background worker that periodically acquires the necessary coordination to flush staged elements into
     * durable storage in batches. The worker makes progress opportunistically to minimize interference with
     * producers and consumers.
     */
    private void drainMemoryBufferAsync() {
        try {
            while (enableMemoryBuffer && (!memoryBuffer.isEmpty() || !drainingQueue.isEmpty())) {
                boolean ok = false;
                try {
                    if (lock.tryLock(options.lockTryTimeoutNanos, TimeUnit.NANOSECONDS)) {
                        try {
                            drainMemoryBufferSync();
                            ok = true;
                        } finally {
                            lock.unlock();
                        }
                    }
                } catch (Exception ignored) {
                }
                if (!ok) {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            drainingInProgress.set(false);
            if (drainCompletionLatch != null) drainCompletionLatch.countDown();
            if (enableMemoryBuffer && !memoryBuffer.isEmpty()) triggerDrainIfNeeded();
        }
    }

    /**
     * Synchronously flushes staged elements into durable storage in FIFO order, aggregating work into small
     * batches to reduce coordination overhead. Failures result in staged entries being retained for a later
     * retry.
     *
     * @throws IOException if persistence fails and the drain cannot complete
     */
    private void drainMemoryBufferSync() throws IOException {
        if (!enableMemoryBuffer) return;
        while (true) {
            memoryBuffer.drainTo(drainingQueue, MEMORY_DRAIN_BATCH_SIZE);
            if (drainingQueue.isEmpty()) break;
            List<T> batch = new ArrayList<>();
            List<MemoryBufferEntry<T>> entries = new ArrayList<>();
            for (int i = 0; i < MEMORY_DRAIN_BATCH_SIZE; i++) {
                MemoryBufferEntry<T> ent = drainingQueue.poll();
                if (ent == null) break;
                batch.add(ent.item);
                entries.add(ent);
            }
            if (batch.isEmpty()) break;
            try {
                offerBatchLocked(batch, options.withFsync);
            } catch (IOException ex) {
                for (int i = entries.size() - 1; i >= 0; i--) drainingQueue.addFirst(entries.get(i));
                throw ex;
            }
        }
    }

    /**
     * Checks for staged work and performs a synchronous drain when present, indicating whether durable
     * records became available for consumption as a result.
     *
     * @return true if at least one durable record is now available; false otherwise
     * @throws IOException if draining encounters a storage error
     */
    private boolean checkAndDrainMemorySync() throws IOException {
        if (enableMemoryBuffer && (!memoryBuffer.isEmpty() || !drainingQueue.isEmpty())) {
            drainMemoryBufferSync();
            return recordCount > 0;
        }
        return false;
    }

    /**
     * Evaluates whether conditions warrant a background compaction and, when they do, schedules one. The
     * decision considers the amount of consumed data and an optional time-based trigger, and avoids running
     * when a compaction is already in progress or shutdown is underway.
     */
    private void maybeCompactLocked() {
        if (compactionState == CompactionState.RUNNING || shutdownRequested) return;
        if (producerOffset <= 0 || consumerOffset <= 0) return;
        long now = System.nanoTime();
        if (((double) consumerOffset / (double) producerOffset) >= options.compactionWasteThreshold || (options.compactionIntervalNanos > 0 && (now - lastCompactionTimeNanos) >= options.compactionIntervalNanos && consumerOffset > 0)) {
            QueueState snap = currentState();
            compactionState = CompactionState.RUNNING;
            lastCompactionTimeNanos = now;
            compactionExecutor.submit(() -> runCompactionTask(snap));
        }
    }


    /**
     * Executa a tarefa de compactação de dados, gerenciando leitura, escrita e
     * sincronização de estado para garantir integridade e eficiência durante o
     * processo. Este método é responsável por copiar dados relevantes da área
     * de armazenamento principal para um arquivo temporário, realizar a
     * compactação e finalizar a operação com as atualizações necessárias.
     *
     * O fluxo do método é dividido em várias etapas:
     * 1. Remove o arquivo temporário existente, caso ele esteja presente, para garantir um estado limpo.
     * 2. Cria um novo arquivo temporário e acessa seu canal de escrita.
     * 3. Identifica os offsets de início e fim para a área de dados que será compactada,
     *    baseando-se nas posições atuais do consumidor e do produtor. Essa operação é realizada
     *    de forma protegida por um bloqueio (`lock`) para evitar inconsistências durante o acesso
     *    concorrente.
     * 4. Copia o intervalo de dados definido entre os offsets do canal principal para o
     *    canal do arquivo temporário.
     * 5. Se a opção de sincronização de disco (`withFsync`) estiver ativada, força a escrita
     *    do arquivo no disco para garantir persistência.
     * 6. Finaliza a compactação através da aplicação das alterações e libera os recursos.
     *
     * Em cenários de falha, o método gerencia as exceções, redefine o estado de compactação
     * para ocioso (`IDLE`) e remove o arquivo temporário para evitar resíduos no sistema.
     * Finalmente, chama a função de callback para sinalizar o término da operação, seja ela
     * bem-sucedida ou não.
     *
     * @param snap estado atual da fila de dados, utilizado para inicializar e
     *             determinar os offsets de escrita e compactação. Define o
     *             ponto inicial e final para leitura e manipulação dos dados.
     * @throws IOException se ocorrerem falhas de I/O durante a leitura ou escrita nos arquivos.
     */
    private void runCompactionTask(QueueState snap) {
        try {
            Files.deleteIfExists(tempDataPath);
            try (RandomAccessFile tmpRaf = new RandomAccessFile(tempDataPath.toFile(), "rw"); FileChannel tmpCh = tmpRaf.getChannel()) {
                long copyStartOffset;
                long copyEndOffset;

                lock.lock();
                try {
                    // We start from the CURRENT consumer position to avoid copying already consumed data.
                    copyStartOffset = consumerOffset;
                    // We copy up to the current producer position to maximize work done outside the lock.
                    copyEndOffset = producerOffset;
                } finally {
                    lock.unlock();
                }

                if (copyEndOffset > copyStartOffset) {
                    copyRegion(dataChannel, copyStartOffset, copyEndOffset, tmpCh);
                }
                
                if (options.withFsync) tmpCh.force(true);
                finalizeCompaction(copyStartOffset, copyEndOffset, tmpCh);
            }
        } catch (Throwable t) {
            lock.lock();
            try {
                compactionState = CompactionState.IDLE;
            } finally {
                lock.unlock();
            }
            try {
                Files.deleteIfExists(tempDataPath);
            } catch (IOException ignored) {
            }
        } finally {
            onCompactionFinished(compactionState == CompactionState.IDLE, null);
        }
    }

    /**
     * Finalizes a compaction by atomically replacing the active log with the compacted version, recalculating
     * cursors to reflect additional data appended during the copy window, and updating persisted metadata.
     * Upon completion, producers and consumers continue unaffected.
     *
     * @param copyStartOffset durable position where the background copy started
     * @param copyEndOffset   durable position where the background copy ended
     * @param tmpCh           channel containing the compacted content
     * @throws IOException if the replacement or state update cannot be completed
     */
    private void finalizeCompaction(long copyStartOffset, long copyEndOffset, FileChannel tmpCh) throws IOException {
        lock.lock();
        try {
            // 1. Copy any data that was appended while we were copying in background
            if (producerOffset > copyEndOffset) {
                copyRegion(dataChannel, copyEndOffset, producerOffset, tmpCh);
            }

            // 2. Calculate new offsets relative to the new file
            // The new file contains data starting from copyStartOffset.
            long newPO = tmpCh.position();
            long newCO = Math.max(0, consumerOffset - copyStartOffset);
            
            if (recordCount == 0) {
                newCO = 0;
                newPO = 0;
                tmpCh.truncate(0);
            }
            
            if (newCO > newPO) newCO = newPO;
            
            if (options.withFsync) tmpCh.force(true);
            tmpCh.close();

            // 3. Atomic swap
            try {
                Files.move(tempDataPath, dataPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tempDataPath, dataPath, StandardCopyOption.REPLACE_EXISTING);
            }

            RandomAccessFile nRaf = new RandomAccessFile(dataPath.toFile(), "rw");
            FileChannel nCh = nRaf.getChannel();
            try {
                dataChannel.close();
                raf.close();
            } catch (IOException ignored) {
            }
            
            raf = nRaf;
            dataChannel = nCh;
            consumerOffset = newCO;
            producerOffset = newPO;
            persistCurrentStateLocked();
        } finally {
            compactionState = CompactionState.IDLE;
            lock.unlock();
            triggerDrainIfNeeded();
        }
    }

    /**
     * Creates a lightweight snapshot of the queue’s current cursors and counters for use in maintenance and
     * diagnostics.
     *
     * @return current state snapshot
     */
    private QueueState currentState() {
        return new QueueState(consumerOffset, producerOffset, recordCount, lastIndex);
    }

    /**
     * Indicates whether the queue is currently compacting or idle from a maintenance perspective.
     */
    public enum CompactionState {IDLE, RUNNING}

    /**
     * Wrapper for elements staged in the in-memory buffer, capturing arrival time to aid operational
     * decisions and debugging.
     */
    private static class MemoryBufferEntry<T> {
        final T item;
        final long timestamp;

        /**
         * Captures an element destined for later durable append along with its arrival timestamp.
         *
         * @param item element to stage
         */
        MemoryBufferEntry(T item) {
            this.item = item;
            this.timestamp = System.nanoTime();
        }
    }

    /**
     * Immutable snapshot of queue cursors and counters used to coordinate operations like compaction and
     * recovery.
     */
    private static class QueueState {
        final long consumerOffset, producerOffset, recordCount, lastIndex;

        /**
         * Builds a new snapshot capturing the given positions and counters.
         *
         * @param co consumer offset
         * @param po producer offset
         * @param rc record count
         * @param li last logical index
         */
        QueueState(long co, long po, long rc, long li) {
            this.consumerOffset = co;
            this.producerOffset = po;
            this.recordCount = rc;
            this.lastIndex = li;
        }
    }

    /**
     * Configuration for queue behavior including compaction policy, durability, staging, and coordination
     * timing. Instances are mutable builders; use fluent setters to construct the desired configuration.
     */
    public static final class Options {
        double compactionWasteThreshold = 0.5;
        long compactionIntervalNanos = TimeUnit.MINUTES.toNanos(5);
        int compactionBufferSize = 128 * 1024;
        boolean withFsync = true, enableMemoryBuffer = false, resetOnRestart = false, allowShortCircuit = true;
        int memoryBufferSize = 10000;
        long lockTryTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(10), revalidationIntervalNanos = TimeUnit.MILLISECONDS.toNanos(100);
        long maintenanceIntervalNanos = TimeUnit.SECONDS.toNanos(5);
        long maxSizeReconciliationIntervalNanos = TimeUnit.MINUTES.toNanos(1);

        private Options() {
        }

        /**
         * Returns a fresh options instance with conservative defaults that favor safety and predictable
         * maintenance behavior.
         *
         * @return new options instance with default values
         */
        public static Options defaults() {
            return new Options();
        }

        /**
         * Sets the fraction of consumed data that should trigger a compaction when exceeded. Values close to
         * zero compact aggressively; values near one compact rarely.
         *
         * @param t threshold in the range [0.0, 1.0]
         * @return this builder for chaining
         */
        public Options withCompactionWasteThreshold(double t) {
            if (t < 0.0 || t > 1.0) throw new IllegalArgumentException("threshold [0.0, 1.0]");
            this.compactionWasteThreshold = t;
            return this;
        }

        /**
         * Sets a time-based trigger for compaction. When greater than zero, a compaction may be scheduled
         * after at least this interval has elapsed since the last run and there is data eligible to reclaim.
         *
         * @param i minimum interval between compactions
         * @return this builder for chaining
         */
        public Options withCompactionInterval(Duration i) {
            Objects.requireNonNull(i);
            if (i.isNegative()) throw new IllegalArgumentException("negative");
            this.compactionIntervalNanos = i.toNanos();
            return this;
        }

        /**
         * Sets the internal buffer size used during maintenance activities. Larger buffers may improve
         * throughput on some systems at the cost of memory.
         *
         * @param s positive buffer size in bytes
         * @return this builder for chaining
         */
        public Options withCompactionBufferSize(int s) {
            if (s <= 0) throw new IllegalArgumentException("positive");
            this.compactionBufferSize = s;
            return this;
        }

        /**
         * Controls whether enqueues request synchronous flushing from the storage layer. Enabling this
         * increases durability guarantees at the cost of throughput.
         *
         * @param f true to enable synchronous flush on critical operations
         * @return this builder for chaining
         */
        public Options withFsync(boolean f) {
            this.withFsync = f;
            return this;
        }

        /**
         * Controls whether to allow short-circuiting the persistence layer when consumers are waiting.
         * When true (default), if the queue is empty and consumers are blocked waiting, new elements
         * are handed off directly to consumers without hitting the disk.
         * When false, elements are always written to the queue log before delivery, ensuring strict
         * persistence at the cost of latency.
         *
         * @param allow true to allow short-circuit (default), false to enforce persistence path
         * @return this builder for chaining
         */
        public Options withShortCircuit(boolean allow) {
            this.allowShortCircuit = allow;
            return this;
        }

        /**
         * Enables or disables the in-memory staging buffer used to absorb bursts and decouple producers from
         * maintenance phases.
         *
         * @param e true to enable staging
         * @return this builder for chaining
         */
        public Options withMemoryBuffer(boolean e) {
            this.enableMemoryBuffer = e;
            return this;
        }

        /**
         * Sets the maximum number of elements that may be staged in memory when the staging buffer is enabled.
         *
         * @param s positive capacity in elements
         * @return this builder for chaining
         */
        public Options withMemoryBufferSize(int s) {
            if (s <= 0) throw new IllegalArgumentException("memoryBufferSize deve ser positivo");
            this.memoryBufferSize = s;
            return this;
        }

        /**
         * Sets the maximum time the queue will wait when attempting to acquire coordination for draining or
         * maintenance before retrying. Useful to tune responsiveness under contention.
         *
         * @param t maximum wait duration when trying to acquire the lock
         * @return this builder for chaining
         */
        public Options withLockTryTimeout(Duration t) {
            Objects.requireNonNull(t);
            if (t.isNegative()) throw new IllegalArgumentException("negative");
            this.lockTryTimeoutNanos = t.toNanos();
            return this;
        }

        /**
         * Sets the interval at which temporary staging mode should revalidate whether to continue or revert
         * to direct durable appends.
         *
         * @param i time between revalidation checks
         * @return this builder for chaining
         */
        public Options withRevalidationInterval(Duration i) {
            Objects.requireNonNull(i);
            if (i.isNegative()) throw new IllegalArgumentException("negative");
            this.revalidationIntervalNanos = i.toNanos();
            return this;
        }

        /**
         * Sets the interval at which the background maintenance task runs.
         * This task is responsible for tasks like opportunistic size reconciliation.
         * Default is 5 seconds.
         *
         * @param i interval between maintenance runs
         * @return this builder for chaining
         */
        public Options withMaintenanceInterval(Duration i) {
            Objects.requireNonNull(i);
            if (i.isNegative() || i.isZero()) throw new IllegalArgumentException("positive");
            this.maintenanceIntervalNanos = i.toNanos();
            return this;
        }

        /**
         * Sets the maximum interval allowed without a precise size update. If this interval elapses
         * without the maintenance task successfully acquiring a lock opportunistically, the next
         * run will force a lock to ensure the size is reconciled.
         * <p>
         * If set to ZERO, forced locking is disabled, and size reconciliation will only happen
         * when the lock is free (opportunistic only).
         * Default is 1 minute.
         *
         * @param d maximum interval between forced reconciliations, or ZERO to disable
         * @return this builder for chaining
         */
        public Options withMaxSizeReconciliationInterval(Duration d) {
            Objects.requireNonNull(d);
            if (d.isNegative()) throw new IllegalArgumentException("negative");
            this.maxSizeReconciliationIntervalNanos = d.toNanos();
            return this;
        }

        /**
         * When enabled, disregards any previously persisted metadata at startup and reconstructs state solely
         * from the durable log.
         *
         * @param r true to force a rebuild on restart
         * @return this builder for chaining
         */
        public Options withResetOnRestart(boolean r) {
            this.resetOnRestart = r;
            return this;
        }

        /**
         * Produces an immutable snapshot of the current options for distribution to internal components.
         *
         * @return immutable view of the configured values
         */
        public Snapshot snapshot() {
            return new Snapshot(this);
        }

        /**
         * Immutable view of option values used by internal components to avoid accidental mutation.
         */
        public static class Snapshot {
            final double compactionWasteThreshold;
            final long compactionIntervalNanos, lockTryTimeoutNanos, revalidationIntervalNanos, maintenanceIntervalNanos, maxSizeReconciliationIntervalNanos;
            final int compactionBufferSize, memoryBufferSize;
            final boolean enableMemoryBuffer, allowShortCircuit;

            /**
             * Captures the current values from a mutable options builder for safe sharing.
             *
             * @param o source options builder
             */
            Snapshot(Options o) {
                this.compactionWasteThreshold = o.compactionWasteThreshold;
                this.compactionIntervalNanos = o.compactionIntervalNanos;
                this.compactionBufferSize = o.compactionBufferSize;
                this.enableMemoryBuffer = o.enableMemoryBuffer;
                this.memoryBufferSize = o.memoryBufferSize;
                this.lockTryTimeoutNanos = o.lockTryTimeoutNanos;
                this.revalidationIntervalNanos = o.revalidationIntervalNanos;
                this.maintenanceIntervalNanos = o.maintenanceIntervalNanos;
                this.maxSizeReconciliationIntervalNanos = o.maxSizeReconciliationIntervalNanos;
                this.allowShortCircuit = o.allowShortCircuit;
            }
        }
    }
}
