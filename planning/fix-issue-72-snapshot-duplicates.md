# Fix: Duplicatas intermitentes após restart do seed — Issue #72

## Contexto

O teste `shouldRecoverAfterSeedRestartWithoutDuplicatesOrLoss` falha intermitentemente detectando mensagens duplicadas (`INDEX-2-0 x2, INDEX-1-1 x2, INDEX-1-0 x2`) após restart do seed node. A falha ocorre porque o ciclo `resetState()` → `installSnapshot()` no `QueueClusterService` possui race conditions e perda de metadata.

## Análise de Causa Raiz

Foram identificadas **3 causas raiz** inter-relacionadas:

### Causa 1: `resetState()` não drena o memory buffer

O [resetState()](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/queue/QueueClusterService.java#L484-L495) atual faz polling em loop (`queue.poll(100ms)`) até `queue.size() == 0`. Porém:

- `queue.size()` contabiliza itens no memory buffer do `MemoryStager`
- `queue.poll()` tenta fazer `drainSync()` quando `recordCount == 0`, mas se itens estiverem no buffer **durante** o poll, pode haver uma janela onde o drain do buffer re-insere itens no disco **após** o offset já ter sido resetado
- O stager pode estar fazendo `drainAsync()` em background, potencialmente persistindo itens após o `localOffsetStore.reset()`

### Causa 2: `installSnapshot()` perde metadata V3 (key/headers)

O [installSnapshot()](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/queue/QueueClusterService.java#L500-L507) utiliza `queue.offer((T) item)`, que invoca `offer(null, NQueueHeaders.empty(), object)`:
- Qualquer key ou headers presentes no record original são **descartados**
- Isso pode causar inconsistências em consumers que dependem de metadata para deduplicação ou roteamento

### Causa 3: `getSnapshotChunk()` retorna apenas `T` sem metadata

O [getSnapshotChunk()](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/queue/QueueClusterService.java#L460-L480) utiliza `queue.readRange()`, que internamente usa `safeDeserialize()` para retornar apenas o payload `T`. O **key**, **headers** e **index** do record são perdidos durante a serialização do snapshot.

Para um fix completo, o snapshot deveria incluir o record completo. Porém, **atualmente o NGrid não usa key/headers nas mensagens do teste que falha** (o seed faz `queue.offer(value)` sem key), então esta causa contribui para inconsistência futura mas **não é a causa direta da duplicata atual**.

> [!IMPORTANT]
> A **causa direta** das duplicatas é a **Causa 1**: o race condition no `resetState()` permite que itens do memory buffer sejam drenados para disco **após** o reset de offsets, causando re-entrega.

## Proposed Changes

### QueueClusterService

#### [MODIFY] [QueueClusterService.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/queue/QueueClusterService.java)

**1. `resetState()` — Parar stager + truncar em vez de poll loop**

```diff
 public void resetState() throws Exception {
-    // For queues, we clear by polling all items
-    // This is called before installing a snapshot
-    while (queue.size() > 0) {
-        queue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
-    }
-    // Reset consumer offsets so they align with the new snapshot indices.
-    // Without this, stale offsets cause duplicate message delivery after
-    // snapshot install because the NQueue assigns new indices starting from 1.
-    localOffsetStore.reset();
-    LOGGER.info(() -> "Queue " + queueName + " state reset for snapshot install (offsets cleared)");
+    // 1. Close the queue to drain memory buffer and flush all pending data
+    queue.close();
+    // 2. Reopen with a fresh state (deleting data files)
+    queue.truncateAndReopen();
+    // 3. Reset consumer offsets so they align with the new snapshot indices
+    localOffsetStore.reset();
+    LOGGER.info(() -> "Queue " + queueName + " state reset for snapshot install (truncated + offsets cleared)");
 }
```

A ideia é substituir o loop de polling (que tem race conditions com o stager) por uma sequência atômica: `close()` → `truncateAndReopen()`. O `close()` já faz `drainSync()` internamente, garantindo que nenhum item fique no buffer. O `truncateAndReopen()` é um novo método que deleta os arquivos de dados e recria a queue vazia.

---

### NQueue

#### [MODIFY] [NQueue.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/queue/NQueue.java)

**Novo método `truncateAndReopen()`**

Adicionar um método público que deleta os arquivos de dados da queue e a reabre limpa, reusando as mesmas options:

```java
/**
 * Truncates all queue data and reopens with a clean state.
 * <p>
 * This is a destructive operation: all records, metadata, and consumer
 * progress are permanently lost. The queue is left open and ready for
 * new enqueues with indices starting from 1.
 * <p>
 * Intended for snapshot install scenarios where the entire queue content
 * is replaced atomically.
 *
 * @throws IOException if the files cannot be deleted or recreated
 */
public void truncateAndReopen() throws IOException {
    // Queue must be closed before calling this
    if (!closed) {
        throw new IllegalStateException("Queue must be closed before truncateAndReopen");
    }
    
    // Delete data files
    Files.deleteIfExists(dataPath);
    Files.deleteIfExists(metaPath);
    Files.deleteIfExists(tempDataPath);
    
    // Reopen with fresh state
    Path queueDir = dataPath.getParent();
    RandomAccessFile newRaf = new RandomAccessFile(dataPath.toFile(), "rw");
    FileChannel newCh = newRaf.getChannel();
    QueueState freshState = new QueueState(0, 0, 0, 0);
    
    // Reset internal state
    this.raf = newRaf;
    this.dataChannel = newCh;
    this.metaRaf = new RandomAccessFile(metaPath.toFile(), "rw");
    this.metaChannel = this.metaRaf.getChannel();
    this.consumerOffset = 0;
    this.producerOffset = 0;
    this.recordCount = 0;
    this.lastIndex = 0;
    this.globalSequence.set(0);
    this.approximateSize.set(0);
    this.closed = false;
    this.shutdownRequested = false;
    this.handoffItem = null;
    this.lastDeliveredIndex = -1;
    this.outOfOrderCount = 0;
    
    // Reinitialize collaborators
    if (enableMemoryBuffer && stager != null) {
        stager = new MemoryStager<>(options, lock,
                batch -> offerBatchLocked(batch, options.withFsync),
                () -> compactionEngine != null
                        && compactionEngine.getState() == CompactionState.RUNNING);
    }
    
    compactionEngine = new CompactionEngine(
            options, dataPath, tempDataPath, lock,
            this::currentState,
            this::applyCompactionResult,
            compactionRunning -> {
                if (stager != null) stager.onCompactionFinished(compactionRunning);
            });
    
    persistCurrentStateLocked();
}
```

> [!NOTE]
> A reimplementação dos collaborators (stager + compactionEngine) garante que nenhuma referência stale persista após o truncate. O `maintenanceExecutor` reutiliza a instância existente pois é um ScheduledExecutorService que simplesmente agenda tarefas.

---

### Refatoração de `resetState()` no QueueClusterService

Com o método `truncateAndReopen()` disponível, o `resetState()` fica:

```java
public void resetState() throws Exception {
    queue.close();
    queue.truncateAndReopen();
    localOffsetStore.reset();
    LOGGER.info(() -> "Queue " + queueName + " state reset (truncated + offsets cleared)");
}
```

---

## Verificação Plan

### Testes Unitários Existentes

1. **NQueueMemoryBufferTest** — Valida comportamento do memory buffer, compaction, drain
   ```bash
   mvn test -pl . -Dtest=dev.nishisan.utils.queue.NQueueMemoryBufferTest
   ```

2. **QueueCatchUpIntegrationTest** — Valida catch-up via snapshot (usa `resetState` + `installSnapshot` indiretamente)
   ```bash
   mvn test -pl . -Dtest=dev.nishisan.utils.ngrid.QueueCatchUpIntegrationTest
   ```

### Novo Teste Unitário

Criar `QueueClusterServiceSnapshotTest.java` em `src/test/java/dev/nishisan/utils/ngrid/queue/` que testa:

1. **`resetState` limpa toda a queue incluso memory buffer** — Oferece N itens, chama `resetState()`, verifica `queue.size() == 0`
2. **Round-trip `getSnapshotChunk` → `resetState` → `installSnapshot`** — Oferece N itens, lê snapshot chunks, reseta, instala, verifica que contagem e conteúdo são iguais
3. **Sem duplicatas após `resetState` + `installSnapshot`** — Verifica que índices não se sobrepõem

```bash
mvn test -pl . -Dtest=dev.nishisan.utils.ngrid.queue.QueueClusterServiceSnapshotTest
```

### Todos os Testes

```bash
mvn test -pl .
```

> [!WARNING]
> O teste Docker (`NGridTestcontainersSmokeTest`) requer `ngrid.test.docker=true` e Docker. Não será executado como parte do `mvn test` padrão. Para validação final de flakiness, rodar manualmente:
> ```bash
> mvn test -pl . -Dtest=dev.nishisan.utils.ngrid.testcontainers.NGridTestcontainersSmokeTest -Dngrid.test.docker=true
> ```
