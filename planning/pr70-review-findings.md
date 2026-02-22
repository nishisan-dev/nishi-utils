# Correções dos Findings da PR #70

Os 3 findings foram identificados pelo Codex Review na [PR #70](https://github.com/nishisan-dev/nishi-utils/pull/70) e representam bugs reais de integridade de dados no NQueue e DistributedQueue.

---

## Proposed Changes

### Finding 1 (P1) — Forward keyed offers com metadata para o leader

**Problema:** Em [DistributedQueue.offer(key, headers, value)](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/structures/DistributedQueue.java#L156-L169), quando o nó não é leader, a L167 chama `invokeLeader(queueOfferCommand, value)` enviando **apenas** o `value` como body do `ClientRequestPayload`. O key e headers são descartados. No lado do leader, [executeLocal](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/structures/DistributedQueue.java#L322-L346) faz `queueService.offer((T) body)`, que usa a overload sem key/headers.

**Solução:** Criar uma classe `OfferPayload` serializável que encapsula `key`, `headers` e `value` juntos. Alterar `offer(key, headers, value)` para usar este envelope no `invokeLeader`, e alterar `executeLocal` para extrair os 3 campos do payload.

> [!IMPORTANT]
> Este é o finding mais crítico (P1). Sem ele, offers roteados via followers perdem silenciosamente metadata, causando inconsistência de dados entre nós.

#### [NEW] [OfferPayload.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/structures/OfferPayload.java)

Classe `Serializable` atuando como envelope para `byte[] key`, `NQueueHeaders headers` e `T value`:

```java
public final class OfferPayload<T extends Serializable> implements Serializable {
    private final byte[] key;
    private final NQueueHeaders headers;
    private final T value;
    // constructor, getters
}
```

#### [MODIFY] [DistributedQueue.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/structures/DistributedQueue.java)

- **L167:** Trocar `invokeLeader(queueOfferCommand, value)` por `invokeLeader(queueOfferCommand, new OfferPayload<>(key, headers, value))`
- **L324-L328 (`executeLocal`):** Trocar cast `(T) body` por `OfferPayload` unwrap:

```diff
 if (queueOfferCommand.equals(command)) {
     recordQueueOffer();
-    queueService.offer((T) body);
+    OfferPayload<T> payload = (OfferPayload<T>) body;
+    queueService.offer(payload.key(), payload.headers(), payload.value());
     notifySubscribers();
     return Boolean.TRUE;
 }
```

---

### Finding 2 (P2) — Retornar disponibilidade pós-drain no `checkAndDrain`

**Problema:** Em [MemoryStager.checkAndDrain](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/queue/MemoryStager.java#L259-L265), após o `drainSync()` ser executado com sucesso (L261), o return na L262 é `recordCount > 0` — mas `recordCount` é o parâmetro **antigo** passado pelo caller antes do drain. Se era 0, retorna `false` mesmo que o drain tenha acabado de adicionar records ao storage.

**Solução:** A interface de `checkAndDrain` precisa consultar o estado pós-drain. Como `checkAndDrain` é chamado dentro do queue lock (no `NQueue.poll(timeout)`), não podemos acessar `NQueue.recordCount` diretamente. A solução é trocar o parâmetro por um `LongSupplier` ou, de forma mais simples, fazer o `checkAndDrain` receber um callback que retorna o `recordCount` atualizado.

Na prática, a forma mais limpa: o `MemoryStager` já recebe o `FlushCallback` que persiste os itens, e o `NQueue` que chama `checkAndDrain` atualiza `recordCount` dentro do `offerBatchLocked`. Portanto basta consultar o `recordCount` **depois** do drain.

#### [MODIFY] [MemoryStager.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/queue/MemoryStager.java)

Adicionar um `LongSupplier` para consultar `recordCount` pós-drain:

```diff
+    @FunctionalInterface
+    interface RecordCountSupplier {
+        long getRecordCount();
+    }
+
+    private final RecordCountSupplier recordCountSupplier;
```

Alterar assinatura e corpo do `checkAndDrain`:

```diff
-    boolean checkAndDrain(long recordCount) throws IOException {
+    boolean checkAndDrain(RecordCountSupplier recordCountSupplier) throws IOException {
         if (!memoryBuffer.isEmpty() || !drainingQueue.isEmpty()) {
             drainSync();
-            return recordCount > 0;
+            return recordCountSupplier.getRecordCount() > 0;
         }
         return false;
     }
```

#### [MODIFY] [NQueue.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/queue/NQueue.java)

Atualizar todas as chamadas a `stager.checkAndDrain(recordCount)` para usar lambda `() -> recordCount`:

```diff
-    stager.checkAndDrain(recordCount)
+    stager.checkAndDrain(() -> recordCount)
```

> [!NOTE]
> Há 3 call-sites no método `poll(long timeout, TimeUnit unit)` nas linhas ~534, ~548 e ~553.

---

### Finding 3 (P2) — Preservar index pré-alocado no fallback do stager

**Problema:** Em [NQueue.offerViaStager](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/queue/NQueue.java#L452-L467), L454 já aloca um index via `globalSequence.incrementAndGet()`, criando um `PreIndexedItem` com esse index. Quando o stager retorna `Long.MIN_VALUE` (fallback), a L461 chama `offerDirectLocked(key, headers, object)`, que aloca **outro** index via `globalSequence.incrementAndGet()` na L837. Isso resulta em gaps nos índices.

**Solução:** Criar uma variante `offerDirectLocked` que aceita um `PreIndexedItem` já indexado, reutilizando o index pré-alocado em vez de gerar um novo.

#### [MODIFY] [NQueue.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/queue/NQueue.java)

Adicionar overload de `offerDirectLocked`:

```java
private long offerDirectWithPreIndexedLocked(PreIndexedItem<T> pItem) throws IOException {
    return offerBatchLocked(List.of(pItem), true);
}
```

Alterar `offerViaStager` para usar o item pré-indexado no fallback:

```diff
     long seq = globalSequence.incrementAndGet();
     PreIndexedItem<T> pItem = new PreIndexedItem<>(object, seq, key, headers);
     long result = stager.offerToMemory(key, headers, pItem, revalidateIfFull);
     if (result == Long.MIN_VALUE) {
         lock.lock();
         try {
-            return offerDirectLocked(key, headers, object);
+            return offerDirectWithPreIndexedLocked(pItem);
         } finally {
             lock.unlock();
         }
     }
```

---

## Verification Plan

### Testes Automatizados

1. **Testes unitários do NQueue (NQueueMemoryBufferTest):**
   ```bash
   mvn test -pl . -Dtest=dev.nishisan.utils.queue.NQueueMemoryBufferTest -Dsurefire.useFile=false
   ```

2. **Testes unitários gerais do NQueue:**
   ```bash
   mvn test -pl . -Dtest=dev.nishisan.utils.queue.NQueueTest -Dsurefire.useFile=false
   ```

3. **Testes de integração de key/headers (QueueKeyHeadersIntegrationTest):**
   ```bash
   mvn test -pl . -Dtest=dev.nishisan.utils.ngrid.QueueKeyHeadersIntegrationTest -Dsurefire.useFile=false
   ```

4. **Suíte completa para garantir não-regressão:**
   ```bash
   mvn test -pl . -Dsurefire.useFile=false
   ```

### Testes Novos

Seria importante perguntar se temos forma de testar o cenário de forward follower→leader em teste unitário/integração (Finding 1). O `QueueKeyHeadersIntegrationTest` testa replicação de key/headers mas preciso confirmar se cobre o cenário de **offer via follower**.
