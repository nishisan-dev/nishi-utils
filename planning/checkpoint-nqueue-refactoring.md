# Checkpoint – Refatoração NQueue (Extração de Colaboradores)

**Data:** 2026-02-22T13:22:00-03:00  
**Branch:** atual (verificar com `git status`)

---

## Objetivo

Extrair a lógica de **memory staging** e **compaction** do `NQueue.java` para duas classes package-private: `MemoryStager.java` e `CompactionEngine.java`.

---

## Estado Atual

### ✅ Concluído

1. **`MemoryStager.java`** – Criado e funcional.
   - Encapsula `memoryBuffer`, `drainingQueue`, `memoryBufferModeUntil`.
   - Campos `memoryBuffer`, `drainingQueue`, `memoryBufferModeUntil` são `package-private` (eram `private`, promovidos para acesso pelo `NQueue` e testes).
   - Callbacks: `FlushCallback`, `CompactionStateSupplier`.

2. **`NQueue.java`** – Refatorado para delegar ao `MemoryStager` e `CompactionEngine`.
   - Classes internas `PreIndexedItem`, `MemoryBufferEntry`, `QueueState` promovidas para package-private.
   - Métodos de testability adicionados: `testGetMemoryBuffer()`, `testGetMemoryBufferModeUntil()`, `testGetCompactionState()`, `testSetCompactionState()`, `testActivateMemoryMode()`, `testAwaitDrainCompletion()`.
   - Chamadas a `compactionEngine.maybeCompact()` atualizadas (removido parâmetro `dataChannel`).

3. **`NQueueMemoryBufferTest.java`** – Parcialmente atualizado.
   - 3 testes que usavam reflection (`setField`, `setAtomicLong`, `getFieldValue`) foram migrados para usar os métodos de testability.
   - Métodos helpers de reflection (`setField`, `setAtomicLong`, `getMemoryBuffer`) agora estão sem uso (warnings de "never used locally").

4. **Compilação** – Projeto compila sem erros.

### 🔴 Bug Ativo no `CompactionEngine.java`

O `CompactionEngine` foi reescrito **3 vezes** nesta sessão. A versão atual (última escrita) tem a estrutura correta:

- **Phase 1 (background copy):** Copia a região não-consumida do log para um arquivo temporário usando um handle read-only separado. Não segura o lock durante a cópia bulk.
- **Phase 2 (finalize under lock):** Sob uma única aquisição do `queueLock`:
  - Copia tail (dados adicionados durante Phase 1).
  - Fecha `tmpCh` manualmente (sem try-with-resources).
  - `Files.move` atômico.
  - Abre novo handle `rw`.
  - Chama `applyCallback.apply()` que delega ao `NQueue.applyCompactionResult()`.

**Bug anterior identificado e corrigido:**
- O código anterior usava try-with-resources para `tmpCh`, que causava **double-close** (manual + ARM). O `FileChannel.close()` é idempotente em Java, mas o verdadeiro problema era que operações críticas (tail copy, move, apply) ficaram **fora do lock**, criando uma janela onde dados escritos pelo NQueue eram perdidos quando o `Files.move` substituía o arquivo.

**Estado dos testes (última execução):**

| Teste | Status | Descrição do Erro |
|-------|--------|-------------------|
| `NQueueTest` (8 testes) | 2 ERROR | `parallelOfferAndPollShouldHandleComplexPayloads`: poll retorna empty; `handlesLoadFluctuationsAndTransitions`: NoSuchElement |
| `NQueueCompactionTest` (6 testes) | 1 FAIL | `compactsWhenWasteThresholdExceeded`: consumerOffset=65 esperado 0 |
| `NQueueMemoryBufferTest` (12 testes) | 2 FAIL | `testMemoryModeActivatedDuringCompaction`: expected 20 got 10; `testCompactionFinishedSwitchesBackToPersistentMode`: timeout |
| `NQueueOrderDetectorTest` (3 testes) | 1 FAIL | `memoryBufferAndCompaction_withOrderDetection`: out-of-order count 265 esperado 0 |

> [!IMPORTANT]
> A última versão do `CompactionEngine.java` (com Phase 1/2 sob lock parcial) **ainda não foi testada**. Foi escrita logo antes deste checkpoint. Os erros acima são da versão anterior (Phase 1/2/3 separados com operações fora do lock).

---

## Próximos Passos

1. **Rodar os testes** com a versão atual do `CompactionEngine.java`:
   ```bash
   cd /home/lucas/Projects/nishisan/nishi-utils
   mvn clean test -Dtest="NQueueTest,NQueueMemoryBufferTest,NQueueCompactionTest,NQueueOrderDetectorTest" -pl .
   ```

2. **Se `NQueueTest` passar** → o bug de perda de dados foi corrigido.

3. **Se `compactsWhenWasteThresholdExceeded` ainda falhar** → investigar se a compactação está sendo disparada e executada com sucesso. Adicionar log temporário no `catch (Throwable t)` do `runCompactionTask` para verificar exceções silenciadas.

4. **Corrigir `NQueueMemoryBufferTest`:**
   - `testMemoryModeActivatedDuringCompaction` (line 192): Espera 20 items mas recebe 10. Pode ser que a compactação agora roda de verdade e consome items.
   - `testCompactionFinishedSwitchesBackToPersistentMode` (line 659): O teste simula compaction finishing via `testSetCompactionState(IDLE)` mas não dispara o callback `onCompactionFinished` do stager. Precisa chamar o stager diretamente ou expor um método de testability para simular o callback.

5. **Corrigir `NQueueOrderDetectorTest`:**
   - `memoryBufferAndCompaction_withOrderDetection`: out-of-order count alto. Provavelmente consequência da compactação desordenando indices. Verificar se `recordDeliveryIndex` está sendo chamado corretamente após compactação.

6. **Limpar código:**
   - Remover helpers de reflection não utilizados em `NQueueMemoryBufferTest` (`setField`, `setAtomicLong`, `getMemoryBuffer`, import `Method`).
   - Remover campo `queueDir` não utilizado em `NQueue` (warning).

7. **Commit atômico** após todos os testes passarem.

---

## Arquivos Modificados (não commitados)

| Arquivo | Tipo | Descrição |
|---------|------|-----------|
| `src/main/java/dev/nishisan/utils/queue/CompactionEngine.java` | NEW | Engine de compactação extraída do NQueue |
| `src/main/java/dev/nishisan/utils/queue/MemoryStager.java` | NEW | Staging em memória extraído do NQueue |
| `src/main/java/dev/nishisan/utils/queue/NQueue.java` | MODIFY | Delegação para colaboradores, métodos testability |
| `src/test/java/dev/nishisan/utils/queue/NQueueMemoryBufferTest.java` | MODIFY | Migração de reflection para testability APIs |

---

## Notas Técnicas

- O `CompactionEngine` **não recebe mais o `dataChannel` do NQueue**. Abre handles separados via `dataPath`.
- O `MemoryStager` tem campos `memoryBuffer`, `drainingQueue`, `memoryBufferModeUntil` como **package-private** para acesso via testability methods do NQueue.
- O `CompactionEngine` tem campo `state` como **package-private** (`volatile`) para acesso via testability methods do NQueue.
- O `NQueue.applyCompactionResult()` fecha o canal antigo, seta o novo, persiste estado e dispara drain do stager.
