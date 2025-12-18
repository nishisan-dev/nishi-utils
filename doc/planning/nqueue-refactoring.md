# Planejamento de Refatoração: NQueue.java

## Contexto
A classe `NQueue.java` implementa uma fila persistente baseada em arquivo com suporte a concorrência. Atualmente, a classe cresceu em complexidade, misturando lógicas de persistência, gerenciamento de buffer de memória, compactação e operações de fila. O objetivo é refatorar para melhorar a legibilidade e manutenibilidade, sem alterar o comportamento externo (API pública) ou quebrar a compatibilidade com os testes existentes.

## Objetivos
1.  **Melhorar a Legibilidade:** Organizar o código de forma que o fluxo principal (offer/poll) seja claro.
2.  **Reduzir Duplicidade:** Unificar lógicas de leitura/escrita e tratamento de tempo.
3.  **Organização Estrutural:** Agrupar responsabilidades relacionadas (Compactação, Buffer de Memória).
4.  **Desambiguação de Tempo:** Padronizar nomes de variáveis e unidades de tempo (ex: sempre sufixar com `Nanos` ou `Millis` internamente).

## Estratégia de Refatoração

### 1. Segregação de Responsabilidades (Inner Classes / Helpers)
A classe atual possui muitos campos e métodos privados que atendem a domínios específicos. Propõe-se agrupar essas lógicas em classes internas (ou helpers privados) para limpar o namespace da classe principal.

*   **`MemoryBufferManager` (ou similar):**
    *   Encapsular: `memoryBuffer`, `drainingQueue`, `drainExecutor`, `revalidationExecutor`, `memoryBufferModeUntil`, flags atômicas de drenagem.
    *   Métodos a mover/adaptar: `offerToMemory`, `drainMemoryBuffer`, `activateMemoryMode`, `revalidateMemoryMode`.
    *   Benefício: `NQueue` passa a delegar a complexidade de "fallback para memória" para este componente.

*   **`CompactionManager`:**
    *   Encapsular: `compactionExecutor`, `compactionState`, `compactionFuture`, `tempDataPath`.
    *   Métodos a mover/adaptar: `maybeCompactLocked`, `runCompaction`, `finalizeCompaction` (este último precisa de acesso delicado ao lock principal, talvez permaneça como método privado mas usando o manager para estado).

### 2. Padronização de Tempo
*   Revisar todos os usos de `timeout`.
*   A API pública `poll(long timeout, TimeUnit unit)` será mantida.
*   Internamente, converter imediatamente para `long nanos` e usar variáveis como `timeoutNanos` ou `deadlineNanos`.
*   Substituir cálculos soltos de `System.nanoTime()` por métodos auxiliares se repetitivos.

### 3. Consolidação de I/O
*   Os métodos `readAtInternal`, `rebuildState` e `offerBatchLocked` manipulam `FileChannel` e `ByteBuffer` diretamente.
*   Criar métodos privados claros para operações atômicas de disco:
    *   `readRecordHeader(offset)`
    *   `readRecordPayload(offset, length)`
    *   Isso ajuda a remover a duplicidade entre a leitura normal e a reconstrução de estado.

### 4. Fluxo de `offer` e `poll`
*   Simplificar o método `offer`. Atualmente ele tem múltiplos caminhos de execução (`if memoryBuffer`, `tryLock`, `else`).
*   Estruturar como:
    ```java
    public long offer(T object) {
        if (memoryManager.shouldUseMemory()) {
            return memoryManager.enqueue(object);
        }
        try {
            if (lock.tryLock(...)) {
                // Happy path: direct write
            } else {
                // Fallback
                return memoryManager.enqueue(object);
            }
        } ...
    }
    ```

## Passos da Implementação
1.  **Preparação:** Garantir que todos os testes (`NQueueTest`, `NQueueCompactionTest`) estejam passando.
2.  **Extração de Constantes e Fields:** Reorganizar o topo da classe.
3.  **Refatoração do Buffer de Memória:** Criar a classe interna e mover a lógica, mantendo o lock na classe principal onde necessário.
4.  **Refatoração da Compactação:** Isolar a lógica de compactação.
5.  **Limpeza de I/O:** Refatorar `readAtInternal` e métodos auxiliares.
6.  **Revisão Final:** Verificar nomes de variáveis e Javadoc.

## Critérios de Aceite
*   Nenhum teste existente deve falhar.
*   A assinatura dos métodos públicos deve permanecer idêntica.
*   O código deve estar livre de duplicidade óbvia em lógica de leitura/escrita.
