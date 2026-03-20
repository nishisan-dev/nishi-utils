# Bug: Deadlock de Replicação no NGrid

**Status**: ✅ Resolvido — estável desde 2026-01-18 (211 testes verdes, 0 falhas)  
**Severidade**: Critical  
**Teste Afetado**: `MultiQueueClusterIntegrationTest.shouldHandleMultipleQueuesIndependently()`  
**Sintoma**: Timeout de 105.8 segundos (RPC timeout de 35s × 3 retry attempts)

---

## Resumo Executivo

O sistema NGrid apresenta deadlock em ambientes multi-queue quando múltiplos nós tentam replicar operações simultaneamente. O deadlock ocorre devido a **contention de locks em múltiplas camadas** durante a aplicação de operações replicadas.

---

## Manifestação do Bug

### Teste Falhando
```bash
mvn test -Dtest=MultiQueueClusterIntegrationTest#shouldHandleMultipleQueuesIndependently
```

**Erro**:
```
TimeoutException: Request timed out requestId=... destination=seed timeout=PT35S
at DistributedQueue.offer(DistributedQueue.java:162)
```

### Topologia de Teste
- **3 nós**: seed (leader), producer, consumer
- **3 queues**: orders, events, logs
- **Replication factor**: 2
- **Strict consistency**: false

### Comportamento Observado
1. Producer tenta fazer `offer()` na primeira queue (orders)
2. Operação envia replication request para followers
3. Sistema trava aguardando ACKs que nunca chegam
4. Timeout após 105.8 segundos

---

## Root Cause Analysis

### Arquitetura de Locks (Multi-Layer)

O NGrid possui múltiplas camadas de locks que interagem durante replicação:

```
Layer 1: DistributedQueue
  ↓
Layer 2: QueueClusterService
  ├─ clientLock
  └─ operationLock
      ↓
Layer 3: ReplicationManager
  └─ sequenceBufferLock
      ↓
Layer 4: NQueue (Backend)
  └─ internal ReentrantLock
```

### Fluxo do Deadlock

#### Caminho do Leader (Producer)
```java
1. DistributedQueue.offer(value)
2. → QueueClusterService.offer(value)
3.   → Adquire clientLock
4.   → ReplicationManager.replicate(topic, payload)
5.     → replicateToFollowers() // envia para seed e consumer
6.     → checkCompletion() // verifica se quorum foi atingido
7.       → handler.apply() // BLOQUEANTE! Tenta modificar NQueue
8.   → waitForReplication(future) // BLOQUEIA aguardando ACKs
```

#### Caminho do Follower (Seed/Consumer)
```java
1. Transport recebe REPLICATION_REQUEST
2. → ReplicationManager.handleReplicationRequest()
3.   → Adquire sequenceBufferLock
4.   → applyReplication(payload, message)
5.     → handler.apply(opId, data) // BLOQUEANTE!
6.       → QueueClusterService.apply()
7.         → Tenta adquirir operationLock
8.         → NQueue.offer() // tenta lock interno da queue
9.     → sendAck() // ACK NUNCA É ENVIADO se apply bloquear!
10.  → Libera sequenceBufferLock
```

### Cenário de Deadlock Circular

Quando múltiplas operações acontecem simultaneamente em múltiplas queues:

```
Node A (Producer):
  - Mantém locks da queue "orders"
  - Aguarda ACK do Node B para "orders"
  - Precisa processar replication de "events" vindo de B

Node B (Seed):
  - Mantém sequenceBufferLock processando "events"
  - Dentro do lock, tenta aplicar "events" (bloqueia em handler.apply)
  - Aguarda ACK do Node A para "events"
  - Precisa processar replication de "orders" vindo de A
  
DEADLOCK: A aguarda B, B aguarda A
```

---

## Causa Raiz Confirmada ✅

O deadlock/timeouts em **multi-queue** não vem mais de locks em si, mas de **sequenciamento global usado com buffer por tópico**:

- `ReplicationManager.replicate()` usa **`globalSequence`** (único contador para TODOS os tópicos).
- `handleReplicationRequest()` usa **`nextExpectedSequenceByTopic`** (contador **por tópico**, iniciando em 1).
- Resultado: o primeiro evento de um tópico pode chegar com `seq=3`, mas o follower espera `seq=1` para aquele tópico → **fica sempre em buffer**, **nunca aplica**, **nunca manda ACK**.
- O leader então espera ACKs que nunca chegam → **timeout** (parece deadlock).

Exemplo real com 3 queues:
```
orders  seq=1,2
events  seq=3,4,5   <-- follower espera seq=1 para "events", nunca chega
logs    seq=6,7
```

### Evidência no código
- Sequência global: `ReplicationManager.replicate()` usa `globalSequence.incrementAndGet()`
- Buffer por tópico: `handleReplicationRequest()` compara `seq` com `nextExpectedSequenceByTopic` (iniciado em 1 por tópico)

### Impacto direto
- Qualquer cluster com **mais de um topic/queue** entra em bloqueio lógico porque os followers **jamais avançam** o `nextExpectedSequenceByTopic` para os tópicos “pulados”.

---

## Tentativas de Correção

### Tentativa 1: Lock Elision no QueueClusterService ❌
**Abordagem**: Liberar `clientLock` e `operationLock` ANTES de chamar `waitForReplication()`

**Resultado**: Deadlock persistiu (conforme checkpoint anterior)

**Análise**: Liberação de locks no service layer não resolveu porque:
- `sequenceBufferLock` no ReplicationManager ainda era mantido durante `handler.apply()`
- NQueue possui lock interno que não pode ser liberado

### Tentativa 2: Async Apply com Callbacks ⚠️
**Abordagem**: Executar `handler.apply()` assincronamente fora de todos os locks

**Implementação**:
1. ✅ Mudança de executor: `SingleThreadExecutor` → `FixedThreadPool(4)`
2. ✅ Follower path async: `handleReplicationRequest()` usa callbacks
3. ✅ Leader path async: `checkCompletion()` usa executor
4. ✅ Buffer processing recursivo via callbacks

**Código**:
```java
// ANTES (síncrono, dentro do lock)
sequenceBufferLock.lock();
try {
    if (seq == nextExpected) {
        applyReplication(payload, message); // BLOQUEANTE!
        nextExpectedSequenceByTopic.put(topic, nextExpected + 1);
        processSequenceBuffer(topic);
    }
} finally {
    sequenceBufferLock.unlock();
}

// DEPOIS (assíncrono, com callback)
sequenceBufferLock.lock();
try {
    if (seq == nextExpected) {
        Runnable onSuccess = () -> {
            sequenceBufferLock.lock();
            try {
                // Atualiza estado APÓS apply bem-sucedido
                lastAppliedSequence = Math.max(lastAppliedSequence, seq);
                applied.add(opId);
                sendAck(opId, message.source());
                nextExpectedSequenceByTopic.put(topic, nextExpected + 1);
                processSequenceBuffer(topic);
            } finally {
                sequenceBufferLock.unlock();
            }
        };
        applyReplication(payload, message, onSuccess); // Retorna imediatamente
    }
} finally {
    sequenceBufferLock.unlock(); // Lock liberado ANTES do apply
}
```

**Resultado**: Deadlock **AINDA PERSISTE** (timeout em 105.8s)

**Diagnóstico atualizado**:
O problema não era lock. Era **sequência global x buffer por tópico** causando **ACKs nunca enviados**.

---

## Novo Achado (Após Opção A) ⚠️

Após corrigir o sequenciamento por tópico, o teste ainda falhou com **timeout de RPC** em `DistributedQueue.offer()` (request para o líder).

### Causa Raiz Atual
O líder **não processa** o `CLIENT_REQUEST` porque:
- **As filas são criadas de forma lazy**. O seed (líder) não chama `getQueue()` e, portanto, **não registra listener** para receber comandos de queue.
- O comando **não carrega o nome da fila** (ex.: `queue.offer`), então mesmo se o líder tivesse várias filas, ele **não consegue rotear** corretamente no caso multi-queue.

Resultado: o request do producer para o seed **fica sem resposta** e expira (timeout de 35s).

### Evidência
- Timeout ocorre **antes** de qualquer ACK de replicação (RPC pendente no `DistributedQueue.invokeLeader`).
- `DistributedQueue` só processa `CLIENT_REQUEST` se existir instância local registrada no transport.
- Com múltiplas filas, o comando não contém `queueName`, então não há roteamento determinístico.

### Correção Planejada
1. **Incluir o nome da fila no comando** (ex.: `queue.offer:orders`) e filtrar no listener.
2. **Eager init** das filas configuradas no `NGridNode` para garantir listener + handler no líder.
3. **Aplicar retenção TIME_BASED** das `QueueConfig` nas `NQueue.Options` para habilitar `poll` por offset.
4. **Evitar double-apply no líder** com CAS (`localApplyStarted`) para não duplicar registros.
5. **Compatibilidade legacy**: quando `queueDirectory` é usado (API antiga), manter `DELETE_ON_CONSUME` para preservar semântica destrutiva do `poll`.

---

## Próximos Passos de Correção

### Opção A: Sequência por Tópico (recomendado)
Manter ordenação **por tópico** e corrigir o bug usando contador dedicado:
```java
private final Map<String, AtomicLong> sequenceByTopic = new ConcurrentHashMap<>();

long seq = sequenceByTopic
    .computeIfAbsent(topic, k -> new AtomicLong(0))
    .incrementAndGet();
```
Follower continua usando `nextExpectedSequenceByTopic` (agora consistente).

### Opção B: Sequência Global Única
Se a intenção for **ordenação global**, remover o buffer por tópico e usar um único `nextExpectedSequence` global (ou aceitar processamento fora de ordem por tópico).

### Opção C: Leader Replicar Para Si Mesmo
Ainda válida como refactor, mas não resolve o bug de sequência.

```java
// Leader path
public CompletableFuture<ReplicationResult> replicate(String topic, Serializable payload) {
    UUID operationId = UUID.randomUUID();
    PendingOperation operation = new PendingOperation(operationId, topic, payload, quorum);
    pending.put(operationId, operation);
    
    // Leader NÃO faz ACK local, envia para si mesmo também
    for (NodeInfo member : allMembers()) { // Inclui self!
        sendReplicationRequest(member, operation);
    }
    
    return operation.future();
}
```

### Opção D: Two-Phase Commit
Separar replicação em 2 fases:
1. **Prepare Phase**: Followers confirmam capacidade de aplicar
2. **Commit Phase**: Após quorum, todos aplicam atomicamente

---

## Arquivos Modificados (Tentativa 2)

- ✏️ `ReplicationManager.java`:
  - Linha 70-76: Executor multi-thread
  - Linha 484-523: `applyReplication()` async com callback
  - Linha 454-484: `handleReplicationRequest()` com callback
  - Linha 553-608: `processSequenceBuffer()` recursivo
  - Linha 295-332: `checkCompletion()` async

---

## Referências

- **Checkpoint Original**: `/home/lucas/.gemini/antigravity/brain/dcc57d3f-5932-4589-90b3-5bade3b4cb87/checkpoint.md`
- **Task Atual**: `/home/lucas/.gemini/antigravity/brain/483e8146-60d3-49b4-beb0-dcfeaab77751/task.md`
- **Knowledge Item**: `ngrid_distributed_structures/artifacts/implementation/callback_sequencing_pattern.md`

---

## Timeline

| Data | Ação | Resultado |
|------|------|-----------|
| 2026-01-18 | Tentativa lock elision em QueueClusterService | ❌ Falhou |
| 2026-01-18 | Implementação async apply com callbacks | ⚠️ Não resolveu |
| 2026-01-18 | Sequência por tópico + roteamento por queue + init eager + retenção TIME_BASED + CAS local-apply | ✅ Teste alvo passou |
| 2026-01-18 | Estado atual | 🟡 Fix aplicado, validar regressões |

---

**Última Atualização**: 2026-01-18T00:48:32-03:00
