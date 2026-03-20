# NGrid — Melhoria de Gestão de Memória no ReplicationManager

## Contexto

Três estruturas no `ReplicationManager` crescem indefinidamente sem qualquer mecanismo de eviction ou TTL,
representando um risco real de **memory leak gradual** em clusters de longa vida e alto throughput:

| Estrutura | Tipo | Papel | Problema |
|---|---|---|---|
| `applied` | `CopyOnWriteArraySet<UUID>` | Dedup guard — evita reaplicação | Nunca é podada |
| `log` | `ConcurrentHashMap<UUID, ReplicatedRecord>` | Auditoria de operações aplicadas | Nunca é podada |
| `replicationLogBySequence` | `Map<String, NavigableMap<Long, Payload>>` | Log de resend para followers | **Já tem eviction** via `replicationLogRetention` |

A boa notícia: o `ReplicationConfig` já possui o campo `replicationLogRetention` (default `1000`), e o
`replicationLogBySequence` já o respeita. Precisamos apenas estender o mesmo padrão para `applied` e `log`.

> [!NOTE]
> **Não há new dependency necessária.** A solução usa estruturas puramente Java, sem Guava ou Caffeine.

---

## Proposed Changes

### Replication — Eviction de `applied` e `log`

#### [MODIFY] [ReplicationConfig.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationConfig.java)

Adicionar dois novos parâmetros de configuração no `Builder`:

- **`appliedSetMaxSize`** (default: `5000`) — tamanho máximo do dedup guard `applied`.
- **`operationLogMaxSize`** (default: `2000`) — tamanho máximo do audit log `log`.

A lógica de default garante que os novos campos são **10× a replicação padrão** mas menores que o infinito atual.

#### [MODIFY] [ReplicationManager.java](file:///home/lucas/Projects/nishisan/nishi-utils/src/main/java/dev/nishisan/utils/ngrid/replication/ReplicationManager.java)

**Mudança 1 — `applied` com eviction por tamanho:**

Substituir o `CopyOnWriteArraySet<UUID>` por um `LinkedHashSet<UUID>` protegido por lock
(o `applied` já é acessado sob `sequenceBufferLock` nos paths críticos), com eviction do elemento
mais antigo via `Iterator.next() + remove()` quando o tamanho ultrapassa `appliedSetMaxSize`.

```java
// ANTES
private final Set<UUID> applied = new CopyOnWriteArraySet<>();

// DEPOIS — LinkedHashSet com ordem de inserção garante eviction FIFO
private final LinkedHashSet<UUID> applied = new LinkedHashSet<>();
```

Locais onde `applied.add(opId)` é chamado (linhas 416, 672, 812): envolver com chamada ao helper
`trimApplied()` logo após o `add`.

```java
private void trimApplied() {
    // chamado sempre sob sequenceBufferLock
    int maxSize = config.appliedSetMaxSize();
    while (applied.size() > maxSize) {
        Iterator<UUID> it = applied.iterator();
        if (it.hasNext()) {
            it.next();
            it.remove();
        }
    }
}
```

> [!IMPORTANT]
> O `applied` atualmente usa `CopyOnWriteArraySet`, que é thread-safe mas imutável por inserção. Com
> `LinkedHashSet`, toda leitura e escrita deve ser protegida por `sequenceBufferLock` (já existente).
> Verificar se todas as leituras de `applied.contains()` estão sob o lock — se alguma não estiver,
> adicionar proteção.

**Mudança 2 — `log` com eviction por tamanho:**

O `log` (auditoria de operações) pode crescer até N operações pendentes + todas as já committed sem remoção.
O `pending.remove(operationId)` já acontece em `completeOperation`, mas o `log.put` nunca tem um correspondente
`log.remove`.

Adicionar um método `trimLog()` chamado no scheduler `timeoutScheduler` a cada intervalo de cleanup (ex: 5× o `operationTimeout`).

```java
private void trimLog() {
    int maxSize = config.operationLogMaxSize();
    if (log.size() <= maxSize) return;
    // Manter apenas as entradas COMMITTED mais recentes - remove as mais antigas
    log.entrySet().removeIf(e -> e.getValue().status() == OperationStatus.COMMITTED
            && log.size() > maxSize);
}
```

Registrar o job no `start()`:
```java
long cleanupPeriodMs = Math.max(5000L, config.operationTimeout().toMillis() * 5);
timeoutScheduler.scheduleAtFixedRate(this::trimLog, cleanupPeriodMs, cleanupPeriodMs, TimeUnit.MILLISECONDS);
```

**Mudança 3 — Expor métricas de tamanho:**

Adicionar getters para monitoramento e para os testes unitários:

```java
public int getAppliedSetSize()    { ... }
public int getOperationLogSize()  { ... }
```

---

## Verification Plan

### Testes Automatizados

Existe a classe `ReplicationSuccessIntegrationTest` e `SequenceGapRecoveryIntegrationTest` que testam o caminho
de commit e gaps. Usaremos como base.

**Novo teste unitário** a ser criado:

#### [NEW] `ReplicationMemoryEvictionTest.java`

Caminho: `src/test/java/dev/nishisan/utils/ngrid/replication/ReplicationMemoryEvictionTest.java`

Cenários a cobrir:
1. **`applied` não ultrapassa `appliedSetMaxSize`** — aplicar N+100 operações com `appliedSetMaxSize=N`, verificar que `getAppliedSetSize() <= N`.
2. **`log` não ultrapassa `operationLogMaxSize`** — commitar N+100 operações, acionar `trimLog()` e verificar `getOperationLogSize() <= N`.
3. **Dedup ainda funciona após eviction** — re-enviar uma operação cujo UUID foi evictado do `applied`; ela deve ser reaplicada apenas UMA vez (dado que o `processSequenceBuffer` também filtra por sequência).

**Comando para rodar:**
```bash
cd /home/lucas/Projects/nishisan/nishi-utils
mvn test -pl . -Dtest=ReplicationMemoryEvictionTest -am
```

**Testes existentes de regressão** — rodar para garantir que a refatoração não quebrou nada:
```bash
cd /home/lucas/Projects/nishisan/nishi-utils
mvn test -pl . -Dtest="ReplicationSuccessIntegrationTest,SequenceGapRecoveryIntegrationTest,QueueRestartConsistencyTest,QueueNodeFailoverIntegrationTest" -am
```

**Suite completa da ngrid:**
```bash
cd /home/lucas/Projects/nishisan/nishi-utils
mvn test -pl . -Dtest="dev.nishisan.utils.ngrid.**" -am
```

---

## Resumo do Impacto

| Estrutura | Antes | Depois |
|---|---|---|
| `applied` | Crescimento ilimitado | Capped em `appliedSetMaxSize` (default 5000) |
| `log` | Crescimento ilimitado | Trimmed periodicamente até `operationLogMaxSize` (default 2000) |
| `replicationLogBySequence` | Já limitado | Sem alteração |

**Risco de regressão:** Baixo. O único cuidado é garantir que a troca de `CopyOnWriteArraySet` por `LinkedHashSet`
não introduza race condition — o que é mitigado pelo uso existente do `sequenceBufferLock`.
