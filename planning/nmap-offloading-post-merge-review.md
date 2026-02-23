# Post-Merge Review — `feature/nmap-offloading`

> **Merge realizado em:** 2026-02-21  
> **Branch origem:** `origin/feature/nmap-offloading`  
> **Branch destino:** `docs/nqueue-agent-skill`  
> **Status do merge:** ✅ Concluído sem conflitos (estratégia `ort`, auto-merge em `QueueClusterService.java`)

---

## O que foi mergeado

A feature introduz um sistema de **offloading de storage plugável** para o `NMap`. O backend que antes era fixo em `ConcurrentHashMap` foi extraído para a interface `NMapOffloadStrategy`, com três implementações:

| Classe | Descrição |
|---|---|
| `InMemoryStrategy` | Wraps `ConcurrentHashMap` — comportamento original (default) |
| `DiskOffloadStrategy` | Toda entrada persiste em disco via `ObjectOutputStream`, com soft cache em memória |
| `HybridOffloadStrategy` | Hot cache em memória (LRU ou SIZE_THRESHOLD) + spill para disco quando threshold é atingido |

**Outros arquivos adicionados:**

- `NMapMetrics` — constantes de métricas para observabilidade via `StatsUtils`
- `EvictionPolicy` — enum `LRU` / `SIZE_THRESHOLD`
- `NMapOffloadStrategyFactory` — `@FunctionalInterface` para injeção da strategy via `NMapConfig`
- `NMapConfig` — novo campo `offloadStrategyFactory`

**Bugfix colateral (em `QueueClusterService`):**
- Reset de offsets de consumer durante `resetStateForSnapshotInstall()` para evitar entrega duplicada de mensagens após snapshot install.

---

## Pontos Positivos ✅

1. **Backward compatibility total** — caminho padrão instancia `InMemoryStrategy`, mantendo comportamento existente.
2. **Strategy Pattern bem aplicado** — `NMap` delega 100% das operações de dados para a strategy.
3. **`isInherentlyPersistent()`** — decisão elegante para evitar redundância entre WAL e disk strategy.
4. **`internalMap()` seguro** — proteção com `instanceof InMemoryStrategy` + `UnsupportedOperationException`.
5. **Testes robustos** — `NMapOffloadTest` e `HybridOffloadStrategyTest` cobrem restart-survival, clear, large dataset e iteração.
6. **`HybridOffloadStrategy` thread-safe** — usa `ReentrantReadWriteLock` com `try/finally` em todos os paths.

---

## Pendências e Bugs a Corrigir 🔧

### 🔴 CRÍTICO — Hash Collision no `pathForKey()` (ambas as classes)

**Afeta:** `DiskOffloadStrategy` e `HybridOffloadStrategy`

```java
private Path pathForKey(K key) {
    int hash = key.hashCode();
    String fileName = String.format("%08x", hash) + ENTRY_SUFFIX;
    return offloadDir.resolve(fileName);  // ← colisão silencia dados!
}
```

**Problema:** Dois objetos com o mesmo `hashCode()` mas chaves diferentes sobrescrevem o mesmo arquivo em disco. O dado mais antigo é **perdido silenciosamente**.

**Correção sugerida:**
Incluir a própria chave no nome do arquivo usando um hash de conteúdo (ex: SHA-1/MD5 da chave serializada) ou UUID gerado no `put()` e persistido no índice. Alternativa mais simples: ao detectar colisão, prefixar sequencialmente (`_1`, `_2`...).

```java
// Proposta: usar hash da chave serializada
private Path pathForKey(K key) throws IOException {
    byte[] keyBytes = serialize(key);
    String hash = HexFormat.of().formatHex(
        MessageDigest.getInstance("SHA-1").digest(keyBytes)
    );
    return offloadDir.resolve(hash + ENTRY_SUFFIX);
}
```

**Teste a criar:** `diskOffloadHashCollisionTest()` — duas chaves com mesmo `hashCode()` devem coexistir.

---

### 🟡 MÉDIO — `DiskOffloadStrategy` não é thread-safe (operações compostas)

O `put()` faz `get()` + I/O + `index.put()` + `cache.put()` sem lock. Diferente do `HybridOffloadStrategy` que tem `ReentrantReadWriteLock`.

**Ação:** Adicionar `ReentrantReadWriteLock` ou documentar explicitamente que `DiskOffloadStrategy` não é thread-safe e que o `NMap` deve garantir serialização.

---

### 🟡 MÉDIO — `snapshot()` pode bloquear em strategies de disco

```java
// NMap.java
public Map<K, V> snapshot() {
    return Collections.unmodifiableMap(new ConcurrentHashMap<>(storage.asMap()));
}
```

Para `DiskOffloadStrategy`, `asMap().entrySet()` itera todos os arquivos em disco. Com 100k entradas, isso bloqueia a thread com 100k I/Os síncronos.

**Ação:** Adicionar documentação no Javadoc que `snapshot()` pode ser custoso para strategies persistentes, ou fornecer um `snapshotAsync()`.

---

### 🟢 BAIXO — `InMemoryStrategy.asMap()` retorna mapa interno mutável

```java
@Override
public Map<K, V> asMap() {
    return data;  // expõe ConcurrentHashMap diretamente
}
```

Conveniente para `NMapPersistence`, mas quebra encapsulamento. Baixo risco atual dado o uso `package-private`.

---

## Ordem de Prioridade para Próxima Sessão

1. [ ] **Corrigir `pathForKey()`** em `DiskOffloadStrategy` e `HybridOffloadStrategy` (hash collision)
2. [ ] **Adicionar teste** `diskOffloadHashCollisionTest()`
3. [ ] **Avaliar thread-safety** do `DiskOffloadStrategy` — lock ou documentação
4. [ ] **Javadoc warning** em `NMap.snapshot()` para strategies de disco
