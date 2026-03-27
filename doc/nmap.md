# NMap — Mapa Persistente Standalone

O `NMap<K,V>` é um mapa concorrente com persistência local opcional, baseado em **WAL (Write-Ahead Log) + Snapshots** e uma arquitetura plugável de **Storage Strategies**. Análogo ao `NQueue`, pode ser usado de forma **independente** do NGrid.

![Diagrama de Componentes](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/nmap_component.puml)

## Quick Start

### 1. Persistência com defaults (WAL + fsync)

```java
try (NMap<String, String> map = NMap.open(Path.of("/data"), "my-map")) {
    map.put("chave", "valor");
    Optional<String> v = map.get("chave");
}
```

### 2. Uso in-memory (sem persistência)

```java
try (NMap<String, Integer> counters = NMap.open(Path.of("/tmp"), "counters",
        NMapConfig.inMemory())) {
    counters.put("hits", 42);
}
```

### 3. Configuração customizada via Builder

```java
NMapConfig config = NMapConfig.builder()
        .mode(NMapPersistenceMode.ASYNC_WITH_FSYNC)
        .snapshotIntervalOperations(5_000)
        .snapshotIntervalTime(Duration.ofMinutes(2))
        .batchSize(50)
        .batchTimeout(Duration.ofMillis(5))
        .healthListener((name, type, cause) ->
            log.error("Falha em {}: {} - {}", name, type, cause.getMessage()))
        .build();

try (NMap<String, byte[]> blobs = NMap.open(Path.of("/data"), "blobs", config)) {
    blobs.put("file1", bytes);
}
```

### 4. Disk Offload Strategy (100% em disco)

Ideal para mapas grandes que não cabem em heap. Entradas são serializadas individualmente em arquivos no disco, com um cache via `SoftReference` para acessos frequentes.

```java
NMapConfig cfg = NMapConfig.builder()
        .mode(NMapPersistenceMode.DISABLED) // WAL desnecessário — strategy já persiste
        .offloadStrategyFactory(DiskOffloadStrategy::new)
        .build();

try (NMap<String, String> map = NMap.open(Path.of("/data"), "offloaded", cfg)) {
    map.put("key", "value");
    // Entrada armazenada em disco, não em heap
}
```

### 5. Hybrid Offload Strategy (hot/cold tiering)

Mantém entradas quentes em memória e faz offload das frias para disco, com política de eviction configurável (LRU ou SIZE_THRESHOLD).

```java
NMapConfig cfg = NMapConfig.builder()
        .mode(NMapPersistenceMode.DISABLED) // WAL desnecessário — strategy já persiste
        .offloadStrategyFactory((baseDir, name) ->
            HybridOffloadStrategy.<String, String>builder(baseDir, name)
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(10_000)
                .build())
        .build();

try (NMap<String, String> sessions = NMap.open(Path.of("/data"), "sessions", cfg)) {
    sessions.put("session-123", "user-data");
    // Entradas quentes ficam em memória; frias migram para disco automaticamente
}
```

### 6. Hybrid com Métricas (Observabilidade)

```java
StatsUtils stats = ... ; // sua instância de métricas

NMapConfig cfg = NMapConfig.builder()
        .mode(NMapPersistenceMode.DISABLED)
        .offloadStrategyFactory((baseDir, name) ->
            HybridOffloadStrategy.<String, String>builder(baseDir, name)
                .evictionPolicy(EvictionPolicy.LRU)
                .maxInMemoryEntries(5_000)
                .stats(stats) // emite métricas via StatsUtils
                .build())
        .build();

try (NMap<String, String> map = NMap.open(Path.of("/data"), "observed", cfg)) {
    map.put("k", "v");
    // Métricas NMAP.GET_HIT, NMAP.CACHE_MISS, NMAP.EVICTION, etc. são emitidas
}
```

## Arquitetura

### Componentes

| Classe | Tipo | Responsabilidade |
|---|---|---|
| `NMap<K,V>` | Fachada | API pública — `put/get/remove/forEach/close`. Delega storage para a strategy ativa |
| `NMapConfig` | Configuração | Configuração imutável via Builder pattern |
| `NMapOffloadStrategy<K,V>` | Interface | Abstração do backend de storage (onde os dados vivem) |
| `NMapOffloadStrategyFactory` | Interface funcional | Factory para criação lazy de strategies |
| `InMemoryStrategy<K,V>` | Strategy | Storage default — `ConcurrentHashMap` em heap |
| `DiskOffloadStrategy<K,V>` | Strategy | Storage 100% em disco (entry-per-file + `SoftReference` cache) |
| `HybridOffloadStrategy<K,V>` | Strategy | Hot/cold tiering — memória para hot, disco para cold com eviction |
| `EvictionPolicy` | Enum | Políticas de eviction: `LRU`, `SIZE_THRESHOLD` |
| `NMapPersistence<K,V>` | Engine | Engine de persistência WAL + Snapshot (para strategies não-persistentes) |
| `NMapPersistenceMode` | Enum | Modo de persistência: `DISABLED`, `ASYNC_NO_FSYNC`, `ASYNC_WITH_FSYNC` |
| `NMapOperationType` | Enum | Tipo de operação: `PUT`, `REMOVE` |
| `NMapWALEntry` | Record | Entrada no WAL (timestamp, tipo, key, value) |
| `NMapMetadata` | Record | Metadados de versionamento do snapshot |
| `NMapHealthListener` | Interface funcional | Callback para falhas de persistência |
| `NMapMetrics` | Constantes | Chaves de métricas para integração com `StatsUtils` |

### Fluxo de Decisão de Storage

```
NMap.open(baseDir, name, config)
  │
  ├─ config.offloadStrategyFactory != null?
  │    ├── SIM → Instancia strategy via factory (Disk/Hybrid/Custom)
  │    └── NÃO → Usa InMemoryStrategy (default)
  │
  └─ config.mode != DISABLED && !strategy.isInherentlyPersistent()?
       ├── SIM → Cria NMapPersistence (WAL + Snapshot)
       └── NÃO → Sem persistência WAL (strategy já é persistente ou modo disabled)
```

> **Nota:** `DiskOffloadStrategy` e `HybridOffloadStrategy` retornam `isInherentlyPersistent() = true`, tornando o WAL redundante. O `InMemoryStrategy` retorna `false`, exigindo WAL para durabilidade.

## Estratégias de Offload

| Strategy | Dados em | Persistência inerente | Heap usage | Throughput | Uso típico |
|---|---|---|---|---|---|
| `InMemoryStrategy` | Heap (ConcurrentHashMap) | Não | Total | Máximo | Caches, mapas de sessão, dados transientes |
| `DiskOffloadStrategy` | Disco (entry-per-file) | Sim | Mínimo (índice + SoftRef) | Moderado (I/O por leitura) | Mapas grandes, dados que não cabem em heap |
| `HybridOffloadStrategy` | Heap (hot) + Disco (cold) | Sim | Controlado (maxInMemory) | Alto para hot / Moderado para cold | Workloads com localidade temporal (sessions, LRU caches) |

### DiskOffloadStrategy — Detalhes

- Cada entrada é serializada em um arquivo individual no diretório `<baseDir>/<mapName>/offload/`
- Nomes de arquivo usam hash SHA-1 da key serializada (collision-free)
- `SoftReference` cache evita re-reads para entradas quentes; JVM reclama sob pressão de memória
- Thread safety via `ReentrantReadWriteLock`
- Adequado para mapas com até ~1M entradas (um arquivo por entry)

### HybridOffloadStrategy — Detalhes

- Hot cache em `LinkedHashMap` (access-ordered para LRU, insertion-ordered para `SIZE_THRESHOLD`)
- Quando `hotCache.size() > maxInMemoryEntries`, entradas são evictadas para disco
- `get()` de uma entrada cold faz warm-up automático (lê do disco → promove para hot cache → pode triggerar nova eviction)
- Na `close()`, todas as entradas hot são flushadas para disco para sobreviver ao restart
- Diretório de offload: `<baseDir>/<mapName>/hybrid-offload/`
- Suporta métricas via `StatsUtils` (veja seção Métricas)

## Fluxo de Persistência (WAL + Snapshot)

![Fluxo de Persistência](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/nmap_persistence_flow.puml)

**Estratégia WAL + Snapshot (apenas para `InMemoryStrategy`):**

1. Toda mutação (`put`/`remove`) é registada como `NMapWALEntry` no WAL
2. Entradas são escritas em batch por uma thread background (modo ASYNC) ou imediatamente (modo SYNC via `appendSync`)
3. Periodicamente (por contagem de operações ou por tempo), um snapshot completo é gravado e o WAL é rotacionado
4. Na recuperação, o snapshot é carregado primeiro, seguido do replay do WAL remanescente

### Modos de Persistência

| Modo | Durabilidade | Throughput | Uso típico |
|---|---|---|---|
| `DISABLED` | Nenhuma | Máximo | Caches, dados transientes |
| `ASYNC_NO_FSYNC` | Eventual | Alto | Dados recuperáveis de outra fonte |
| `ASYNC_WITH_FSYNC` | Forte | Moderado | Dados críticos, offsets de fila |

### Health Listener

O `NMapHealthListener` é uma interface funcional que notifica falhas de I/O sem interromper operações do mapa:

```java
@FunctionalInterface
public interface NMapHealthListener {
    void onPersistenceFailure(String mapName, PersistenceFailureType type, Throwable cause);

    enum PersistenceFailureType {
        WAL_WRITE, WAL_OPEN, SNAPSHOT_WRITE, SNAPSHOT_LOAD, META_WRITE
    }
}
```

## Métricas (`NMapMetrics`)

O `HybridOffloadStrategy` emite métricas via `StatsUtils` quando configurado com `.stats(statsInstance)`. As chaves de métricas são definidas em `NMapMetrics`:

### Hit Counters (frequência/rate)

| Chave | Descrição |
|---|---|
| `NMAP.GET_HIT` | Cada chamada a `get()` |
| `NMAP.CACHE_HIT` | `get()` encontrou a entrada em memória (hot) |
| `NMAP.CACHE_MISS` | `get()` precisou ler do disco (cold) |
| `NMAP.PUT` | Cada chamada a `put()` |
| `NMAP.REMOVE` | Cada chamada a `remove()` |
| `NMAP.EVICTION` | Entrada evictada de memória para disco |
| `NMAP.WARM_UP` | Entrada promovida de disco para memória |
| `NMAP.DISK_IO_ERROR` | Falha de I/O em disco |

### Gauges (valores atuais)

| Chave | Descrição |
|---|---|
| `NMAP.HOT_SIZE` | Entradas em memória (hot cache) |
| `NMAP.COLD_SIZE` | Entradas offloaded em disco |
| `NMAP.TOTAL_SIZE` | Total de entradas (hot + cold) |

### Latências (médias)

| Chave | Descrição |
|---|---|
| `NMAP.DISK_READ_LATENCY` | Tempo médio (ms) de leituras de disco |
| `NMAP.DISK_WRITE_LATENCY` | Tempo médio (ms) de escritas em disco |

## API Pública

| Método | Retorno | Descrição |
|---|---|---|
| `NMap.open(baseDir, name)` | `NMap<K,V>` | Abre mapa com defaults (`ASYNC_WITH_FSYNC` + `InMemoryStrategy`) |
| `NMap.open(baseDir, name, config)` | `NMap<K,V>` | Abre mapa com configuração customizada |
| `put(key, value)` | `Optional<V>` | Insere/atualiza, retorna valor anterior |
| `get(key)` | `Optional<V>` | Consulta por chave |
| `remove(key)` | `Optional<V>` | Remove e retorna valor anterior |
| `putAll(entries)` | `void` | Insere múltiplas entradas |
| `clear()` | `void` | Remove todas as entradas |
| `containsKey(key)` | `boolean` | Verifica existência |
| `size()` | `int` | Número de entradas |
| `isEmpty()` | `boolean` | Mapa vazio? |
| `keySet()` | `Set<K>` | Visão imutável das chaves |
| `entrySet()` | `Set<Entry>` | Visão imutável das entradas |
| `forEach(action)` | `void` | Itera todas as entradas |
| `snapshot()` | `Map<K,V>` | Cópia imutável do estado atual (⚠️ I/O intensivo para strategies em disco) |
| `name()` | `String` | Nome do mapa |
| `persistenceFailureCount()` | `long` | Contagem de falhas de I/O |
| `close()` | `void` | Fecha e libera recursos |

## Configuração (`NMapConfig`)

| Parâmetro | Default | Descrição |
|---|---|---|
| `mode` | `DISABLED` (Builder) / `ASYNC_WITH_FSYNC` (factory method) | Modo de persistência WAL |
| `snapshotIntervalOperations` | `10.000` | Operações entre snapshots |
| `snapshotIntervalTime` | `5 min` | Tempo máximo entre snapshots |
| `batchSize` | `100` | Tamanho do batch de escrita no WAL |
| `batchTimeout` | `10 ms` | Timeout para flush do batch |
| `healthListener` | no-op | Callback de falhas de persistência |
| `offloadStrategyFactory` | `null` (usa `InMemoryStrategy`) | Factory para criação da strategy de storage |

## Integração com NGrid

O NGrid utiliza o `NMap` internamente para seus mapas distribuídos:

- **`NGridNode`** instancia `NMapConfig` e chama `NMap.open()` ao criar mapas
- **`MapClusterService`** acessa `NMapPersistence` para persistir operações replicadas
- **`NGridAlertEngine`** implementa `NMapHealthListener` para disparar alertas de falha

## Estrutura de Arquivos no Disco

### InMemoryStrategy + WAL

```
<baseDir>/<mapName>/
├── snapshot.dat      ← Snapshot completo serializado (Java ObjectStream)
├── wal.log           ← WAL append-only com entradas binárias
└── map.meta          ← Metadados binários (version, offset, timestamp)
```

### DiskOffloadStrategy

```
<baseDir>/<mapName>/
└── offload/
    ├── <sha1-hash-1>.dat   ← Entrada serializada (key + value)
    ├── <sha1-hash-2>.dat
    └── ...
```

### HybridOffloadStrategy

```
<baseDir>/<mapName>/
└── hybrid-offload/
    ├── <sha1-hash-1>.dat   ← Entradas cold (evictadas de memória)
    ├── <sha1-hash-2>.dat
    └── ...
```

> **Nota:** Na `close()`, o `HybridOffloadStrategy` flusha todas as entradas hot para disco, garantindo que o estado sobreviva ao restart.

## Testes

| Teste | Tipo | Cobertura |
|---|---|---|
| `NMapTest` | Unitário | CRUD, putAll, clear, snapshot, in-memory, modos de persistência |
| `NMapRecoveryTest` | Integração | Crash recovery via WAL replay e snapshot |
| `NMapOffloadTest` | Integração | DiskOffloadStrategy — CRUD, recovery, entry-per-file |
| `HybridOffloadStrategyTest` | Integração | HybridOffloadStrategy — eviction LRU/SIZE_THRESHOLD, warm-up, close flush |
| `NMapExample` | Exemplo | Uso standalone completo como referência |
