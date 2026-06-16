# NMap — Mapa Persistente Standalone

O `NMap<K,V>` é um mapa concorrente com persistência local opcional, baseado em **WAL (Write-Ahead Log) + Snapshots**. Análogo ao `NQueue`, pode ser usado de forma **independente** do NGrid.

![Diagrama de Componentes](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/nmap_component.puml)

## Quick Start

```java
// Uso mais simples — persistência com defaults
try (NMap<String, String> map = NMap.open(Path.of("/data"), "my-map")) {
    map.put("chave", "valor");
    Optional<String> v = map.get("chave");
}

// Uso in-memory (sem persistência)
try (NMap<String, Integer> counters = NMap.open(Path.of("/tmp"), "counters",
        NMapConfig.inMemory())) {
    counters.put("hits", 42);
}

// Configuração customizada via Builder
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

## Arquitetura

### Componentes

| Classe | Responsabilidade |
|---|---|
| `NMap<K,V>` | Fachada pública — API `put/get/remove/forEach/close` |
| `NMapConfig` | Configuração imutável via Builder pattern |
| `NMapPersistence<K,V>` | Engine de persistência WAL + Snapshot |
| `NMapPersistenceMode` | Modo de persistência: `DISABLED`, `ASYNC_NO_FSYNC`, `ASYNC_WITH_FSYNC` |
| `NMapOperationType` | Tipo de operação: `PUT`, `REMOVE` |
| `NMapWALEntry` | Record que representa uma entrada no WAL |
| `NMapMetadata` | Metadados persistidos de snapshot e última mutação |
| `NMapHealthListener` | Callback funcional para falhas de persistência |
| `NMapOffloadStrategy<K,V>` | Interface do backend de armazenamento (in-memory / disco) |
| `OffloadMode` | Seleção declarativa do backend: `IN_MEMORY`, `DISK`, `HYBRID`, `SEGMENT` |
| `InMemoryStrategy` | Backend em heap (`ConcurrentHashMap`) — default |
| `DiskOffloadStrategy` | Um arquivo por entrada, fan-out de sharding configurável, hot-cache LRU |
| `HybridOffloadStrategy` | Hot em memória + cold em disco, evicção `LRU`/`SIZE_THRESHOLD` |
| `SegmentOffloadStrategy` | Log-structured (Bitcask-like): poucos segmentos append-only, LZ4 opt-in, compactação |
| `OffloadLayout` | Mapeia chaves → caminhos com fan-out (`shardDepth`/`shardWidth`) configurável |

### Fluxo de Persistência

![Fluxo de Persistência](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/nmap_persistence_flow.puml)

**Estratégia WAL + Snapshot:**

1. Toda mutação (`put`/`remove`) é registada como `NMapWALEntry` no WAL
2. Entradas são escritas em batch por uma thread background (modo ASYNC) ou imediatamente (modo SYNC)
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

## API Pública

| Método | Retorno | Descrição |
|---|---|---|
| `NMap.open(baseDir, name)` | `NMap<K,V>` | Abre mapa com defaults (`ASYNC_WITH_FSYNC`) |
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
| `snapshot()` | `Map<K,V>` | Cópia imutável do estado atual |
| `name()` | `String` | Nome do mapa |
| `persistenceFailureCount()` | `long` | Contagem de falhas de I/O |
| `lastMutationTimestamp()` | `long` | Epoch millis da última mutação bem-sucedida, ou `0` se nunca houve |
| `close()` | `void` | Fecha e libera recursos |

## Configuração (`NMapConfig`)

| Parâmetro | Default | Descrição |
|---|---|---|
| `mode` | `DISABLED` | Modo de persistência |
| `snapshotIntervalOperations` | `10.000` | Operações entre snapshots |
| `snapshotIntervalTime` | `5 min` | Tempo máximo entre snapshots |
| `batchSize` | `100` | Tamanho do batch de escrita no WAL |
| `batchTimeout` | `10 ms` | Timeout para flush do batch |
| `healthListener` | no-op | Callback de falhas |
| `offloadMode` | `IN_MEMORY` | Backend declarativo (`IN_MEMORY`/`DISK`/`HYBRID`/`SEGMENT`) |
| `offloadStrategyFactory` | `null` | Estratégia custom; **tem precedência** sobre `offloadMode` |
| `hotCacheMaxEntries` | `10.000` | Teto do hot-cache (DISK/HYBRID/SEGMENT); `0` desabilita |
| `evictionPolicy` | `LRU` | Política de evicção do modo `HYBRID` |
| `shardFanOut(depth, width)` | `2 / 2` | Fan-out de diretórios do modo `DISK` (`depth*width ≤ 40`) |
| `numSegments` | `16` | Nº de segmentos (1..128) do modo `SEGMENT` |
| `compressionEnabled` | `false` | Compressão LZ4 do value no modo `SEGMENT` |
| `compactionThreshold` | `0.5` | Fração de bytes mortos que dispara compactação (`SEGMENT`) |

> **Seleção do backend:** se `offloadStrategyFactory` for definido, ele é usado
> (extension point para estratégias custom). Caso contrário, o `offloadMode`
> determina a estratégia, montada pelo `NMap` a partir dos knobs acima.

## Integração com NGrid

O NGrid utiliza o `NMap` internamente para seus mapas distribuídos:

- **`NGridNode`** instancia `NMapConfig` e chama `NMap.open()` ao criar mapas
- **`MapClusterService`** acessa `NMapPersistence` para persistir operações replicadas
- **`NGridAlertEngine`** implementa `NMapHealthListener` para disparar alertas de falha

## Estrutura de Arquivos no Disco

Modo em memória com persistência WAL + snapshot (estratégias não inerentemente
persistentes):

```
<baseDir>/<mapName>/
├── snapshot.dat      ← Snapshot completo serializado (Java ObjectStream)
├── wal.log           ← WAL append-only com entradas binárias
└── meta.json         ← Metadados (versão do snapshot, contadores)
```

Modo `DISK` (`DiskOffloadStrategy`) — um arquivo por entrada, com fan-out
configurável (default `hash[0:2]/hash[2:4]/hash.dat`):

```
<baseDir>/<mapName>/offload/
└── ab/cd/abcd…sha1.dat   ← um arquivo por chave
```

Modo `SEGMENT` (`SegmentOffloadStrategy`) — poucos segmentos append-only:

```
<baseDir>/<mapName>/segment-offload/
├── seg-000.log   ← log append-only (registros PUT/TOMBSTONE)
├── seg-000.hint  ← índice persistido (chave → offset) p/ startup rápido
├── seg-001.log
└── ...           ← total de arquivos = 2 × numSegments
```

## Offloading log-structured (modo `SEGMENT`)

O `SegmentOffloadStrategy` é um backend estilo **Bitcask** voltado a mapas de
alta cardinalidade, onde "um arquivo por entrada" esgota inodes e torna lentas as
operações de diretório (backup, `find`, `rsync`). Em vez disso, as entradas são
agrupadas em um número fixo e pequeno de **segmentos append-only** (1..128).

- **Roteamento:** a chave vai para `segmento = (32 bits do SHA-1 da chave) % numSegments`,
  estável entre reinícios (a chave sempre cai no mesmo segmento). `numSegments`
  deve ser fixo para um diretório já populado.
- **Registro:** `magic | version | flags | type(PUT/TOMBSTONE) | keyLen | valLen | key | value | CRC32`.
  A chave fica sempre em claro (permite reconstruir o índice sem descomprimir);
  o value é comprimido com **LZ4** quando `compressionEnabled`.
- **Índice em memória:** `chave → (segmento, offset, tamanho)`; leitura é um único
  `read` posicional. Update gera novo append (a versão antiga vira lixo); remoção
  grava um tombstone.
- **Recovery:** carrega o `.hint` (validado por CRC) e faz **replay incremental**
  apenas do delta após o `coveredOffset`; sem hint válido, faz replay completo do
  log. Registros truncados/corrompidos na cauda são descartados (`truncate`).
- **Compactação (GC):** quando `bytesMortos / tamanho ≥ compactionThreshold`, uma
  thread em background reescreve o segmento mantendo só os registros vivos (swap
  atômico via arquivo temporário + `ATOMIC_MOVE`), regravando o `.hint`. A
  operação segura o lock daquele segmento; os demais seguem operando.
- **Durabilidade:** o `fsync` por append é derivado do `NMapPersistenceMode`
  (`ASYNC_WITH_FSYNC` força em cada escrita; os demais confiam no flush do SO).

**Trade-off de memória:** o índice (chave → offset) reside todo em heap — é o
custo do modelo Bitcask. Ele não guarda o value (só o offset), mas em
cardinalidades muito altas (dezenas de milhões de chaves) exige heap dedicado.
Resolve o problema prioritário de inodes/diretório; um índice esparso/em disco é
trabalho futuro.

![Offload Segmentado](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/nmap_segment_offload.puml)

## Testes

| Teste | Tipo | Cobertura |
|---|---|---|
| `NMapTest` | Unitário | CRUD, putAll, clear, snapshot, in-memory, modos de persistência |
| `NMapRecoveryTest` | Integração | Crash recovery via WAL replay e snapshot |
| `NMapOffloadTest` | Unitário | `DiskOffloadStrategy`: CRUD, restart, layout legado, fan-out configurável, hot-cache, concorrência |
| `HybridOffloadStrategyTest` | Unitário | Evicção `LRU`/`SIZE_THRESHOLD`, warm-up, persistência, métricas |
| `SegmentOffloadStrategyTest` | Unitário | `SegmentOffloadStrategy`: CRUD, restart/hint/replay incremental, compactação, compressão, tolerância a cauda corrompida, limites de `numSegments` |
| `NMapConfigOffloadModeTest` | Unitário | Seleção via `OffloadMode`, precedência da factory, validações |
| `NMapDestroyTest` | Unitário | `destroy()` por estratégia (remoção de diretórios) |
| `NMapExample` | Exemplo | Uso standalone completo como referência |
