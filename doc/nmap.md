# NMap — Mapa Persistente Standalone

O `NMap<K,V>` é um mapa concorrente com persistência local opcional, baseado em **WAL (Write-Ahead Log) + Snapshots**. Análogo ao `NQueue`, pode ser usado de forma **independente** do NGrid.

![Diagrama de Componentes](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/docs/diagrams/nmap_component.puml)

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
| `NMapMetadata` | Metadados de versionamento do snapshot |
| `NMapHealthListener` | Callback funcional para falhas de persistência |

### Fluxo de Persistência

![Fluxo de Persistência](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/docs/diagrams/nmap_persistence_flow.puml)

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

## Integração com NGrid

O NGrid utiliza o `NMap` internamente para seus mapas distribuídos:

- **`NGridNode`** instancia `NMapConfig` e chama `NMap.open()` ao criar mapas
- **`MapClusterService`** acessa `NMapPersistence` para persistir operações replicadas
- **`NGridAlertEngine`** implementa `NMapHealthListener` para disparar alertas de falha

## Estrutura de Arquivos no Disco

```
<baseDir>/<mapName>/
├── snapshot.dat      ← Snapshot completo serializado (Java ObjectStream)
├── wal.log           ← WAL append-only com entradas binárias
└── meta.json         ← Metadados (versão do snapshot, contadores)
```

## Testes

| Teste | Tipo | Cobertura |
|---|---|---|
| `NMapTest` | Unitário | CRUD, putAll, clear, snapshot, in-memory, modos de persistência |
| `NMapRecoveryTest` | Integração | Crash recovery via WAL replay e snapshot |
| `NMapExample` | Exemplo | Uso standalone completo como referência |
