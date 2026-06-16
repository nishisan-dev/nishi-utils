# Plano — Evolução do offload em disco do NMap (issue #155: A1 + A2 + A3)

## Context

A issue **#155** reporta, com evidência de produção, que o `DiskOffloadStrategy` do `NMap`
(`nishi-utils-core`, pacote `dev.nishisan.utils.map`) **grava 1 arquivo por entrada**:

```
find <offload-dir> | wc -l   ->  8.102.094 arquivos (~8,1M inodes)
du -ksh <offload-dir>        ->  31G   (block amplification: valores ~3,8KB em blocos de 4KB)
```

Consequências: risco de esgotar inodes, ops de diretório lentíssimas (`find`/`du`/`rsync`/backup),
e open/clear/compaction lentos. O Javadoc da classe já antecipa o limite (`< 1M entradas`).

Este ciclo endereça **apenas os 3 temas de NMap** da issue (os temas de NQueue — B1/B2/B3 — ficam fora):

- **A1 (prioridade máxima):** novo backend que agrupa N entradas por arquivo, eliminando o 1-arquivo-por-entrada.
- **A2:** `OffloadLayout.SHARD_CHARS = 2` é hardcoded → tornar o fan-out de sharding configurável.
- **A3:** eviction/cap de memória só existe no builder do `HybridOffloadStrategy`; o `DiskOffloadStrategy`
  usa `ConcurrentHashMap<K, SoftReference<V>>` sem teto previsível → expor controle de hot-cache via `NMapConfig`.

**Resultado pretendido:** uma nova estratégia log-structured (Bitcask-like) que colapsa 8,1M arquivos
para no máximo ~256 arquivos, sharding (nº de segmentos) configurável 1..128, compressão LZ4 opt-in,
hot-cache com teto previsível, e configuração declarativa via `NMapConfig`.

## Decisões tomadas (confirmadas com o usuário)

1. Escopo = **A1 + A2 + A3** no mesmo ciclo. NQueue fora.
2. O range **1..128 = número de SEGMENTOS** da nova estratégia. Além disso, a `DiskOffloadStrategy`
   atual (file-per-entry) ganha **fan-out de diretório configurável** (A2 literal).
3. Backend A1 = **log-structured append-only estilo Bitcask** (segmentos append-only + índice em memória
   + tombstone + compactação reescrevendo o vivo, reusando o padrão atômico do `CompactionEngine`).
4. **Sem migração**: a nova estratégia vale só para dados novos; não lê/converte o formato file-per-entry.
   Em produção, o mapa atual é repovoado pela fonte de dados.
5. Compressão = **opt-in, LZ4 por registro sobre o value** (reusa `Lz4FrameCompressor` já existente).

---

## A1 — Nova estratégia `SegmentOffloadStrategy<K,V>`

`final`, pacote `dev.nishisan.utils.map`, implementa `NMapOffloadStrategy<K,V>`, `isInherentlyPersistent() = true`
(o NMap não cria WAL para ela — ver `NMap.java:92`).

### Layout em disco
```
<baseDir>/<name>/segment-offload/
├── seg-000.log   ← append-only (registros PUT/TOMBSTONE)
├── seg-000.hint  ← índice persistido p/ startup rápido
├── ... (até seg-<numSegments-1>)
```
Inodes totais = `2 × numSegments` (≤ 256 com 128 segmentos). Temporários de compactação:
`seg-NNN.log.compacting` (mesmo FS, para `ATOMIC_MOVE`).

### Roteamento chave → segmento
Reusar `OffloadLayout.keyHash(key)` (SHA-1, sem colisão prática). `segment = (uint32 dos 8 primeiros
hex do digest) % numSegments` (módulo, pois `numSegments` pode não ser potência de 2). SHA-1 já é uniforme.

### Formato do registro (`.log`) — length-prefixed + magic + CRC
```
RECORD_MAGIC=0x4E4D5347 ("NMSG") | version(1B) | flags(1B: bit0=VALUE_LZ4) | type(1B: 0=PUT,1=TOMBSTONE)
 | keyLen(int) | valLen(int) | keyBytes (Java serialization, SEMPRE em claro)
 | valBytes (Java serialization; LZ4 via Lz4FrameCompressor.wrap se flag) | crc32(int, sobre tudo acima)
```
- **Chave sempre em claro** → permite reconstruir índice/segmento no replay sem o value.
- **Compressão só do value**, por registro (não por bloco — log é append de registros individuais).
- **CRC32 por registro** (`java.util.zip.CRC32`): detecta corrupção interna (o WAL atual só detecta
  truncamento na cauda). Barato.
- TOMBSTONE: grava `keyBytes`, `valLen=0`.

### Índice em memória
```java
record EntryLocation(int segmentId, long offset, int recordLength, int valueLength, boolean compressed) {}
```
Um `ConcurrentHashMap<K, EntryLocation>` **por segmento** (array `[numSegments]`), alinhado ao lock por
segmento. `size()` = soma dos `index[i].size()`. Get faz um único `FileChannel.read` posicional no offset.

### Concorrência
Um `ReentrantReadWriteLock` por segmento. Mutações sob `writeLock(seg)` (append no `FileChannel` próprio
do segmento, posição no fim; atualiza índice/cache/contador de bytes mortos). Get sob `readLock(seg)`.
`clear` adquire todos os write-locks em ordem (padrão `lockAllWrite`/`unlockAllWrite` de
`DiskOffloadStrategy.java:133-146`): trunca `.log` para 0, apaga `.hint`, limpa índice/cache. `destroy`
reusa `DiskOffloadStrategy.deleteDirectoryRecursively` (já `static` package-private, `DiskOffloadStrategy.java:519`).
Iteração: snapshot de chaves por segmento + `get` por chave (padrão de `HybridOffloadStrategy.entrySet`).

### Recovery — hint file + replay incremental (v1)
No `open()`, por segmento:
1. Se `.hint` válido (magic `0x4E4D5348` "NMSH" + CRC do arquivo ok): carrega índice do hint (lê só
   `key + offset + length`, sem desserializar values) — startup rápido.
2. **Replay incremental** da região `[coveredOffset .. tamanho do .log)` (delta gravado após o último
   flush) para capturar puts/tombstones recentes. Último registro de cada chave vence; TOMBSTONE remove.
3. Sem `.hint` (ou corrompido) → **replay puro** do `.log` inteiro e regrava o `.hint`.

`.hint` é regravado no `close()` e ao fim de cada compactação (não a cada put). `.log` é a fonte da verdade.

**Tolerant-truncate na cauda** (reusa filosofia de `NMapPersistence.loadWal`, `NMapPersistence.java:461-508`):
ao varrer, magic divergente / lens fora do range / registro além de `channel.size()` / CRC inválido
→ `channel.truncate(offset)` e para. Descarta registro parcial de crash durante append.

### Compactação / GC (reusa padrão `CompactionEngine`)
- Contabilidade: `AtomicLong deadBytes` por segmento (somado quando put sobrescreve chave existente ou
  TOMBSTONE invalida `EntryLocation` viva). Trigger: `deadBytes / channel.size() >= compactionThreshold`
  (default 0.5; análogo a `CompactionEngine.java:129`).
- Algoritmo 2-fase (`CompactionEngine.runCompactionTask`, `CompactionEngine.java:165-286`):
  1. **Fase 1 (background):** snapshot do índice sob `readLock`; copia para `.compacting` **só os registros
     vivos**, construindo o novo índice (offsets no arquivo novo).
  2. **Fase 2 (finalize sob `writeLock(seg)`):** anexa a cauda gravada após o snapshot; `force(true)` se
     fsync ligado; fecha temp; `Files.move(ATOMIC_MOVE, fallback REPLACE_EXISTING)`; reabre `FileChannel`;
     substitui `index[seg]` pelo novo; zera `deadBytes`; regrava `.hint`.
- Executor single-thread daemon (`nmap-segment-compaction`), acionado assíncrono via `maybeCompact(seg)`
  após mutações quando o trigger dispara. **Não** roda no `open()`. Expor `compactNow(seg)` package-private p/ teste.

### Durabilidade (fsync)
Como `isInherentlyPersistent()=true`, a durabilidade é da própria estratégia. Derivar `fsyncOnWrite` de
`config.mode()`: `ASYNC_WITH_FSYNC` → `channel.force(true)` por append e antes do move na compactação;
`ASYNC_NO_FSYNC`/`DISABLED` → sem fsync por append (move atômico ainda garante consistência estrutural).
Append atômico: registro inteiro num `ByteBuffer`, `while(buf.hasRemaining()) channel.write(buf)`.

### Hot-cache LRU bounded (A3)
`hotCacheMaxEntries` configurável: `LinkedHashMap` LRU (`accessOrder=true`) por segmento, teto
`max(1, hotCacheMaxEntries/numSegments)` (padrão de `HybridOffloadStrategy.java:135,141`). Evict só
descarta da memória (dado já durável no `.log`). `hotCacheMaxEntries=0` → sem cache.

---

## A2 — Fan-out configurável do `OffloadLayout` / `DiskOffloadStrategy`

`OffloadLayout` deixa de ter `SHARD_CHARS` hardcoded (`OffloadLayout.java:39`) e passa a ser instanciável
com `shardDepth` (níveis de diretório) e `shardWidth` (chars por nível). Default `(2,2)` preserva
`hash[0:2]/hash[2:4]/hash.dat` — dados já gravados continuam legíveis.

```java
final class OffloadLayout {
    OffloadLayout(int shardDepth, int shardWidth);   // valida: depth>=0, width>=1, depth*width<=40
    static OffloadLayout defaults();                 // (2,2)
    static String keyHash(Object key);               // permanece static (não depende do layout)
    Path shardedPath(...); Path legacyPath(...); Path preferredExistingPath(...);
    boolean isSharded(...); boolean shouldPrefer(...); void deleteFileQuietly(...);
    static void clearDirectoryContentsRecursively(...);
}
```
`shardedPath` monta `shardDepth` níveis de `shardWidth` chars; `shardDepth=0` → flat.
`preferredExistingPath` mantém fallback sharded→legacy (`OffloadLayout.java:91-98`) — preserva leitura legada.

`DiskOffloadStrategy`: novo construtor `(Path baseDir, String name, int shardDepth, int shardWidth,
int hotCacheMaxEntries)`; manter `(Path, String)` delegando aos defaults para não quebrar
`DiskOffloadStrategy::new` usado em `NMap.java:53`, `NMapOffloadTest.java:48`, `NMapDestroyTest.java:105,126`,
`NMapConcurrentBenchmarkTest.java:67`. Trocar as chamadas estáticas a `OffloadLayout` por `this.layout`.

**Restrição a documentar:** mudar o fan-out de um diretório já populado torna os caminhos antigos órfãos —
o fan-out deve ser fixado na criação do mapa.

---

## A3 — Configuração declarativa no `NMapConfig`

### Novo enum
```java
public enum OffloadMode { IN_MEMORY, DISK, HYBRID, SEGMENT }   // default IN_MEMORY
```

### Novos campos no `NMapConfig.Builder` (+ validações em `validate()`, `NMapConfig.java:118`)
| Campo | Default | Aplica a | Validação |
|---|---|---|---|
| `offloadMode` | `IN_MEMORY` | todos | non-null |
| `numSegments` | `16` | SEGMENT | `1..128` |
| `compressionEnabled` | `false` | SEGMENT | — |
| `hotCacheMaxEntries` | `10_000` | DISK/HYBRID/SEGMENT | `>= 0` |
| `evictionPolicy` | `LRU` | HYBRID | non-null |
| `shardDepth` / `shardWidth` | `2` / `2` | DISK | `depth*width<=40`, `width>=1` |
| `compactionThreshold` | `0.5` | SEGMENT | `0.0 < t <= 1.0` |

### Montagem da estratégia no `NMap` (substitui `NMap.java:85-89`)
```
if (config.offloadStrategyFactory() != null) return factory.create(baseDir, name);  // extension point (prioridade)
switch (config.offloadMode()) {
  IN_MEMORY -> new InMemoryStrategy<>();
  DISK      -> new DiskOffloadStrategy<>(baseDir, name, shardDepth, shardWidth, hotCacheMaxEntries);
  HYBRID    -> HybridOffloadStrategy.builder(...).evictionPolicy(...).maxInMemoryEntries(hotCacheMaxEntries).build();
  SEGMENT   -> new SegmentOffloadStrategy<>(baseDir, name, numSegments, compressionEnabled,
                   hotCacheMaxEntries, compactionThreshold, fsyncFromMode(config.mode()));
}
```
`offloadStrategyFactory` continua como override de maior prioridade (custom strategies) — sem fallback
legado confuso. Preserva todos os testes/usos atuais que setam factory.

### Hot-cache bounded no `DiskOffloadStrategy` (A3 literal)
Substituir `ConcurrentHashMap<K, SoftReference<V>>` (`DiskOffloadStrategy.java:87`) pelo LRU bounded
por stripe, parametrizado por `hotCacheMaxEntries`. Dá teto de memória previsível (vai ao definitivo:
remove o SoftReference, não mantém os dois caminhos).

---

## Arquivos a criar / modificar

**Criar:**
- `nishi-utils-core/src/main/java/dev/nishisan/utils/map/SegmentOffloadStrategy.java` — núcleo log-structured.
- `nishi-utils-core/src/main/java/dev/nishisan/utils/map/SegmentRecord.java` — encode/decode do registro (+CRC, +LZ4).
- `nishi-utils-core/src/main/java/dev/nishisan/utils/map/OffloadMode.java` — enum.
- `nishi-utils-core/src/test/java/dev/nishisan/utils/map/SegmentOffloadStrategyTest.java`
- `nishi-utils-core/src/test/java/dev/nishisan/utils/map/NMapConfigOffloadModeTest.java`
- `doc/diagrams/nmap_segment_offload.puml` — diagrama do offload segmentado.

**Modificar:**
- `OffloadLayout.java` — fan-out configurável (A2).
- `DiskOffloadStrategy.java` — construtor com depth/width/hotCache; LRU bounded; layout por instância.
- `NMapConfig.java` — knobs declarativos + validações (A3).
- `NMap.java` — `buildStrategy` por `OffloadMode` (factory como override).
- `doc/nmap.md` e `README.md` — offloading, config, layout, testes, diagrama (usar URL `doc/diagrams/`).
- Testes que usam `OffloadLayout` estático (`NMapOffloadTest.java:60,65`, `HybridOffloadStrategyTest.java:67,72`)
  → migrar para `OffloadLayout.defaults().shardedPath(...)` (manter `keyHash` static).

## Commits atômicos (granularidade por responsabilidade)
1. **A2** — `OffloadLayout` configurável + construtor do `DiskOffloadStrategy` (+ ajuste dos testes estáticos).
2. **A3-config** — `OffloadMode` + campos/validações no `NMapConfig` + `NMap.buildStrategy` + LRU bounded no `DiskOffloadStrategy`.
3. **A1 core** — `SegmentOffloadStrategy` + `SegmentRecord` (CRUD/tombstone/índice/hot-cache; sem compactação/compressão).
4. **A1 compactação** — GC 2-fase + hint file + recovery/replay incremental.
5. **A1 compressão** — LZ4 opt-in por registro.
6. **Doc** — `nmap.md`, `README.md`, `nmap_segment_offload.puml`.

Testes acompanham cada commit funcional. Sem co-autoria de IA nas mensagens de commit (regra do projeto).
Branch já correta: `feature/ngrrd-python-geometry-dump`? **Não** — criar branch nova `feature/nmap-segment-offload-155`.

## Plano de testes (espelha `NMapOffloadTest` / `HybridOffloadStrategyTest`, `@TempDir`)

**`SegmentOffloadStrategyTest`:** CRUD; update de chave (previous + size estável); restart com `.hint` e
sem `.hint` (replay puro); recovery incremental (delta pós-hint após crash simulado); tombstone+compactação
(`.log` encolhe, `deadBytes` zera, vivos legíveis após reopen); truncated-tail e CRC inválido na cauda
(íntegros sobrevivem); colisão de hashCode "Aa"/"BB"; concorrência 8 threads (+ compactação concorrente);
compressão on/off (igualdade + flag no byte); `numSegments` 1 e 128; validação 0/129 → `IllegalArgumentException`;
hot-cache bounded; `destroy` remove diretório; `forEach`/`keySet`/`entrySet`/`snapshot`; via NMap API
`offloadMode(SEGMENT)` end-to-end + survives restart.

**`NMapConfigOffloadModeTest`:** cada `OffloadMode` produz a estratégia certa (`map.strategy() instanceof ...`,
padrão de `NMapOffloadTest.java:243`); `offloadStrategyFactory` tem prioridade sobre `offloadMode`;
validações (numSegments 1/128 ok, 0/129 falham; `compactionThreshold` fora de (0,1]; `shardWidth=0`; `depth*width>40`).

**Existentes:** ajustar helpers de `OffloadLayout`; novo teste de fan-out custom no `DiskOffloadStrategy`
(depth=1/width=3 grava em `hash[0:3]/hash.dat`); defaults 2×2 não regridem.

Rodar `mvn -pl nishi-utils-core test` antes de cada commit; validação local das mudanças (CI não roda suíte ngrid).

## Riscos e trade-offs
- **Memória do índice (maior risco):** Bitcask mantém o índice todo em heap. ~80–100 B/entrada (sem a chave)
  → **~650–800 MB só de índice para 8,1M chaves**, + as chaves em heap. O índice NÃO guarda value (só offset),
  bem mais leve que manter values, mas exige heap dedicado nessa escala. Documentar como limitação conhecida;
  índice em disco/sparse é trabalho futuro (fora deste ciclo). Ainda assim A1 resolve o problema prioritário
  (inodes/diretórios).
- **Write amplification do log:** updates/removes anexam; chaves quentes inflam o `.log` até a compactação
  (mitigado por `compactionThreshold` + GC background). Melhor que a block amplification atual.
- **Startup:** hint resolve o caso comum; hint defasado/corrompido cai em replay (incremental ou puro).
- **fsync vs throughput:** exposto via `NMapPersistenceMode`, consistente com o resto do NMap.
- **Sem migração:** `SegmentOffloadStrategy` só lê seu formato; `DiskOffloadStrategy` mantém leitura legada (flat+2×2).
- **"Ir ao definitivo":** `NMap.buildStrategy` declarativo limpo (factory único override); LRU substitui
  SoftReference (não coexistem); `OffloadLayout` parametrizado remove o hardcode (mantém só o default 2×2).

## Documentação (DoD do projeto)
- `doc/nmap.md`: componentes (+`SegmentOffloadStrategy`, `OffloadMode`); tabela de config (novos knobs);
  estrutura de arquivos (`segment-offload/seg-NNN.log`+`.hint`); nova seção "Offloading log-structured
  (SEGMENT/Bitcask)" (registro, índice, compactação, recovery, LZ4, durabilidade); testes.
- `README.md`: features + exemplo declarativo `offloadMode(SEGMENT).numSegments(64).compressionEnabled(true)...`.
- `doc/diagrams/nmap_segment_offload.puml`: NMap→estratégia, N segmentos (`.log`+`.hint`), índice K→EntryLocation,
  hot-cache LRU, compaction worker 2-fase (ATOMIC_MOVE), `Lz4FrameCompressor` no value.
- **Atenção:** as URLs de diagrama em `doc/nmap.md` hoje usam `.../main/docs/diagrams/...` (plural) mas os
  arquivos estão em `doc/diagrams/` (singular) → estão quebradas. Usar `doc/diagrams/` no novo asset e
  corrigir as referências existentes do `nmap.md` no commit de doc.

## Verificação end-to-end
1. `mvn -pl nishi-utils-core clean install` (build limpo, Java 21).
2. `mvn -pl nishi-utils-core test` — suíte completa verde (nova + existentes).
3. `mvn test -Dtest=SegmentOffloadStrategyTest` e `-Dtest=NMapConfigOffloadModeTest`.
4. Smoke manual: `NMap.open` com `offloadMode(SEGMENT).numSegments(8)`, inserir 100k entradas, conferir
   que há ≤16 arquivos em `segment-offload/` (vs 100k no file-per-entry), fechar/reabrir e validar leitura;
   forçar removes + `compactNow` e conferir que o `.log` encolhe.
5. Validar render dos `.puml` via `uml.nishisan.dev` sem erro de sintaxe.

## Primeiro passo da execução
Criar branch `feature/nmap-segment-offload-155` e copiar este plano para `/planning/` (regra do projeto).
