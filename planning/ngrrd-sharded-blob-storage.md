# Plano: Sharded Blob Storage para ngrrd (escala 30k+ séries)

## Contexto

Hoje a ngrrd materializa **um arquivo `.ngrr` por série**. Em produção, com 30 mil+
métricas, o processo fica lento por **exaustão de file handles**: o `NgrrdWriter`
(`writer/NgrrdWriter.java:166`) abre um `SeriesChannel` (`RandomAccessFile`+`FileChannel`,
**sem mmap**) por toda a vida do handle, e o `NgrrdReader` abre um canal efêmero por query.
Pior, cada writer sobe **uma thread daemon dedicada por série** (`NgrrdWriter.java:169-175`,
`ngrrd-writer-<seriesKey>`). Resultado: ~30k FDs **+** ~30k threads.

A geometria de uma série é **fixa e pré-alocada na criação** (`format/SeriesGeometry.java`:
`fileTotalBytes()` determinístico; offsets calculáveis via `cellOffset`). Isso torna viável
um **filesystem virtual** em Java: um *volume* com N shards (default **64**) pré-alocados,
onde cada série vira uma **região contígua** dentro de um shard, acessada por random-access
via `MappedByteBuffer` (memória paginada do Linux). FDs caem de ~30k para **64 + nº de
segmentos mmap**, independente da quantidade de séries.

**Insight central:** o conteúdo de um slot no blob é **byte-a-byte idêntico** a um `.ngrr`
standalone (o formato `SeriesFileCodec` permanece intacto). Migrar é, essencialmente, **copiar
os bytes do `.ngrr` para uma região do shard + indexar no catálogo**.

**Resultado pretendido:** absorver o backend na API pública via namespace `ngrrd://<volume>/<path>`,
manter o backend atual (LOCAL_DISK/OBJECT_STORAGE) inalterado, eliminar o gargalo de FDs **e** de
threads, e entregar uma ferramenta Python para migrar diretórios com milhões de `.ngrr` para o blob.

### Decisões aprovadas (usuário)
1. **Namespace** → value type leve `NgrrdUri` (parse de `ngrrd://<volume>/<path>`) + overloads na
   façade `Ngrrd`. **Sem** `FileSystemProvider`/NIO SPI (o literal `Path.of("ngrrd://...")` não
   dispatcha para provider custom de qualquer forma).
2. **Endereçamento** → suportar **ambos**: locator (`<path>` vira o seriesKey direto) **e**
   `tags`+`seriesKeyTemplate` (cliente atual migra trocando só o binding).
3. **Threads** → **incluir** o writer-executor compartilhado (faseado após o storage blob).

### Decisões técnicas internas (arquiteto — não dependem do usuário)
- **Roteamento série→shard:** `floorMod(unsigned64(SHA-256(catalogKey)[0..8]), N)`. Reusa
  `format/DefinitionHash.sha256` (Java) e `hashlib.sha256` (Python) → portável, determinístico,
  bem distribuído. `catalogKey` = a **storage key exata** que o writer passa a `openSeries`, i.e.
  `series/<seriesKey>.ngrr` (= path relativo ao rootDir). Algoritmo+versão gravados no superblock.
- **Catálogo central** (não slot-directory por shard): lookup/`list` O(1)/O(n) em memória, um único
  ponto de atomicidade. Crash-safety via **WAL + snapshot atômico**, espelhando o padrão já validado
  em `core/.../map/NMapPersistence.java` (framing `u32 len|payload`, replay tolerante a cauda truncada,
  `ATOMIC_MOVE`).
- **Alocador por shard:** slab por *size-class* (geometria fixa ⇒ poucos tamanhos distintos) com
  free-list intrusiva + bump cursor. `regionBytes = align4096(fileTotalBytes)`. **O catálogo é a
  fonte de verdade**; o alocador (superblock) é cache **regenerável** no reopen → elimina necessidade
  de atomicidade cross-file.
- **mmap:** cada shard = 1 `FileChannel` + array de `MappedByteBuffer` de **1 GiB** (teto de 2 GiB do
  `MappedByteBuffer`). Regiões **nunca cruzam** fronteira de segmento (pad até o próximo). Hot path usa
  **acessadores absolutos** (`putDouble(idx,v)`/`get(idx)`, sem mutar `position`) ⇒ escritas em regiões
  disjuntas são concorrentes sem lock. `force()` por série usa **`MappedByteBuffer.force(index,length)`**
  (Java 13+, disponível no 21) ⇒ msync só das páginas da própria série.
- **`StorageBindings`** ganha o componente definitivo `blobVolume` (sem path paralelo legado, cf.
  diretriz "ir ao definitivo"); ajustar a única call-site `new StorageBindings(null,null)` em
  `StorageFactoryTest`.
- **Config "geral"** (`ngrrd-blob-base-path`) vive numa camada de **topologia/deployment**
  (registry programático + YAML opcional), **nunca** no YAML de definição da série (preserva o
  invariante de não vazar root físico/credenciais na definição).
- **Backend blob = disco local** (mmap coerente). S3 permanece como está.

---

## Fases de implementação

### Fase 0 — Especificação do formato do volume (contrato Java↔Python)
**Entregável:** `doc/oss/ngrrd-blob-volume.md` (versionado; é o contrato que destrava as duas
implementações) + diagramas PlantUML em `docs/diagrams/` (layout do volume e fluxo de migração),
incorporados via `uml.nishisan.dev` (cf. CLAUDE.md §7).

Especificar **byte-a-byte** (big-endian, magic+version+CRC32 zlib, larguras fixas):
- `volume.meta`: uuid, shardCount N (imutável), segmentBytes, routingAlgorithm+version, generation.
- Superblock do shard (`shard-NN.blob`, 1ª página de 4096B reservada): MAGIC `NGRRBLOB`, version,
  shardId, N, headerBytes, shardCapacityBytes, bumpCursor, tabela de size-classes
  `{regionBytes, freeListHead, freeCount}`, CRC. Free-list intrusiva: 1ª palavra de 16B de cada
  região livre = `{nextFreeOffset u64, regionBytes u64}`.
- Catálogo (`catalog.bin`): header (MAGIC `NGRRCTLG`, version, shardCount, uuid, generation,
  entryCount, CRC) + entradas `{keyLen u16, key UTF-8, shardId u32, regionOffset u64,
  regionBytes u64, state u8, entryCrc u32}` + trailer CRC.
- WAL (`catalog.wal`): frames `{u32 len | NCWA, version, opType(ALLOC/FREE), generation, key,
  shardId, offset, regionBytes}`.

### Fase 1 — Backend blob (Java, storage layer)
**Novo pacote `dev.nishisan.utils.oss.storage.blob`:**
- `BlobStorage implements NgrrdStorage, SeriesChannelProvider, AutoCloseable` — dono do volume;
  `openSeries(key)` faz lookup O(1) no catálogo (canal "unbound" `size()=0` quando a série não
  existe ⇒ dispara `createFresh` no writer); `allocateRegion(key,bytes)` sob `structuralLock`
  (probe de shard cheio + `journal.appendAlloc` + `catalog.put`); `list/get/delete/exists` sobre o
  catálogo. Análogo a `LocalDiskStorage`.
- `BlobSeriesChannel implements SeriesChannel` — visão `(MappedShard, baseOffset, length)`;
  `readRegion/writeRegion` por acessadores absolutos; `force()` = `segment.force(idxBase, len)`
  parcial; `close()` **não** desmapeia (mapping é do shard).
- `MappedShard` — `FileChannel` + `MappedByteBuffer[]` (segmentos de 1 GiB) + superblock em memória.
- `ShardSuperblock` / `CatalogCodec` / `VolumeMetadata` — codecs stateless (espelho da Fase 0).
- `BlobCatalog` (`ConcurrentHashMap<String,CatalogEntry>`) / `CatalogEntry` (record) /
  `CatalogJournal` (WAL append-only + snapshot, modelado em `NMapPersistence`).
- `ShardAllocator` (slab/size-class + bump + free-list; `rebuildFrom(catalog)` no reopen).
- `ShardFullException`/`BlobVolumeException extends NgrrdStorageException`.

**Wiring (aditivo, sem breaking de comportamento):**
- `api/StorageBackendType.java` — adicionar `SHARDED_BLOB` (aliases `shardedBlob`/`sharded_blob`/`blob`)
  no `@JsonCreator from(...)`.
- `storage/StorageFactory.java` — novo braço `case SHARDED_BLOB -> new BlobStorage(requireNonNull(
  bindings.blobVolume(), ...))`; record `StorageBindings` ganha 3º componente `BlobVolume blobVolume`
  + factory `forBlob(BlobVolume)`. Ajustar `StorageFactoryTest`.

Writer/reader (`NgrrdWriter`/`NgrrdReader`) **não mudam** — falam só com `SeriesChannel`/
`SeriesChannelProvider`.

### Fase 2 — Config geral + API pública (Java)
**Novo pacote `dev.nishisan.utils.oss.blob`:**
- `BlobVolume` (interface `AutoCloseable`: `name()`, `basePath()`, `shardCount()`), `BlobVolumeConfig`
  (record: name, basePath, shardCount=64, growthPolicy), `BlobVolumeRegistry` (abre volumes **uma vez**
  por processo, idempotente por nome; `require/lookup/volumes/close`), `NgrrdBlob` (builder
  `registry().basePath(...).volume(...).build()` + `fromYaml(Path topologyYaml)` reusando
  `config/VariableInterpolator` para `${VAR}`).
- `NgrrdUri` — `parse("ngrrd://<volume>/<path>")`/`parse(URI)`/`of(volume,path)`; normaliza o path
  (rejeita `..`/`/` inicial, espelhando `LocalDiskStorage.resolve`); `<path>` vira o seriesKey.

**Façade `Ngrrd.java` (overloads aditivos):**
- `open(BlobVolumeRegistry, NgrrdUri, String yaml [, OpenOptions])` e
  `open(BlobVolume, NgrrdUri, String yaml)` — resolvem o volume, montam `forBlob(volume)` e chamam
  um **novo caminho privado de abertura que recebe o seriesKey já resolvido** (em vez de
  `resolveSeriesKey(template, tags)`). Tudo a jusante (writer/reader/durabilidade/geometria) inalterado.
- **Modo compat:** `fromYaml(..., forBlob(volume), tags, ...)` continua resolvendo via template+tags —
  migração de cliente existente = trocar só o binding.

YAML de topologia (arquivo **separado** do YAML de série):
```yaml
ngrrd-blob-base-path: ${NGRRD_BLOB_BASE:/var/ngrrd/blobs}
defaults: { shardCount: 64 }
volumes:
  - name: ifaceStats            # -> /var/ngrrd/blobs/ifaceStats
  - name: flows
    basePath: /data/flows
    shardCount: 128
```

### Fase 3 — Ferramenta de migração (Python)
**Estender `python/ngrrd-python/`** (não criar pacote novo — reusa o parser binário, evita drift;
runtime stdlib-only). Novo subpacote `src/ngrrd_python/blob/`:
- `spec.py` (structs do superblock/catálogo — espelho da Fase 0), `routing.py` (SHA-256 mod N),
  `volume.py` (cria/abre shards + superblock + fsync), `catalog.py` (escrita binária + checkpoint/resume
  + índice de idempotência), `walk.py` (gera `series/<key>.ngrr` relativo ao rootDir, lazy via
  `os.scandir` — derivação do catalogKey **idêntica** ao Java), `validate.py` (reusa `_parse_header` de
  `reader.py`: MAGIC/version/CRC[0..92)/`file_total_bytes==file_size`), `migrate.py` (orquestra),
  `verify.py` (paridade byte-a-byte slot↔origem + contagens + distribuição), `cli.py`.
- `pyproject.toml`: novo entry point `ngrrd-blob-migrate = "ngrrd_python.blob.cli:main"`.
- Refatorar: extrair header-decode reutilizável de `reader.py`; promover `build_ngrr_image` de
  `tests/test_reader.py` para um helper compartilhado de testes.

**CLI:** `migrate <source-root> <target-volume> [--series-prefix series] [--shards 64] [--workers N]
[--dry-run] [--resume] [--force] [--strict] [--keep-source(on)] [--verify] [--json]` e
`verify <volume> [--source-root ...]` / `inspect <volume>`. Robustez para milhões de arquivos:
iterador lazy; **workers particionados por shard** (`multiprocessing`, sem contenção no catálogo);
resumibilidade via catálogo; ordem **slot→fsync→catálogo→fsync** (catálogo nunca referencia bytes não
duráveis); idempotência estilo `verifyOrReplaceIfIdentical`; `--keep-source` default (deleção só
pós-verify, por série, sob flag explícita). **Invariante crítico:** migração roda **offline** (sem
writer Java ativo); CLI emite aviso/soft-guard.

### Fase 4 — Writer-executor compartilhado (Java)
Substituir (modelo **definitivo**, sem caminho legado paralelo) o `Executors.newSingleThreadExecutor`
por-série de `NgrrdWriter.java:169` por um **scheduler compartilhado** (pool limitado, default ~`nCPU`):
- Cada série mantém sua fila de comandos; o scheduler atribui filas não-vazias a workers com
  **afinidade por hash da série**, garantindo **no máximo um worker por série a qualquer instante**
  ⇒ preserva ordem total por série e o contrato 1-writer/N-readers + `ReadWriteLock` por handle.
- `flush()/checkpoint()` continuam síncronos (drenam a fila da série). `force()` segue fora do lock.
- Resolve a escala de threads (30k handles ⇒ ~`nCPU` threads em vez de 30k).

### Fase 5 — Testes cross-language + documentação
**Java (em `nishi-utils-oss/src/test/.../storage/blob/` e `.../blob/`):** `BlobVolumeRoundTripTest`,
`BlobVolumeReopenTest`, `BlobCatalogCrashRecoveryTest` (cauda truncada/CRC ruim), `BlobShardConcurrencyTest`
(escritas disjuntas no mesmo shard), `BlobSlotParityTest` (slot == `.ngrr` standalone byte-a-byte),
`BlobRoutingVectorsTest`, `NgrrdUriTest`, e teste do writer-scheduler (ordem por série + durabilidade).
**Python:** `test_blob_spec/routing/walk/migrate/resume/verify/cli`.
**Cross-language (decisivos), como `*IT.java` que invocam o Python via `ProcessBuilder`, sob novo profile
`-Pblob-crosslang`:** `PythonMigratesJavaReadsIT` (Python migra → Java abre o volume e lê os mesmos
pontos), `JavaWritesPythonVerifiesIT` (Java cria volume → Python valida catálogo/headers),
`RoutingParityIT` (vetor `blob/routing-vectors.tsv` compartilhado e regenerado a partir do Java),
`GoldenCatalogParityIT` (golden volume decodificado nos dois lados).
**Docs (DoD CLAUDE.md §7):** atualizar `doc/oss/ngrrd.md` (seção do backend blob + escala), finalizar
`doc/oss/ngrrd-blob-volume.md`, diagramas PlantUML. Atualizar `README` do `ngrrd-python` (o reader
permanece read-only; `blob/` escreve volumes). Copiar este plano aprovado para `/planning`.

---

## Roadmap futuro (fora deste escopo)
- **`FileSystemProvider` NIO real (scheme `ngrrd://`)** — interop genérica com `Paths.get(URI)`/`Files.*`
  para ferramentas externas. Adicionável depois como **adaptador fino** sobre o mesmo
  `BlobVolumeRegistry`; a escolha do `NgrrdUri` (Fase 2) **não** o impede. **Adiado a pedido do usuário.**
- **Resharding (mudar N)** — N é imutável por volume; ferramenta offline que cria volume novo e recopia.
- **Compactor offline** — fecha buracos do alocador após mudança de geometria e reduz capacidade.
- **Backend blob sobre object storage** — hoje o blob é exclusivo de disco local (mmap coerente).

---

## Arquivos críticos
**Modificar:**
- `nishi-utils-oss/src/main/java/dev/nishisan/utils/oss/api/StorageBackendType.java` — enum + alias.
- `.../storage/StorageFactory.java` — switch `SHARDED_BLOB` + `StorageBindings.blobVolume`/`forBlob`.
- `.../Ngrrd.java` — overloads `open(...)` + caminho privado com seriesKey pré-resolvido.
- `.../writer/NgrrdWriter.java` — scheduler compartilhado (Fase 4).
- `python/ngrrd-python/pyproject.toml` + `src/ngrrd_python/reader.py` + `tests/test_reader.py` (refactor).
- `doc/oss/ngrrd.md`; `nishi-utils-oss/pom.xml` (profile `blob-crosslang`); `StorageFactoryTest`.

**Criar:** pacotes `.../storage/blob/` e `.../blob/` (Java, Fases 1–2); `src/ngrrd_python/blob/` (Fase 3);
`doc/oss/ngrrd-blob-volume.md` + diagramas (Fase 0/5); testes das Fases 5.

**Reusar (não reinventar):** `format/SeriesFileCodec`/`SeriesGeometry`/`DefinitionHash` (formato intacto),
`core/.../map/NMapPersistence` (padrão WAL+snapshot), `config/VariableInterpolator` (`${VAR}`),
`storage/StorageKey` (catalogKey), `reader.py:_parse_header` + `test_reader.py:build_ngrr_image` (Python).

---

## Verificação (end-to-end)
1. **Build/unit:** `mvn -pl nishi-utils-oss clean install` e `mvn -pl nishi-utils-oss test`
   (inclui blob round-trip, reopen, crash-recovery, slot-parity, routing vectors, NgrrdUri, scheduler).
2. **Python:** `cd python/ngrrd-python && pip install -e ".[dev]" && pytest`.
3. **Cross-language:** `mvn -pl nishi-utils-oss verify -Pblob-crosslang` (requer `python3` + wheel
   instalado): valida Python↔Java nas duas direções + paridade de roteamento/catálogo.
4. **Cenário manual de escala:** gerar uma árvore de N `.ngrr` (via geometria de teste, ex.
   `iface-traffic-local-disk.yaml`) → `ngrrd-blob-migrate migrate <root> <volume> --verify` →
   abrir cada série via `Ngrrd.open(registry, NgrrdUri.parse("ngrrd://<vol>/<key>"), yaml)` e conferir
   que os pontos lidos batem com o `.ngrr` de origem; confirmar que o processo mantém **poucos FDs**
   (`ls /proc/<pid>/fd | wc -l` ~ 64+segmentos) e ~`nCPU` threads para milhares de séries abertas.
5. **Regressão:** suíte completa nos backends existentes (LOCAL_DISK/OBJECT_STORAGE) inalterada;
   `mvn -pl nishi-utils-oss verify -Pngrrd-integration` (LocalStack) verde.
