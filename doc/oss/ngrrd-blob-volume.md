# ngrrd Blob Volume — especificação de formato (v1)

> **Status:** contrato normativo compartilhado entre a implementação Java
> (`dev.nishisan.utils.oss.storage.blob`) e a ferramenta de migração Python
> (`ngrrd_python.blob`). Qualquer mudança no layout binário exige **bump de versão**
> e atualização simultânea dos dois lados + dos testes cross-language.

## 1. Motivação

Um *blob volume* é um "filesystem virtual" que substitui o esquema de **um arquivo
`.ngrr` por série** por um conjunto fixo de **shards** pré-alocados. Cada série
(uma imagem `.ngrr` de tamanho fixo) passa a ser uma **região contígua** dentro de
um shard, acessada por random-access via `MappedByteBuffer` (memória paginada).
Objetivo: com 30k+ séries, manter **64 file handles + poucos mmap segments** em vez
de dezenas de milhares de FDs.

**Invariante-chave:** o conteúdo de uma região é **byte-a-byte idêntico** a um
arquivo `.ngrr` standalone (formato `SeriesFileCodec`, inalterado). Migrar = copiar
os bytes do `.ngrr` para uma região + indexar no catálogo.

## Diagramas

Layout físico do volume:

![Layout do blob volume](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/oss/diagrams/blob_volume_layout.puml)

Fluxo de migração (Python) + leitura (Java):

![Migração e leitura](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/oss/diagrams/blob_migration_sequence.puml)

## 2. Convenções

- **Endianness:** big-endian em **tudo** (paridade com o `.ngrr`/`SeriesFileCodec`).
- **CRC:** CRC-32 (polinômio IEEE 802.3, igual a `java.util.zip.CRC32` e
  `zlib.crc32`), sempre como `u32` sobre um intervalo de bytes explícito.
- **MAGIC:** 8 bytes ASCII por estrutura de volume (distingue dos 4 bytes `"NGRR"`
  do arquivo de série).
- **Alinhamento:** `PAGE = 4096`. `align(x) = ceil(x / 4096) * 4096`.
- **Tipos:** `u8, u16, u32, u64` inteiros sem sinal; offsets/comprimentos são `u64`.

## 3. Layout de diretório

```
<base>/<volume>/
  volume.meta            # metadados do volume (§4)
  catalog.bin            # catálogo: seriesKey -> (shard, offset, len, state) (§6)
  catalog.bin.tmp        # staging do atomic-replace do snapshot do catálogo
  catalog.wal            # journal append-only desde o último snapshot (§7)
  catalog.wal.old        # rotação incompleta (replay no load; padrão NMapPersistence)
  shard-00.blob          # shard 0 (superblock §5 + data region)
  shard-01.blob
  ...
  shard-<NN>.blob        # shard N-1
```

`<base>` = `ngrrd-blob-base-path` (config geral). `<volume>` = nome lógico do
namespace (ex.: `ifaceStats`). O nome do shard é `shard-%02d.blob` quando `N <= 100`
e `shard-%0<w>d.blob` com `w = len(str(N-1))` caso contrário (zero-padded, base 10).

## 4. `volume.meta` (56 bytes)

| off | tipo    | campo               | notas                                   |
|----:|---------|---------------------|-----------------------------------------|
| 0   | u8[8]   | MAGIC               | `"NGRRVOL1"` (0x4E 47 52 52 56 4F 4C 31)|
| 8   | u16     | formatVersion       | = 1                                     |
| 10  | u16     | reserved            | 0                                       |
| 12  | u32     | shardCount N        | **imutável** após criação               |
| 16  | u8[16]  | volumeUuid          | igual em todos os shards/catálogo       |
| 32  | u64     | segmentBytes        | tamanho do segmento mmap (default 1 GiB)|
| 40  | u16     | routingAlgorithm    | = 1 (`SHA256_PREFIX64`, §8)             |
| 42  | u16     | routingVersion      | = 1                                     |
| 44  | u64     | generation          | monotônica; bump a cada mutação estrutural |
| 52  | u32     | metaCrc32           | CRC sobre `[0..52)`                      |

## 5. Superblock do shard (`shard-NN.blob`, primeira página = 4096 bytes)

A primeira `PAGE` de cada shard é reservada ao superblock; a *data region* começa
em `headerBytes = 4096`.

| off  | tipo   | campo              | notas                                    |
|-----:|--------|--------------------|------------------------------------------|
| 0    | u8[8]  | MAGIC              | `"NGRRBLOB"`                             |
| 8    | u16    | formatVersion      | = 1                                      |
| 10   | u16    | reserved           | 0                                        |
| 12   | u32    | shardId            | 0..N-1                                   |
| 16   | u32    | shardCount N       | validação cruzada com `volume.meta`      |
| 20   | u32    | reserved           | 0                                        |
| 24   | u64    | headerBytes        | = 4096                                   |
| 32   | u64    | shardCapacityBytes | tamanho atual do arquivo-shard (≥ 4096)  |
| 40   | u64    | bumpCursor         | **hint**; valor autoritativo é rederivado do catálogo (§9) |
| 48   | u8[16] | volumeUuid         | = `volume.meta.volumeUuid`               |
| 64   | u64    | generation         | última escrita estrutural neste shard    |
| 72   | u8[…]  | reserved           | zeros até o offset 4092                   |
| 4092 | u32    | superblockCrc32    | CRC sobre `[0..4092)`                     |

**Data region:** começa em `4096`. As regiões são **page-aligned** (offset e
tamanho múltiplos de 4096) e **nunca cruzam fronteira de segmento** (`segmentBytes`):
ao alocar por bump, se `floor(off / segmentBytes) != floor((off+regionBytes-1) / segmentBytes)`,
o cursor avança para o início do próximo segmento (o gap de padding é abandonado —
desprezível, pois `regionBytes << segmentBytes`).

## 6. Catálogo — snapshot (`catalog.bin`)

**Header (64 bytes):**

| off | tipo   | campo          | notas                          |
|----:|--------|----------------|--------------------------------|
| 0   | u8[8]  | MAGIC          | `"NGRRCTLG"`                   |
| 8   | u16    | formatVersion  | = 1                            |
| 10  | u16    | reserved       | 0                              |
| 12  | u32    | shardCount N   | validação cruzada              |
| 16  | u8[16] | volumeUuid     | validação cruzada              |
| 32  | u64    | generation     | gen do snapshot                |
| 40  | u32    | entryCount E   | nº de entradas que seguem      |
| 44  | u8[16] | reserved       | 0                              |
| 60  | u32    | headerCrc32    | CRC sobre `[0..60)`            |

**Entradas (E registros, tamanho variável):** cada entrada começa logo após o header.

| campo        | tipo        | notas                                            |
|--------------|-------------|--------------------------------------------------|
| keyLen       | u16         | comprimento em bytes da chave UTF-8 (≤ 65535)    |
| key          | u8[keyLen]  | catalogKey UTF-8 = `series/<seriesKey>.ngrr` (§8)|
| shardId      | u32         | shard onde a região reside (autoritativo)        |
| regionOffset | u64         | offset absoluto na data region do shard (≥ 4096) |
| regionBytes  | u64         | tamanho da região física (slot) = `align(objectBytes)` |
| objectBytes  | u64         | tamanho lógico do objeto (= `fileTotalBytes` da série); `get` devolve exatamente estes bytes |
| state        | u8          | 1 = LIVE, 2 = DELETED (reservado; v1 só persiste LIVE, §9) |
| entryCrc32   | u32         | CRC sobre os bytes da entrada `[keyLen..state]`  |

**Trailer:** `u32 catalogCrc32` = CRC sobre **toda a região de entradas**
(`bytes[64 .. fileLen-4)`).

> O snapshot v1 contém **apenas** entradas `LIVE` (§9) — `bumpCursor` por shard
> = maior `regionOffset+regionBytes`. O estado `DELETED` é reservado para um
> futuro compactor.

## 7. Catálogo — journal (`catalog.wal`)

Append-only; cada mutação estrutural (ALLOC/FREE) é gravada **antes** de ser
publicada em memória. Framing idêntico a `NMapPersistence`:

```
record := u32 frameLen | payload[frameLen]
payload :=
  u8[4]      ENTRY_MAGIC = "NCWA"
  u16        entryVersion = 1
  u8         opType        (1=ALLOC, 2=FREE)
  u8         reserved
  u64        generation
  u16        keyLen
  u8[keyLen] key
  u32        shardId
  u64        regionOffset
  u64        regionBytes
  u64        objectBytes
  u32        payloadCrc32  (CRC sobre payload[0 .. len-4))
```

**Replay:** lê registros sequencialmente; uma cauda truncada (bytes insuficientes
para `frameLen` ou para o payload completo) é **descartada via truncate** e encerra
o replay (igual a `NMapPersistence.loadWal`). CRC inválido aborta o replay daquele
registro em diante (cauda corrompida).

**Snapshot/rotação:** escreve `catalog.bin.tmp` → `ATOMIC_MOVE` para `catalog.bin`
→ renomeia `catalog.wal`→`catalog.wal.old` e cria `catalog.wal` vazio → remove
`catalog.wal.old`. No load: se `catalog.wal.old` existir, replaya-o antes de
`catalog.wal`.

## 8. Roteamento série → shard (`SHA256_PREFIX64`, v1)

```
catalogKey = storage key exata passada a openSeries = "series/<seriesKey>.ngrr"
             (= path relativo ao rootDir na árvore .ngrr de origem)
digest     = SHA-256(catalogKey em UTF-8)            # 32 bytes
h64        = uint64 big-endian dos 8 primeiros bytes de digest
preferred  = h64 mod N                                # unsigned
```

- **Java:** `Long.remainderUnsigned(ByteBuffer.wrap(digest).getLong(), N)`.
- **Python:** `int.from_bytes(hashlib.sha256(key.encode("utf-8")).digest()[:8], "big") % N`.

`preferred` é apenas o shard **preferencial** na criação. Se ele estiver cheio,
aplica-se *linear probing* determinístico `(preferred + p) mod N`, `p = 1..N-1`. A
localização **efetiva** é gravada no catálogo (`shardId`), que é a fonte de verdade
em qualquer leitura/reabertura. Numa migração para volume vazio não há probing
relevante (há espaço); Java e Python concordam no `preferred` para distribuição
consistente de alocações futuras.

## 9. Modelo de verdade e reconstrução do alocador

- **O catálogo é a fonte de verdade** de `seriesKey → (shardId, offset, len)`.
  O snapshot v1 persiste **apenas entradas `LIVE`**.
- **O alocador (bumpCursor + free-lists por size-class) é rederivado do catálogo
  na reabertura**, não persistido como verdade. O superblock guarda `bumpCursor`
  apenas como hint e `shardCapacityBytes` como dado durável (reflete `setLength`).
- **Reconstrução por shard, no open:** `bumpCursor = max(offset + regionBytes)`
  sobre as entradas `LIVE` do shard (`= headerBytes` se nenhuma); free-lists
  **vazias**.
- **Free dentro da sessão:** `delete`/`atomicReplace` que liberam uma região
  empilham o slot num free-list **em memória** (reaproveitado pela próxima
  alocação da mesma size-class na mesma sessão). Após reinício, free-lists
  começam vazias: regiões liberadas no topo são reabsorvidas pelo `bumpCursor`
  (que cai para o high-water das `LIVE`), e regiões liberadas no interior ficam
  **leaked** até um compactor offline (deletes são raros em séries temporais).
- Isso elimina qualquer necessidade de atomicidade cross-file: só o catálogo
  (WAL+snapshot) precisa ser durável-atômico; superblock e free-lists são regeneráveis.

**Crescimento de shard:** `setLength(newCapacity)` + `force` do arquivo → atualiza
`shardCapacityBytes` no superblock (msync) → só então a alocação é jornalizada e
publicada. `setLength` é idempotente; um crash entre grow e journal apenas deixa
espaço não usado (recuperado pela próxima alocação).

## 10. Durabilidade e concorrência

- **Hot path (escrita de célula):** não toca catálogo nem lock estrutural. Escritas
  de séries distintas atingem **regiões disjuntas** do mesmo `MappedByteBuffer` e
  usam acessadores absolutos (`putDouble(index,…)`/`get(index)`), logo são
  concorrentes sem corrida de `position`.
- **`force()` por série:** `MappedByteBuffer.force(index, length)` (Java 13+)
  sincroniza só as páginas da própria região.
- **Mutação estrutural (create/delete/grow):** serializada por um `structuralLock`
  por volume; rara; fora do hot path. Ordem: **journal (durável) → publica em
  memória → atualiza superblock (cache)**.
- **Migração offline:** a ferramenta Python deve rodar **sem writer Java ativo**
  sobre as mesmas séries (janela de manutenção). Único escritor dos shards durante
  a migração são os próprios workers, particionados por shard.

## 11. Versionamento

Toda estrutura carrega `MAGIC + formatVersion`. Um leitor que encontre
`formatVersion` desconhecido **falha explicitamente** (nunca interpreta às cegas).
Mudança incompatível ⇒ `formatVersion = 2` + migração documentada.
