# ngrrd: evolução do storage para objeto único estilo RRDtool (#144)

## Contexto

O `nishi-utils-oss` (formato **ngrrd**, pacote `dev.nishisan.utils.oss`) persiste hoje
**um objeto por `(rra, ds, cf, bloco)`** sob `agg/<seriesKey>/<rra>__<ds>__<cf>/<stepSec>/<blockStart>.ngrrd`,
mais um **manifesto YAML versionado** como fonte de verdade. A persistência incremental
(5.3.0) tornou os blocos `agg/` reais no `ngrrd-consumer` e, com isso, **escancarou o problema da #144**:
em disco local o produto cartesiano `rra × ds × cf × janela` vira uma explosão de arquivos
minúsculos (dezenas a centenas de milhões em steady-state) — inviável por inodes, listagem e backup.
A retenção hoje é feita deletando blocos antigos, perdendo a propriedade de ring buffer de tamanho fixo do RRD.

**Objetivo:** reescrever o storage para **um único objeto binário por série**, igual em espírito ao
arquivo `.rrd` do RRDtool: header de geometria + dicionário de DS + defs de archives (`rra × cf`)
+ live-state (`last_up`/`last_ds`/acumuladores) + **ring buffers in-place** de todos os `(archive × ds)`,
pré-alocado em tamanho fixo determinístico. Isso elimina o manifesto, troca retenção-por-delete por
**sobrescrita no ring** e mata a explosão de arquivos.

**Decisões já tomadas com o usuário:**
1. **Escrita:** in-place no disco local (`FileChannel`, grava só regiões alteradas, arquivo pré-alocado) + PUT do objeto inteiro no S3. Mesmo formato binário nos dois backends.
2. **Legado:** o objeto único vira o **único** formato — remover blocos+manifesto totalmente, sem fallback selecionável (diretriz "ir sempre à frente").
3. **Migração:** **clean cut + bump major** (6.0.0). Sem migração in-place; dados antigos em blocos são abandonados e o ring repovoa pela ingestão corrente.

## Reuso (engine pura — não muda)

`engine/CounterDeriver`, `engine/PrimaryDataPoint` (com `Memento`/`snapshot`/`restore`/`consolidate`),
`engine/RraConsolidator` (xff + AVERAGE/MAX/MIN/LAST), `engine/BestFitSelector.pick`,
`engine/TimeBucket.alignDown`. O modelo worker-thread + `LinkedBlockingQueue` + `Command` (Write/Flush/Checkpoint/Shutdown) do `NgrrdWriter` é mantido; só o corpo muda.

## Formato binário da série ("NGRR", big-endian)

Geometria derivada da `NgrrdDefinition` (determinística):
- `cols[0..D-1]` = nomes de DS derivados, na ordem de `spec.dataSources()` filtrada por `archives.appliesTo` (ordem canônica das colunas).
- `rawCols[0..R-1]` = DS raw que alimentam derive (para counterPrev/`last_ds`).
- `arch[0..A-1]` = pares `(rra, cf)` achatados na ordem de declaração; `A = Σ rra.cf.size()`. Cada archive tem ring de `rows × D` doubles. `groupSize[a] = arch[a].stepSec / baseStepSec`.

```
=== SEÇÃO ESTÁTICA (imutável após create) ===
0    MAGIC "NGRR" (4) | 4 FORMAT_VERSION u16 | 6 FLAGS u16 | 8 baseStepSec i32
12   definitionHash SHA-256 (32) | 44 D i32 | 48 R i32 | 52 A i32
56   staticSectionBytes i64 | 64 liveStateOffset i64 | 72 liveStateBytes i64
80   ringDataOffset i64 (8-align) | 88 fileTotalBytes i64 | 96 headerCrc32 i32  (CRC sobre [0..96))
104  DS_DICT: por coluna { nameLen u16, name utf8, originRawLen u16, originRaw utf8, dsType u8 }
     ARCH_TABLE: por archive { rraNameLen u16, rraName utf8, cf u8, stepSec i32, rows i32, xff f64 }
     (pad até liveStateOffset, 8-align)

=== LIVE-STATE (tamanho fixo, sobrescrito in-place) ===
lastUpEpochMs i64
R × counterPrev { prevValue f64 (NaN=none), prevTsEpochMs i64 }                  -- last_ds
D × pdpEmProgresso { sum f64,count i32,min f64,max f64,last f64,missing i32, slotSec i64 }
A × ringPtr { curRow i32, curRowEpochSec i64 }                                   -- -1 = vazio
A*D × cdpEmProgresso { cdpPartial f64, foldedCount i32, missingCount i32 }       -- (a,i) row-major
liveStateCrc32 i32   (CRC sobre a live-state anterior; recomputado a cada force)

=== RING-DATA (8-align, sobrescrito in-place) ===
por archive a: rows × D doubles, row-major (row,col); NaN = missing
cell(a,row,col) = ringDataOffset + Σ_{b<a}(rows_b·D·8) + ((row·D)+col)·8
```

Índice do ring (RRD-true): `curRow` = CDP mais novo; `tsSec(row) = curRowEpochSec - ((curRow-row+rows) % rows)·stepSec`. Wrap sobrescreve o mais antigo → retenção = `rows×stepSec`, **sem deletes**.

**CRC/durabilidade:** sem CRC de arquivo inteiro (inviabilizaria in-place). `headerCrc32` (estático, uma vez) + `liveStateCrc32` (pequeno, regravado a cada `force()`). Sem CRC por linha de ring: confia em (a) pré-alocação, (b) ordem grava-ring → force → grava-live-state (ponteiro nunca avança além de dados sincronizados), (c) sentinela NaN. Se `liveStateCrc32` divergir no open: zera acumuladores em progresso e counterPrev, **mantém os rings** (equivalente a "perdeu o PDP corrente" do RRD).

## Algoritmo do writer (RRD-true, contínuo)

Sem `blockSizeSec`/rollover. Por amostra (`handleWrite`):
1. Deriva via `CounterDeriver` (atualiza counterPrev/`last_ds`), atualiza `lastUpEpochMs`; se derivado NaN, retorna (counterPrev já atualizado).
2. `i = colIndex(derivedName)`; alinha `slotSec = alignDown(tsSec, baseStepSec)`. Se `slotSec` avançou: `finalizeBaseStep(i)` do PDP completo (preenchendo PDPs missing dos slots pulados — gap fill), reset do PDP. Se retrocedeu: `onLateSample` e drop.
3. `pdp[i].add(derived)`.

`finalizeBaseStep(i)`: para cada archive `a`, consolida o PDP (`consolidate(cf)`) e funde no acumulador de CDP `cdp[a][i]` (AVERAGE→sum+observedCount; MAX/MIN→running; LAST→último; conta missing). Quando `foldedCount==groupSize[a]`: finaliza com **semântica idêntica ao `RraConsolidator`** (`missingRatio > xff → NaN`), emite o CDP no ring (`curRow=(curRow+1)%rows`, atualiza `curRowEpochSec`, `writeRegion` da célula imediatamente), reseta o acumulador. Gaps grandes (> `rows·stepSec`) reancoram o ring em vez de girar `rows` vezes.

`checkpoint()` = `flush()`: regrava live-state (com `liveStateCrc32`) + células de ring pendentes, depois `force()` (fsync no disco / PUT no S3). Garante leitura do que já consolidou antes do próximo passo (PDP/CDP em progresso **não** ficam visíveis ao reader — paridade RRD). `close()`: persist + force + close. Sem `ManifestUpdater`.

Open: computa geometria; se ausente → `allocate(fileTotalBytes)` + grava estática + live-state vazio + rings NaN + force; se presente → valida MAGIC/version/headerCrc; se `definitionHash` mudou → recria (clean cut, WARN); senão hidrata live-state (counterPrev + acumuladores + ponteiros).

## Abstração de canal (in-place vs whole-PUT)

Interface de capacidade nova, sem poluir `NgrrdStorage` (que mantém get/put/list/atomicReplace para o YAML de schema):

```java
interface SeriesChannel extends AutoCloseable {
    long size(); void allocate(long totalBytes);
    byte[] readRegion(long offset, int len); void writeRegion(long offset, byte[] data);
    void force(); void close();
}
interface SeriesChannelProvider { SeriesChannel openSeries(String key); boolean seriesExists(String key); }
```

- `LocalDiskStorage implements ...SeriesChannelProvider`: `FileChannel(READ,WRITE,CREATE)`, `allocate=truncate/extend`, region read/write por `position`, `force=channel.force(true)`. In-place real (sem tmp+rename para a série).
- `S3Storage implements ...SeriesChannelProvider`: imagem `byte[]` em memória (get no open), region writes na imagem + dirty flag, `force/close = put(key, image)` (read-modify-write do objeto inteiro).
- `StorageKey.series(naming, seriesKey)` → `{seriesPrefix}/{seriesKey}.ngrr`. Invariante: **um writer por série** (corrida de RMW no S3 documentada; opcional endurecer com PUT condicional If-Match no futuro).

## Reader

Reescrever `NgrrdReader`: `BestFitSelector.pick` escolhe a RRA; mapeia `(rraName, query.cf())`→índice de archive; abre `SeriesChannel` próprio (snapshot read-only); lê header+live-state+ring da coluna `i`; reconstrói `(epochMs,value)` por `curRow/curRowEpochSec/stepSec`; recorta `[start,end)`; `downsample` (algoritmo atual inalterado). `SeriesResult` com a mesma forma. `ViewExecutor`/presets inalterados na API. Sem manifesto, sem fetch por bloco.

## Arquivos

**Novos** (em `nishi-utils-oss/src/main/java/dev/nishisan/utils/oss/`):
`writer/SeriesGeometry.java`, `format/SeriesHeader.java`, `format/SeriesLiveState.java`,
`format/SeriesFileCodec.java`, `format/DefinitionHash.java` (move `computeDefinitionHash` do `ManifestCodec`),
`storage/SeriesChannel.java`, `storage/SeriesChannelProvider.java`.

**Reescritos:** `writer/NgrrdWriter.java`, `reader/NgrrdReader.java`,
`storage/LocalDiskStorage.java`, `storage/S3Storage.java`, `storage/StorageKey.java`,
`Ngrrd.java` (`DefaultHandle` sem updater; remove `persistedBlocks()` da API do writer),
`config/NgrrdDefinitionValidator.java` (remove checks de `blockSizeSec`),
`definition/TimeSpec.java` (remove `blockSizeSec`), `definition/ObjectNaming.java`
(remove raw/agg/manifest/state prefixes; add `seriesPrefix` default `"series"`),
`definition/StorageSpec.java` (remove `manifestPolicy`), `definition/WritePolicy.java`
(remove `persistenceMode`/`persistLastValue`).

**Removidos:** `writer/{ManifestUpdater,BlockWindow,SeriesWriterState,SeriesStateCodec,PersistedBlock}.java`,
`format/{NgrrdManifest,RraManifest,ManifestBlock,ManifestCodec,BlockHeader,BlockCodec,EncodedBlock}.java`,
`definition/ManifestPolicy.java`, `api/{ManifestUpdateMode,PersistenceMode}.java`.

**POMs (5.3.0 → 6.0.0):** `pom.xml:9`, e `<parent>` em `nishi-utils-core/pom.xml:10`, `nishi-utils-oss/pom.xml:10`, `ngrid-test/pom.xml:10`.

**Recursos/docs:** `nishi-utils-oss/src/test/resources/iface-traffic-errors-v1.yaml` e `iface-traffic-local-disk.yaml` (novo schema), `doc/oss/ngrrd.md` (layout binário/físico, tamanho de arquivo, tabela "Paridade com RRDtool", exemplos sem `blockSizeSec`/manifest/persistenceMode), `doc/diagrams/ngrrd_{write_flow,read_flow,topology}.puml`.

## Fases de execução (commits atômicos)

0. Bump 6.0.0 nos 4 POMs + atualizar YAML de teste para o novo schema.
1. `SeriesGeometry`, `SeriesHeader`, `SeriesLiveState`, `SeriesFileCodec`, `DefinitionHash` + `SeriesFileCodecTest`.
2. `SeriesChannel`/`SeriesChannelProvider` em `LocalDiskStorage` e `S3Storage`; `StorageKey.series` + testes de canal/paridade.
3. Reescrever `NgrrdWriter` (máquina de estados); deletar `BlockWindow`/`SeriesWriterState`/`SeriesStateCodec`/`PersistedBlock`/`ManifestUpdater` + testes do writer.
4. Reescrever `NgrrdReader`; manter `ViewExecutor` + testes do reader.
5. Limpeza de schema/definition + deletar `format/Block*`/`format/Manifest*`/`EncodedBlock`/`ManifestPolicy`/`ManifestUpdateMode`/`PersistenceMode`.
6. `Ngrrd`/`DefaultHandle` rewire.
7. Corrigir/reescrever testes afetados; rodar a suíte.
8. Docs + diagramas PlantUML.

## Testes

**Deletar:** `format/BlockCodecTest`, `format/BlockHeaderTest`, `format/ManifestCodecTest` (→ `DefinitionHashTest`).
**Reescrever:** `writer/NgrrdWriterIncrementalTest` (round-trip do SeriesFileCodec; reabertura reidrata counterPrev+CDP parcial; checkpoint legível antes do passo do ring — manter `EXPECTED_BPS`), `writer/NgrrdRetentionTest` (wraparound do ring: `rows=3`, ingere 5 janelas, sobrevivem só as 3 mais novas — via reader), `writer/NgrrdWriterE2ETest`, `NgrrdFacadeTest` (1 `.ngrr` por série), `storage/StorageKeyTest`, `IfaceTrafficSmokeIT` (mantém semântica 6/4 DS + reset; assert "exatamente 1 `.ngrr` por série" prova a #144).
**Novos:** `SeriesInPlaceTest` (region write/readback nos offsets), `RingWraparoundTest`, `ReopenRehydrationTest` (continuidade de counter), `CheckpointReadabilityTest`, `S3SeriesChannelParityIT` (paridade de imagem disco×S3), equivalência writer-incremental × `RraConsolidator`.
**Inalterados:** engine/config/formula/late-sample/bestfit/timebucket/ringbuffer/metrics.

## Riscos / edge-cases

1. **Continuidade de counter no reopen:** counterPrev na live-state, persistido no force; crash sem force perde no máximo o PDP corrente (1 ponto de taxa).
2. **`stepSec >> baseStep`:** CDP em progresso persiste por muitos checkpoints (está na live-state, ok); gap > `rows·stepSec` reancora o ring (não gira `rows` vezes).
3. **CDP/PDP em progresso nunca viram ponto no reader** (paridade RRD).
4. **Crash em escrita in-place:** ordem ring→force→live-state garante que o ponteiro não avança além de dados sincronizados; `liveStateCrc32` protege a live-state, rings ficam.
5. **Corrida RMW no S3** com dois writers na mesma chave: last-PUT-wins — invariante "1 writer/série" documentada.
6. **Mudança de geometria** (definitionHash) → recria rings (clean cut, WARN).
7. **Ordem das colunas** estável (declaração de `dataSources` filtrada por appliesTo); reordenar muda o hash → recria.

## Verificação

```bash
# build + unit (storage local, sem Docker)
mvn -pl nishi-utils-oss clean verify

# IT de S3 via LocalStack (Docker) — paridade do SeriesChannel disco×S3
mvn -pl nishi-utils-oss verify -Pngrrd-integration

# build completo do monorepo (dependências entre módulos)
mvn clean install -DskipTests
```

Checagem manual end-to-end (via `IfaceTrafficSmokeIT` ou snippet): abrir handle local, ingerir as 6 métricas
em vários steps, `checkpoint()`, ler presets `daily`/`weekly` e confirmar pontos derivados corretos;
**verificar que existe exatamente 1 arquivo `.ngrr` por série** no diretório raiz (prova direta da #144).
Validar wraparound: ingerir além de `rows×stepSec` e confirmar que só a janela mais recente é lida.
DoD docs: `doc/oss/ngrrd.md` + diagramas PlantUML atualizados e renderizando via `uml.nishisan.dev`.
