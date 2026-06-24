# Plano — SPA Calculadora de Disco & IOPS do ngrrd

## Context

O ngrrd grava cada série como um `.ngrr` de tamanho **fixo e pré-alocado** (paridade RRDtool):
o tamanho em disco é 100% determinado pela geometria da definição YAML (`MetricSeriesDefinition`).
Hoje não existe nenhuma ferramenta para, a partir de um YAML, prever (a) o tamanho de cada série,
(b) o total ocupado por uma frota (X devices × Y interfaces) e (c) o IOPS de durabilidade exigido —
nem para comparar os três backends de storage. O operador precisa disso para dimensionar volume e
escolher backend antes de subir produção (ex.: `iface-traffic-v1` em escala Vivo/TEMS).

**Resultado pretendido:** um SPA estático (1 arquivo HTML, offline) que importa um ou mais YAMLs de
definição ngrrd, computa o tamanho byte-exato por série (port fiel de `SeriesGeometry`), aplica o
multiplicador de frota, soma um total agregado e estima IOPS + recomendações operacionais para
`localDisk`, `shardedBlob` e `objectStorage` (S3).

## Decisões (confirmadas com o usuário)

1. **Stack:** HTML single-file vanilla (JS+CSS inline, **js-yaml vendorizado inline** — MIT, compatível com GPLv3; zero build; abre offline). Sem adicionar toolchain Node ao repo.
2. **Backends:** os 3 (`localDisk`, `shardedBlob`, `objectStorage`/S3) lado a lado, com comparação.
3. **Frota:** múltiplas definições, cada uma com sua frota (devices × ifaces, ou nº direto de séries), somadas num **grand total**.
4. **Saída:** números + **recomendações operacionais** (nº de shards, alerta de FD no local disk, PUT/s e banda no S3, IOPS de durabilidade e efeito do idle-skip).

## Fórmula de tamanho (port byte-exato de `SeriesGeometry`)

Fonte da verdade: `nishi-utils-oss/.../format/SeriesGeometry.java:59-169` e
`SeriesFileCodec.FIXED_HEADER_BYTES = 96` (`SeriesFileCodec.java:52`). **Sem compressão**; o arquivo
em disco == `fileTotalBytes` (`LocalDiskStorage.java:180-183` faz `setLength(totalBytes)`).

Sejam `D` = nº de colunas e `A` = nº de archives:

- **Colunas (`columnsOf`, :130-146):** uma por DS na ordem de declaração. `derivedName = derive.output.name` se houver bloco derive, senão `ds.name`. `rawName = ds.name`. Filtro: se `archives.appliesTo.include` for **não-vazio**, mantém só DS cujo `derivedName ∈ include` (include vazio ⇒ todas as colunas).
- **Archives (`archiveDefsOf`, :149-159):** produto achatado `rra × cf`. `A = Σ_rra len(rra.cf)`. Cada CF é um **ring separado**.

```
align8(v)          = ceil(v/8)*8                       // usar aritmética (bitwise JS é 32-bit)
utf8Len(s)         = new TextEncoder().encode(s).length

dictBytes          = Σ_colunas ( 2 + utf8(derivedName) + 2 + utf8(rawName) + 1 )   // = Σ (5 + |derived| + |raw|)
archBytes          = Σ_archives ( 2 + utf8(rraName) + 1 + 4 + 4 + 8 )              // = Σ (19 + |rraName|)
staticSectionBytes = 96 + dictBytes + archBytes
liveStateOffset    = align8(staticSectionBytes)
liveStateBytes     = 12 + 64*D + 12*A + 16*A*D          // (:162-169)
ringDataOffset     = align8(liveStateOffset + liveStateBytes)
ringBytes          = Σ_archives ( rows_k * D * 8 )       // termo dominante; 8 = 1 double/célula
fileTotalBytes     = ringDataOffset + ringBytes          // == bytes em disco
```

**Goldens de paridade (pinados como teste):**
- `iface-traffic-blob.yaml` (fixture do repo; D=2, A=3): **346.104 B** (confirmado por `SeriesGeometrySizingTest`).
- `iface-traffic-v1.yaml` (D=8, A=7): **2.774.472 B (≈ 2,646 MiB)** — ring = 2.772.480 B domina.

## Modelos de overhead por backend

Tamanho **lógico** (`fileTotalBytes`) é idêntico nos 3; muda o empacotamento:

- **localDisk** (`LocalDiskStorage`): 1 arquivo `.ngrr` por série. Físico ≈ `align(fileTotalBytes, fsBlock)` (fsBlock default **4096**, editável) + ~1 inode. **1 FD/série** quando o writer está aberto.
- **shardedBlob** (`BlobStorage`): região **page-aligned (4096)** dentro de 1 shard. Físico/série = `ceil(fileTotalBytes/4096)*4096`. Overhead de volume: `shardCount × 4096` (superblocks) + `volume.meta` 56 B + catálogo (`~35 B + |catalogKey|` por série, `catalogKey = "series/<seriesKey>.ngrr"`). Defaults: `shardCount=64`, `segmentBytes=1 GiB` (`BlobVolumeConfig.java:18-19`). Capacidade declarada é sparse (múltiplos de 1 GiB/shard); footprint real = Σ regiões.
- **objectStorage / S3** (`S3Storage`): 1 objeto/série = `fileTotalBytes` exato + `~1 KB` metadado de objeto (editável). + 1 objeto `schema/<def>.yaml` por **definição** (~poucos KB).

## Modelo de IOPS

Caminho de escrita: `writeCell` (8 B) e `persistLiveState` vão para page cache/mmap; **durabilidade só em `force()`**, chamado **só no checkpoint** (`NgrrdWriter.checkpointAndForce:629-666`). Não há checkpoint periódico embutido — a app é quem chama. **Idle-skip** (`changedSinceForce`, :630-635): checkpoint sem sample nova faz early-return ⇒ **0 IOPS** (só conta `checkpoint_coalesced`).

Inputs (com defaults, editáveis): `interval = baseStepSec` (auto do YAML), `checkpointsPerCycle = 1`, `idleFraction = 0`, `durability = FSYNC`.

```
S                     = Σ_def (devices × ifacesPerDevice)        // ou nº direto de séries
forcePerSeriesPerCycle= checkpointsPerCycle × (1 − idleFraction)
writeDurabilityOps/s  = S × forcePerSeriesPerCycle / interval
```
Por `force` efetivo, por backend:
- shardedBlob → 1 `msync` de região (páginas sujas; ~1–2 páginas) → `writeOps/s` msync.
- localDisk → 1 `fsync` de arquivo → `writeOps/s` fsync (com **S FDs** — gargalo).
- S3 → 1 **PUT do objeto inteiro** → `writeOps/s` PUT **e banda = writeOps/s × fileTotalBytes**.

Leitura (modelada à parte, opcional): `dashboardQps × seriesPerQuery` reads/s; no S3 cada open = 1 GET do objeto inteiro.

Exemplo a exibir como sanity check (10.000 dev × 48 ifaces, iface-traffic-v1): **480k séries ≈ 1,21 TiB**; **1.600 ops/s** de durabilidade (msync no blob / fsync no local) e no S3 **1.600 PUT/s × 2,65 MB ≈ 4,2 GB/s** — mostra por que blob/local vencem em escala.

## Recomendações operacionais (engine de heurísticas)

- **shardedBlob:** sugerir `shardCount` para manter cada shard abaixo de um alvo (default 16 GiB/shard): `max(64, ceil(totalRegionBytes / alvoPorShard))`. Mostrar uso por shard e crescimento em segmentos de 1 GiB.
- **localDisk:** alerta de **file descriptors** — `S` FDs simultâneos vs `ulimit` (input, default 1024 e 65536); flag de pressão de inode/dentry ("1 arquivo por série").
- **S3:** destacar PUT/s, banda de PUT e custo por request; nota de read-modify-write (PUT reescreve o objeto inteiro).
- **idle-skip:** exibir que checkpoints coalescidos custam **0 IOPS** e que o force/PUT é proporcional à ingestão, não à frequência de checkpoint.

## Entregável

**Arquivo único:** `doc/oss/sizing-calculator.html` (junto da spec `doc/oss/ngrrd.md`).

Estrutura interna (tudo inline no HTML):
1. **js-yaml minificado** vendorizado (cabeçalho de licença MIT).
2. **Engine de geometria** (port fiel das funções acima: `utf8Len`, `align8`, `columnsOf`, `archivesOf`, `liveStateBytes`, `computeGeometry`).
3. **Modelos de backend** (overhead localDisk/blob/S3) e **modelo de IOPS**.
4. **Modelo de frota** + **engine de recomendações**.
5. **Self-test embutido:** roda `computeGeometry` sobre o `iface-traffic-blob.yaml` embutido e assere `=== 346088`; mostra badge "✓ engine validado" (verde) / vermelho se divergir.
6. **UI:**
   - Import: drag-drop / file picker / colar YAML (vários → vários cards).
   - Card por definição: resumo (D colunas, A archives, lista de RRAs) + tamanho/série nos 3 backends + inputs de frota (devices, ifaces/device ou nº de séries).
   - Controles globais: parâmetros de overhead (fsBlock, shardCount/alvo, metadado S3, ulimit) e de IOPS (interval auto, checkpoints/ciclo, idleFraction, durability, dashboardQps).
   - Resultados: tabela por definição + **grand total** comparando os 3 backends (tamanho lógico, físico c/ overhead, total da frota, write-IOPS, read-IOPS, banda PUT S3) + lista de recomendações.
   - Export: copiar resultado como Markdown/CSV.

## Arquivos a criar/alterar

- **Criar** `doc/oss/sizing-calculator.html` (todo o SPA).
- **Editar** `doc/oss/ngrrd.md` — adicionar nota curta na seção de sizing (~`:201-231`) linkando a calculadora (atende DoD de docs do CLAUDE.md).
- **Criar** teste de golden em `nishi-utils-oss/src/test/java/.../format/` (ex.: `SeriesGeometrySizingTest`) que carrega o fixture `iface-traffic-blob.yaml` (`src/test/resources/`) via `NgrrdYamlLoader` e assere `new SeriesGeometry(def).fileTotalBytes() == 346088`. Trava o número que o self-test do SPA replica → garante paridade JS↔Java e detecta regressão de geometria.
- **Copiar** este plano para `planning/` no repo (regra 7 do CLAUDE.md) na implementação.

## Reuso (não reinventar)

- Fórmula: **portar verbatim** `SeriesGeometry.java` (não aproximar).
- Formatação humana de bytes: espelhar `GeometryChangeReport.humanBytes` (`migration/GeometryChangeReport.java:81-94`).
- Constantes de volume: `BlobVolumeConfig.java:18-19` (shardCount=64, segment=1 GiB), padding de página 4096 (`BlobStorage.alignToPage:464`).
- Campos do YAML / validações a espelhar: `definition/*.java` e `config/NgrrdDefinitionValidator.java:51-330`. Parsing leniente (case-insensitive em enums; só campos de geometria importam).

## Verificação (end-to-end)

1. **Self-test do engine:** abrir o HTML → badge "✓ engine validado" verde (346104 confere).
2. **Golden Java:** `mvn -pl nishi-utils-oss test -Dtest=SeriesGeometrySizingTest` passa (== 346104). Confirma que o número-âncora do SPA é o do engine real.
3. **Caso real:** importar `iface-traffic-v1.yaml` → conferir **2.774.472 B (2,646 MiB)/série**; aplicar 10.000 × 48 → total ≈ **1,21 TiB** e ~**1.600 ops/s**; conferir comparação dos 3 backends e a banda de **~4,2 GB/s** PUT no S3.
4. **Cross-check doc:** importar o exemplo de 6 colunas/3 RRAs×2 CFs da doc (`ngrrd.md:210-221`) → **≈ 1,59 MiB** (6 archives).
5. **Browser MCP:** carregar o arquivo via claude-in-chrome, importar o YAML e capturar screenshot dos resultados como evidência.
6. **Offline:** abrir o HTML sem rede (file://) e confirmar parsing + cálculo (js-yaml inline, sem CDN).
