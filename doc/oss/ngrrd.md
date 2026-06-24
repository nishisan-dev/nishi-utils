# ngrrd — Nishi Grid Round-Robin Database

Formato de séries temporais em **paridade com o RRDtool**: um **único objeto
binário por série** (arquivo `.ngrr`), de tamanho fixo, pré-alocado, com ring
buffers atualizados in-place. Persistência em backends pluggable (disco local e
Object Storage S3-compatível) e definição declarativa em YAML.

- **Pacote Java:** `dev.nishisan.utils.oss`
- **Artefato Maven:** `dev.nishisan:nishi-utils-oss`
- **apiVersion no YAML:** `ngrrd/v1`
- **Kind:** `MetricSeriesDefinition`

---

## Conceitos

| Termo | Significado |
|-------|-------------|
| **DS** (Data Source) | Definição lógica de um sinal coletado (`COUNTER`, `GAUGE`, `DERIVE`, `ABSOLUTE`). |
| **PDP** (Primary Data Point) | Amostra consolidada no step base (`time.baseStepSec`). |
| **CDP** (Consolidated Data Point) | Agregado de N PDPs no step do RRA. |
| **CF** (Consolidation Function) | `AVERAGE`, `MAX`, `MIN`, `LAST`. |
| **RRA** (Round-Robin Archive) | Arquivo finito com seu próprio `stepSec`, `rows` e CFs. |
| **XFF** | Fração máxima de PDPs missing num CDP antes de virá-lo `NaN`. |
| **Coluna** | Um DS persistido — derivado (ex.: `in_bps`) ou as-is (`GAUGE` sem `derive`); todas compartilham a geometria das RRAs. |
| **Archive** | Par `(rra, cf)` — cada um possui um ring buffer de `rows × nº de colunas` doubles. |
| **Série (`.ngrr`)** | O único objeto físico por série: header + dicionários + live-state + rings. |
| **Counter reset / wrap** | Descontinuidade detectada automaticamente em DS COUNTER. |

> **Uma série modela várias métricas.** Assim como um RRD real de interface de
> rede (traffic in/out, errors, discards…), um único `MetricSeriesDefinition`
> declara **múltiplos DS**. Cada DS COUNTER pode virar uma série derivada — ex.:
> `in_octets` → `in_bps`. Todas as métricas derivadas viram **colunas** dentro
> do mesmo objeto `.ngrr`.

---

## Arquitetura

![Topologia C4](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrrd_topology.puml)

- **Engine pura** (`engine/`): determinística e sem IO — `CounterDeriver`,
  `PrimaryDataPoint`, `RraConsolidator`, `BestFitSelector`, `TimeBucket`,
  `LateSampleHandler`, `FormulaEvaluator`.
- **Formato** (`format/`): `SeriesGeometry` (offsets determinísticos a partir da
  definição), `SeriesFileCodec` (header + live-state + CRC32), `SeriesHeader` e
  `SeriesLiveState`.
- **Storage** (`storage/`): interface `NgrrdStorage` + `SeriesChannelProvider`
  (`LocalDiskStorage` com `FileChannel` in-place; `S3Storage` com imagem em
  memória + PUT). O `SeriesChannel` abstrai escrita por região.
- **Writer** (`writer/`): worker thread única; consolidação **contínua** estilo
  RRDtool (`cdp_prep`); `checkpoint()` materializa o CDP em progresso como
  parcial e torna o estado durável.
- **Reader** (`reader/`): `NgrrdReader` + `ViewExecutor` — lê os ring buffers
  diretamente do objeto único, sem manifesto.
- **Métricas** (`metrics/`): `NgrrdMetrics` + listener para Micrometer/JMX/log.

---

## Fluxo de escrita

![Fluxo de escrita](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrrd_write_flow.puml)

1. Na abertura, antes de abrir o `SeriesChannel`, a geometria gravada é
   reconciliada com a nova ([Evolução de schema](#evolução-de-schema-ongeometrychange--schemarevision)):
   objeto inexistente é pré-alocado no tamanho final com os rings em `NaN`;
   geometria idêntica apenas reidrata o estado vivo (counterPrev + acumuladores
   + ponteiros); geometria divergente aplica `onGeometryChange`.
2. Cliente enfileira `Sample` por DS raw.
3. Worker aplica o `SampleDeriver` conforme o **tipo** do DS (paridade RRD):
   `GAUGE` as-is, `COUNTER` taxa via `CounterDeriver` (com detecção de
   reset/wrap), `DERIVE` delta/Δt com sinal, `ABSOLUTE` valor/Δt. Flags
   `RESET`/`WRAP` viram métricas e o valor anterior (`last_ds`) é atualizado.
4. O valor derivado entra no PDP do step base corrente da coluna.
5. Ao avançar o slot base, o PDP completo é dobrado no CDP em progresso de cada
   archive; quando o passo do archive fecha, o CDP é finalizado (XFF) e gravado
   no ring (avançando o ponteiro, sobrescrevendo o mais antigo — retenção como
   ring buffer). A região do live-state é regravada junto (sem `fsync`) para
   manter ponteiro e células coerentes para leitores concorrentes do handle.
6. `checkpoint()`/`flush()` emite o CDP em progresso como **parcial** (legível
   antes do passo fechar) e — sob a durabilidade `FSYNC` (padrão) — torna o
   objeto durável (`fsync` no disco / PUT no S3). Sob `OS_CACHE` o checkpoint
   apenas materializa os bytes (sem `fsync`); ver [Durabilidade do
   checkpoint](#durabilidade-do-checkpoint-fsync--os_cache).

---

## Fluxo de leitura

![Fluxo de leitura](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrrd_read_flow.puml)

1. `ViewExecutor` traduz `PresetDef` → `ViewQuery` e resolve `seriesKey` via
   `IdentitySpec.seriesKeyTemplate`.
2. `NgrrdReader` escolhe a melhor RRA via `BestFitSelector` e mapeia
   `(rra, cf)` → archive.
3. Abre o objeto da série, valida o header (geometria/hash) e lê a live-state
   (`curRow`/`curRowEpochSec`) e o ring do archive.
4. Reconstrói os pares `(timestamp, valor)` a partir do ponteiro do ring,
   recorta para a `window` e aplica `maxPoints` (downsample uniforme).

> **Sem manifesto.** A geometria e os ponteiros do ring são auto-descritos no
> próprio objeto `.ngrr`; o reader nunca varre o storage por listagem.

---

## Layout binário da série (NGRR)

Todos os campos em big-endian. Os offsets são determinísticos a partir da
geometria (`SeriesGeometry`), o que permite escrita in-place e pré-alocação.

```
=== SEÇÃO ESTÁTICA (imutável após create) ===
0   MAGIC "NGRR" (4) | 4 version u16 | 6 schemaRevision u16 | 8 baseStepSec i32
12  definitionHash SHA-256 (32) | 44 D i32 | 48 A i32
52  staticSectionBytes i64 | 60 liveStateOffset i64 | 68 liveStateBytes i64
76  ringDataOffset i64 | 84 fileTotalBytes i64 | 92 headerCrc32 i32  (CRC sobre [0..92))
96  DS_DICT[D]    { nameLen u16, name, originRawLen u16, originRaw, dsType u8 }
    ARCH_TABLE[A] { rraNameLen u16, rraName, cf u8, stepSec i32, rows i32, xff f64 }
    (pad 8-align até liveStateOffset)

=== LIVE-STATE (tamanho fixo, sobrescrito in-place) ===
lastUpEpochMs i64
D   × { prevValue f64, prevTsEpochMs i64 }                      -- last_ds (counterPrev)
D   × { sum f64, count i32, min f64, max f64, last f64, missing i32, slotSec i64 }  -- pdp_prep
A   × { curRow i32, curRowEpochSec i64 }                        -- rra_ptr
A*D × { cdpPartial f64, foldedCount i32, missingCount i32 }     -- cdp_prep
liveStateCrc32 i32

=== RING-DATA (8-align, sobrescrito in-place) ===
por archive a: rows × D doubles (8 bytes), row-major (row,col); NaN = missing
```

- `MAGIC` = ASCII `"NGRR"` (`0x4E475252`).
- Sem timestamps no payload — derivam de `curRowEpochSec` + `stepSec` + posição no ring.
- `D` = nº de colunas (DS derivados); `A` = nº de archives (`Σ rra.cf`).
- **CRC:** `headerCrc32` cobre o header fixo (escrito uma vez); `liveStateCrc32`
  é regravado a cada `force()`. Os rings não têm CRC por linha — a integridade
  vem da pré-alocação + ordem (grava ring → `force` → grava live-state) +
  sentinela `NaN`. Live-state ilegível na reabertura ⇒ recriação segura.

---

## Layout físico (storage)

```
<seriesPrefix>/<seriesKey>.ngrr        # único objeto de dados por série
<schemaPrefix>/<definitionName>.yaml   # snapshot opcional de schema
```

Um objeto por série, em qualquer backend. Em disco local é gravado in-place
(`FileChannel`); em S3 o objeto inteiro é regravado por PUT (read-modify-write)
no `checkpoint()`/`close()`.

---

## Backend `sharded blob` (escala 30k+ séries)

O backend padrão materializa **um arquivo `.ngrr` por série**. Em produção com
dezenas de milhares de métricas isso esgota *file handles* e abre milhares de
threads de writer. O backend **`sharded blob`** resolve o gargalo de FDs: um
*volume* com N shards pré-alocados (default **64**), onde cada série é uma
**região contígua** dentro de um shard, acessada via `MappedByteBuffer` (memória
paginada). Independente do número de séries, são apenas **64 file handles + os
segmentos mmap**. A imagem da região é **byte-a-byte idêntica** a um `.ngrr`
standalone (formato inalterado).

O formato do volume (superblock, catálogo WAL+snapshot, roteamento) está
especificado em [`ngrrd-blob-volume.md`](ngrrd-blob-volume.md). É exclusivo de
disco local (mmap coerente); S3 permanece como está.

### Configuração e abertura

A "config geral" (o `ngrrd-blob-base-path`) e os volumes vivem numa camada de
topologia programática — **fora** do YAML de série (que carrega só `backend: blob`):

```java
// Registro de volumes (abrir uma vez por processo; fechar no shutdown):
BlobVolumeRegistry volumes = NgrrdBlob.registry()
        .basePath(Path.of("/var/ngrrd/blobs"))      // ngrrd-blob-base-path
        .volume("ifaceStats")                        // -> /var/ngrrd/blobs/ifaceStats, 64 shards
        .build();

// Abertura por locator ngrrd://<volume>/<seriesPath> (o path vira o seriesKey):
try (NgrrdHandle h = Ngrrd.open(volumes,
        NgrrdUri.parse("ngrrd://ifaceStats/device:r1/iface:eth0"), seriesYaml)) {
    h.write("in_octets", new Sample(ts, 12345L));
    SeriesResult daily = h.read("daily").get("in_bps");
}

// Modo compat (migração de cliente existente = trocar só o binding):
StorageFactory.StorageBindings bindings = volumes.require("ifaceStats").bindings();
try (NgrrdHandle h = Ngrrd.fromYaml(seriesYaml, bindings, tags, null)) { /* ... */ }
```

O volume é **compartilhado** entre handles: `NgrrdHandle.close()` não o fecha
(responsabilidade do `BlobVolumeRegistry`). Para migrar dados existentes, veja a
ferramenta Python `ngrrd-blob-migrate` em `python/ngrrd-python`.

---

## Tamanho do arquivo — fixo e pré-alocado

Em paridade com o RRDtool, o arquivo tem **tamanho fixo e determinístico** desde
a criação:

```
fileTotalBytes = 96 (header) + dicionários + live-state + Σ_archives(rows × D × 8)
```

O termo dominante é a soma dos rings. Para o exemplo prático abaixo (6 colunas
derivadas, 3 RRAs × 2 CFs = 6 archives):

| Archive (rra × cf) | rows | bytes do ring (rows × 6 × 8) |
|--------------------|------|------------------------------|
| `rra_5m_30d` × {AVG,MAX} | 8640 | 2 × 414.720 = 829.440 B |
| `rra_1h_6mo` × {AVG,MAX} | 4320 | 2 × 207.360 = 414.720 B |
| `rra_2h_1y`  × {AVG,MAX} | 4380 | 2 × 210.240 = 420.480 B |

Total ≈ **1,59 MiB em um único arquivo** `.ngrr` (header + live-state somam
~1 KB). Compare com o modelo anterior (blocos): ~2,4 MiB espalhados em ~27,6 mil
arquivos — a motivação da [issue #144](https://github.com/nishisan-dev/nishi-utils/issues/144).

> **Calculadora de dimensionamento.** Para estimar o tamanho de cada série, o
> total de uma frota (X devices × Y interfaces) e o IOPS de durabilidade nos três
> backends a partir de um ou mais YAMLs, abra [`sizing-calculator.html`](sizing-calculator.html)
> — um SPA estático (offline) que porta byte-a-byte a `SeriesGeometry`. O golden
> da paridade JS↔Java é travado por `SeriesGeometrySizingTest`.

### Paridade com o RRDtool

| | RRDtool | ngrrd (NGRR) |
|--|---------|--------------|
| **Arquivos** | 1 arquivo único | 1 objeto `.ngrr` por série |
| **Alocação** | pré-alocado no tamanho final | pré-alocado no tamanho final |
| **Atualização** | sobrescrita in-place no ring | sobrescrita in-place no ring |
| **Expiração** | ring buffer (sobrescreve o mais antigo) | ring buffer (sobrescreve o mais antigo) |

---

## Definição YAML — exemplo mínimo

```yaml
apiVersion: ngrrd/v1
kind: MetricSeriesDefinition
metadata:
  name: iface-traffic-errors-v1
  schemaRevision: 1            # incremente ao mudar a geometria (ver "Evolução de schema")

spec:
  time:
    baseStepSec: 300
    lateSamplePolicy:
      maxLatenessSec: 600
      onLate: "bucket_if_possible"

  identity:
    seriesKeyTemplate: "device:{deviceId}/iface:{interfaceId}"
    tags:
      - {name: deviceId}
      - {name: interfaceId}

  dataSources:
    - name: in_octets
      type: COUNTER
      counterBits: 64
      heartbeatSec: 900
      resetPolicy:
        detectCounterReset: true
        maxResetDeltaRatio: 0.90
      derive:
        output:
          name: in_bps
          formula: "delta * 8 / deltaT"
          clampNegativeToZero: true
          onReset: "unknown"
          onWrap: "auto"

  archives:
    appliesTo: {include: [in_bps], exclude: []}
    rras:
      - {name: rra_5m_30d, stepSec: 300, rows: 8640, cf: [AVERAGE, MAX], xff: 0.5}

  views:
    selection: {strategy: best_fit, maxPointsDefault: 2000, fallbackOrder: [raw, agg]}
    presets:
      - {name: daily, window: P1D, targetStepSec: 300, cf: AVERAGE, maxPoints: 400, series: [in_bps]}

  storage:
    backend: localDisk
    durability: fsync            # fsync (padrão) | os_cache — ver "Durabilidade do checkpoint"
    onGeometryChange: fail       # fail (padrão) | migrate | recreate — ver "Evolução de schema"
    objectNaming: {scheme: deterministic, seriesPrefix: series, schemaPrefix: schema}
    writePolicy: {mode: append_only, idempotency: {key: "{seriesKey}", onConflict: "verify_or_replace_if_identical"}}

  quality:
    emitMetrics: [missing_ratio, ingest_lag_sec, late_sample_count, counter_reset_count, wrap_detected_count]
```

Interpolação de variáveis: `${VAR}` e `${VAR:default}` são resolvidos no load
contra `System.getenv` (substituível em testes).

> **Notas de schema (7.0.0).** Mudança de **comportamento (breaking)**: o default
> ao abrir uma série com geometria divergente passou de _clean cut_ silencioso
> para **`onGeometryChange: fail`** — um restart nunca mais descarta a história
> por omissão. Veja [Evolução de schema](#evolução-de-schema-ongeometrychange--schemarevision).
> Além disso, **todo DS vira coluna persistida** (paridade RRD: `GAUGE` as-is,
> `DERIVE`/`ABSOLUTE` derivando por tipo) — antes só DS com `derive.output` eram
> arquivados. O campo de 16 bits do header (antes `flags`) passou a carregar
> `metadata.schemaRevision`.

> **Notas de schema (6.0.0).** O formato de série única removeu `time.blockSizeSec`,
> `storage.manifestPolicy` e `writePolicy.persistenceMode` — a persistência é
> sempre incremental (rrdtool-like) e a retenção é o ring de cada RRA
> (`rows × stepSec`). `objectNaming` usa `seriesPrefix` (e `schemaPrefix`).

---

## Exemplo prático — uma interface de rede completa

No mundo real você não monitora um único contador: uma interface de rede tem
**tráfego de entrada e saída, erros e descartes (discards)**. No ngrrd cada uma
dessas métricas é um **DataSource** (DS) — tipicamente um `COUNTER` SNMP — que a
engine converte automaticamente em uma série de taxa via `derive.output`
(`in_octets`, em bytes acumulados, → `in_bps`, em bits/s). Cada DS derivado é
uma **coluna** do objeto `.ngrr`.

| DS coletado (raw) | Tipo | Série derivada | Unidade |
|-------------------|------|----------------|---------|
| `in_octets` / `out_octets` | COUNTER 64-bit | `in_bps` / `out_bps` | bit/s |
| `in_errors` / `out_errors` | COUNTER 32-bit | `in_eps` / `out_eps` | errors/s |
| `in_discards` / `out_discards` | COUNTER 32-bit | `in_dps` / `out_dps` | discards/s |

```yaml
apiVersion: ngrrd/v1
kind: MetricSeriesDefinition
metadata:
  name: iface-full-v1

spec:
  time:
    baseStepSec: 300
    timestampAlignment: epoch
    lateSamplePolicy:
      maxLatenessSec: 600
      onLate: "bucket_if_possible"
    missingValue: "NaN"

  identity:
    seriesKeyTemplate: "device:{deviceId}/iface:{interfaceId}"
    tags:
      - {name: deviceId}
      - {name: interfaceId}
      - {name: region}
      - {name: vendor}
      - {name: role}

  dataSources:
    - name: in_octets
      type: COUNTER
      counterBits: 64
      heartbeatSec: 900
      min: 0
      max: null
      resetPolicy: {detectCounterReset: true, maxResetDeltaRatio: 0.90}
      derive:
        output: {name: in_bps, unit: "bit/s", formula: "delta * 8 / deltaT", clampNegativeToZero: true, onReset: "unknown", onWrap: "auto"}

    - name: out_octets
      type: COUNTER
      counterBits: 64
      heartbeatSec: 900
      min: 0
      max: null
      resetPolicy: {detectCounterReset: true, maxResetDeltaRatio: 0.90}
      derive:
        output: {name: out_bps, unit: "bit/s", formula: "delta * 8 / deltaT", clampNegativeToZero: true, onReset: "unknown", onWrap: "auto"}

    - name: in_errors
      type: COUNTER
      counterBits: 32
      heartbeatSec: 900
      min: 0
      max: null
      resetPolicy: {detectCounterReset: true, maxResetDeltaRatio: 0.90}
      derive:
        output: {name: in_eps, unit: "errors/s", formula: "delta / deltaT", clampNegativeToZero: true, onReset: "unknown", onWrap: "auto"}

    - name: out_errors
      type: COUNTER
      counterBits: 32
      heartbeatSec: 900
      min: 0
      max: null
      resetPolicy: {detectCounterReset: true, maxResetDeltaRatio: 0.90}
      derive:
        output: {name: out_eps, unit: "errors/s", formula: "delta / deltaT", clampNegativeToZero: true, onReset: "unknown", onWrap: "auto"}

    - name: in_discards
      type: COUNTER
      counterBits: 32
      heartbeatSec: 900
      min: 0
      max: null
      resetPolicy: {detectCounterReset: true, maxResetDeltaRatio: 0.90}
      derive:
        output: {name: in_dps, unit: "discards/s", formula: "delta / deltaT", clampNegativeToZero: true, onReset: "unknown", onWrap: "auto"}

    - name: out_discards
      type: COUNTER
      counterBits: 32
      heartbeatSec: 900
      min: 0
      max: null
      resetPolicy: {detectCounterReset: true, maxResetDeltaRatio: 0.90}
      derive:
        output: {name: out_dps, unit: "discards/s", formula: "delta / deltaT", clampNegativeToZero: true, onReset: "unknown", onWrap: "auto"}

  archives:
    appliesTo:
      include: [in_bps, out_bps, in_eps, out_eps, in_dps, out_dps]
      exclude: []
    rras:
      - {name: rra_5m_30d, stepSec: 300,  rows: 8640, cf: [AVERAGE, MAX], xff: 0.5}
      - {name: rra_1h_6mo, stepSec: 3600, rows: 4320, cf: [AVERAGE, MAX], xff: 0.5}
      - {name: rra_2h_1y,  stepSec: 7200, rows: 4380, cf: [AVERAGE, MAX], xff: 0.5}

  views:
    selection: {strategy: best_fit, maxPointsDefault: 2000, fallbackOrder: [raw, agg]}
    presets:
      - {name: daily,        window: P1D,  targetStepSec: 300,  cf: AVERAGE, maxPoints: 400,  series: [in_bps, out_bps, in_eps, out_eps, in_dps, out_dps]}
      - {name: weekly,       window: P7D,  targetStepSec: 3600, cf: AVERAGE, maxPoints: 500,  series: [in_bps, out_bps]}
      - {name: monthly,      window: P30D, targetStepSec: 3600, cf: AVERAGE, maxPoints: 1200, series: [in_bps, out_bps]}
      - {name: errors_daily, window: P1D,  targetStepSec: 300,  cf: MAX,     maxPoints: 400,  series: [in_eps, out_eps, in_dps, out_dps]}

  storage:
    backend: localDisk
    durability: fsync            # fsync (padrão) | os_cache — ver "Durabilidade do checkpoint"
    objectNaming: {scheme: deterministic, seriesPrefix: series, schemaPrefix: schema}
    writePolicy: {mode: append_only, idempotency: {key: "{seriesKey}", onConflict: "verify_or_replace_if_identical"}}

  quality:
    emitMetrics: [missing_ratio, ingest_lag_sec, late_sample_count, counter_reset_count, wrap_detected_count]
```

> Exemplo ilustrativo. A variante de 4 DS efetivamente exercitada nos testes está
> em `nishi-utils-oss/src/test/resources/iface-traffic-errors-v1.yaml`.

---

## Uso programático

Abertura, ingestão das métricas e leitura — o fluxo espelha os testes
`NgrrdFacadeTest` e `IfaceTrafficSmokeIT`.

```java
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.api.DataPoint;
import dev.nishisan.utils.oss.storage.StorageFactory;

Path yaml = Path.of("iface.yaml");
var bindings = StorageFactory.StorageBindings.forLocalDisk(Path.of("/var/ngrrd"));
var tags = Map.of(
        "deviceId", "r1", "interfaceId", "eth0",
        "region", "br-sp", "vendor", "x", "role", "core");

try (NgrrdHandle handle = Ngrrd.fromYaml(yaml, bindings, tags)) {

    // Ingestão: uma coleta SNMP por step base, todas as métricas no mesmo ts.
    long ts = System.currentTimeMillis();          // alinhe ao baseStepSec
    handle.write("in_octets",    new Sample(ts, inOctets));
    handle.write("out_octets",   new Sample(ts, outOctets));
    handle.write("in_errors",    new Sample(ts, inErrors));
    handle.write("out_errors",   new Sample(ts, outErrors));

    handle.checkpoint();   // torna o CDP em progresso durável e legível

    // Leitura por preset: retorna todas as séries do preset de uma vez.
    Map<String, SeriesResult> daily = handle.read("daily");
    SeriesResult inBps  = daily.get("in_bps");
    for (DataPoint p : inBps.points()) {
        // p.tsEpochMs(), p.value()  → Double.NaN representa gap/unknown
    }
}
```

`write` é assíncrono: o cliente apenas enfileira e uma worker thread única aplica
`CounterDeriver`, encaixa nos `PrimaryDataPoint` e consolida continuamente nos
ring buffers. `checkpoint()` (ou `flush()`) torna o CDP em progresso durável e
legível antes do passo do RRA fechar. Um `RESET`/`WRAP` de counter vira ponto
`NaN` na série derivada (`onReset: unknown`).

Para uma consulta ad-hoc (janela, step e CF explícitos), use `ViewQuery` — o
`BestFitSelector` escolhe o RRA com melhor resolução para o `targetStepSec` e
`maxPoints` aplica downsample uniforme:

```java
import java.time.Duration;
import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.ViewQuery;

ViewQuery q = new ViewQuery(Duration.ofDays(7), 3600, ConsolidationFunction.AVERAGE, 500);
SeriesResult weekIn = handle.read("in_bps", q);
```

Para S3 (AWS, MinIO ou Ceph), troque apenas o binding de storage:

```java
S3Settings s3 = S3Settings.forEndpoint(
        "my-bucket", "us-east-1",
        URI.create("https://minio.local:9000"),
        accessKey, secretKey);
var bindings = StorageFactory.StorageBindings.forS3(s3);
```

### Leitura via Python (`ngrrd-python`)

O subprojeto `python/ngrrd-python/` traz um reader read-only que espelha o
`SeriesFileCodec`: abre o `.ngrr` via `mmap`, valida os CRC32 e materializa os
ring buffers apenas sob demanda. Além de ler dados, ele expõe a **geometria**
completa do arquivo sem tocar nos rings.

```python
from ngrrd_python import NgrrdReader

with NgrrdReader("series.ngrr") as reader:
    # Resumo barato (nomes de coluna e "rra/CF").
    print(reader.get_metadata())

    # Dump completo da geometria: header com offsets, colunas (nome derivado,
    # DS raw e tipo) e archives (cf, step, rows, xff, group_size, ring_bytes).
    geometry = reader.describe_geometry()
    for column in geometry["columns"]:
        print(column["derived_name"], column["raw_name"], column["raw_type"])
    for archive in geometry["archives"]:
        print(archive["rra_name"], archive["cf"], archive["step_sec"], archive["rows"])
```

Pelo CLI `ngrrd-dump`, omitir `--archive` (ou passar `--geometry`) emite só a
geometria, em JSON ou XML — útil para inspecionar a estrutura de um arquivo sem
ler as séries:

```bash
ngrrd-dump series.ngrr                      # geometria em JSON
ngrrd-dump series.ngrr --geometry --format xml
ngrrd-dump series.ngrr --archive daily --cf AVERAGE   # dados de um archive
```

### Concorrência no mesmo handle

O `NgrrdHandle` é seguro para **1 writer lógico + N readers** no mesmo
processo, sem serialização externa:

- `read(...)` pode ser chamado por N threads, concorrentes entre si e com
  `write(...)`/`checkpoint()`. Cada leitura observa um estado consistente da
  série; leituras não bloqueiam umas às outras.
- `write(...)` é thread-safe (enfileira para a worker thread única), mas a
  série permanece single-writer lógico: com múltiplas threads escrevendo, a
  ordem relativa entre elas é indefinida.
- `checkpoint()`/`flush()` são síncronos: drenam a fila do writer antes de
  retornar.

A garantia é implementada por um `ReadWriteLock` por handle, compartilhado
entre o writer e os leitores: a worker thread adquire o write-lock em volta de
cada mutação do objeto da série (e regrava o live-state sempre que o ring
avança, mantendo ponteiro e células coerentes); leitores adquirem o read-lock
na sequência live-state → ring. `force()` (fsync/PUT) ocorre fora do lock.

**Visibilidade por backend:** no disco local, CDPs de passos já fechados
tornam-se legíveis assim que o passo fecha; o CDP em progresso (parcial)
torna-se legível após `checkpoint()`. No S3, toda leitura reflete o último
`checkpoint()` publicado (PUT atômico) — nada fica visível entre checkpoints.

> **Invariante:** um writer por série, e todo acesso concorrente passa pelo
> mesmo handle. O objeto da série sofre escrita in-place no disco e
> read-modify-write no S3: um segundo processo leitor pode observar estado
> rasgado no disco, e dois writers concorrentes na mesma chave causariam perda
> silenciosa (last-write-wins no S3).

### Durabilidade do checkpoint (FSYNC | OS_CACHE)

Cada `checkpoint()`/`flush()` faz duas coisas distintas: **materializa** os bytes
do CDP em progresso (escrita in-place no disco / read-modify-write na imagem em
memória do S3) e, em seguida, **força** a durabilidade. A política `Durability`
controla apenas o segundo passo:

| Modo | Por checkpoint | Visibilidade (disco) | Janela de perda |
|------|----------------|----------------------|-----------------|
| `FSYNC` (padrão) | `fsync` no disco / `PUT` no S3 | CDP parcial legível | nenhuma (durável a cada checkpoint **com dado novo**) |
| `OS_CACHE` | materializa, **sem** `fsync` | CDP parcial legível (page cache) | tail não descarregado, só em **crash abrupto** |

**Idle-skip (coalescing).** Um `checkpoint()`/`flush()` sem nenhuma amostra aplicada
desde o último force é um **no-op de durabilidade**: a re-emissão do CDP parcial seria
byte-idêntica e o `force()`/`PUT` já seria inócuo, então ambos são pulados — a série já
está durável naquele estado. Corta forces/PUTs redundantes quando os checkpoints são
mais frequentes que a ingestão, **sem** perda de visibilidade ou durabilidade (a leitura
do slot em progresso é idêntica). Cada pulo emite `checkpoint_coalesced_count`. Por isso
a linha "durável a cada checkpoint" vale para checkpoints **com dado novo**; um checkpoint
ocioso não força porque não há nada novo a tornar durável.

Em `OS_CACHE`, os leitores no mesmo processo continuam enxergando o CDP parcial
logo após o checkpoint (a escrita in-place já está no page cache do SO), mas o
SO decide quando descarregar para o disco. Um `close()` limpo **sempre**
descarrega o pendente (o `SeriesChannel.close()` força no fechamento), de modo
que a janela de perda se restringe a um crash/queda de energia abrupta — útil
para backfills e séries voláteis em que throughput vale mais que durabilidade
por checkpoint.

> **`OS_CACHE` é exclusivo de disco local.** No `OBJECT_STORAGE` (S3) o `force()`
> **é** o próprio `PUT` (a publicação); pular o force nunca publicaria a série.
> A abertura **rejeita** `OBJECT_STORAGE` + `OS_CACHE` com `IllegalArgumentException`.

**Configuração.** O default vem do YAML (`spec.storage.durability: fsync | os_cache`;
ausente = `fsync`) e pode ser **sobrescrito por abertura** via `Ngrrd.OpenOptions`
— a mesma definição abre `FSYNC` em produção e `OS_CACHE` num job de backfill:

```java
import dev.nishisan.utils.oss.Ngrrd.OpenOptions;
import dev.nishisan.utils.oss.api.Durability;

// Precedência: override de abertura > default do YAML > FSYNC.
try (NgrrdHandle handle = Ngrrd.fromYaml(yaml, bindings, tags,
        OpenOptions.durability(Durability.OS_CACHE))) {
    // ... ingestão de alto throughput; close() final descarrega o pendente.
}
```

Uma camada consumidora que exponha um booleano `commit.fsync` mapeia-o
diretamente: `true → Durability.FSYNC`, `false → Durability.OS_CACHE`.

> **Evolução planejada.** Um terceiro modo `GROUP_COMMIT` (coalescer N
> checkpoints num único `force`, limitando a janela de perda sem `fsync` por
> checkpoint) — análogo ao `RelayDurability.GROUP_COMMIT` do core — fica para um
> follow-up.

---

## Evolução de schema (onGeometryChange + schemaRevision)

Quando a definição muda a **geometria** de uma série (adicionar/remover/reordenar
DS, adicionar/remover archives, ou alterar `baseStepSec`/`stepSec`/`rows`), o
`geometryHash` gravado no header diverge do esperado. O ngrrd **nunca descarta a
história por omissão**: o tratamento é governado por `onGeometryChange`, com uma
trava por `schemaRevision`.

![Reconciliação de geometria](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrrd_geometry_reconcile.puml)

### `onGeometryChange`

| Valor | Efeito quando a geometria diverge |
|-------|-----------------------------------|
| `fail` (**padrão**) | Aborta a abertura com `NgrrdGeometryChangeException` e um relatório do que mudou; a série não é tocada. |
| `migrate` | Reescreve a série preservando a história compatível (remapeia colunas/archives por nome; coluna nova começa `NaN`). Mudança que exija reamostragem recai em falha. |
| `recreate` | Descarta a série antiga e cria uma nova vazia (clean cut). |

Precedência de resolução: **`OpenOptions` (abertura) > `spec.storage.onGeometryChange` (YAML) > `fail`**.

```java
// override por abertura (ex.: janela de manutenção)
Ngrrd.fromYaml(yaml, bindings, tags, null,
        Ngrrd.OpenOptions.onGeometryChange(OnGeometryChange.MIGRATE));
```

### `schemaRevision` — a trava anti-acidente

`metadata.schemaRevision` (inteiro `0..65535`, default `0`) é persistido no header
e funciona como gate: um rewrite (`migrate`/`recreate`) **só dispara quando a
revisão da definição é maior que a gravada na série**. Se a geometria mudou mas a
revisão **não** foi incrementada, a abertura **sempre falha** — uma edição
acidental de YAML nunca vira reescrita de um dataset inteiro sozinha.

> Após o upgrade, a primeira mudança intencional de geometria exige incrementar
> `schemaRevision` (séries antigas têm revisão `0`). Geometria idêntica reabre
> normalmente, sem exigir bump.

### Paridade RRD: DS sem `derive`

O **tipo** do DS define a transformação; o bloco `derive` é opcional:

| Tipo | Sem `derive` | Com `derive.output` |
|------|--------------|---------------------|
| `GAUGE` | valor as-is (apenas filtro `min`/`max` → `NaN` fora da faixa) | — |
| `COUNTER` | taxa `delta/Δt` com detecção de reset/wrap | fórmula/clamp/onReset/onWrap custom |
| `DERIVE` | taxa `delta/Δt` (permite negativo, sem wrap/reset) | idem + fórmula |
| `ABSOLUTE` | `valor/Δt` (contador zerado a cada leitura) | idem |

O nome da coluna é o `derive.output.name` quando há derivação; senão, o próprio
`ds.name()`.

```yaml
dataSources:
  - name: temperature      # GAUGE puro: persiste o valor as-is
    type: GAUGE
    heartbeatSec: 900
    min: -40
    max: 125
```

### CLI `ngrrd-migrate`

Para datasets grandes, prefira uma janela de manutenção offline. O CLI varre
todas as séries do dataset, classifica a divergência e — fora do `--dry-run` —
reescreve em lote com paralelismo limitado:

```bash
# Relatório (não escreve nada): N séries, quantas migráveis, bytes a reescrever
java -cp nishi-utils-oss.jar dev.nishisan.utils.oss.cli.NgrrdMigrateCli \
     definition.yaml /var/ngrrd --dry-run

# Execução: migra em lote (default MIGRATE), 8 threads
java -cp nishi-utils-oss.jar dev.nishisan.utils.oss.cli.NgrrdMigrateCli \
     definition.yaml /var/ngrrd --parallelism 8 --on-geometry-change MIGRATE
```

Backend `objectStorage`: configure `NGRRD_S3_BUCKET`, `NGRRD_S3_REGION` e
(opcional) `NGRRD_S3_ENDPOINT` em vez do `rootDir`.

> **Quando usar cada caminho.** Migração *eager no boot* acontece naturalmente ao
> abrir cada handle com `onGeometryChange: migrate` (paralelizada pela aplicação);
> para reescritas grandes (dezenas de GB), use o CLI numa janela controlada,
> sempre com `--dry-run` antes.

---

## Métricas emitidas

| Nome | Tipo | Origem |
|------|------|--------|
| `late_sample_count` | counter | sample anterior ao slot corrente |
| `counter_reset_count` | counter | flag RESET do CounterDeriver |
| `wrap_detected_count` | counter | flag WRAP do CounterDeriver |
| `last_ingest_lag_sec` | gauge | atraso da última sample observado |
| `last_missing_ratio` | gauge | proporção de PDPs missing no último CDP fechado |
| `checkpoint_coalesced_count` | counter | checkpoint redundante pulado (idle-skip: sem amostra nova desde o último force) |

Plugue Micrometer/JMX/log com `NgrrdMetricsListener` passado a
`Ngrrd.fromYaml(yaml, bindings, tags, listener)`.

> **Forma canônica com `seriesKey`.** Desde a 8.1.0 os callbacks têm a forma
> canônica `on*(seriesKey, …)` (as legadas sem `seriesKey` permanecem válidas por
> delegação). `last_ingest_lag_sec` passou a ser **efetivamente emitida** pelo
> writer (era declarada-porém-não-emitida).
>
> **Coleta central no blob volume.** Em vez de um listener por série, configure um
> listener default no volume via `NgrrdBlob.registry().qualityListener(listener)`:
> ele é propagado a cada handle aberto por `Ngrrd.open(...)` (que antes ignorava
> qualquer listener). Para a observabilidade **operacional** do volume (fill ratio,
> grow/alloc/free, checkpoints), ver a seção *Observabilidade do volume* em
> [`ngrrd-blob-volume.md`](./ngrrd-blob-volume.md).

---

## Build & testes

```bash
# unitário (apenas storage local, sem Docker)
mvn -pl nishi-utils-oss verify

# inclui IT de S3 via LocalStack (precisa Docker Engine >= 25)
mvn -pl nishi-utils-oss verify -Pngrrd-integration
```
