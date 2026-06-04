# ngrrd — Nishi Grid Round-Robin Database

Formato de séries temporais com paridade conceitual ao RRDtool, persistência
em backends pluggable (disco local e Object Storage S3-compatível) e definição
declarativa em YAML.

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
| **Bloco** | Unidade física de persistência (`blockSizeSec`). Vira 1 arquivo `.ngrrd`. |
| **Manifesto** | YAML versionado (`v{N}.yaml`) que lista todos os blocos por RRA. |
| **Counter reset / wrap** | Discontinuidade detectada automaticamente em DS COUNTER. |

> **Uma série modela várias métricas.** Assim como um RRD real de interface de
> rede (traffic in/out, errors, discards…), um único `MetricSeriesDefinition`
> declara **múltiplos DS**. Cada DS COUNTER pode virar uma série derivada — ex.:
> `in_octets` → `in_bps`. Ver a seção *Exemplo prático — uma interface de rede
> completa*.

---

## Arquitetura

![Topologia C4](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrrd_topology.puml)

- **Engine pura** (`engine/`): determinística e sem IO — `CounterDeriver`,
  `PrimaryDataPoint`, `RraConsolidator`, `BestFitSelector`, `TimeBucket`,
  `LateSampleHandler`, `FormulaEvaluator`, `RingBuffer`.
- **Formato** (`format/`): `BlockHeader` (30 bytes), `BlockCodec` com CRC32,
  `ManifestCodec` em YAML + SHA-256 do YAML original.
- **Storage** (`storage/`): interface `NgrrdStorage` + `LocalDiskStorage` e
  `S3Storage` (SDK v2, suporta endpointOverride para MinIO/Ceph).
- **Writer** (`writer/`): worker thread única, fila bloqueante, `ManifestUpdater`
  periódico via `ScheduledExecutorService`.
- **Reader** (`reader/`): `NgrrdReader` + `ViewExecutor` (preset → ViewQuery).
- **Métricas** (`metrics/`): `NgrrdMetrics` + listener para Micrometer/JMX/log.

---

## Fluxo de escrita

![Fluxo de escrita](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrrd_write_flow.puml)

1. Cliente enfileira `Sample` por DS raw.
2. Worker aplica `CounterDeriver` em DS COUNTER com `derive.output`, gerando o
   DS derivado (ex.: `in_octets` → `in_bps`). Flags `RESET`/`WRAP` viram
   métricas.
3. Encaixa em `PrimaryDataPoint` por bucket do step base.
4. Ao cruzar `blockEndEpoch`, fecha o bloco: para cada `(RRA, CF)`, consolida
   PDPs em CDPs (`RraConsolidator`), encoda via `BlockCodec`, grava no Storage
   sob `aggPrefix/{seriesKey}/{rra}_{ds}_{cf}/{stepSec}/{blockStart}.ngrrd`.
5. `ManifestUpdater` grava snapshots versionados a cada `intervalSec`.

---

## Fluxo de leitura

![Fluxo de leitura](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/diagrams/ngrrd_read_flow.puml)

1. `ViewExecutor` traduz `PresetDef` → `ViewQuery` e resolve `seriesKey` via
   `IdentitySpec.seriesKeyTemplate`.
2. `NgrrdReader` escolhe a melhor RRA via `BestFitSelector`.
3. Lê a versão **mais recente** do manifesto e filtra blocos por
   `(rra, dsName, cf)`.
4. Baixa cada bloco, decodifica (com verificação CRC32), recorta para
   `window`, aplica `maxPoints` (downsample uniforme).

> O manifesto é a **fonte de verdade**. O reader nunca varre o storage por
> listagem para construir séries.

---

## Layout binário do bloco

```
+----------------+--------+-----------+-------+----+----+--------------------+------+---------+
| MAGIC (4)      | VER(2) | FLAGS(2)  | STEP  | CF | DS | BLOCK_START_EPOCH  | ROWS | CRC32   |
| 0x4E 47 52 44  |        |           | (4)   |(1) |(1) |       (8)          | (4)  | (4)     |
+----------------+--------+-----------+-------+----+----+--------------------+------+---------+
| PAYLOAD: ROWS x 8 bytes (double IEEE-754; NaN = unknown/missing)                            |
+--------------------------------------------------------------------------------------------+
```

- `MAGIC` = ASCII `"NGRD"` (`0x4E475244`).
- Sem timestamps no payload — derivam de `blockStartEpoch + i*stepSec`.
- `FLAGS` reserva bits para `compressed` (futuro), `partial` (snapshot não
  fechado) e `agg` (bloco agregado vs raw).
- `CRC32` cobre header (excluindo o próprio campo) + payload completo.

---

## Layout físico (storage)

```
<rawPrefix>/<seriesKey>/<ds>/<stepSec>/<blockStartEpoch>.ngrrd
<aggPrefix>/<seriesKey>/<rraName>__<dsName>__<cf>/<stepSec>/<blockStartEpoch>.ngrrd
<manifestPrefix>/<seriesKey>/v{N}.yaml
<schemaPrefix>/<definitionName>.yaml
```

Idempotência via chave determinística + `verify_or_replace_if_identical` no
`NgrrdStorage`.

---

## Tamanho do arquivo — fixo ou dinâmico?

Resposta curta: **cada bloco `.ngrrd` tem tamanho fixo e determinístico**; o
**total por série é limitado** (não cresce indefinidamente). O modelo, porém, é
diferente do RRDtool clássico.

### Por bloco (fixo)

Assim que um bloco fecha, seu tamanho é exato:

```
tamanhoBloco   = 30 (header) + linhasPorBloco × 8 (payload, double IEEE-754)
linhasPorBloco = blockSizeSec / rra.stepSec
```

Para `blockSizeSec = 21600` (6 h) e os RRAs do exemplo (assumindo `blockSizeSec`
múltiplo do `stepSec` de cada RRA):

| RRA | stepSec | linhas/bloco | bytes/bloco |
|-----|---------|--------------|-------------|
| `rra_5m_30d` | 300 | 72 | 606 B |
| `rra_1h_6mo` | 3600 | 6 | 78 B |
| `rra_2h_1y` | 7200 | 3 | 54 B |

### Por série (limitado, não infinito)

O writer expira blocos fora da janela de retenção de cada RRA
(`retentionSec = rows × stepSec`), comportando-se como um **ring buffer no nível
de bloco**. O footprint estável de cada combinação `(RRA, DS, CF)` é:

```
footprint   = rows × (8 + 30 × stepSec / blockSizeSec)  bytes
blocosVivos = ceil(rows × stepSec / blockSizeSec)
```

O payload total fecha exatamente em `rows × 8`; o termo extra é o overhead dos
headers dos `blocosVivos`. Para o exemplo prático abaixo (6 DS, 3 RRAs e 2 CFs
— `AVERAGE` + `MAX`):

| RRA | rows | blocos vivos | footprint por (RRA,DS,CF) |
|-----|------|--------------|---------------------------|
| `rra_5m_30d` | 8640 | 120 | 72.720 B (~71 KiB) |
| `rra_1h_6mo` | 4320 | 720 | 56.160 B (~55 KiB) |
| `rra_2h_1y` | 4380 | 1460 | 78.840 B (~77 KiB) |

Total da série em retenção plena: `6 DS × 3 RRA × 2 CF` ≈ **2,4 MiB**,
distribuídos em ≈ **27,6 mil arquivos** (um por janela de 6 h, por combinação).

### Diferença para o RRDtool clássico

| | RRDtool | ngrrd |
|--|---------|-------|
| **Arquivos** | 1 arquivo único | muitos arquivos pequenos (1 por bloco × RRA × DS × CF) |
| **Alocação** | pré-alocado no tamanho final desde o dia 1 | cresce de zero até o teto e estabiliza |
| **Expiração** | sobrescrita in-place no ring | remoção de blocos antigos fora da janela |

### Tuning

Em RRAs grossos cada bloco guarda poucas linhas, então o header de 30 B pesa
(ex.: `rra_2h_1y` → 54 B/bloco, ~56% de overhead). Regra prática: escolha um
`blockSizeSec` grande o suficiente em relação ao `stepSec` do RRA mais grosso
para amortizar o header e reduzir a contagem de arquivos.

> **Nota (implementação atual):** apenas blocos **agregados** (RRAs) são
> materializados. `StorageKey` reserva o prefixo `raw` na convenção de nomes, mas
> o writer atual não grava blocos raw — o footprint é 100% definido pelos RRAs.

---

## Definição YAML — exemplo mínimo

```yaml
apiVersion: ngrrd/v1
kind: MetricSeriesDefinition
metadata:
  name: iface-traffic-errors-v1

spec:
  time:
    baseStepSec: 300
    blockSizeSec: 21600
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
    objectNaming: {scheme: deterministic, rawPrefix: raw, aggPrefix: agg, manifestPrefix: manifest, schemaPrefix: schema}
    writePolicy: {mode: append_only, idempotency: {key: "{seriesKey}/{ds}/{stepSec}/{blockStartEpoch}", onConflict: "verify_or_replace_if_identical"}}
    manifestPolicy: {updateMode: periodic_snapshot, intervalSec: 900}

  quality:
    emitMetrics: [missing_ratio, ingest_lag_sec, late_sample_count, counter_reset_count, wrap_detected_count]
```

Interpolação de variáveis: `${VAR}` e `${VAR:default}` são resolvidos no load
contra `System.getenv` (substituível em testes).

---

## Exemplo prático — uma interface de rede completa

No mundo real você não monitora um único contador: uma interface de rede tem
**tráfego de entrada e saída, erros e descartes (discards)**. No ngrrd cada uma
dessas métricas é um **DataSource** (DS) — tipicamente um `COUNTER` SNMP — que a
engine converte automaticamente em uma série de taxa via `derive.output`
(`in_octets`, em bytes acumulados, → `in_bps`, em bits/s).

A definição abaixo modela **6 métricas** numa única série:

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
    blockSizeSec: 21600
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
    objectNaming: {scheme: deterministic, rawPrefix: raw, aggPrefix: agg, manifestPrefix: manifest, schemaPrefix: schema}
    writePolicy: {mode: append_only, idempotency: {key: "{seriesKey}/{ds}/{stepSec}/{blockStartEpoch}", onConflict: "verify_or_replace_if_identical"}}
    manifestPolicy: {updateMode: periodic_snapshot, intervalSec: 900}

  quality:
    emitMetrics: [missing_ratio, ingest_lag_sec, late_sample_count, counter_reset_count, wrap_detected_count]
```

> Exemplo ilustrativo. A variante de 4 DS efetivamente exercitada nos testes está
> em `nishi-utils-oss/src/test/resources/iface-traffic-errors-v1.yaml`.

---

## Uso programático

Abertura, ingestão das 6 métricas e leitura — o fluxo espelha os testes
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
    handle.write("in_discards",  new Sample(ts, inDiscards));
    handle.write("out_discards", new Sample(ts, outDiscards));

    handle.flush();   // bloqueia até o bloco corrente fechar e persistir

    // Leitura por preset: retorna todas as séries do preset de uma vez.
    Map<String, SeriesResult> daily = handle.read("daily");
    SeriesResult inBps  = daily.get("in_bps");
    SeriesResult outBps = daily.get("out_bps");
    for (DataPoint p : inBps.points()) {
        // p.tsEpochMs(), p.value()  → Double.NaN representa gap/unknown
    }
}
```

`write` é assíncrono: o cliente apenas enfileira e uma worker thread única aplica
`CounterDeriver`, encaixa nos `PrimaryDataPoint` e fecha o bloco ao cruzar
`blockSizeSec`. `flush()` força o fechamento imediato. Um `RESET`/`WRAP` de
counter vira ponto `NaN` na série derivada (`onReset: unknown`).

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

---

## Métricas emitidas

| Nome | Tipo | Origem |
|------|------|--------|
| `late_sample_count` | counter | sample anterior ao bloco corrente |
| `counter_reset_count` | counter | flag RESET do CounterDeriver |
| `wrap_detected_count` | counter | flag WRAP do CounterDeriver |
| `last_ingest_lag_sec` | gauge | atraso da última sample observado |
| `last_missing_ratio` | gauge | proporção de PDPs missing no último bloco fechado |

Plugue Micrometer/JMX/log com `NgrrdMetricsListener` passado a
`Ngrrd.fromYaml(yaml, bindings, tags, listener)`.

---

## Build & testes

```bash
# unitário (apenas storage local, sem Docker)
mvn -pl nishi-utils-oss verify

# inclui IT de S3 via LocalStack (precisa Docker Engine >= 25)
mvn -pl nishi-utils-oss verify -Pngrrd-integration
```
