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

## Uso programático

```java
import dev.nishisan.utils.oss.Ngrrd;
import dev.nishisan.utils.oss.NgrrdHandle;
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesResult;
import dev.nishisan.utils.oss.storage.StorageFactory;

Path yaml = Path.of("series.yaml");
Path root = Path.of("/var/ngrrd");
var bindings = StorageFactory.StorageBindings.forLocalDisk(root);
var tags = Map.of("deviceId", "r1", "interfaceId", "eth0");

try (NgrrdHandle handle = Ngrrd.fromYaml(yaml, bindings, tags)) {
    handle.write("in_octets", new Sample(System.currentTimeMillis(), 12345L));
    SeriesResult daily = handle.read("daily").get("in_bps");
}
```

Para S3 (AWS, MinIO ou Ceph):

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
