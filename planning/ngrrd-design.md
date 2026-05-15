# Plano — Pacote `oss` / Formato `ngrrd`

## Contexto

Hoje o repositório `nishi-utils` entrega `NMap` (KV persistente), `NQueue` (fila persistente) e `NGrid` (cluster distribuído). **Não existe nada para séries temporais**. O objetivo é introduzir um novo pacote `dev.nishisan.utils.oss` que ofereça um formato proprietário batizado **`ngrrd`** (Nishi Grid Round-Robin Database), com:

- **Paridade conceitual com RRD**: Data Sources tipados (`COUNTER`, `GAUGE`, `DERIVE`, `ABSOLUTE`), múltiplos RRAs com `stepSec`/`rows`/`cf[]`/`xff`, heartbeat por DS, detecção de counter reset/wrap (32/64-bit), derivações automáticas (counter → rate em bps/eps), Late Sample Policy, XFF.
- **Definição declarativa por YAML** no estilo do enunciado (`apiVersion: ngrrd/v1`, `kind: MetricSeriesDefinition`) com seções `time`, `identity`, `dataSources`, `archives`, `views`, `storage`, `quality`.
- **Storage plugável**: **disco local** e **Object Storage S3-compatível** (S3, MinIO, Ceph via endpoint override), modelo **append-only por blocos temporais** (ex.: 6h por bloco), com **manifesto versionado**.
- **API de leitura** com `best_fit` selection (escolhe o RRA certo para uma `window`/`targetStepSec`/`maxPoints`) e presets prontos (`daily`/`weekly`/`monthly`/`yearly`).

`ngrrd` é uma estrutura autônoma — **não herda implementação** de NMap, NQueue ou NGrid. Aproveita apenas o **idioma do projeto** (Builder, facade estática, YAML+`${VAR:default}` loader, Jackson, SLF4J) e o pom.xml existente. O design é próprio do formato.

---

## Decisões Arquiteturais Centrais

1. **YAML é fonte de verdade.** Uma definição `ngrrd` carregada do YAML produz um descriptor imutável (`NgrrdDefinition`) que dirige toda a engine.
2. **Append-only, sem random-write.** Cada bloco cobre `blockSizeSec` (ex. 21600 = 6h). Ao cruzar a fronteira, o bloco é fechado, checksum'd e escrito. Idempotência por chave determinística: `{seriesKey}/{ds}/{stepSec}/{blockStartEpoch}`.
3. **Manifesto versionado por série.** `manifest/<seriesKey>/v{N}.yaml` (Jackson YAML, human-readable) lista RRAs ativos, blocos, checksums, ranges temporais. Snapshot periódico (`manifestPolicy.intervalSec`). Recovery na inicialização lê o `vN` mais alto válido.
4. **Determinismo absoluto.** Dado o YAML + epoch alignment, todo bucket é reprodutível. Timestamps são do sample, nunca do servidor. `lateSamplePolicy.maxLatenessSec` define o que fazer com atrasos.
5. **Filosofia RRD preservada.** `rows × stepSec` define crescimento. Sem expansão dinâmica. RRAs são finitos por definição.
6. **Storage backend desacoplado da engine.** A engine sabe escrever/ler "blobs por chave"; o backend (Disk/S3) implementa a interface. Engine não conhece `Path` nem `S3Client`.
7. **Sem fallback para legado** (CLAUDE.md): o backend é plugável desde o início, sem versão "in-memory only".

---

## Estrutura de Pacotes Proposta

```
dev.nishisan.utils.oss/                     (raiz do novo pacote)
│
├── Ngrrd                                   Facade pública
│                                            Ngrrd.fromYaml(Path), Ngrrd.builder()...
│
├── api/                                    Tipos públicos (records imutáveis)
│   ├── Sample                              (long tsEpochMs, double value)  raw input
│   ├── DataPoint                           (long tsEpochMs, double value)  output consolidado
│   ├── DataSourceType                      COUNTER | GAUGE | DERIVE | ABSOLUTE
│   ├── ConsolidationFunction               AVERAGE | MAX | MIN | LAST
│   ├── SeriesKey                           Identidade resolvida (devíceId, interfaceId, ...)
│   ├── ViewQuery                           window + targetStepSec + cf + maxPoints
│   ├── SeriesResult                        DataPoint[] + meta (rraName, cf, stepSec)
│   └── NgrrdHandle                         Handle vivo: write(sample), read(view), close()
│
├── definition/                             Modelo da definição YAML (imutável)
│   ├── NgrrdDefinition                     Root: apiVersion + kind + metadata + spec
│   ├── TimeSpec                            baseStepSec, blockSizeSec, alignment, lateSamplePolicy
│   ├── IdentitySpec                        seriesKeyTemplate + tags[]
│   ├── DataSourceDef                       name, type, counterBits, heartbeat, min/max, resetPolicy, derive
│   ├── DeriveDef                           output: name, unit, formula, clampNegativeToZero, onReset, onWrap
│   ├── ResetPolicyDef                      detectCounterReset, maxResetDeltaRatio
│   ├── ArchiveSpec                         appliesTo + rras[]
│   ├── RraDef                              name, stepSec, rows, cf[], xff
│   ├── ViewSpec                            selection (strategy, fallbackOrder) + presets[]
│   ├── PresetDef                           name, window (ISO-8601 P1D/P7D/...), targetStepSec, cf, maxPoints, series[]
│   ├── StorageSpec                         backend, objectNaming, writePolicy, manifestPolicy
│   └── QualitySpec                         emitMetrics[]
│
├── config/                                 Carregamento da definição
│   ├── NgrrdYamlLoader                     Jackson YAMLFactory + interpolação ${VAR}/${VAR:default}
│   │                                        (mesmo idioma do NGridConfigLoader, código independente)
│   └── NgrrdDefinitionValidator            Valida coerência: stepSec múltiplo de baseStep,
│                                            rras dentro de retenção, presets resolvíveis, etc.
│
├── engine/                                  Núcleo da engine RRD (puro, sem IO)
│   ├── TimeBucket                          Helper: align(ts, step) = (ts/step)*step
│   ├── CounterDeriver                      COUNTER → rate; detecta reset (delta < -maxResetDeltaRatio * prev)
│   │                                        e wrap (32/64-bit baseado em counterBits)
│   ├── PrimaryDataPoint                    Acumulador no step base (sum/count para AVERAGE, etc.)
│   ├── RraConsolidator                     Aplica cf + xff: se missing > xff, gera NaN
│   ├── LateSampleHandler                   bucket_if_possible (bloco aberto) | late_drop (descarta + métrica)
│   ├── BestFitSelector                     Dado ViewQuery, escolhe RRA: maior stepSec ≤ targetStepSec
│   │                                        com cobertura suficiente, respeitando fallbackOrder
│   └── RingBuffer                          Estrutura round-robin in-memory para o bloco atual
│
├── format/                                  Layout binário dos blocos
│   ├── BlockHeader                         Magic (0x4E475244 "NGRD") + version + flags + stepSec + cf
│   │                                        + ds + blockStartEpoch + rows + crc32
│   ├── BlockCodec                          Encode/decode bloco completo (header + payload)
│   ├── BlockPayload                        Sequência fixa de (double value | NaN) — sizeof = rows * 8 bytes
│   │                                        (timestamps inferidos: blockStart + i*stepSec)
│   └── ManifestCodec                       Jackson YAML para manifest versionado
│
├── storage/                                 Backends plugáveis
│   ├── NgrrdStorage                        Interface: put(key, bytes), get(key), list(prefix),
│   │                                        exists(key), delete(key), atomicReplace(key, bytes)
│   ├── StorageKey                          Esquema determinístico: raw|agg|manifest|schema prefix
│   │                                        + seriesKey/ds/stepSec/blockStartEpoch
│   ├── LocalDiskStorage                    java.nio.Files + atomic move (tmp + rename)
│   ├── S3Storage                           SDK S3 (a definir) com endpointOverride para MinIO/Ceph
│   └── StorageFactory                      Instancia conforme StorageSpec.backend
│
├── writer/
│   ├── NgrrdWriter                         Recebe Sample, roteia para CounterDeriver/RraConsolidator,
│   │                                        mantém bloco aberto em RingBuffer, flush ao rotacionar
│   ├── BlockRotator                        Detecta cruzamento de blockSizeSec, fecha + persiste
│   └── ManifestUpdater                     Snapshot periódico do manifest (incrementa versão)
│
├── reader/
│   ├── NgrrdReader                         Resolve RRA via BestFitSelector, lê blocos do Storage,
│   │                                        materializa Series para o range da query
│   └── ViewExecutor                        Executa PresetDef → ViewQuery → SeriesResult
│
└── metrics/
    └── NgrrdMetrics                         Counters/gauges: missing_ratio, ingest_lag_sec,
                                              late_sample_count, counter_reset_count, wrap_detected_count
                                              (usa StatsUtils se conveniente, mas API própria)
```

---

## Fluxo de Escrita (ingestão)

```
user.write(Sample s, DataSourceName ds)
        │
        ▼
 NgrrdWriter
   ├── if (ds.type == COUNTER) → CounterDeriver.applyDelta(prev, s) ──► rate + flags (reset/wrap)
   ├── LateSampleHandler.check(s.ts, currentBlockWindow)
   │       ├── on time          → PrimaryDataPoint.add(value)
   │       ├── bucket_if_possible → encaixar em bloco aberto
   │       └── late_drop         → NgrrdMetrics.lateSamples++, descarta
   ├── (no fim do stepSec base) → RraConsolidator gera CDP para cada RRA aplicável
   ├── RingBuffer.put(rraName, cdpIndex, value)
   └── BlockRotator.maybeRotate(now)
           └── (se cruzou blockSizeSec)
                  ├── BlockCodec.encode(ringBuffer)
                  ├── NgrrdStorage.put(StorageKey.raw(seriesKey, ds, stepSec, blockStart), bytes)
                  └── ManifestUpdater.recordBlock(...)
```

---

## Fluxo de Leitura (view)

```
user.read(ViewQuery q)  // ou ViewExecutor.run(presetName)
        │
        ▼
 NgrrdReader
   ├── BestFitSelector.pick(q, definition.archives.rras)
   │       └── retorna RraDef com maior stepSec ≤ q.targetStepSec
   │           cobrindo q.window, respeitando fallbackOrder (raw|agg)
   ├── identifica blocos necessários (blockStartEpoch list)
   ├── NgrrdStorage.get(StorageKey.*(seriesKey, ds, stepSec, blockStart)) por bloco
   ├── BlockCodec.decode → DataPoint[]
   ├── corta para q.window, decima se exceder q.maxPoints (downsample on-the-fly)
   └── SeriesResult
```

---

## Layout Físico (storage)

Usando `objectNaming.scheme: deterministic` do YAML:

```
<rawPrefix>/<seriesKey>/<ds>/<stepSec>/<blockStartEpoch>.ngrrd
<aggPrefix>/<seriesKey>/<rraName>/<stepSec>/<blockStartEpoch>.ngrrd
<manifestPrefix>/<seriesKey>/v{N}.yaml
<schemaPrefix>/<defName>.yaml                   (cópia da definição usada)
```

Exemplo concreto (S3):
```
s3://my-bucket/raw/device:r1/iface:eth0/in_octets/300/1747339200.ngrrd
s3://my-bucket/agg/device:r1/iface:eth0/rra_5m_30d/300/1747339200.ngrrd
s3://my-bucket/manifest/device:r1/iface:eth0/v42.yaml
```

Idempotência (`writePolicy.idempotency.key`): a chave determinística garante que reprocessamentos sobrescrevam o mesmo objeto. `onConflict: verify_or_replace_if_identical` lê + compara checksum antes de regravar.

---

## Layout Binário do Bloco (`format/BlockCodec`)

```
+----------------+--------+-----------+-------+----+----+--------------------+------+---------+
| MAGIC (4)      | VER(2) | FLAGS(2)  | STEP  | CF | DS | BLOCK_START_EPOCH  | ROWS | CRC32   |
| 0x4E 47 52 44  |        |           | (4)   |(1) |(1) |       (8)          | (4)  | (4)     |
+----------------+--------+-----------+-------+----+----+--------------------+------+---------+
| PAYLOAD: ROWS × 8 bytes (double IEEE-754; NaN = unknown/missing)                            |
+--------------------------------------------------------------------------------------------+
```

- **Sem timestamps no payload**: derivados de `BLOCK_START_EPOCH + i*STEP`. Compacto.
- **CRC32 sobre header (excluindo o próprio campo) + payload** para detecção de corrupção.
- **FLAGS**: bits para `compressed`, `partial` (bloco ainda não completo), `agg` (RRA vs raw).
- **Compressão**: aplicada ao payload antes da escrita (decidir Zstd vs gzip — pergunta abaixo).

---

## Validação da Definição (`NgrrdDefinitionValidator`)

Checks obrigatórios antes de instanciar `Ngrrd`:

- `time.baseStepSec > 0` e divisível por 60 (recomendado).
- `time.blockSizeSec` múltiplo de `baseStepSec` e ≥ `max(rra.stepSec)`.
- Cada `rra.stepSec` múltiplo de `baseStepSec`.
- Cada `rra.rows > 0`; `xff` em [0.0, 1.0).
- `appliesTo.include` referencia DS existentes (raw ou derivados).
- `derive.formula` parseável (subset whitelisted: `delta`, `deltaT`, `*`, `/`, `+`, `-`, literais).
- `views.presets[].targetStepSec` resolvível por algum RRA com `fallbackOrder`.
- `identity.seriesKeyTemplate` referencia apenas tags declaradas.

---

## Módulo Maven `nishi-utils-oss` (decisão consolidada)

Novo módulo no monorepo (irmão de `nishi-utils-core` e `ngrid-test`):

```
nishi-utils/
├── nishi-utils-core/   (existente — não modificado)
├── ngrid-test/         (existente — não modificado)
└── nishi-utils-oss/    (NOVO)
    ├── pom.xml
    ├── src/main/java/dev/nishisan/utils/oss/...
    └── src/test/java/dev/nishisan/utils/oss/...
```

Justificativa: quem usa apenas `NMap`/`NQueue` não puxa AWS SDK. Release cycle independente. Parent POM (`/pom.xml`) declara o módulo em `<modules>`.

### Dependências do `nishi-utils-oss/pom.xml`

**Já no parent BOM (via Spring Boot 3.5.6):**
- `com.fasterxml.jackson.dataformat:jackson-dataformat-yaml` (YAML + manifest)
- `com.fasterxml.jackson.datatype:jackson-datatype-jsr310` (`Instant`/`Duration`)
- `org.slf4j:slf4j-api` (logging)
- `commons-codec:commons-codec` (SHA-256/CRC via `DigestUtils`)
- `org.apache.commons:commons-compress` (gzip — para uso futuro quando compressão for ativada)
- `org.junit.jupiter:junit-jupiter` (test)
- `org.testcontainers:testcontainers` (test)

**Novas (adicionar ao parent `<dependencyManagement>` + ao `pom.xml` do módulo):**
- `software.amazon.awssdk:bom` (BOM da AWS SDK v2 — pinned via property `aws.sdk.version`, ex.: `2.28.0`)
- `software.amazon.awssdk:s3` (cliente S3; MinIO/Ceph via `endpointOverride` + `pathStyleAccess`)
- `software.amazon.awssdk:url-connection-client` (HTTP client leve — evita pull do Netty completo se não usar async)
- `org.testcontainers:localstack` (test scope — IT contra S3 real-ish)
- _Opcional alternativa de IT_: `org.testcontainers:minio` ou imagem genérica MinIO

**JaCoCo no pom do módulo**: replicar a config de `nishi-utils-core` mas SEM excluir `dev/nishisan/utils/oss/**` — queremos cobertura desde o início.

---

## Roteiro de Implementação (cobertura completa do YAML)

Escopo único — entrega o YAML do enunciado funcionando integralmente (sem MVP enxuto). Fases são marcos de commit/PR atômicos, mas a definition of done é o conjunto inteiro.

### F0 — Scaffolding do módulo
- Cria `nishi-utils-oss/pom.xml` herdando do parent.
- Adiciona `<module>nishi-utils-oss</module>` ao `/pom.xml`.
- Configura JaCoCo no módulo (sem exclusões).
- Cria `/planning/ngrrd-design.md` com cópia deste plano.
- Branch: `feature/ngrrd-scaffolding`.
- Commit: `chore(oss): scaffolding do módulo nishi-utils-oss`.

### F1 — Definição YAML completa
- POJOs imutáveis em `definition/`: `NgrrdDefinition`, `TimeSpec`, `IdentitySpec`, `DataSourceDef`, `DeriveDef`, `ResetPolicyDef`, `ArchiveSpec`, `RraDef`, `ViewSpec`, `PresetDef`, `StorageSpec`, `QualitySpec`.
- `NgrrdYamlLoader` em `config/`: Jackson YAMLFactory + interpolação `${VAR}`/`${VAR:default}` (idioma do projeto, código próprio).
- `NgrrdDefinitionValidator` com todos os checks listados na seção "Validação".
- Resource de teste: `iface-traffic-errors-v1.yaml` (cópia literal do enunciado).
- Testes: carga, validação positiva e cenários de erro (rra.stepSec não múltiplo, derive.formula inválida, preset não resolvível).
- Branch: `feature/ngrrd-definition`. Commit atômico.

### F2 — Engine pura (sem IO)
- `TimeBucket.align(ts, step)`.
- `CounterDeriver`: aplica `delta*8/deltaT` (e variantes do `formula`); detecta reset via `maxResetDeltaRatio`; detecta wrap 32/64 via `counterBits`; emite flags `RESET`, `WRAP`; aplica `onReset`/`onWrap` (`unknown`/`auto`); `clampNegativeToZero`.
- `PrimaryDataPoint` por DS no `baseStepSec`.
- `RraConsolidator`: aplica `cf[]` (AVERAGE/MAX/MIN/LAST) e `xff` (se `missing/total > xff` → NaN).
- `LateSampleHandler`: `bucket_if_possible` (encaixa se bloco aberto), `late_drop` (descarta + métrica).
- `BestFitSelector`: maior `rra.stepSec ≤ targetStepSec` que cobre `window`, respeitando `fallbackOrder`.
- `RingBuffer` para o bloco em curso.
- Parser do `derive.formula` (subset: `delta`, `deltaT`, literais, `+ - * /`).
- Testes unitários cobrindo cada caso do `resetPolicy`, `xff`, `bestFit` para os 4 presets do YAML, late samples.
- Branch: `feature/ngrrd-engine`. Múltiplos commits atômicos por componente.

### F3 — Formato binário e manifest
- `BlockHeader` + `BlockCodec` (layout descrito na seção "Layout Binário").
- `BlockPayload` `rows × 8 bytes` (double IEEE-754; NaN = missing).
- Compressão: NÃO implementada no MVP. FLAGS já reserva bit `compressed`. Hook para gzip via commons-compress documentado como TODO ativável sem mudança de codec.
- CRC32 no header (`java.util.zip.CRC32`).
- `ManifestCodec`: Jackson YAML. Modelo: `version`, `seriesKey`, `definitionHash` (SHA-256 do YAML original), `rras[].blocks[]` (cada bloco com `blockStartEpoch`, `rows`, `crc32`, `storageKey`).
- Testes: round-trip de blocos, corrupção de CRC, manifesto com múltiplas versões.
- Branch: `feature/ngrrd-format`. Commit atômico.

### F4 — Storage backends (Disk + S3)
- `NgrrdStorage` interface (`put`, `get`, `list`, `exists`, `delete`, `atomicReplace`).
- `StorageKey` builder com prefixos do YAML (`rawPrefix`, `aggPrefix`, `manifestPrefix`, `schemaPrefix`).
- `LocalDiskStorage`: `java.nio.Files`, escrita via tmp + `Files.move(ATOMIC_MOVE, REPLACE_EXISTING)`.
- `S3Storage`: AWS SDK v2, suporta `endpointOverride` + `pathStyleAccess` (para MinIO/Ceph). `atomicReplace` via PUT direto (S3 não suporta atomic-rename, mas key determinística + idempotência cobrem).
- `StorageFactory` lê `StorageSpec.backend` e instancia.
- Testes unitários `LocalDiskStorage` com `@TempDir`.
- Testes IT `S3Storage`: novo profile `ngrrd-integration` que sobe LocalStack via Testcontainers; testes nomeados `*IT.java` rodam via Failsafe (`mvn verify -Pngrrd-integration`).
- Branch: `feature/ngrrd-storage`. Commits separados Disk e S3.

### F5 — Writer e Reader
- `NgrrdWriter`: thread única dedicada (padrão produtor-consumidor com `LinkedBlockingQueue`), recebe `Sample`, atravessa o pipeline da engine, mantém RingBuffer, dispara `BlockRotator` ao cruzar `blockSizeSec`. Idempotência aplicada na chave de storage (`writePolicy.idempotency.key` + `verify_or_replace_if_identical`).
- `BlockRotator`: detecção de cruzamento, encode via `BlockCodec`, persistência via `NgrrdStorage`.
- `ManifestUpdater`: thread agendada (`ScheduledExecutorService`) com `manifestPolicy.intervalSec`; lê estado atual e grava `manifest/<seriesKey>/v{N}.yaml`.
- `NgrrdReader`: resolve `RraDef` via `BestFitSelector`, lista blocos via manifesto (não via `Storage.list` — manifesto é fonte de verdade), lê em paralelo, decodifica, recorta para `window`, aplica `maxPoints` (downsample uniforme).
- `ViewExecutor`: traduz `PresetDef` → `ViewQuery` → `SeriesResult`.
- Testes e2e:
  - Disco: ingest sintético de 1h de samples a cada 5min em DS COUNTER (com 1 reset proposital), valida `daily` preset retornando ~12 pontos no intervalo.
  - S3 (LocalStack): mesma ingestão, valida persistência + leitura.
  - Recovery: restart do writer após gravar 2 blocos; verifica que `ManifestUpdater` reconstroi corretamente.
- Branch: `feature/ngrrd-writer-reader`. Commits atômicos.

### F6 — Métricas e Quality
- `NgrrdMetrics` com counters `missing_ratio`, `ingest_lag_sec`, `late_sample_count`, `counter_reset_count`, `wrap_detected_count` (todos do `quality.emitMetrics` do YAML).
- Listener simples (`NgrrdMetricsListener`) para integração externa (Micrometer, logs).
- Testes: induz cada cenário (atraso, reset, wrap) e valida que o contador correspondente incrementa.
- Branch: `feature/ngrrd-metrics`. Commit atômico.

### F7 — Facade `Ngrrd` e API pública
- `Ngrrd.fromYaml(Path)` → carrega + valida + instancia tudo (engine + writer + reader + storage + metrics).
- `Ngrrd.builder()` para casos programáticos (sem YAML).
- `NgrrdHandle` exposto: `write(SeriesKey, dsName, Sample)`, `read(presetName, tags)`, `read(ViewQuery)`, `close()`.
- Garante shutdown ordenado: flush do bloco aberto + último manifest snapshot.
- Testes: lifecycle (start → write → read → close) sem leaks de thread/file handle.
- Branch: `feature/ngrrd-facade`. Commit atômico.

### F8 — Documentação e diagramas
- `docs/oss/ngrrd.md` (visão geral, conceitos RRD, exemplos YAML, fluxos).
- `docs/diagrams/ngrrd_topology.puml` (C4 Container).
- `docs/diagrams/ngrrd_write_flow.puml` (sequência ingest).
- `docs/diagrams/ngrrd_read_flow.puml` (sequência view).
- Markdown embeda via `https://uml.nishisan.dev/proxy?src=...`.
- README do módulo (`nishi-utils-oss/README.md`) com quickstart.
- Branch: `docs/ngrrd`. Commit atômico.

### F9 — Integração final
- Atualiza `CLAUDE.md` raiz com referência a `nishi-utils-oss` em "Repository Structure".
- Smoke completo: cria `nishi-utils-oss/src/test/java/.../IfaceTrafficSmokeIT.java` que carrega o YAML do enunciado, ingere 30 dias sintéticos, valida todos os 5 presets (`daily`/`weekly`/`monthly`/`yearly`/`errors_daily`) e ambos backends (disco + S3 via LocalStack).
- Tag de release alinhada com convenção do projeto.
- Commit atômico.

---

## Esforço estimado (alto nível)

- F0 + F1 + F8: 1 dia
- F2 (engine RRD completa): 2 dias
- F3 (formato + manifest): 1 dia
- F4 (storage local + S3 + LocalStack): 1.5 dias
- F5 (writer + reader e2e): 2 dias
- F6 (métricas): 0.5 dia
- F7 (facade): 0.5 dia
- F9 (smoke + integração): 1 dia
- **Total estimado**: 9–10 dias úteis de implementação focada.

---

## Critérios de Aceite

1. Carrega o YAML do enunciado (`iface-traffic-errors-v1`) sem erro e expõe todos os campos.
2. Ingestão de COUNTER detecta reset/wrap conforme `resetPolicy`, emite `counter_reset_count`/`wrap_detected_count`.
3. Derivação `octets → bps` aplica `delta*8/deltaT`, `clampNegativeToZero`, `onReset: unknown`.
4. Persiste em disco com idempotência por bloco (re-execução = mesmo arquivo, mesmo checksum).
5. Persiste em S3 (LocalStack/MinIO via Testcontainers).
6. `views.presets[name=daily]` retorna ~288 pontos para janela `P1D` com `targetStepSec=300`.
7. `BestFitSelector` escolhe `rra_1h_6mo` para `weekly` e `rra_2h_1y` para `yearly`.
8. Recovery após restart lê último `manifest/v{N}.yaml` válido.
9. Cobertura mínima atende `jacoco.minimum.coverage=0.40`.

---

## Verificação (end-to-end)

```bash
# Build do monorepo (inclui o novo módulo)
mvn clean install -DskipTests

# Build isolado do módulo
mvn -pl nishi-utils-oss clean install

# Unit tests do módulo
mvn -pl nishi-utils-oss test

# Test específico
mvn -pl nishi-utils-oss test -Dtest='NgrrdYamlLoaderTest'

# Integration tests (LocalStack via Testcontainers)
mvn -pl nishi-utils-oss verify -Pngrrd-integration

# Smoke completo do YAML do enunciado (após F9)
mvn -pl nishi-utils-oss verify -Pngrrd-integration -Dit.test=IfaceTrafficSmokeIT

# Cobertura JaCoCo
mvn -pl nishi-utils-oss verify
# Relatório em: nishi-utils-oss/target/site/jacoco/index.html
```

### Cenários de validação manual (após F9)

1. **Carga do YAML completo**: `Ngrrd.fromYaml(Path.of("iface-traffic-errors-v1.yaml"))` retorna handle sem erro.
2. **Ingestão sintética**: gerar 30 dias × 288 amostras/dia para `in_octets`/`out_octets`/`in_errors`/`out_errors`; conferir `counter_reset_count` = 0 e `late_sample_count` = 0.
3. **Reset injetado**: simular `in_octets` que cai de 10¹² para 0 num passo; conferir `counter_reset_count` = 1 e `in_bps` no ponto = NaN (`onReset: unknown`).
4. **View daily**: ler preset `daily` para uma série e validar ~288 pontos.
5. **View yearly**: ler preset `yearly` para 365 dias e validar `maxPoints ≤ 5000`.
6. **Switch para S3**: alterar `storage.backend: objectStorage` apontando para LocalStack; repetir cenários 2–5.
7. **Restart**: derrubar o writer no meio de um bloco; reabrir; conferir que recovery via último `manifest/v{N}.yaml` válido reconstrói estado sem perda dos blocos já fechados.

---

## Documentação (DoD)

- `docs/oss/ngrrd.md` — visão geral, conceitos RRD, exemplos YAML, fluxo de escrita/leitura.
- `docs/diagrams/ngrrd_topology.puml` — C4 Container do pacote `oss`.
- `docs/diagrams/ngrrd_write_flow.puml` — sequência ingest → bloco → storage.
- `docs/diagrams/ngrrd_read_flow.puml` — sequência view → bestFit → blocos → series.
- Embed via `https://uml.nishisan.dev/proxy?src=<URL_RAW>` no markdown.
