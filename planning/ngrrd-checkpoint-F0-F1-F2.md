# Checkpoint ngrrd — F0/F1/F2 concluídas

> **Data:** 2026-05-15
> **Branch atual:** `feature/ngrrd-engine` (HEAD)
> **Pre-requisito:** plano completo em [`/planning/ngrrd-design.md`](./ngrrd-design.md)

---

## 1. Estado da Arquitetura

### Módulo `nishi-utils-oss` (publicável independentemente)

```
nishi-utils-oss/
├── pom.xml                      (herda do parent; AWS SDK v2 BOM importado; profile ngrrd-integration)
└── src/
    ├── main/java/dev/nishisan/utils/oss/
    │   ├── Ngrrd.java                            (façade stub — expandida em F7)
    │   ├── api/                                  (13 enums + Sample/DataPoint/SeriesResult/ViewQuery)
    │   ├── definition/                           (23 records imutáveis do YAML)
    │   ├── config/                               (NgrrdYamlLoader, NgrrdDefinitionValidator,
    │   │                                          VariableInterpolator, NgrrdDefinitionException)
    │   └── engine/                               (8 componentes puros: TimeBucket, FormulaEvaluator,
    │                                              CounterDeriver, PrimaryDataPoint, RraConsolidator,
    │                                              LateSampleHandler, BestFitSelector, RingBuffer)
    └── test/
        ├── java/dev/nishisan/utils/oss/
        │   ├── NgrrdTest                         (smoke do stub)
        │   ├── config/                           (Variable, Loader, Validator — 18 testes)
        │   └── engine/                           (8 classes de teste — 43 testes)
        └── resources/
            └── iface-traffic-errors-v1.yaml      (cópia do YAML do enunciado, apiVersion ngrrd/v1)
```

### Pacotes raiz (`/pom.xml`)

- Novo `<module>nishi-utils-oss</module>` registrado.
- `aws.sdk.version=2.28.0` e `testcontainers.localstack.version=1.21.3` adicionados em properties.
- AWS SDK v2 BOM importado no `<dependencyManagement>`.

---

## 2. Decisões Arquiteturais Consolidadas

1. **Nome do formato:** `ngrrd` (Nishi Grid Round-Robin Database). Pacote Java: `dev.nishisan.utils.oss`. `apiVersion` no YAML: `ngrrd/v1`. Kind: `MetricSeriesDefinition`.
2. **Módulo Maven separado** (`nishi-utils-oss`) — não vive dentro do `nishi-utils-core`.
3. **Escopo de entrega:** cobertura completa do YAML do enunciado (sem MVP enxuto).
4. **S3 SDK:** AWS SDK v2 (`software.amazon.awssdk:s3` + `url-connection-client`). MinIO/Ceph atendidos via `endpointOverride` + `pathStyleAccess`.
5. **Compressão de bloco:** ausente no MVP; bit `compressed` reservado em FLAGS. Gzip via `commons-compress` é ativável no futuro sem mudar o codec.
6. **Não força reuso** de NMap/NQueue/NGrid — `ngrrd` é construto autônomo. Apenas o idioma do projeto (Builder, façade estática, loader YAML com `${VAR:default}`) é replicado em código próprio.
7. **Heurística wrap vs reset (CounterDeriver):** tenta wrap primeiro quando `counterBits` está configurado e `detectCounterReset = true`. Só declara reset se `wrappedDelta ≥ maxValue/2` (queda grande demais para ser overflow).
8. **Determinismo:** todo bucket é alinhado em múltiplos de `baseStepSec` desde epoch UTC. Timestamps são sempre do sample, nunca do servidor.

---

## 3. Fases Concluídas

| Fase | Branch | Status | Cobertura |
|------|--------|--------|-----------|
| F0 — Scaffolding | `feature/ngrrd-scaffolding` | ✓ commit `a9acc0b` | NgrrdTest |
| F1 — Definição YAML + Loader + Validator | `feature/ngrrd-definition` | ✓ commit (1 commit atômico) | 18 testes |
| F2 — Engine pura (sem IO) | `feature/ngrrd-engine` (HEAD) | ✓ commit (1 commit atômico) | 43 testes |

**Total atual:** **62 testes verdes**, JaCoCo ≥ 40% atendido, `mvn -pl nishi-utils-oss -am verify` passa limpo.

---

## 4. Pendências Abertas (F3 → F9) — Checklist Detalhado

### F3 — Formato binário e manifesto (~1 dia)
**Branch:** `feature/ngrrd-format` (criar a partir de `feature/ngrrd-engine`).
**Subpacote alvo:** `dev.nishisan.utils.oss.format`.

- [ ] `BlockHeader` record com layout: `MAGIC(4) "NGRD" | VER(2) | FLAGS(2) | STEP(4) | CF(1) | DS(1) | BLOCK_START_EPOCH(8) | ROWS(4) | CRC32(4)`.
- [ ] `BlockCodec.encode(BlockHeader, double[] payload) -> byte[]` e `decode(byte[]) -> {header, payload}`. Sem timestamps no payload — derivam de `blockStartEpoch + i*stepSec`. NaN = missing.
- [ ] FLAGS reserva bits: `compressed`, `partial` (bloco ainda não fechado), `agg` (RRA vs raw). Implementar accessors mas deixar `compressed` sempre falso no MVP.
- [ ] CRC32 (`java.util.zip.CRC32`) sobre header (excluindo o próprio campo CRC) + payload.
- [ ] `ManifestCodec` (Jackson YAML) com modelo: `version`, `seriesKey`, `definitionHash` (SHA-256 do YAML original), `rras[].blocks[]` (cada com `blockStartEpoch`, `rows`, `crc32`, `storageKey`).
- [ ] Testes: round-trip block (encode→decode), round-trip manifest, detecção de CRC corrompido (mutação de 1 byte deve falhar), manifest com múltiplas versões.
- [ ] Commit atômico: `feat(oss): formato binário NGRD e manifesto versionado`.

### F4 — Storage backends LocalDisk + S3 (~1.5 dias)
**Branch:** `feature/ngrrd-storage`.
**Subpacote alvo:** `dev.nishisan.utils.oss.storage`.

- [ ] `NgrrdStorage` interface: `put(key, bytes)`, `get(key)`, `list(prefix)`, `exists(key)`, `delete(key)`, `atomicReplace(key, bytes)`.
- [ ] `StorageKey` builder com prefixos do YAML (`rawPrefix`, `aggPrefix`, `manifestPrefix`, `schemaPrefix`) e template `{seriesKey}/{ds}/{stepSec}/{blockStartEpoch}`.
- [ ] `LocalDiskStorage`: `java.nio.Files`, escrita via tmp + `Files.move(ATOMIC_MOVE, REPLACE_EXISTING)`. Cria diretórios pais on demand.
- [ ] `S3Storage`: AWS SDK v2 (`S3Client` síncrono com `UrlConnectionHttpClient`). Suporta `endpointOverride(URI)` + `S3Configuration.pathStyleAccessEnabled(true)` (MinIO/Ceph). `atomicReplace` via PUT direto (S3 não suporta atomic rename; key determinística cobre).
- [ ] `StorageFactory.from(StorageSpec)` — instancia conforme `backend` (LOCAL_DISK ou OBJECT_STORAGE).
- [ ] Implementação de `verify_or_replace_if_identical`: lê existente, compara SHA-256/CRC32, decide. Pode viver dentro do `NgrrdStorage` ou em wrapper.
- [ ] Testes unitários `LocalDiskStorageTest` com `@TempDir`.
- [ ] **IT `S3StorageIT`** em `*IT.java` para Failsafe; usa `org.testcontainers:localstack:1.21.3` (já declarado). Profile `ngrrd-integration` no `pom.xml` já configurado. Validar: `mvn -pl nishi-utils-oss verify -Pngrrd-integration`.
- [ ] Commit atômico: `feat(oss): storage backend local + S3 com IT LocalStack`.

### F5 — Writer + Reader e2e (~2 dias)
**Branch:** `feature/ngrrd-writer-reader`.
**Subpacotes alvo:** `dev.nishisan.utils.oss.writer` e `dev.nishisan.utils.oss.reader`.

- [ ] `NgrrdWriter`: thread única dedicada (`LinkedBlockingQueue<Sample> + worker`), recebe `Sample`, atravessa engine (`CounterDeriver` → `PrimaryDataPoint` → `RraConsolidator`), mantém `RingBuffer` por RRA por DS.
- [ ] Mapeamento por DS: cada DS COUNTER (raw, ex.: `in_octets`) gera o DS derivado (ex.: `in_bps`). PDPs/CDPs são computados sobre o DERIVADO; os RRAs do YAML referenciam o DS derivado por nome em `archives.appliesTo.include`.
- [ ] `BlockRotator`: detecta cruzamento de `blockSizeSec` e dispara `BlockCodec.encode(...)` + `NgrrdStorage.put(...)`.
- [ ] Idempotência: aplicada no Storage (não no Writer). Usa `verify_or_replace_if_identical` (F4).
- [ ] `ManifestUpdater`: `ScheduledExecutorService` que grava `manifest/<seriesKey>/v{N}.yaml` a cada `manifestPolicy.intervalSec`. Lê última versão para detectar `N`. Reuso do `ManifestCodec` (F3).
- [ ] `NgrrdReader`: resolve RRA via `BestFitSelector` (F2) e lista blocos pelo MANIFESTO (fonte de verdade — não usar `Storage.list`). Lê blocos em paralelo, decodifica, recorta para `window`, aplica `maxPoints` (downsample uniforme).
- [ ] `ViewExecutor.run(presetName, tags)`: resolve seriesKey via `IdentitySpec.seriesKeyTemplate`, traduz `PresetDef` → `ViewQuery`, delega ao `NgrrdReader`.
- [ ] Testes e2e:
    - [ ] **Disco**: ingest sintético 1h de samples 5min em DS COUNTER (com 1 reset proposital), valida `daily` preset com ~12 pontos.
    - [ ] **S3 (LocalStack IT)**: mesma ingestão, valida persistência + leitura.
    - [ ] **Recovery**: restart do writer após 2 blocos; valida que `ManifestUpdater` reconstroi via último `v{N}.yaml` válido.
- [ ] Commit: separar `feat(oss): writer ngrrd` e `feat(oss): reader ngrrd` se útil.

### F6 — Métricas e Quality (~0.5 dia)
**Branch:** `feature/ngrrd-metrics`.
**Subpacote alvo:** `dev.nishisan.utils.oss.metrics`.

- [ ] `NgrrdMetrics` com counters/gauges para os 5 nomes em `quality.emitMetrics`: `missing_ratio`, `ingest_lag_sec`, `late_sample_count`, `counter_reset_count`, `wrap_detected_count`.
- [ ] Injeção: o `NgrrdWriter` aceita um `NgrrdMetrics` opcional via construtor; engine emite via callbacks (não polui as funções puras).
- [ ] `NgrrdMetricsListener` interface — integração externa (Micrometer, logs, JMX).
- [ ] Testes: induz cada cenário (sample atrasado → `late_sample_count`++; counter reset → `counter_reset_count`++; wrap → `wrap_detected_count`++).
- [ ] Commit atômico: `feat(oss): métricas de quality ngrrd`.

### F7 — Façade Ngrrd e API pública (~0.5 dia)
**Branch:** `feature/ngrrd-facade`.
**Arquivo principal:** `dev.nishisan.utils.oss.Ngrrd` (expandir o stub atual).

- [ ] `Ngrrd.fromYaml(Path)` → carrega definição (F1) + valida + monta storage (F4) + writer (F5) + reader (F5) + métricas (F6). Retorna `NgrrdHandle`.
- [ ] `Ngrrd.builder()` para casos programáticos sem YAML.
- [ ] `NgrrdHandle` interface pública: `write(SeriesKey, dsName, Sample)`, `read(presetName, Map<String,String> tags)`, `read(ViewQuery)`, `close()`.
- [ ] `close()` garante shutdown ordenado: flush do bloco aberto + última gravação do manifest snapshot + finalização do thread do writer.
- [ ] Testes: lifecycle (start → write → read → close) sem leaks de thread/file handle (use `Thread.getAllStackTraces()` ou Awaitility).
- [ ] Commit atômico: `feat(oss): façade Ngrrd com fromYaml e shutdown ordenado`.

### F8 — Documentação e diagramas (~1 dia)
**Branch:** `docs/ngrrd`.

- [ ] `docs/oss/ngrrd.md` — visão geral, conceitos RRD (DS/RRA/CF/XFF), exemplos YAML, fluxo de escrita/leitura, exemplos de código.
- [ ] `docs/diagrams/ngrrd_topology.puml` — C4 Container do pacote `oss`.
- [ ] `docs/diagrams/ngrrd_write_flow.puml` — sequência ingest → engine → bloco → storage.
- [ ] `docs/diagrams/ngrrd_read_flow.puml` — sequência view → bestFit → manifest → blocos → series.
- [ ] Markdown embeda diagramas via `https://uml.nishisan.dev/proxy?src=<URL_RAW>`.
- [ ] `nishi-utils-oss/README.md` com quickstart (Maven, exemplo `Ngrrd.fromYaml`).
- [ ] Commit atômico: `docs(oss): documentação e diagramas do ngrrd`.

### F9 — Smoke completo e integração final (~1 dia)
**Branch:** `feature/ngrrd-smoke`.

- [ ] `IfaceTrafficSmokeIT` (em `nishi-utils-oss/src/test/java/.../IfaceTrafficSmokeIT.java`):
    - [ ] Carrega `iface-traffic-errors-v1.yaml` via `Ngrrd.fromYaml(...)`.
    - [ ] Ingere 30 dias × 288 amostras/dia para `in_octets`/`out_octets`/`in_errors`/`out_errors`.
    - [ ] Valida os 5 presets do YAML: `daily` (~288 pontos), `weekly` (168), `monthly` (720), `yearly` (~4380 limitado a 5000), `errors_daily`.
    - [ ] Roda em ambos backends: disco (`@TempDir`) e S3 (LocalStack via Testcontainers).
    - [ ] Cenário de reset injetado: valida `counter_reset_count == 1` e `in_bps` no ponto = NaN.
- [ ] Atualiza `/CLAUDE.md` raiz: adicionar `nishi-utils-oss` em "Repository Structure".
- [ ] Tag de release alinhada com convenção do projeto (`gh release create` se disponível).
- [ ] Commit atômico: `feat(oss): smoke completo IfaceTraffic + integração final`.

---

## 5. Pontos de Atenção para a Próxima Sessão

1. **Branch atual:** `feature/ngrrd-engine`. Próxima fase (F3) deve começar com `git checkout -b feature/ngrrd-format`.
2. **Layout binário do bloco** (acordado): `MAGIC(4) "NGRD" | VER(2) | FLAGS(2) | STEP(4) | CF(1) | DS(1) | BLOCK_START_EPOCH(8) | ROWS(4) | CRC32(4) | PAYLOAD = ROWS × 8 bytes (double IEEE-754; NaN = missing)`. Sem timestamps no payload — derivados de `blockStartEpoch + i*stepSec`.
3. **Manifest layout** (acordado): Jackson YAML; campos `version`, `seriesKey`, `definitionHash` (SHA-256 do YAML original), `rras[].blocks[]` com `blockStartEpoch`, `rows`, `crc32`, `storageKey`.
4. **Idempotência:** key determinística `{seriesKey}/{ds}/{stepSec}/{blockStartEpoch}` resolve `verify_or_replace_if_identical`. Implementar no Storage (não no Writer).
5. **CounterDeriver e DataSourceDef:** O DS derivado (`in_bps`) hoje é apenas um placeholder em `derive.output.name`. O Writer (F5) precisa mapear cada DS COUNTER para um "DS lógico derivado" usado para gerar PDPs/CDPs.
6. **LocalStack IT:** já há a dependência `org.testcontainers:localstack:1.21.3` declarada. Profile Failsafe `ngrrd-integration` ativa `*IT.java` em `nishi-utils-oss/`. Não esquecer `TESTCONTAINERS_RYUK_DISABLED=true` (já no profile).
7. **JaCoCo:** módulo está sem exclusões (`dev/nishisan/utils/oss/**` totalmente coberto pelo gate de 40%). Adicionar mais testes em F3-F5 mantém isso confortável; F4 (storage S3) pode pressionar — considerar excluir `dev/nishisan/utils/oss/storage/S3Storage` se IT não rodar no `mvn verify` default.
8. **package-info.java:** sempre criar um para novos subpacotes para evitar warnings do javadoc.

---

## 6. Próximos Passos Sugeridos

Iniciar nova sessão de chat. Mensagem inicial sugerida ao agente:

> Continuação do projeto ngrrd. Estado atual em `/planning/ngrrd-checkpoint-F0-F1-F2.md`. Branch HEAD: `feature/ngrrd-engine`. Implementar F3 (formato binário + manifest) conforme o plano em `/planning/ngrrd-design.md`. Criar branch `feature/ngrrd-format`, codificar `BlockHeader`/`BlockCodec`/`ManifestCodec` com round-trip tests e teste de detecção de corrupção via CRC32. Commit atômico ao final.

---

## 7. Verificação Local

```bash
# A partir de qualquer branch:
git log --oneline -10                      # confirma commits F0/F1/F2
mvn -pl nishi-utils-oss -am verify         # passa com 62 testes verdes e JaCoCo >= 40%
ls nishi-utils-oss/src/main/java/dev/nishisan/utils/oss/  # api  config  definition  engine  Ngrrd.java  package-info.java
```
