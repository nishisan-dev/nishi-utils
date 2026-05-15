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

## 4. Pendências Abertas (F3 → F9)

| Fase | O que entrega | Esforço estimado |
|------|---------------|------------------|
| **F3** — Formato binário + manifest | `format/BlockHeader`, `BlockCodec` (magic `NGRD` + version + flags + stepSec + cf + ds + blockStartEpoch + rows + crc32 + payload), `ManifestCodec` (Jackson YAML, definitionHash SHA-256) | 1 dia |
| **F4** — Storage backends | `storage/NgrrdStorage` interface, `StorageKey`, `LocalDiskStorage` (atomic move), `S3Storage` (AWS SDK v2), `StorageFactory`. IT com LocalStack via Testcontainers no profile `ngrrd-integration`. | 1.5 dias |
| **F5** — Writer + Reader e2e | `writer/NgrrdWriter` (thread única + LinkedBlockingQueue), `BlockRotator`, `ManifestUpdater`, `reader/NgrrdReader`, `ViewExecutor`. Testes e2e em disco + S3 + recovery. | 2 dias |
| **F6** — Métricas/Quality | `metrics/NgrrdMetrics` com `missing_ratio`, `ingest_lag_sec`, `late_sample_count`, `counter_reset_count`, `wrap_detected_count`. | 0.5 dia |
| **F7** — Façade Ngrrd | `Ngrrd.fromYaml(Path)` + `Ngrrd.builder()`. `NgrrdHandle` com `write/read/close`, shutdown ordenado. | 0.5 dia |
| **F8** — Documentação | `docs/oss/ngrrd.md`, diagramas PlantUML em `docs/diagrams/`, README do módulo. | 1 dia |
| **F9** — Smoke completo | `IfaceTrafficSmokeIT` carregando o YAML do enunciado, ingestão de 30 dias sintéticos, valida os 5 presets em disco e S3. Atualiza `CLAUDE.md` raiz. | 1 dia |

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
