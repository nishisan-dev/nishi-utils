# Roadmap evolutivo — Estado/Enum no NGRRD + Generalização de entidades (TEMS)

## Context

Hoje o pipeline de telemetria TEMS é: **ape-probe** coleta via SNMP/ICMP e publica um snapshot por ciclo no Kafka (`ape.metrics.snapshot`, DTO `MetricSnapshotDto` em `tevent-lib`) → **ngrrd-consumer** mapeia cada métrica para uma série e persiste no **NGRRD** (lib `nishi-utils-oss`, um RRD-like de `double`) → **webapp v2** (React/ECharts) renderiza dashboards.

Três necessidades motivaram este roadmap:

1. **Estado/contexto junto dos contadores.** Poder gravar `ifOperStatus` (e, genericamente, qualquer enum/booleano) lado a lado com os contadores de tráfego — inclusive para alimentar, no futuro, um modelo de variação de tráfego que precisa saber quais interfaces estavam ON/OFF numa janela. O NGRRD hoje só guarda `double`; texto puro como série temporal não é suportado (e o modelo de ML consome número, não string, então isso é aceitável).
2. **Ingestão desacoplada do dashboard.** Já é verdade hoje (a persistência é dirigida só por config de mapping; o catálogo Mongo auto-descobre séries) — confirmado, não é trabalho.
3. **Generalização.** O webapp v2 está preso ao shape "device = grade de interfaces IP". A visão é chegar a algo tão flexível quanto Cacti/Grafana (fonte gerenciável com bateria/temperatura, host Linux com cpu/mem/disco) — sem jogar fora a UI de rede atual, que é boa como produto out-of-the-box.

**Resultado pretendido:** um trilho único faseado em que cada fase entrega valor sozinha — Fase 1 fecha o contrato de estado/enum ponta a ponta; Fases 2+ pavimentam o caminho de views customizadas parametrizáveis por YAML sobre a UI OOTB.

---

## Decisões travadas (brainstorm)

| Decisão | Escolha |
|---|---|
| Forma do roadmap | **Trilho único faseado** (cada fase entrega valor isolada) |
| Representação de estado/enum | **Numérico + dicionário** (GAUGE 0/1/ordinal + `dictionary` ordinal↔label; texto puro fica no catálogo). NGRRD continua grid de `double`, zero mudança de formato binário |
| Onde mora o contrato | **First-class no nishi-utils** (`dictionary` no schema de `DataSourceDef`) — série auto-descritiva; o label flui via `/meta` → webapp |
| Onde gravar o estado | **Mesma série** da interface, via migração guardada (`OnGeometryChange.MIGRATE` + `schemaRevision`) |
| Consolidação do estado | **Opção A (zero risco no formato):** RRA paralela `cf:[LAST,MAX]` + **guard no validator** que barra DS-com-dictionary arquivado só com AVERAGE. Sem tocar a geometria binária |
| ML | **Fora de escopo** — só garantir estado bem persistido/exportável; o modelo é projeto à parte |
| Generalização frontend | Manter a UI de rede **OOTB** + adicionar **camada de custom views via YAML** (Grafana/Cacti-like); não entregar tudo agora, mas pavimentar |
| Entidade piloto | Genérico primeiro; piloto concreto (fonte gerenciável / host Linux) decidido na Fase 2 |

---

## Achados de código que sustentam o plano

- **NGRRD aceita múltiplos DS por série e tipo GAUGE gravado as-is** (`SampleDeriver.java:44`); a Ape **já coleta e emite `ifOperStatus`** (OID `1.3.6.1.2.1.2.2.1.8`) como célula ENUM com `rawValue = ordinal` (1=up/2=down), cadência `status`.
- **`appliesTo` é global por série** (um único bloco `archives` — `NgrrdSpec.java:18`, `ArchiveSpec.java:14`; offsets em `SeriesGeometry.java:229-232` assumem colunas globais). → motiva a Opção A (RRA paralela + validator), não per-coluna.
- **Migração de "adicionar DS" é suportada e preserva histórico** (`GeometryReconciler.java:74-99`, `GeometryMigrator` backfilla NaN; teste existente `NgrrdGeometryMigrationTest.migrateNoYamlPreservaHistoriaAoAdicionarDataSource`). **Exige bump de `metadata.schemaRevision`** — hoje ausente (=0); sem o bump, **toda série existente falha ao abrir** e a ingestão para.
- **Não criar mapping novo para a cadência `status`.** Cache de handle é `(mappingName, seriesKey)` e o `.ngrr` é só por `seriesKey` (`NgrrdHandleCache.java:107`, `StorageKey.java:29-41`) → dois mappings na mesma `seriesKey` = **dois escritores no mesmo arquivo**. Confirmado que `performance` e `status` rendem a **mesma `seriesKey`** (`device:{deviceId}/iface:{ifaceId}/group:traffic-v1`, só usa deviceId+ifaceId). → **estender o mapping existente**.
- **`MetricMappingEngine.appendCells` grava `cell.rawValue()` se for `Number`** (`MetricMappingEngine.java:60-71`), sem caso especial de ENUM; se a Ape mandar o label textual em vez do ordinal, a célula é silenciosamente descartada → adicionar fallback `enumOrdinal()`.
- **DS meta é computada ao vivo da definition** em `SeriesReadService.dataSourcesMeta` (`web/SeriesReadService.java:343-357`), servida em `GET /api/v1/series/{id}/meta`. O catálogo Mongo **não** guarda meta por-DS → surfacing do `dictionary` é só uma linha aqui (sem mudança no Mongo).
- **Webapp:** painéis são derivados por `unit` em `V2IfacePage.tsx:84-97`, mas **DS sem `derived` é pulado** (linha 88) → `oper_status` (GAUGE cru) não aparece sozinho; precisa de painel dedicado (padrão `pingPanels.ts`/`V2PingPage.tsx`). `SeriesChart.tsx:193-209` hard-coda `type:'line'` → precisa de modo `state`. `PanelDef` em `IfacePanels.tsx:9-13` (`SystemPanelDef` já é precedente de extensão).
- **Catálogo não tem query por tag/glob arbitrário** — seleção por `mappingName` + facets fixos + `treePath` (prefixo) + `q` free-text. Relevante para a Fase 2.

---

## Fase 1 — Contrato de estado/enum ponta a ponta (entrega: `ifOperStatus` no chart de interface)

Objetivo: gravar e visualizar estado de interface junto dos contadores, com contrato genérico reutilizável para qualquer enum/booleano.

### 1A. nishi-utils-oss (lib) — `dictionary` first-class + guard de consolidação

- **`definition/DataSourceDef.java`**: adicionar componente opcional `@JsonProperty("dictionary") Map<Integer,String> dictionary` (com default `Map.of()` no compact-constructor, padrão de `AppliesTo.java:21-24`). Jackson é tolerante (`@JsonIgnoreProperties(ignoreUnknown=true)` + `FAIL_ON_UNKNOWN_PROPERTIES=false`). **Não entra no `geometryHash`** (`SeriesGeometry.java:239-253`) → mudar dicionário não dispara migração.
- **`config/NgrrdDefinitionValidator.java`**: nova checagem cross-field (chamada de `validate(...)`, ~linha 58, modelada em `validateArchives`): se um DS tem `dictionary` não-vazio, exigir que ao menos uma RRA que o cobre declare um CF não-AVERAGE (`LAST`/`MAX`/`MIN`); senão lançar `NgrrdDefinitionException`. Mirar teste em `NgrrdDefinitionValidatorTest`.
- **Testes**: parse de `dictionary`; guard do validator (positivo e negativo); read de um GAUGE com dictionary preservando valor as-is (mirar `NgrrdGaugeFacadeTest`). Migração "adiciona DS" já coberta por `NgrrdGeometryMigrationTest`.
- **Release**: bump `nishi-utils-parent` 7.2.0 → **7.3.0**; `mvn clean install`; release via `gh` (changelog: dictionary + validator guard). Sem esse release o consumer não enxerga `dictionary()`.

### 1B. ngrrd-consumer — definition, mapping e surfacing

- **`config/definitions/iface-traffic-v1.yaml`**:
  - Novo DS `oper_status` (`type: GAUGE`, `heartbeatSec` tolerante à cadência `status`) com `dictionary: {1: up, 2: down, 3: testing, 4: unknown, 5: dormant, 6: notPresent, 7: lowerLayerDown}` (IF-MIB).
  - Incluir `oper_status` no `archives.appliesTo.include`.
  - Nova RRA paralela `cf: [LAST, MAX]` (mesmos `stepSec`/`rows` das atuais). Aceita-se os anéis "desperdiçados" (LAST-de-contador, AVERAGE-de-estado) — nunca lidos.
  - **`metadata.schemaRevision: 1`** (obrigatório). `onGeometryChange: MIGRATE` já está setado (linha ~228).
- **`config/mappings/iface-traffic-v1.yaml`** (estender o existente, NÃO criar novo):
  - `metricMap += ifOperStatus: oper_status`.
  - `match.cadences: [performance, status]`.
- **`MetricMappingEngine.appendCells`** (`src/main/java/.../MetricMappingEngine.java:56-74`): fallback defensivo — se `cell.rawValue()` não for `Number` e o cell for ENUM, usar `cell.enumOrdinal()` como valor da amostra. (Garante robustez caso a Ape mande label.)
- **`web/SeriesReadService.java:343-357`** (`dataSourcesMeta`): `ds.put("dictionary", dataSource.dictionary())` — surfacing no `/api/v1/series/{id}/meta`. Sem mudança no catálogo Mongo.
- **`src/main/resources/openapi/openapi.yaml`**: adicionar `dictionary` ao schema `DataSourceMeta`.
- **`pom.xml`**: `nishi-utils-core.version` → 7.3.0.
- **Ape**: nenhuma mudança em Fase 1 (`ifOperStatus` já é coletado/emitido na cadência `status` com `rawValue=ordinal`). Apenas validar que essa cadência está ativa no plano do device-alvo.

### 1C. webapp — lane de estado (step/banda) humanizada

- **`src/api/types.ts`**: `DataSourceMeta += dictionary?: Record<string,string>`; estender `PanelDef` (em `IfacePanels.tsx:9-13` ou tipo derivado) com `render?: 'line' | 'state'` e `dictionary?: Record<string,string>`.
- **`src/components/SeriesChart.tsx`** (193-209, 218-242): modo `render==='state'` — `type:'line'` com `step:'end'`, `connectNulls:false`, sem smooth/area; `yAxis` categórico (ou faixa fixa) usando os labels do dicionário; humanizar `tooltip.valueFormatter` (224) e `yAxis.axisLabel.formatter` (238) via `dictionary` (mesmos hooks do `bit/s`). Opcional: bandas de cor up/down via `markArea`.
- **`src/v2/pages/V2IfacePage.tsx`** (84-97, 108-131, 211-220): injetar painel dedicado de `oper_status` (`unit:'state'`, `render:'state'`, `dictionary` vindo do `/meta`), à parte do agrupamento por `unit` (que pula DS sem `derived`). No caminho live SSE, tratar valor de gauge cru (não só `rateBps`/`ratePerSecond`). O fetch de dados do painel de estado deve pedir **`cf=LAST`** (verificar o parâmetro `cf` no `/series/{id}/data`).
- **`DataPanelV2.tsx`**: repassar `render`/`dictionary` ao `SeriesChart`; `isBps` (linha 70) continua `false` para `unit:'state'`, desligando overlays de capacidade.

### Migração operacional (Fase 1)

1. Releasar/instalar nishi-utils 7.3.0; bumpar o consumer.
2. **Dry-run** da migração numa cópia do diretório de dados de produção (`NgrrdMigrateCli --dry-run`) confirmando que adicionar `oper_status` é `structurallyMigratable` e preserva histórico.
3. Deploy do consumer com `schemaRevision: 1` + `MIGRATE`. As séries existentes migram on-open (coluna nova nasce NaN); novas amostras de `oper_status` começam a popular.

### Verificação (Fase 1)

- `mvn test` em nishi-utils (validator + dictionary + gauge as-is) e `mvn clean install` (multi-módulo).
- `mvn test` no consumer; rodar contra um snapshot de `samples/` com cadência `status`; via API: `GET /series/{id}/data?ds=oper_status&cf=LAST` retorna 1/2; `GET /series/{id}/meta` retorna `dictionary`; contadores intactos.
- Confirmar migração sem perda de histórico nas séries pré-existentes.
- webapp: build + subir; abrir página da interface → lane de estado com up/down humanizado, alinhada à curva de tráfego.

---

## Fase 2 — Custom views parametrizáveis via YAML (sobre a UI OOTB)

Objetivo: habilitar dashboards customizados dirigidos por config, reutilizando os componentes existentes, sem tocar nas páginas curadas de rede. Onboarding de uma entidade não-rede vira **só config**.

- **Schema de view (YAML)** em `config/views/*.yaml`: `{ id, title, entitySelector (mappingName + facets + treePath/q), panels: PanelDef[] (com unit/title/ds/render/dictionary) }`. Carregado pelo `NgrrdYamlLoader` (já usado pelo consumer).
- **Backend**: `ViewService` + endpoints `GET /api/v1/views` e `/views/{id}` em `web/WebServer.java`; schema no `openapi.yaml`.
- **webapp**: `ViewConfig` em `types.ts`; `fetchView(id)` em `api/client.ts`; `src/v2/pages/V2ViewPage.tsx` (resolve séries via `searchSeries(entitySelector)` e renderiza `panels.map(<DataPanelV2/>)`, estruturalmente igual ao `V2PingPage.tsx`); rota em `src/App.tsx` (template: rota de ping). Reuso total de `DataPanelV2`/`SeriesChart` (incl. o modo `state` da Fase 1).
- **Onboarding da entidade piloto** (fonte gerenciável ou host Linux — decidir no início da Fase 2): receita 100% config — Ape `custom-metrics.yaml` (já provado com `deviceTemperature`) → consumer `config/definitions` + `config/mappings` → `config/views` YAML.
- **Restrição conhecida**: seleção por `mappingName`/facets/`treePath`/`q`. Se a view exigir match por tag arbitrária/glob de seriesKey, isso vira mudança no `/series` (catálogo) — empurrar para Fase 3.

---

## Fase 3 — Norte Cacti/Grafana-like (pavimentar, não entregar agora)

Documentado como direção, fora do escopo de entrega imediata:

- Editor de views na própria UI (persistir views no Mongo) em vez de só YAML.
- Mais tipos de chart, thresholds configuráveis, templating/variáveis (estilo Grafana).
- Query por **tag arbitrária/glob** no catálogo (`/series` + camada de query) — habilita selectors realmente livres.
- Catálogo de tipos de entidade.
- **Ponte de ML/export** (motivação original): capability de extrair features alinhadas no tempo (séries + estado ON/OFF por janela) do NGRRD para treino. Fora do escopo agora; o contrato de estado da Fase 1 já deixa o dado pronto.

---

## Riscos e mitigações

| Risco | Mitigação |
|---|---|
| Esquecer `schemaRevision` → ingestão para (todas as séries falham ao abrir) | Checklist de deploy; dry-run obrigatório; teste que valida o bump |
| Ape mandar label textual em `rawValue` → estado descartado | Fallback `enumOrdinal()` no `appendCells` + teste |
| Mexer no release da lib usada em produção pesada (Cardinal) | Mudança é aditiva e fora do `geometryHash`; suite completa + release versionado |
| Anéis "desperdiçados" da Opção A | Bounded e pequenos; documentar; per-RRA appliesTo fica como otimização futura opcional |
| Migração de geometria em produção | `NgrrdMigrateCli --dry-run` em cópia dos dados antes do deploy |

## Documentação (DoD — CLAUDE.md §7)

- `docs/` do ngrrd-consumer: atualizar com o contrato de estado/enum (dictionary, consolidação LAST/MAX, migração) e, na Fase 2, o schema de custom views.
- `docs/diagrams/`: diagrama de sequência `state_ingestion_flow.puml` (Ape→consumer→NGRRD→/meta→webapp) e, na Fase 2, C4 container atualizado com a camada de views.
- Plano aprovado copiado para `/planning` no início da execução; commits atômicos por fase/sub-fase; branch `feature/ngrrd-state-enum-contract`.
