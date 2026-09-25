# Issue #171 — Consultar existência de série e abrir sem criar (local e cluster)

Status: design aprovado em 2026-09-25. Versão alvo: **8.6.0**. Consumidor: nishisan-dev/tems#78.

## Motivação

Hoje todo `open` cria a série quando ela não existe (local: `NgrrdWriter.openOrCreate`; cluster:
`PlacementResolver` faz `PLACE` e o storage cria o arquivo no `OPEN`). O TEMS percorre um catálogo
externo (Mongo) com centenas de milhares de séries por rodada; uma entrada sem arquivo nos storages
vira uma série vazia pré-alocada no primeiro acesso. A aplicação precisa detectar o caso, não criar e,
quando for comprovadamente inexistente, limpar a entrada do catálogo externo.

## Decisões

1. **Existência no cluster = placement no catálogo** (`ngrrd.catalog`). `MIGRATING` conta como
   existente.
2. **`false` só é definitivo depois do líder.** Hit na réplica local responde sem RPC; miss local é
   confirmado no líder em lote (`ngrrd.catalog.lookup`). Qualquer falha ao perguntar lança exceção —
   nunca vira `false`. Só um `false` confirmado (ou `SeriesNotFoundException`) autoriza o consumidor a
   remover a entrada do catálogo externo.
3. **Lotes limitados.** `ngrrd.catalog.lookup` e a verificação por nó são paginados em chamadas de no
   máximo `catalogLookupBatchSize` chaves (default 2000), sequenciais. Mensagens grandes disputando a
   conexão com a ingestão estouram o tempo e derrubam a conexão (visto na #169).
4. **Abrir sem criar** é uma flag em `Ngrrd.OpenOptions` (`createIfMissing`, default `true`),
   uniforme nos dois modos. O default preserva o comportamento atual.
5. **Exceção única** `dev.nishisan.utils.oss.api.SeriesNotFoundException` nos dois modos. Não herda
   de `NgrrdClusterException`: um `catch` de falha de transporte/timeout nunca a captura por engano.
6. **Placement sem arquivo** (cliente caiu entre `PLACE` e `OPEN`, disco perdido): `exists` responde
   `true` (há placement); `open` sem criar recebe `SeriesStatus.NOT_FOUND` do storage e lança
   `SeriesNotFoundException`; `verify` responde `MISSING_ON_OWNER`.

## API pública

### nishi-utils-oss

- `Ngrrd.OpenOptions(Durability durability, OnGeometryChange onGeometryChange, boolean createIfMissing)`
  - Construtor secundário `(Durability, OnGeometryChange)` → `createIfMissing = true`.
  - Factories existentes (`defaults`, `durability`, `onGeometryChange`, `of`) inalteradas (`true`).
  - `withCreateIfMissing(boolean)` devolve cópia.
- `dev.nishisan.utils.oss.api.SeriesNotFoundException extends RuntimeException` com `seriesKey()`.
- `Ngrrd.exists(...)`, overloads espelhando `open`/`fromYaml`:
  - `exists(BlobVolumeRegistry registry, NgrrdUri locator, String yamlContent)`
  - `exists(BlobVolume volume, NgrrdUri locator, String yamlContent)`
  - `exists(String yamlContent, StorageFactory.StorageBindings bindings, Map<String,String> tags)`
  - A chave física é `StorageKey.series(objectNaming, seriesKey)`; nenhum I/O de criação. O YAML é
    necessário só para derivar o `seriesPrefix` (hoje o TEMS hardcoda `"series"`).

### nishi-utils-ngrrd-cluster

- `NgrrdClusterClient`:
  - `boolean exists(String seriesKey)`
  - `Map<String, Boolean> exists(Collection<String> seriesKeys)`
  - `Optional<SeriesInfo> find(String seriesKey)`
  - `Map<String, SeriesVerification> verify(Collection<String> seriesKeys)`
- `api.SeriesInfo(String seriesKey, String ownerNodeId, PlacementState state, String targetNodeId,
  long updatedAtEpochMs)`.
- `api.SeriesVerification` (enum): `PRESENT`, `MISSING_ON_OWNER`, `NOT_PLACED`, `UNVERIFIED`.
- `NgrrdClusterConfig.catalogLookupBatchSize` (default 2000, > 0), com chave YAML correspondente.
- Protocolo:
  - `Commands.CATALOG_LOOKUP = "ngrrd.catalog.lookup"` — `CatalogLookupRequest(List<String> seriesKeys)`
    → `CatalogLookupResponse(SeriesStatus status, String leaderNodeId, Map<String, SeriesPlacement> found)`;
    só o líder responde `OK`; demais nós respondem `NOT_LEADER` com hint.
  - `Commands.SERIES_EXISTS_BATCH = "ngrrd.series.exists.batch"` —
    `SeriesExistsBatchRequest(List<String> seriesKeys)` → `SeriesExistsBatchResponse(Set<String> present)`;
    não passa pela checagem de ownership (como `SERIES_EXISTS`).
  - `OpenRequest` ganha `Boolean createIfMissing` (null = `true`, compatível com clientes antigos).
  - `SeriesStatus.NOT_FOUND`.

## Fluxos no cluster

### exists / find
1. Para cada chave: placement na réplica local (`CatalogService.placementLocal`) ou override do
   resolver, o mais recente vence (mesma regra do `PlacementResolver`). Hit → existe.
2. Misses → `CATALOG_LOOKUP` no líder, paginado em `catalogLookupBatchSize`, sequencial, sob o prazo
   `retryTimeout`, com o mesmo tratamento de `NOT_LEADER` (hint) e retentativa de transporte do `PLACE`.
3. Chave ausente na resposta do líder → `false`. Falha (sem líder, timeout, transporte, líder antigo
   que não conhece o comando) → `NgrrdClusterException`; nenhuma resposta parcial.

### open com `createIfMissing=false`
1. Handle já em cache no cliente → devolve (a série existe).
2. `PlacementLookup.resolveExisting(seriesKey, maxWait)`: nunca faz `PLACE`. Placement local/override
   `ACTIVE` ou `MIGRATING` → usa. Miss → `CATALOG_LOOKUP` da chave. Ausente →
   `SeriesNotFoundException`.
3. `OpenRequest.createIfMissing=false`. No storage, com ownership OK e objeto ausente
   (`volume.storage().exists(objectKey)` falso) → `SeriesStatus.NOT_FOUND` sem abrir/criar.
4. `NOT_FOUND` no `OPEN`/`reopen()` → `SeriesNotFoundException`. O `reopen()` (self-healing após
   `NOT_OPEN`) preserva a flag; se o arquivo sumiu, o handle falha em vez de recriar.
5. `MIGRATING`, `WRONG_OWNER`, `NOT_OPEN` seguem o fluxo atual de espera/redirect.

### verify
1. Resolve placements (local + lookup em lote no líder). Sem placement → `NOT_PLACED`.
2. Agrupa por `ownerNodeId`; para cada nó, `SERIES_EXISTS_BATCH` paginado. Presente → `PRESENT`.
3. Ausente → relê o placement no líder (`CATALOG_LOOKUP`); se o dono mudou (migração concluída no
   meio), repergunta uma vez ao novo dono; se continuar ausente → `MISSING_ON_OWNER`; se o placement
   sumiu → `NOT_PLACED`.
4. Falha ao falar com um nó → `UNVERIFIED` para as chaves daquele nó (o lote dos outros nós segue).
   Falha no lookup do líder → exceção (sem `NOT_PLACED` presumido).

## Modo local

`NgrrdWriter` (construtor completo) passa a receber `createIfMissing`; antes do
`GeometryReconciler.reconcile` e do `openSeries`, se `!createIfMissing && !provider.seriesExists(storageKey)`
→ `SeriesNotFoundException`, sem alocar nada. Vale para localDisk, S3 e sharded blob.
`Ngrrd.buildHandle` repassa `options.createIfMissing()`.

## Contrato de consistência (Javadoc)

- `exists`/`find` depois de um `PLACE` feito por outro cliente: se a réplica local ainda não o viu, o
  miss é confirmado no líder, que já tem o placement → `true`.
- Durante migração (`MIGRATING`): `true`; `open` sem criar segue o fluxo normal.
- `false` significa "o líder atual não tem placement para a chave no momento da consulta". Não é
  atômico com um `open` concorrente de outro cliente que crie a série logo em seguida.
- `exists` não lê o storage; placement sem arquivo aparece como `true`. Use `verify` para conciliar.

## Compatibilidade

- `open` sem a opção nova: comportamento idêntico.
- Cliente novo + líder antigo: `CATALOG_LOOKUP` desconhecido → erro → `exists` lança (nunca `false`).
- Storage antigo ignora `createIfMissing` (Jackson `FAIL_ON_UNKNOWN_PROPERTIES=false`) e recriaria no
  caso placement-sem-arquivo. Regra operacional: atualizar todos os storages antes de usar
  `createIfMissing=false`, `exists` ou `verify` no cliente.
- Storage novo + cliente antigo: `createIfMissing` null → cria, como hoje.

## Testes

- **oss (unit):** localDisk e sharded blob — `exists` existente/inexistente; `open` com
  `createIfMissing=false` em série inexistente lança `SeriesNotFoundException` e nada é criado (sem
  arquivo, sem entrada no catálogo do blob); série existente abre; default continua criando.
- **cluster (unit):**
  - `PlacementResolver.resolveExisting`: nunca faz `PLACE`; miss confirmado no líder; ausente →
    `SeriesNotFoundException`; falha de transporte/sem líder → `NgrrdClusterException`.
  - Paginação: 5000 misses com limite 2000 → 3 chamadas de `CATALOG_LOOKUP`.
  - `PlacementRequestHandler` responde `CATALOG_LOOKUP` como líder e `NOT_LEADER` fora dele.
  - `StorageRequestHandler`: `OPEN` com `createIfMissing=false` e objeto ausente → `NOT_FOUND`,
    nenhum objeto criado; `SERIES_EXISTS_BATCH`.
  - `verify`: agrupamento por nó, paginação, `UNVERIFIED` isolado por nó, re-resolução após migração.
  - Config: `catalogLookupBatchSize` default e validação.
- **cluster (`*ClusterTest`, perfil `ngrrd-cluster`):** série existente; inexistente (nenhum placement,
  nenhum arquivo em nenhum storage, nenhuma entrada no catálogo); `MIGRATING` → `exists=true` e open
  sem criar conclui; placement sem arquivo → `verify=MISSING_ON_OWNER` e open sem criar →
  `SeriesNotFoundException`.

## Documentação

- `doc/oss/ngrrd.md`: `createIfMissing` e `Ngrrd.exists`.
- `doc/oss/ngrrd-cluster.md` (+ `ngrrd-cluster-operacao.md`): `exists`/`find`/`verify`, contrato de
  consistência, `catalogLookupBatchSize`, ordem de atualização.
- `doc/CHANGELOG.md` 8.6.0; versões nos POMs/README.
