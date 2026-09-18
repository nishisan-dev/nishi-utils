# ngrrd Cluster — armazenamento distribuído para o ngrrd

## Contexto

O `ngrrd-consumer` (TEMS) persiste dezenas de milhares de séries via `nishi-utils-oss` num único
processo com um único volume blob. O troubleshooting apontou o disco como limitador provável
conforme o volume cresce, embora ainda **sem medição precisa** (IOPS, capacidade ou leitura).
Hoje a única saída é crescer verticalmente a máquina.

Objetivo: permitir que o ngrrd cresça horizontalmente. Um cluster de **storage nodes** guarda as
séries; um **coordenador** (o líder eleito entre os storage nodes) decide onde cada série mora,
redistribui quando um nó entra e drena quando um nó sai. Para quem usa o ngrrd a API continua a
mesma: `NgrrdHandle` com `write`, `flush`, `checkpoint`, `read`, `close`.

Decisões já tomadas com o usuário:

| Decisão | Escolha |
|---|---|
| Topologia | Cliente fino remoto: o consumer continua processo separado; storage nodes dedicados com disco próprio |
| Replicação de séries | **Nunca.** Cada série vive em exatamente um nó. Redundância é problema de infraestrutura |
| Coordenador | Líder eleito entre os storage nodes (um único tipo de processo) |
| Escopo | Os três estágios numa spec: (1) distribuir séries novas, (2) redistribuir ao entrar nó, (3) drenar ao sair/manutenção |
| Gargalo | Não medido. O desenho inclui métricas por nó para confirmar |
| Conexão do cliente | **A:** o cliente entra no NGrid como membro sem disco de séries, com role de inelegível a liderança |

Reuso do que já existe (verificado no código):

- **NGrid** (`nishi-utils-core`): `Transport`/`TcpTransport` (request-response via `sendAndAwait`, listeners
  públicos), `ClusterCoordinator` (membership, eleição, `LeadershipListener`, `MembershipListener`),
  `DistributedMap` (catálogo replicado, leitura local `EVENTUAL`, valores Jackson com `@class`),
  `NodeInfo.roles`/`priority` já propagados por gossip.
- **ngrrd** (`nishi-utils-oss`): backend `SHARDED_BLOB` expõe `get(key)`, `put(key, bytes)`,
  `delete(key)`, `list(prefix)` da imagem completa de uma série (`storage/blob/BlobStorage.java:300-351`);
  `BlobVolumeStats` dá séries/bytes/fill por volume; `Sample`, `ViewQuery`, `SeriesResult`, `DataPoint`
  são records simples; série típica ≈ 1,6 MiB (`doc/oss/ngrrd.md:225-236`).

Lacunas confirmadas que o plano cobre:

- `roles` não tem efeito comportamental e `NGridNodeBuilder` não permite configurá-lo.
- Não existe cache/pool de handles por `seriesKey` no oss (cada `open` cria um `NgrrdWriter`).
- Registro de comandos no NGrid é manual (`TransportListener` + match por string de comando).
- Payload binário viaja como JSON+Base64 (LZ4 acima de 512 B, frame até 64 MB): aceitável para
  amostras e para migração em chunks; não é caminho de streaming.

---

## Desenho

### 1. Módulo e dependências

Novo módulo Maven **`nishi-utils-ngrrd-cluster`** (publicado), pacote raiz `dev.nishisan.utils.oss.cluster`,
dependendo de `nishi-utils-core` e `nishi-utils-oss`. Nenhum dos dois passa a depender do outro.
Versão do reactor sobe para **8.3.0**.

```
dev.nishisan.utils.oss.cluster
  ├─ NgrrdCluster                 façade pública (connect / storage node)
  ├─ api/        NgrrdClusterConfig, NgrrdClusterClient, NgrrdClusterException, ErrorCode
  ├─ catalog/    SeriesPlacement, PlacementState, StorageNodeStatus, NodeState, CatalogService
  ├─ protocol/   Commands (constantes), payloads request/response (records)
  ├─ client/     RemoteSeriesHandle, PlacementResolver, WriteBuffer, RetryPolicy
  ├─ node/       NgrrdStorageNode, SeriesHandleRegistry, StorageRequestHandler, NodeStatusReporter,
  │              LocalReconciler
  ├─ placement/  PlacementPolicy (interface) + LeastLoadedPlacementPolicy
  ├─ rebalance/  Rebalancer, MigrationCoordinator (líder), MigrationExecutor (src/dst), MigrationState
  ├─ admin/      AdminService (líder), NgrrdClusterAdminCli
  └─ metrics/    NgrrdClusterMetricsListener, NodeMetricsSnapshot
```

### 2. Papéis e membership

- **Storage node**: `NGridNode` com role `storage`, `priority` alto (ex.: 100), mapas `ngrrd.catalog`
  e `ngrrd.nodes` configurados, e um `BlobVolume` local. Todo storage node é elegível a líder.
- **Cliente**: `NGridNode` com roles `client` + `leader-ineligible`, sem volume, mesmos mapas
  configurados (regra do NGrid: mapas registrados em todos os participantes). `dataDir` obrigatório
  do NGrid aponta para diretório temporário/efêmero do cliente.
- **Líder = coordenador**: roda `PlacementPolicy`, `Rebalancer`, `MigrationCoordinator` e `AdminService`.
  Ativado/desativado por `LeadershipListener`.

**Mudança no core (pequena e genérica):**

1. `NodeInfo.ROLE_LEADER_INELIGIBLE = "leader-ineligible"`: `ClusterCoordinator` exclui membros com
   esse role em todos os pontos de escolha de candidato (`recomputeLeader` em
   `cluster/coordination/ClusterCoordinator.java:816-820`, `isLocalHighestAffinity` ~`:1070`,
   `outranks` `:1198-1204`, e o terceiro `max` em ~`:1298`). Como roles viaja em `HandshakePayload`/`PeerUpdatePayload`,
   todos os nós concordam sem mudança de wire format.
2. `NGridNodeBuilder.roles(String...)` (hoje fixa `emptySet()` em `structures/NGridNodeBuilder.java:299-300`).
3. Teste em core provando `DistributedMap<String, record>` (put/get/replicação); se falhar, corrigir
   `MapReplicationCodec`, não trocar record por POJO no módulo novo.

### 3. Catálogo (dois `DistributedMap`, escrita sempre via líder)

`ngrrd.catalog : DistributedMap<String /*seriesKey*/, SeriesPlacement>`

```java
record SeriesPlacement(String ownerNodeId, String targetNodeId /*null fora de migração*/,
                       PlacementState state /*ACTIVE | MIGRATING*/, String migrationId /*null*/,
                       long createdAtEpochMs, long updatedAtEpochMs) {}
```

`ngrrd.nodes : DistributedMap<String /*nodeId*/, StorageNodeStatus>`

```java
record StorageNodeStatus(String nodeId, NodeState state /*ACTIVE | DRAINING | DRAINED*/,
                         long seriesCount, long usedBytes, long capacityBytes,
                         long reportedAtEpochMs) {}
```

- `NodeStatusReporter` em cada storage node publica seu status a cada `statusReportInterval`
  (default 10 s) a partir de `BlobVolume.stats()` (`catalogEntryCount`, soma de `shardUsedBytes`) e
  `capacityBytes` da config.
- Placement é decidido **somente pelo líder**, lendo sua cópia local dos mapas. Clientes leem o
  catálogo com `Consistency.EVENTUAL` e tratam divergência por resposta `WRONG_OWNER`.
- Em PRODUCTION o NGrid exige `mapPersistenceMode != DISABLED`: catálogo persistido em disco em
  todos os nós. Dimensionamento: 100k séries × ~150 B ≈ 15 MB por nó.

### 4. Protocolo (comandos `CLIENT_REQUEST`, payloads records)

Padrão igual a `DistributedQueue`/`DistributedMap`: cada serviço implementa `TransportListener`,
filtra `ClientRequestPayload.command()` e responde com `CLIENT_RESPONSE`. Prefixo `ngrrd.`.

| Comando | Destino | Corpo | Resposta |
|---|---|---|---|
| `ngrrd.place` | líder | seriesKey, definitionHash | `SeriesPlacement` (cria se não existir; idempotente) |
| `ngrrd.open` | dono | seriesKey, yaml, tags, OpenOptions | OK / `WRONG_OWNER(owner)` / `MIGRATING` |
| `ngrrd.writeBatch` | dono | lista de `(seriesKey, dsName, Sample)` agrupada por série | status por série (OK / WRONG_OWNER / MIGRATING / ERROR) |
| `ngrrd.checkpoint` / `ngrrd.flush` | dono | seriesKey | OK / erro |
| `ngrrd.read` | dono | seriesKey, dsName, `ViewQuery`, endExclusive | `SeriesResult` |
| `ngrrd.readPreset` | dono | seriesKey, presetName, endExclusive | `Map<String, SeriesResult>` |
| `ngrrd.close` | dono | seriesKey | OK (libera referência no registry) |
| `ngrrd.migrate.start` | src | seriesKey, migrationId, dstNodeId | OK/erro |
| `ngrrd.migrate.chunk` | dst | seriesKey, migrationId, seq, total, bytes | OK |
| `ngrrd.migrate.commit` | dst | seriesKey, migrationId, sha256, totalBytes | OK / HASH_MISMATCH |
| `ngrrd.migrate.abort` | src/dst | seriesKey, migrationId | OK |
| `ngrrd.migrate.finish` | src | seriesKey, migrationId | OK (apaga cópia local) |
| `ngrrd.migrate.status` | dst | seriesKey, migrationId | COMMITTED / PARTIAL / UNKNOWN |
| `ngrrd.admin.drain` / `activate` | líder | nodeId | `StorageNodeStatus` |
| `ngrrd.admin.status` | líder | — | nós, contagens, migrações em curso |
| `ngrrd.admin.metrics` | qualquer nó | — | `NodeMetricsSnapshot` |
| `ngrrd.admin.rebalance` | líder | — | dispara um ciclo imediato |

Tamanhos: amostras são pequenas (JSON ok). Chunk de migração = 256 KiB (≈ 7 chunks por série de
1,6 MiB; Base64 + LZ4 do codec). Limite `maxSeriesBytes` configurável (default 64 MiB) para rejeitar
séries acima do cap do frame.

### 5. Cliente transparente

```java
NgrrdClusterClient client = NgrrdCluster.connect(NgrrdClusterConfig.fromYaml(path));
NgrrdHandle h = client.open(yamlDefinition, tags);   // mesma interface de hoje
h.write("in_octets", new Sample(ts, v));
h.checkpoint();
SeriesResult r = h.read("in_octets", query);
h.close();
client.close();
```

`RemoteSeriesHandle implements NgrrdHandle`:

- `open`: resolve `seriesKey` com o mesmo `resolveSeriesKey` do oss (`Ngrrd.java:247`, tornar
  acessível ou replicar o template resolver); consulta catálogo local; se ausente → `ngrrd.place`
  no líder; depois `ngrrd.open` no dono.
- `write`: enfileira no `WriteBuffer` (um por nó de destino). Flush por tamanho (`batchMaxSamples`,
  default 500) ou tempo (`batchMaxDelay`, default 200 ms). Buffer **limitado** (`maxBufferedSamples`
  por nó, default 100k); ao encher, política `BLOCK` (default, backpressure para o consumer Kafka)
  ou `FAIL`. Sem `DROP`.
- `flush`/`checkpoint`: drena o buffer do nó e envia o comando síncrono.
- `read`: RPC direto; `Consistency` não se aplica (dono único).
- Respostas `WRONG_OWNER`/`MIGRATING`: `PlacementResolver` invalida cache, relê catálogo (ou pede ao
  líder), re-enfileira o lote. Backoff 100 ms → 2 s, até `migrationRetryTimeout` (default 5 min);
  depois `NgrrdClusterException(SERIES_UNAVAILABLE)`.
- Dono inalcançável: leituras e checkpoints falham com `SERIES_UNAVAILABLE`; escritas ficam no
  buffer até o cap. **Não há re-placement automático de série de nó caído** (sem réplica, isso
  perderia dados). Quando o nó volta, o catálogo continua válido e o fluxo retoma.

### 6. Storage node

`NgrrdStorageNode` (embutível e com `main` para deploy) sobe: `NGridNode` (role `storage`), `BlobVolume`
via `NgrrdBlob.registry()`, `SeriesHandleRegistry`, `StorageRequestHandler`, `NodeStatusReporter`,
`LocalReconciler`, `MigrationExecutor`, e (quando líder) os serviços de coordenação.

- `SeriesHandleRegistry`: mapa `seriesKey → NgrrdHandle` local aberto via `Ngrrd.open(volume, locator, yaml, options)`.
  Fecha handles ociosos após `handleIdleTtl` (default 15 min) e limita `maxOpenHandles` (LRU).
  Migração de uma série faz `checkpoint` + `close` do handle e marca a série como `MIGRATING`
  localmente (rejeita writes com `MIGRATING`).
- `StorageRequestHandler` verifica, a cada requisição, se o nó é dono segundo a cópia local do
  catálogo; se não, responde `WRONG_OWNER`. Exceção: `ngrrd.open`/`writeBatch` para série cujo
  catálogo local ainda não replicou a entrada recém-criada pelo líder → aceita se o request traz o
  `SeriesPlacement` retornado pelo `ngrrd.place` com `owner == self` (evita corrida líder→dono).
- `LocalReconciler` (no start e ao virar líder): lista `BlobStorage.list("")` do volume e cruza com o
  catálogo.
  - Série no volume e ausente no catálogo → **adoção**: `ngrrd.place` ao líder com `preferredOwner=self`.
    Isso é também o **caminho de migração do ngrrd single-node**: apontar um storage node para o
    volume existente adota todas as séries.
  - Série no volume, catálogo `ACTIVE` em outro nó → órfã (finish de migração perdido) → apaga.
  - Série no catálogo `ACTIVE` em self mas ausente no volume → loga `MISSING_SERIES` e mantém
    entrada (não inventa dados).
- Config YAML do storage node = bloco `node`/`cluster`/`seeds` do NGrid (`NGridConfigLoader`) +
  bloco `ngrrd: { volumePath, capacityBytes, statusReportInterval, handleIdleTtl, maxOpenHandles }`,
  com `${VAR}` como no restante do projeto.

### 7. Placement (estágio 1)

`LeastLoadedPlacementPolicy.choose(ctx)`, ordem total e determinística (o resultado não pode depender
da ordem de iteração do catálogo local, que varia entre nós e reinícios do líder):

1. Candidatos: `state == ACTIVE`, membro ativo e alcançável no `ClusterCoordinator`, `reportedAt`
   dentro de `2 × statusReportInterval`, e **não** saturado (capacidade conhecida com `fillRatio >= 0.95`
   é excluída).
2. Se `preferredOwnerNodeId` (adoção pelo `LocalReconciler`) estiver entre os candidatos, vence.
3. Chave primária: carga efetiva = `seriesCount` reportado + placements feitos pelo líder desde o
   último `reportedAt` daquele nó (`pendingSeriesByNode`; sem isso, uma rajada de séries novas cairia
   toda no mesmo nó até o próximo reporte de status).
4. Secundária: `fillRatio` (capacidade desconhecida vale `0.0`). Terciária: `nodeId`.

Sem candidatos → `NO_STORAGE_NODE_AVAILABLE`. Interface `PlacementPolicy` permite trocar a política em teste.

### 8. Rebalanceamento (estágio 2) e migração

`Rebalancer` (líder, agendado a cada `rebalanceInterval`, default 60 s; disparado também por
`MembershipListener` e por `ngrrd.admin.rebalance`):

1. Lê `ngrrd.nodes` e agrega séries por nó a partir de `ngrrd.catalog` (só `ACTIVE`).
2. Nós `DRAINING`: todas as séries entram na fila de saída.
3. Nós `ACTIVE`: alvo = média de `seriesCount`; se `max − min > max(rebalanceMinDelta, 10% da média)`
   move do mais carregado para o menos carregado até equilibrar. Bytes reportados entram como
   desempate (definições diferentes têm tamanhos diferentes, mas seriesCount é a métrica primária).
4. Respeita `maxConcurrentMigrations` (default 2) e `maxMovesPerCycle` (default 50).

`MigrationCoordinator` (líder) executa cada movimento como máquina de estados idempotente
(`migrationId` UUID):

```
PLANNED → catálogo := MIGRATING(owner=src, target=dst)
        → src: migrate.start          (src quiesce: checkpoint, close, marca MIGRATING local)
        → src → dst: migrate.chunk*   (src lê BlobStorage.get(key), envia N chunks)
        → src → dst: migrate.commit   (dst valida SHA-256, BlobStorage.put(key, bytes))
        → catálogo := ACTIVE(owner=dst)
        → src: migrate.finish         (src BlobStorage.delete(key))
DONE
```

Falhas:

- Antes do `commit` OK: líder envia `abort` a ambos, catálogo volta a `ACTIVE(src)`, src reabre normalmente.
- `commit` OK mas líder cai antes de flipar o catálogo: novo líder acha entrada `MIGRATING` com
  `updatedAt` > `migrationTimeout` (default 10 min), consulta `migrate.status` no dst: `COMMITTED`
  → flipa para dst e manda `finish` ao src; senão → `abort` e volta ao src.
- `finish` perdido: `LocalReconciler` do src apaga a órfã no próximo start/ciclo.
- Cliente vê `MIGRATING` por poucos segundos (série de 1,6 MiB em rede local) e re-tenta.

### 9. Drenagem e manutenção (estágio 3)

- `ngrrd-cluster-admin drain <nodeId>`: líder põe `DRAINING`; `PlacementPolicy` deixa de escolhê-lo;
  `Rebalancer` esvazia; ao atingir `seriesCount == 0` → `DRAINED`. O operador para o processo.
- `activate <nodeId>` reverte para `ACTIVE` (nó volta a receber séries no próximo ciclo).
- `status`: nós, estado, séries, bytes, migrações em curso. `metrics <nodeId>`: `NodeMetricsSnapshot`.
- **Movimentação de nó** (novo host/porta, mesmo `nodeId`, mesmo volume): o catálogo referencia
  `nodeId`; o NGrid atualiza o endereço via handshake/gossip. Nenhuma ação no catálogo.
- **Queda sem drenagem**: séries do nó ficam indisponíveis até ele voltar (decisão explícita: sem
  réplica). Métrica e `status` mostram `UNREACHABLE` para o nó.

### 10. Métricas (para confirmar o gargalo)

`NodeMetricsSnapshot` por storage node: samples/s, batches/s, latência de `writeBatch`,
`checkpoint` (p50/p99, via histograma simples), latência de `read`, handles abertos, séries,
bytes usados, `BlobVolumeStats`, migrações in/out (contagem, bytes, duração), erros por código.
Exposto por `ngrrd.admin.metrics`, por `NgrrdClusterMetricsListener` (integração com o exporter do
consumer) e por log periódico `NGRRD_NODE_STATUS` (padrão de log marker do projeto). No cliente:
tamanho dos buffers, retries por código, latência por nó.

### 11. Testes

- **Unitários (default, `mvn test`)**: `LeastLoadedPlacementPolicy`, `Rebalancer` (planejamento puro
  sobre snapshots), `MigrationCoordinator` com transporte fake, `WriteBuffer` (limites, BLOCK/FAIL,
  agrupamento), `PlacementResolver` (invalidação em WRONG_OWNER), codecs dos payloads (round-trip
  Jackson dos records, inclusive `Duration` em `ViewQuery` com `JavaTimeModule`), `LocalReconciler`
  com volume real em `@TempDir`.
- **Cluster in-process (profile `ngrrd-cluster`, mesmo padrão do `-Presilience`)**: harness
  `NgrrdClusterTestHarness` (1 cliente + N storage nodes com `NGrid.local`-like builder):
  - séries novas distribuídas entre 2 nós, write/read transparentes, checkpoint durável;
  - nó entra → rebalanceamento move séries, dados lidos iguais antes/depois (hash da imagem);
  - drain → nó fica vazio, cliente segue escrevendo sem erro;
  - queda do líder no meio de uma migração → novo líder resolve (COMMITTED e PARTIAL);
  - cliente nunca vira líder (derrubar todos os storage nodes: `leaderInfo()` vazio);
  - adoção de volume single-node existente.
- **Docker IT** (`ngrid-test`, profile `docker-resilience`): fora deste plano; registrar como follow-up
  reaproveitando `NGridNodeContainer` e log markers.
- Nomes de `@Test` em PT-BR; helpers em inglês.

### 12. Documentação

- `doc/oss/ngrrd-cluster.md`: modelo, config, operação (drain/activate/status), limites (sem réplica),
  migração do single-node, métricas.
- `doc/oss/diagrams/ngrrd_cluster_c4_container.puml`, `ngrrd_cluster_sequence_write.puml`,
  `ngrrd_cluster_sequence_migration.puml`, embutidos via `uml.nishisan.dev`.
- Atualizar `README`/`doc/README.md` com o módulo novo e `CLAUDE.md` (build do novo módulo e profile).
- Spec aprovada copiada para `planning/ngrrd-cluster.md`.

---

## Plano de implementação (branch `feature/ngrrd-cluster`, commits atômicos por passo)

Orquestração conforme CLAUDE.md: Scout/Researcher → spec → Builder (sonnet) → Refuter (opus) por marco.

### M0 — Core: elegibilidade por role
1. `NodeInfo.ROLE_LEADER_INELIGIBLE` + filtro nos 4 pontos de candidatura em `ClusterCoordinator`.
2. `NGridNodeBuilder.roles(String...)` e teste em `NGridConfigLoaderTest`/novo teste de builder.
3. Teste de cluster in-process: nó com role inelegível nunca é eleito, mesmo sozinho com maior prioridade.
4. Teste `DistributedMap<String, record>` round-trip com replicação.
5. `mvn -pl nishi-utils-core clean install` (módulo é dependência do novo).

### M1 — Módulo, protocolo, caminho feliz (estágio 1)
1. `nishi-utils-ngrrd-cluster/pom.xml` (deps core+oss, Jackson jsr310, junit, profile `ngrrd-cluster`), registrar no reactor; versão 8.3.0.
2. `catalog/` records + `CatalogService` (wrapper dos dois mapas, leitura local, escrita via líder).
3. `protocol/` comandos e payloads; teste de round-trip Jackson.
4. `node/`: `NgrrdStorageNode`, `SeriesHandleRegistry`, `StorageRequestHandler` (`place` no líder, `open`, `writeBatch`, `checkpoint`, `flush`, `read`, `readPreset`, `close`), `NodeStatusReporter`.
5. `placement/LeastLoadedPlacementPolicy`.
6. `client/`: `NgrrdClusterClient`, `RemoteSeriesHandle`, `WriteBuffer`, `PlacementResolver`, `RetryPolicy`; façade `NgrrdCluster`.
7. Harness de teste + teste "2 nós, séries distribuídas, write/read/checkpoint transparentes".

### M2 — Métricas e status
1. `metrics/NodeMetricsSnapshot`, `NgrrdClusterMetricsListener`, log marker `NGRRD_NODE_STATUS`.
2. `ngrrd.admin.status` / `ngrrd.admin.metrics` no líder/nó; testes.

### M3 — Migração e rebalanceamento (estágio 2)
1. `rebalance/MigrationState` + `MigrationExecutor` (src/dst: quiesce, chunks, commit, abort, finish, status).
2. `MigrationCoordinator` (líder) com timeout e resolução de `MIGRATING` órfão ao assumir liderança.
3. `Rebalancer` (planejamento puro + agendamento).
4. Cliente: tratamento de `MIGRATING` com backoff.
5. Testes: nó entra → move; hash antes/depois; queda do líder mid-migration.

### M4 — Drenagem, reconciliação e admin (estágio 3)
1. `AdminService` (`drain`, `activate`, `rebalance`) + `NgrrdClusterAdminCli`.
2. `LocalReconciler` (adoção, órfãs, missing).
3. Testes: drain até vazio; adoção de volume single-node; órfã apagada.

### M5 — Documentação e release
1. `doc/oss/ngrrd-cluster.md` + diagramas PlantUML; README/CLAUDE.md.
2. Diário de bordo / changelog 8.3.0; `planning/ngrrd-cluster.md` com a spec.

## Verificação

```bash
mvn -pl nishi-utils-core clean install                       # M0 (inclui testes do core, ngrid roda localmente)
mvn -pl nishi-utils-ngrrd-cluster test                       # unitários
mvn -pl nishi-utils-ngrrd-cluster test -Pngrrd-cluster       # cluster in-process
mvn -pl nishi-utils-oss test                                 # oss intocado, garantir verde
mvn verify -Pvalidate-javadoc
```

Critérios de aceite fim a fim (no harness): consumer-like escreve 2k séries em 3 nós; distribuição
≈ uniforme; leitura de qualquer série retorna o que foi escrito; adicionar 4º nó move ~25% das
séries com imagens idênticas (SHA-256); `drain` de um nó zera suas séries sem erro no cliente;
derrubar os storage nodes deixa o cliente sem líder (não se autoelege).

## Riscos e limites declarados

- JSON+Base64 para chunks de migração: adequado para séries de poucos MiB; se surgir série muito
  maior, medir e só então considerar frame binário no codec (fora deste plano).
- Suíte de cluster in-process herda a sensibilidade de tempo do NGrid: fica em profile dedicado,
  fora do CI hospedado, como a suíte ngrid.
- Sem réplica: queda de nó = indisponibilidade das séries dele. Documentado como decisão.
- Record como valor de `DistributedMap` ainda não coberto por teste no core: M0 resolve antes do módulo novo.
