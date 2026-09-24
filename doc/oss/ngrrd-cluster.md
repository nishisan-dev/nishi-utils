# ngrrd Cluster — armazenamento distribuído

> **Módulo:** `nishi-utils-ngrrd-cluster` (`dev.nishisan:nishi-utils-ngrrd-cluster`), pacote raiz
> `dev.nishisan.utils.oss.cluster`. Depende de `nishi-utils-core` (NGrid) e `nishi-utils-oss`
> (ngrrd); nenhum dos dois passa a depender dele.

Para começar, veja o [quickstart](ngrrd-cluster-quickstart.md). Para expansão com tráfego,
exemplos de balanceamento, drenagem e recuperação, consulte o
[guia de operação](ngrrd-cluster-operacao.md).

## 1. Visão geral e motivação

O `ngrrd-consumer` (TEMS) persiste dezenas de milhares de séries via `nishi-utils-oss` num único
processo com um único volume blob. O troubleshooting em produção apontou o disco como limitador
provável conforme o volume cresce — mas **sem medição precisa** (IOPS, capacidade ou latência de
leitura). Até aqui a única saída disponível era crescer verticalmente a máquina.

O ngrrd cluster permite crescer **horizontalmente**: um conjunto de **storage nodes** guarda as
séries, cada uma em exatamente um nó; um **coordenador** — o líder eleito entre os próprios storage
nodes — decide onde cada série mora, redistribui quando um nó entra e drena quando um nó sai. Para
quem consome o ngrrd a API não muda: o mesmo `NgrrdHandle` (`write`, `flush`, `checkpoint`, `read`,
`close`), só que atrás de um cliente que fala com o cluster em vez de abrir o volume localmente.

As métricas por nó (seção 10) existem justamente para **confirmar** o gargalo de disco antes de
investir mais nessa direção — o desenho não presume onde o limite está.

## 2. Modelo

- **Storage node** (`NgrrdStorageNode`): um `NGridNode` com role `storage`, um `BlobVolume` local e
  os handlers do protocolo. Todo storage node é elegível a líder.
- **Líder = coordenador:** o nó eleito líder do NGrid roda, além de servir séries como qualquer
  storage node, o `PlacementRequestHandler`, o `Rebalancer`/`MigrationCoordinator` e o
  `AdminService`. Papéis de coordenação são ativados/desativados por `LeadershipListener` — não há
  processo de coordenador separado.
- **Cliente** (`NgrrdClusterClient`/`RemoteSeriesHandle`): entra na malha do NGrid como membro pleno
  (participa de handshake/gossip), mas com roles `client` + `leader-ineligible` — **nunca** é
  candidato a líder, mesmo sozinho na malha com a maior prioridade configurada (é a exclusão de
  papel introduzida no core, `NodeInfo.ROLE_LEADER_INELIGIBLE`, filtrada em todos os pontos de
  eleição do `ClusterCoordinator`). O cliente não tem `BlobVolume`; seu `dataDir` do NGrid é
  efêmero quando não informado em `NgrrdClusterConfig` (criado por `connect()`, apagado no
  `close()`).
- **Sem réplica — decisão explícita e definitiva do desenho.** Cada série vive em exatamente um
  storage node. Redundância é responsabilidade da infraestrutura (disco redundante, backup), não do
  cluster ngrrd. **Consequência direta:** a queda de um storage node deixa as séries dele
  **indisponíveis até ele voltar** — não existe re-placement automático de uma série de um nó caído
  para outro nó, porque isso implicaria reconstruir dados que só existem na cópia caída (perda de
  dados, não redistribuição). `status`/métricas mostram o nó como `UNREACHABLE`; leituras e
  checkpoints das séries dele falham com `SERIES_UNAVAILABLE`; escritas continuam sendo aceitas no
  `WriteBuffer` do cliente até o limite configurado, e retomam sozinhas quando o nó volta (o
  catálogo continua válido, nada precisa ser refeito).
- **Dimensionamento do cluster: ≥ 3 storage nodes para tolerar a queda de 1.** A liderança do NGrid
  exige maioria entre os membros **elegíveis** a líder — clientes `leader-ineligible` não contam
  para o quórum. Com **2** storage nodes, a queda de um deles já derruba a maioria: o cluster fica
  **sem líder**, e tudo que depende do líder para (`ngrrd.place` de séries novas, `admin.status`,
  `admin.metrics` — este último não, ver nota abaixo —, `admin.rebalance`, rebalanceamento
  automático) para até o nó voltar. Escrita/leitura nas séries do nó sobrevivente continuam
  funcionando normalmente, porque são atendidas pelo dono, não pelo líder. Com **3** storage nodes,
  a queda de 1 ainda deixa maioria (2 de 3) e o cluster continua coordenando normalmente. Esse é o
  motivo pelo qual o dimensionamento mínimo recomendado é 3, não 2.
- `ngrrd.admin.metrics` é servido por **qualquer** storage node (líder ou não) — métricas são
  sempre locais ao nó consultado (ver seção 10), então não depende de haver líder eleito.

## 3. Catálogo

Dois `DistributedMap` do NGrid, replicados entre os storage nodes (o cliente também os registra,
mas só lê):

```java
record SeriesPlacement(String ownerNodeId, String targetNodeId /* null fora de migração */,
                        PlacementState state /* ACTIVE | MIGRATING */, String migrationId /* null */,
                        long createdAtEpochMs, long updatedAtEpochMs) implements Serializable {}

record StorageNodeStatus(String nodeId, NodeState state /* ACTIVE | DRAINING | DRAINED */,
                          long seriesCount, long usedBytes, long capacityBytes,
                          long reportedAtEpochMs) implements Serializable {}
```

- `ngrrd.catalog: DistributedMap<seriesKey, SeriesPlacement>` — onde cada série está hoje e, durante
  migração, para onde vai. `SeriesPlacement` é **auto-validado** no construtor compacto:
  `MIGRATING` exige `targetNodeId`/`migrationId` não nulos; `ACTIVE` exige ambos nulos — não há
  caminho para construir uma instância inconsistente.
- `ngrrd.nodes: DistributedMap<nodeId, StorageNodeStatus>` — último status conhecido de cada
  storage node, publicado pelo próprio nó a cada `statusReportInterval` (via `NodeStatusReporter`).
- **Escrita sempre pelo líder.** Placement é decidido e gravado só pelo `PlacementRequestHandler`,
  no líder. Storage nodes leem sua **cópia local** do catálogo (consistência **eventual** — a
  mesma do `DistributedMap` do NGrid); ao decidir algo que depende de uma verdade mais recente
  (adoção, deleção de órfã, resolução de migração), o código consulta o líder com leitura **forte**
  (`CatalogView.placementStrong`/`nodeStatusStrong`).
- **`Serializable` é obrigatório.** Os dois records implementam `Serializable` porque o
  `NMapPersistence` do core grava o WAL via `ObjectOutputStream` — sem isso o append falha em
  silêncio (`NotSerializableException` engolida) e o catálogo persistente nunca é gravado de fato,
  mesmo com `persistenceMode = ASYNC_WITH_FSYNC` configurado (bug real encontrado e corrigido
  durante o M1c: um nó reiniciado voltava com catálogo vazio).
- **`seriesObjectPrefix` deve ser único e idêntico em todo o cluster.** Todas as definições YAML
  servidas por um mesmo cluster ngrrd precisam usar (ou herdar por omissão) o mesmo
  `storage.objectNaming.seriesPrefix` que o storage node está configurado para reconhecer
  (`StorageNodeConfig#seriesObjectPrefix`, default: o próprio default do `nishi-utils-oss`,
  obtido programaticamente via `ObjectNaming#seriesPrefixOrDefault()` — nunca hardcoded). Isso
  porque o `LocalReconciler` decodifica o `seriesKey` a partir da chave física do objeto no volume
  (`{prefix}/{seriesKey}.ngrr`) sem ter como saber, só olhando o volume, qual prefixo uma definição
  customizada usaria antes de abri-la. Um `OPEN` cuja definição usa um prefixo diferente do
  configurado no nó é **rejeitado** com `SeriesStatus#ERROR`.

## 4. Protocolo

Mesmo padrão de `DistributedQueue`/`DistributedMap` do core: cada serviço implementa um
`TransportListener`, filtra `ClientRequestPayload.command()` e responde via `CLIENT_RESPONSE`.
Prefixo `ngrrd.` em todo comando (`Commands`).

| Comando | Atendido por | Corpo | Resposta |
|---|---|---|---|
| `ngrrd.place` | líder (`LEADER_COMMANDS`) | seriesKey, definitionHash, preferredOwnerNodeId | `SeriesPlacement` (idempotente: se já existir, devolve o mesmo) |
| `ngrrd.open` | dono (`OWNER_COMMANDS`) | seriesKey, yaml, tags, durability, onGeometryChange, placementHint | OK / `WRONG_OWNER(owner)` / `MIGRATING` |
| `ngrrd.writeBatch` | dono | lote de `(seriesKey, dsName, Sample)` | status por série: OK / `WRONG_OWNER` / `MIGRATING` / `NOT_OPEN` / `ERROR` |
| `ngrrd.checkpoint` / `ngrrd.flush` | dono | seriesKey | OK / erro |
| `ngrrd.read` | dono | seriesKey, dsName, windowMs, targetStepSec, cf, maxPoints, endExclusiveEpochMs opcional | `SeriesResult` |
| `ngrrd.readPreset` | dono | seriesKey, presetName, endExclusive opcional | `Map<String, SeriesResult>` |
| `ngrrd.close` | dono | seriesKey | OK (libera a referência no registry local) |
| `ngrrd.series.exists` | qualquer nó | seriesKey | existência física no volume local, sem abrir handle |
| `ngrrd.migrate.start` | origem (`MIGRATION_COMMANDS`) | seriesKey, migrationId, dstNodeId | OK/erro |
| `ngrrd.migrate.chunk` | destino | seriesKey, migrationId, seq, total, bytes | OK |
| `ngrrd.migrate.commit` | destino | seriesKey, migrationId, sha256Hex, totalBytes, storageKey | OK / `HASH_MISMATCH` |
| `ngrrd.migrate.abort` | origem/destino | seriesKey, migrationId | OK |
| `ngrrd.migrate.finish` | origem | seriesKey, migrationId | OK (apaga a cópia local) |
| `ngrrd.migrate.status` | destino | seriesKey, migrationId | `COMMITTED` / `PARTIAL` / `UNKNOWN` / `HASH_MISMATCH` / `ERROR` |
| `ngrrd.admin.drain` / `ngrrd.admin.activate` | líder | nodeId | `StorageNodeStatus` |
| `ngrrd.admin.status` | líder | — | líder, nós, séries/bytes/fill, migrações em curso |
| `ngrrd.admin.metrics` | **qualquer** storage node | — | `NodeMetricsSnapshot` (métricas locais) |
| `ngrrd.admin.rebalance` | líder | — | dispara um ciclo imediato |

Status de série (`SeriesStatus`): `OK`, `WRONG_OWNER`, `MIGRATING`, `NOT_OPEN`, `NOT_LEADER`,
`NO_STORAGE_NODE_AVAILABLE`, `ERROR`.

- **`NOT_OPEN`:** o dono não tem o handle/definição em memória (ex.: reiniciou e ainda não recebeu
  o `ngrrd.open`, ou o `SeriesHandleRegistry` fechou por ociosidade e a auto-cura falhou). O cliente
  reabre e repete a operação uma vez.
- **`WRONG_OWNER`:** o nó contatado não é (mais) o dono segundo seu catálogo local — normalmente
  logo após uma migração. O cliente invalida o cache de placement, releitura (ou pergunta ao
  líder) e reenfileira.
- **`MIGRATING`:** a série está em trânsito; cliente faz backoff (100 ms → 2 s), governado pela
  `RetryPolicy` construída a partir de `NgrrdClusterConfig#retryTimeout` (default 5 min); esgotado
  esse prazo, lança `NgrrdClusterException(MIGRATING)` — não `SERIES_UNAVAILABLE`.
- **Corrida líder→dono:** quando o líder acabou de criar um placement novo, a réplica local do
  catálogo no dono pode ainda não ter convergido. Por isso `ngrrd.open` aceita a operação mesmo com
  o catálogo local "atrasado" **se** o request já carrega o próprio `SeriesPlacement` retornado pelo
  `ngrrd.place` com `owner == self` (`placementHint`) — sem essa exceção, todo `open` logo após um
  `place` falharia com `WRONG_OWNER` por uma corrida de replicação, não por um erro real de dono.
  `writeBatch`/`checkpoint`/`flush`/`read` não carregam esse hint (só fazem sentido depois de um
  `open` bem-sucedido); com o catálogo local ainda atrasado nesses comandos, o nó consulta o líder
  com leitura forte (`placementStrong`) antes de responder `WRONG_OWNER` sem dono.

## 5. Cliente transparente

```java
NgrrdClusterClient client = NgrrdCluster.connect(NgrrdClusterConfig.fromYaml(path, System::getenv));
NgrrdHandle h = client.open(yamlDefinition, tags);   // mesma interface de sempre
h.write("in_octets", new Sample(ts, v));
h.checkpoint();
SeriesResult r = h.read("in_octets", query);
h.close();
client.close();
```

`RemoteSeriesHandle implements NgrrdHandle` — cada operação é roteada ao dono atual, com
retentativa transparente:

- **`open`:** resolve o `seriesKey` (mesmo `resolveSeriesKey` do `nishi-utils-oss`), consulta o
  catálogo local; se ausente, chama `ngrrd.place` no líder; depois `ngrrd.open` no dono retornado.
  `WRONG_OWNER` ao abrir retenta até 5 vezes antes de desistir; `MIGRATING` faz backoff até o prazo
  de retentativa.
- **`write`:** enfileira no `WriteBuffer`, um buffer por nó de destino (não por série) — agrupa
  lotes de séries diferentes que compartilham dono no mesmo `ngrrd.writeBatch`. Flush automático por
  tamanho (`batchMaxSamples`, default 500) ou tempo (`batchMaxDelay`, default 200 ms). O buffer de
  cada nó tem capacidade limitada (`maxBufferedSamplesPerNode`, default 100 000); ao encher, a
  política default é `BLOCK` (o `write()` do chamador bloqueia até haver espaço — backpressure real
  para quem produz via Kafka, por exemplo) ou `FAIL` (lança `BUFFER_FULL` imediatamente). Não existe
  política `DROP`.
- **`flush`/`checkpoint`:** drenam sincronamente o buffer do nó dono antes de enviar o comando.
- **`read`/`readPreset`:** RPC direto ao dono; `Consistency` não se aplica (dono único da série).
- **`WRONG_OWNER` com dono já conhecido na resposta:** o handle atualiza `owner` imediatamente
  (`ownerChanged`), sem esperar um novo `open()` — isso preserva a ordem do backlog reenfileirado
  pelo `WriteDispatcher` (uma versão anterior só atualizava o dono via `open()`/`noteOwner`, e o
  `write()` seguinte continuava enfileirando no dono antigo, fazendo cada lote reroteado "furar a
  fila" na frente do backlog que já tinha migrado).
- **Orçamento TOTAL de `close()`, não por handle.** `NgrrdClusterConfig#closeTimeout` (default 30 s)
  é o prazo **compartilhado** entre todos os handles abertos pelo cliente: `flush` de cada handle
  consome desse mesmo orçamento, e o `CLOSE` remoto (quando sobra tempo) também. Handles que não
  couberem no prazo (dono morto, rede lenta) fecham **sem flush** — a referência local é sempre
  liberada — e a amostra descartada é logada em `ERROR` (contabilizada em `samplesFailed` nas
  métricas do cliente). Esse é o trade-off deliberado: preferir fechar em tempo previsível a travar
  o `close()` indefinidamente esperando um nó morto.
- **Dono inalcançável, série sem réplica:** leituras e checkpoints falham com `SERIES_UNAVAILABLE`;
  escritas ficam no `WriteBuffer` até o cap configurado. Não há re-placement automático (ver seção 2).
- **Retentativa de falha de transporte** (não de aplicação) usa backoff exponencial clampado ao
  deadline efetivo da chamada — uma tentativa contra um nó morto nunca consome mais que o menor
  entre `requestTimeout` e o tempo restante do orçamento chamador (`retryTimeout` normal, ou o
  orçamento de `close()`).

## 6. Storage node

Para compilar, preparar as dependências, salvar o YAML e iniciar um ou três processos Java,
siga o [guia de execução passo a passo](ngrrd-cluster-quickstart.md). Ele inclui o comando
completo de inicialização, a consulta de status e o encerramento. O YAML abaixo é a referência
das opções; o módulo gera um JAR comum, sem dependências embutidas e sem launcher `java -jar`.

`NgrrdStorageNode` sobe um `NGridNode` (role `storage`), o `BlobVolume` local (via
`NgrrdBlob.registry()`), o `SeriesHandleRegistry`, o `StorageRequestHandler`, o
`NodeStatusReporter`, o `LocalReconciler` e, quando líder, os serviços de coordenação
(`PlacementRequestHandler`, `Rebalancer`/`MigrationCoordinator`, `AdminService`).
`NgrrdStorageNodeMain` é o processo standalone de deploy (`--config <yaml>`), sem Spring.

### 6.1. Configuração YAML (`StorageNodeConfig.fromYaml`)

```yaml
node:
  id: storage-1
  host: 10.0.1.11
  port: 7101
  priority: 100                       # opcional, default 100
  dataDir: /var/ngrrd-cluster/storage-1/ngrid
  seed: 10.0.1.11:7101                # opcional
  peers:                               # opcional
    - 10.0.1.12:7101
    - 10.0.1.13:7101

ngrrd:
  volume:
    dir: /var/ngrrd-cluster/storage-1/volume
    name: ifaceStats
    shardCount: 64                     # opcional, default BlobVolumeConfig.DEFAULT_SHARD_COUNT
    segmentBytes: 1073741824           # opcional
    initialShardCapacityBytes: 1073741824  # opcional
    capacityBytes: 2000000000000       # opcional, <= 0/omitido = desconhecida
  statusReportInterval: 10s            # opcional, default 10s
  nodeStatusStaleAfter: 50s            # opcional, default max(5x statusReportInterval, 15s)
  handleIdleTtl: 15m                   # opcional, default 15m
  maxOpenHandles: 10000                # opcional, default 10000
  requestTimeout: 20s                  # opcional, default 20s
  defaultDurability: FSYNC             # opcional
  defaultOnGeometryChange: FAIL        # opcional
  seriesObjectPrefix: series           # opcional — ver seção 3; default: ObjectNaming.seriesPrefixOrDefault()
  rebalance:
    enabled: true                      # opcional, default true
    interval: 60s                      # opcional, default 60s
    minDelta: 50                       # opcional, default 50
    tolerance: 0.10                    # opcional, default 0.10
    maxConcurrentMigrations: 2         # opcional, default 2
    maxMovesPerCycle: 50               # opcional, default 50
    migrationTimeout: 10m              # opcional, default 10m
    chunkBytes: 262144                 # opcional, default 256 KiB
    maxSeriesBytes: 67108864           # opcional, default 64 MiB
  reconcile:
    interval: 10m                      # opcional, default 10m
    orphanGrace: 5m                    # opcional, default 5m
```

Interpolação `${VAR}`/`${VAR:default}` disponível em qualquer valor string, como no restante do
projeto. **`bootDiscoveryWindow`, `affinityHandbackMode`, `placementGraceAfterLeadership`,
`migrationStatusPollInterval` e `metricsListener` não são expostos no YAML** — usam sempre o
default do builder (3 s, `true`, 3 s, 500 ms e nenhum listener, respectivamente); para outro valor,
monte o `StorageNodeConfig` via `builder()` em vez de `fromYaml`.

### 6.2. Parâmetros de eleição herdados do NGrid

Repassados ao `NGridNodeBuilder` por baixo do `StorageNodeConfig`:

- **`bootDiscoveryWindow` (default 3 s):** um nó recém-subido adia a auto-eleição por essa janela
  enquanto descobre peers e seus watermarks de replicação, antes de decidir se reivindica a
  liderança. Mitiga a deferência mútua observada quando um terceiro nó entra na malha
  (`RebalanceClusterTest`/`PlacementUnderLeaderChurnClusterTest`). `ZERO` desliga a deferral
  (comportamento legado do NGrid, eleição imediata).
- **`affinityHandbackMode` (default `true`, ligado nos storage nodes):** liga o handback
  orquestrado de liderança (D11) do NGrid. Sem ele, um nó de maior afinidade que volta à malha sob
  carga contínua pode reclamar a liderança pelo gate de watermark simples, o que sob carga
  sobrepõe dois líderes produzindo ao mesmo tempo — o mecanismo de resolução de dual-leader do core
  descarta a cauda do perdedor, e isso já reverteu flips de catálogo **já confirmados** (imagem
  migrada e apagada na origem, catálogo revertendo para a origem → o próximo `open` recria a série
  **vazia**). Com o handback ligado, o incumbente congela a produção, entrega um snapshot e só
  então cede.
  **Custo aceito do `true`:** se um handback é abortado (o candidato cai, o snapshot não fecha a
  tempo, etc.), o core aplica um **cooldown de 60 s** (`reclaimQuiesceCooldown`) antes de permitir
  que outro handback seja tentado — um nó de maior afinidade que acabou de reiniciar pode passar
  até esse período inteiro sem conseguir **reassumir** a liderança, mesmo já saudável e alcançável
  (a malha continua sendo servida normalmente pelo incumbente atual nesse meio-tempo; só a
  reassunção de liderança fica represada). Aceito porque a alternativa — reclaim imediato por
  watermark — é exatamente o cenário de perda de dados acima.
- **`placementGraceAfterLeadership` (default 3 s, não exposto no YAML):** janela após este nó
  assumir a liderança durante a qual `PlacementRequestHandler` recusa criar placements **novos**
  (responde `NOT_LEADER`, o cliente retenta) — dá tempo da réplica local do catálogo convergir
  antes de decidir sobre séries que já podem existir. Placements **já existentes** continuam
  respondidos normalmente, sem passar por essa janela.

### 6.3. `SeriesHandleRegistry`

Mapa `seriesKey → NgrrdHandle` aberto localmente via `Ngrrd.open(volume, locator, yaml, options)`.
Fecha handles ociosos após `handleIdleTtl` (LRU) e limita `maxOpenHandles`. Toda operação passa por
`withHandle`, que serializa contra fechamento concorrente; `withHandleSelfHealing` tenta
`reopenIfKnown` antes de devolver `NOT_OPEN` — mas **não** reabre uma série fechada por `CLOSE`
explícito do cliente (`closedByClient` distingue os dois casos). Migração de uma série faz
`checkpoint` + `close` do handle local e marca a série `MIGRATING` (rejeita writes locais nesse
meio-tempo).

### 6.4. `LocalReconciler` — reconciliação e adoção

Roda no start do nó (após o catálogo estabilizar), a cada `reconcileInterval` e imediatamente ao
virar líder (só reconcilia o **próprio** volume). Reconhece objetos de série pela convenção
`{seriesObjectPrefix}/{seriesKey}.ngrr`; qualquer outro prefixo no volume é ignorado.

- **Adoção — também o caminho de migração do ngrrd single-node.** Uma série presente no volume mas
  ausente do catálogo é adotada: o nó chama `ngrrd.place` com `preferredOwnerNodeId = self`. Isso
  significa que **apontar um storage node novo para um volume blob de um ngrrd single-node
  existente adota todas as séries dele automaticamente** — não existe uma ferramenta de migração
  separada; o próprio boot do reconciliador é a migração.
- **Órfã — só apagada com confirmação forte, nunca por suspeita.** Uma série presente no volume
  local, mas cujo catálogo diz `ACTIVE` em **outro** dono, só é apagada se **todas** as condições
  seguirem verdadeiras: (a) o handle não está aberto nem em migração localmente; (b) uma leitura
  **forte** ao líder (`placementStrong`, não a cópia eventual do início do ciclo) reconfirma
  `ACTIVE` no outro dono; (c) já se passou `orphanGrace` (default 5 min) desde a última transição
  desse placement; (d) o dono forte confirma, via `ngrrd.series.exists`, que **de fato possui** o
  objeto físico — qualquer falha/timeout dessa checagem é tratada como "não confirmado" (não
  apaga); (e) não é o **primeiro** ciclo desta instância do reconciliador (o primeiro ciclo depois
  de um restart nunca apaga nada, só adota e conta). Essas salvaguardas existem porque uma versão
  anterior chegou a apagar a única cópia viva de uma série durante uma corrida entre migração e
  reconciliação — ver seção 8 sobre a correção do `abort()`.
- **Ausente — nunca inventa dados.** Uma série `ACTIVE` no catálogo local de `self`, mas ausente do
  volume físico, é só logada (marker `MISSING_SERIES`, nível `SEVERE`) e contada
  (`reconcileMissing`); o reconciliador jamais recria uma imagem vazia por conta própria.

## 7. Placement (`LeastLoadedPlacementPolicy`)

Ordem total e determinística — o resultado nunca depende da ordem de iteração do catálogo local:

1. **Candidatos:** `state == ACTIVE`, membro alcançável do `ClusterCoordinator`, e (quando a
   capacidade é conhecida) `fillRatio < 0.95`, incluindo a projeção dos bytes da nova geometria e das entradas pendentes.
   **Exceção de frescor:** se o filtro de "status não velho" (`isFresh`, dentro de
   `nodeStatusStaleAfter`) eliminar **todos** os candidatos ACTIVE+alcançáveis de uma vez — sintoma
   típico de handoff de liderança recente, não de queda real — o filtro é ignorado para aquele
   ciclo; do contrário um handoff concentraria 100% das séries novas no primeiro nó a reportar ao
   novo líder.
2. Se `preferredOwnerNodeId` (adoção do `LocalReconciler`, ou reafirmação de um dono já existente)
   sobreviver aos filtros acima, ele vence direto, sem passar pelo desempate.
3. Caso contrário, desempate em ordem: **(a)** menor carga efetiva dividida pelo peso (`COUNT` usa peso 1). A carga efetiva soma `seriesCount` reportado +
   placements feitos pelo líder desde o último reporte daquele nó (`pendingSeriesByNode` — sem
   isso, uma rajada de séries novas cairia inteira no mesmo nó até o próximo status); **(b)** menor
   `fillRatio` (capacidade desconhecida conta como `0.0`); **(c)** menor `nodeId` — sempre decide,
   garantindo uma ordem total mesmo com tudo empatado.

Sem candidato algum, o líder responde `NO_STORAGE_NODE_AVAILABLE`. `PlacementPolicy` é uma
interface — trocável em teste.

## 8. Rebalanceamento e migração

Procedimentos e exemplos numéricos estão no [guia de operação](ngrrd-cluster-operacao.md).
O rebalanceamento acontece online, com espera temporária para operações da série em migração.
A entrada de um nó não obriga a mover séries: a diferença de contagens precisa superar o
limite configurado. Por exemplo, `20 / 20 / 0` não gera migração com os defaults.

`Rebalancer` roda no líder a cada `rebalanceInterval` (default 60 s; também disparado por
`MembershipListener`, com debounce de 5 s, e por `ngrrd.admin.rebalance`). O intervalo periódico
conta a partir do término do ciclo anterior. O planejamento (`RebalancePlanner`) é puro,
determinístico e ordena por chave antes de decidir, para nunca depender de ordem de iteração:

1. Nós `DRAINING`: todas as séries entram na fila de saída para o nó `ACTIVE` alcançável de menor
   carga corrente (recalculada a cada movimento). Sem destino disponível, nenhum movimento de
   drenagem é planejado neste ciclo.
2. Nós `ACTIVE`, em `COUNT`: enquanto `max − min > max(rebalanceMinDelta, rebalanceTolerance × média)`, move a
   série de menor chave do nó mais carregado para o menos carregado.
3. Respeita `maxConcurrentMigrations` (default 2, no cluster inteiro) e `maxMovesPerCycle`
   (default 50, por ciclo).

Em `CAPACITY` e `WEIGHT`, placement e rebalance compartilham pesos e metas proporcionais.
Drenagem e rebalance só movem séries com geometria confirmada para destinos que comportem os
bytes reais, incluindo reservas; saídas ainda pendentes não liberam orçamento.
Veja [configuração e atualização coordenada](ngrrd-cluster-operacao.md#capacidade-e-distribuição-ponderada-issue-167-itens-1-e-2).

`MigrationCoordinator` executa cada movimento como máquina de estados idempotente por
`migrationId` (UUID):

```
ACTIVE(src) --[migrate()]--> catálogo := MIGRATING(owner=src, target=dst)
    --> src: migrate.start           (src faz quiesce: checkpoint + close + marca MIGRATING local)
    --> src -> dst: migrate.prepare  (dst verifica capacidade e reserva os bytes reais)
    --> src -> dst: migrate.chunk*   (256 KiB por chunk default; src lê BlobStorage.get, envia N chunks)
    --> src -> dst: migrate.commit   (dst valida SHA-256, ativa a cópia)
    --> catálogo := ACTIVE(owner=dst)
    --> src: migrate.finish          (src apaga a cópia local)
```

- **Falha antes do `COMMITTED`:** o líder grava primeiro a reversão do catálogo para `ACTIVE(src)`
  (só se o placement forte ainda for `MIGRATING` com o mesmo `migrationId`) e só **depois**, se essa
  gravação tiver sucesso, envia `abort` a origem e destino — a origem reabre normalmente. Se a
  gravação não puder ser confirmada (pré-condição não satisfeita, ou liderança perdida), nenhum
  `abort` é enviado; o próximo líder resolve via `resumeInFlight()`.
- **`COMMITTED` confirmado mas o líder cai antes de flipar o catálogo:** o **novo** líder, ao
  assumir, varre sua cópia local do catálogo em busca de placements `MIGRATING`
  (`resumeInFlight()`, até 20 tentativas com backoff de 500 ms — cobre a janela em que a réplica
  local do catálogo do nó recém-eleito ainda não convergiu o `MIGRATING` gravado pelo líder
  anterior) e consulta `migrate.status` no destino: `COMMITTED` → flipa para `ACTIVE(dst)` e manda
  `finish` à origem (20 tentativas, 500 ms de backoff); qualquer outro status → aborta e devolve
  a série à origem.
- **Reversão sempre antes de notificar os nós.** Tanto o flip para `ACTIVE(dst)` quanto a reversão
  para `ACTIVE(src)` **revalidam, a cada tentativa de escrita**, que o placement forte ainda é
  `MIGRATING` com o **mesmo** `migrationId` antes de gravar — sem essa revalidação, um `abort()`
  que perde uma corrida contra outro líder (num dual-leader transitório) podia mandar
  `MIGRATE_ABORT` ao destino depois que ele **já tinha sido** commitado de verdade por outra via,
  apagando a única cópia real e revertendo o catálogo para uma origem já vazia — o próximo `open`
  recriava a série do zero. Esse foi um bloqueador de perda de dados encontrado e corrigido durante
  o M3; a notificação aos nós só é enviada **depois** que a escrita decisiva no catálogo é
  confirmada, nunca antes.
- **`finish` perdido:** a cópia órfã na origem é apagada pelo `LocalReconciler` dela, seguindo as
  salvaguardas da seção 6.4.
- **Cliente durante `MIGRATING`:** vê o status por poucos segundos (série típica ≈ 1,6 MiB em rede
  local) e retenta com backoff — ver seção 5.

## 9. Drenagem e manutenção

- `NgrrdClusterAdminCli --seed host:port drain <nodeId>` (seção 11): o líder marca o nó `DRAINING`
  (idempotente); a `LeastLoadedPlacementPolicy` para de escolhê-lo para séries novas; o próximo ciclo do
  `Rebalancer` (disparado imediatamente pelo próprio comando) começa a esvaziá-lo. Ao atingir
  `seriesCount == 0` **e** nenhuma migração em curso como origem, o `Rebalancer` promove o nó a
  `DRAINED` (nunca antes disso, mesmo que o status pareça vazio momentaneamente). Aí sim o
  operador pode parar o processo.
- `activate <nodeId>`: reverte para `ACTIVE`, idempotente; o nó volta a ser candidato no próximo
  ciclo de placement/rebalanceamento.
- `status`: líder, nós (estado, alcançável, séries, bytes, fill%), migrações em curso.
  `metrics <nodeId>`: `NodeMetricsSnapshot` completo daquele nó.
- **Movimentação de nó** (novo host/porta, mesmo `nodeId`, mesmo volume): o catálogo referencia só
  `nodeId` — o NGrid propaga o novo endereço via handshake/gossip automaticamente. Nenhuma ação no
  catálogo é necessária.
- **Queda sem drenagem:** ver seção 2 — indisponibilidade das séries do nó até ele voltar, por
  desenho.

## 10. Métricas

`NodeMetricsSnapshot` (por storage node, via `ngrrd.admin.metrics` de **qualquer** nó — não só o
líder): `leader` (se este nó é líder agora), `seriesCount`, `usedBytes`/`capacityBytes`,
`openHandles`, contadores de `writeBatches`/`samplesWritten`/`samplesFailed`/`checkpoints`/
`flushes`/`reads`, histogramas de latência (`writeBatchLatency`, `checkpointLatency`,
`readLatency`), `errorsByStatus` (por `SeriesStatus`), `blobStats` (resumo de
`BlobVolumeStats`), `migrationsIn`/`migrationsOut` e, desde o M4, `reconcileAdopted`/
`reconcileOrphansDeleted`/`reconcileUnplaced`/`reconcileMissing`/`reconcileLastDurationMs`.

`ClientMetricsSnapshot` (no cliente): `samplesEnqueued`/`samplesSent`/`samplesFailed`,
`batchesSent`, `retriesByStatus`, `bufferedSamples` por nó de destino, `openHandles`,
`rpcLatency` (agregada, não quebrada por comando) e `placeCount`.

Integração opcional via `NgrrdClusterMetricsListener` (storage node e cliente); log periódico
`NGRRD_NODE_STATUS` (marker de log do projeto — base para futuros Docker ITs), `NGRRD_REBALANCE`/
`NGRRD_REBALANCE_MOVE` a cada ciclo de rebalanceamento, `NGRRD_NODE_DRAINED` quando o `Rebalancer`
promove um nó a `DRAINED`, `NGRRD_RECONCILE`/`NGRRD_RECONCILE_ORPHAN_DELETED`/`RECONCILE_UNPLACED`
a cada ciclo do reconciliador, e `NGRRD_STORAGE_NODE_STARTED` (processo pronto, emitido por
`NgrrdStorageNodeMain`).

## 11. CLI de administração

```
java -cp ... dev.nishisan.utils.oss.cluster.admin.NgrrdClusterAdminCli \
  --seed host:port [--client-id x] <status|metrics <nodeId>|drain <nodeId>|activate <nodeId>|rebalance>
```

Entra na malha como cliente transparente (mesmo papel `client`+`leader-ineligible` de
`NgrrdClusterClient`), executa um único comando e sai — sem dependência de biblioteca de CLI,
saída tabular em texto simples. Código de saída `0` em sucesso, `1` em qualquer falha (parsing,
conexão ou erro remoto), sempre reportada em `stderr` — nunca lança para o chamador.

## 12. Testes

- **Unitários (`mvn -pl nishi-utils-ngrrd-cluster test`, padrão do dia a dia):** políticas de
  placement, planejador de rebalanceamento sobre snapshots puros, coordenador de migração com
  transporte fake, `WriteBuffer`/`WriteDispatcher` (limites, `BLOCK`/`FAIL`, agrupamento por nó),
  `PlacementResolver` (invalidação em `WRONG_OWNER`), round-trip Jackson dos payloads do protocolo,
  `LocalReconciler` com volume real em `@TempDir`. Mais de 330 testes unitários, cobertura de linha
  na casa dos 84%.
- **Cluster in-process (profile `ngrrd-cluster`, mesmo espírito do `-Presilience` do core):**
  `mvn -pl nishi-utils-ngrrd-cluster verify -Pngrrd-cluster`. Sobem múltiplos `NGridNode` de
  verdade em processo (handshake, gossip, eleição real) via `NgrrdClusterTestHarness` — mais lentos
  e sensíveis a tempo/recursos da máquina que a suíte unitária. Cobrem: distribuição de séries
  novas entre nós e write/read/checkpoint transparentes (`DistributedWriteReadClusterTest`);
  robustez a reinício de nó (`NodeRestartClusterTest`); rebalanceamento ao entrar nó
  (`RebalanceClusterTest`); queda do líder no meio de uma migração
  (`LeaderFailoverDuringMigrationClusterTest` — ver seção 13); churn de liderança durante placement
  (`PlacementUnderLeaderChurnClusterTest`); drenagem até zero séries (`DrainClusterTest`); adoção de
  volume single-node existente (`AdoptExistingVolumeClusterTest`); status/CLI administrativos
  (`AdminStatusClusterTest`, `AdminCliClusterTest`).
- **Fora do CI hospedado, por desenho** — mesmo motivo e mesmo padrão da suíte de resiliência do
  NGrid (`doc/testes-vermelhos-conhecidos.md`): os `*ClusterTest` deste módulo são sensíveis a
  tempo e recursos do executor e não passam de forma confiável em runner hospedado. `pr-validation.yml`
  não os toca (roda `mvn verify -pl nishi-utils-core -DexcludeNgrid=true`, que nem sequer constrói
  este módulo); rode localmente com `mvn -pl nishi-utils-ngrrd-cluster verify -Pngrrd-cluster`.
- **Docker IT:** fora do escopo deste marco — follow-up natural reaproveitando `NGridNodeContainer`
  e os markers de log da seção 10, no módulo `ngrid-test`.
- Nomes de `@Test` em PT-BR (padrão do TEMS); classes auxiliares, campos e métodos de apoio em
  inglês.

## 13. Limites conhecidos, vermelhos conhecidos e trabalhos futuros

### 13.1. Limites de desenho aceitos

- **Chunks de migração em JSON+Base64.** O transporte do NGrid não tem um caminho binário puro para
  este protocolo — cada `migrate.chunk` viaja como JSON com o payload em Base64 (LZ4 acima de
  512 B, frame até 64 MB). Adequado para séries de poucos MiB (uma série típica ≈ 1,6 MiB vira ~7
  chunks de 256 KiB); se surgir uma série muito maior, medir antes de considerar um frame binário
  dedicado no codec (fora deste plano).
- **Churn de bootstrap do NGrid.** Handoffs de liderança durante o boot de um cluster novo (dois ou
  três nós disputando afinidade/prioridade nos primeiros segundos) são ruído padrão observado em
  todos os marcos — não é um defeito deste módulo, é como o `ClusterCoordinator` do core se
  comporta sob entrada simultânea de peers. `bootDiscoveryWindow` (seção 6.2) mitiga, não elimina.
  Métrica prática: em vez de "cada storage node recebe ≈ 1/4 das séries novas" sob esse churn, o
  critério de aceite observado nos testes é "≥ 1/4 por nó" — a distribuição perfeita só se
  estabiliza depois que a liderança assenta.
- **`ngrrd.admin.status`/`ngrrd.admin.drain`/`ngrrd.admin.activate`/`ngrrd.admin.rebalance`
  bloqueiam sem líder.** São comandos do líder (`LEADER_COMMANDS`); sem maioria elegível eleita
  (seção 2), eles simplesmente não respondem até haver líder — não há timeout curto dedicado além
  do `requestTimeout` do RPC. O tick do `NodeStatusReporter` (`putNodeStatus` → `invokeLeader`)
  tem o mesmo comportamento: pode ficar bloqueado por até ~28 s sem líder eleito. Esse
  comportamento é **pré-existente no core** (não introduzido por este módulo) e já está
  documentado como tal desde o M4.

### 13.2. Vermelho conhecido deste módulo

`LeaderFailoverDuringMigrationClusterTest` falha intermitentemente (~1 em 4 execuções) — ver
detalhe e as duas assinaturas observadas em `doc/testes-vermelhos-conhecidos.md`. Investigação
adicional (fora do escopo deste marco) autorizada para depois do M5.

### 13.3. Trabalhos futuros — pontos abertos no core (fora do escopo deste módulo)

Encontrados durante o desenvolvimento do ngrrd cluster, mas pertencem ao NGrid (`nishi-utils-core`)
e afetam qualquer usuário do NGrid, não só este módulo:

- **Ressincronização de mapa não persistente após restart.** Um `DistributedMap` configurado sem
  persistência (`NMapPersistenceMode.DISABLED`) não recupera seu conteúdo automaticamente ao
  reingressar num cluster já convergido após reiniciar — depende de o líder reenviar um snapshot
  completo por outro gatilho.
- **Higiene do `TcpTransport`.** Três comportamentos observados que merecem endurecimento:
  reconexão infinita sem backoff-teto contra um peer que nunca vai voltar; possibilidade de um nó
  tentar abrir proxy para si mesmo; e um fast-path de `sendAndAwait` que não cobre todos os casos de
  borda de timeout/reconexão.
- **`NMapPersistence` falha em silêncio para valor não serializável.** Um valor sem `Serializable`
  gravado num `DistributedMap` persistente falha o append do WAL sem propagar a exceção — o mapa
  parece persistir, mas não persiste (a causa raiz do bug de catálogo vazio corrigido no M1c; ver
  seção 3). Deveria falhar alto (exceção explícita), não em silêncio.
- **Reclaim de liderança cego à linhagem.** Segue como limitação conhecida desde o handoff por
  afinidade (D10, `doc/CHANGELOG.md`, entrada de 2026-06-11): contadores escalares de progresso são
  cegos à linhagem/epoch — operações aplicadas a partir de ramos descartados inflam o contador e
  inviabilizam um emparelhamento exato entre incumbente e candidato durante o handoff. Follow-up
  epoch-aware (referenciado ali como PR #142) continua pendente.
- **Saída graciosa (`LEAVE`) do NGrid, janela de bootstrap padrão e logs de handoff.** O NGrid não
  tem hoje uma mensagem explícita de saída graciosa de um nó (`LEAVE`) distinta de uma queda —
  todo desligamento de nó é indistinguível de uma falha do ponto de vista dos peers, o que
  contribui para o churn de bootstrap citado em 13.1. Relacionado: o default de janela de bootstrap
  do próprio core (fora do `bootDiscoveryWindow` específico do `NGridNodeBuilder` usado por este
  módulo) e o nível de detalhe dos logs de handoff de liderança poderiam ser revistos juntos.
  **Ainda não confirmado em código** — item registrado a partir de observação de campo; os demais
  pontos desta seção foram confirmados nos artefatos de planejamento (`checkpoint.md`) e no
  `doc/CHANGELOG.md`.

Nenhum destes pontos bloqueia o uso do ngrrd cluster hoje — ficam registrados aqui porque foram
encontrados no caminho e afetam a base sobre a qual este módulo é construído.

## 14. Migração de um ngrrd single-node existente

Não existe (nem é necessária) uma ferramenta de migração dedicada: subir um storage node apontando
`ngrrd.volume.dir`/`ngrrd.volume.name` para o volume blob de uma instalação single-node existente
faz o `LocalReconciler` adotar automaticamente todas as séries dele no primeiro ciclo de
reconciliação (seção 6.4) — o mesmo mecanismo usado para reconhecer séries "órfãs" de qualquer
outro motivo. Não há downtime imposto pelo cluster: o volume pode continuar sendo servido
single-node até o storage node estar pronto, desde que não haja escrita concorrente ao mesmo volume
por dois processos simultaneamente.

## Diagramas

Visão de containers (cliente, storage nodes, catálogo replicado, volume por nó):

![C4 Container do ngrrd cluster](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/oss/diagrams/ngrrd_cluster_c4_container.puml)

Fluxo de escrita/leitura (open → place → open → writeBatch/checkpoint → read, com WRONG_OWNER/NOT_OPEN):

![Sequência de escrita e leitura](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/oss/diagrams/ngrrd_cluster_sequence_write.puml)

Fluxo de migração e rebalanceamento (start, chunks, commit, flip, finish; abort e resolução por novo líder):

![Sequência de migração](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/oss/diagrams/ngrrd_cluster_sequence_migration.puml)

Estados de `SeriesPlacement` e do destino durante uma migração (`MigrateStatus`):

![Estados de migração](https://uml.nishisan.dev/proxy?src=https://raw.githubusercontent.com/nishisan-dev/nishi-utils/main/doc/oss/diagrams/ngrrd_cluster_state_migration.puml)
