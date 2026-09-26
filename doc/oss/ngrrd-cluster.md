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
- **Lag da réplica local do catálogo (`StorageNodeStatus.catalogReplica`, issue #177, desde a
  8.7.0).** Cada storage publica, junto do resto do status, um `CatalogReplicaStatus` (`leader`,
  `lag`, `leaderHighWatermark`, `nextExpectedSequence`, `syncing`, `pendingBootstrap`, `streaming`)
  com o lag **por tópico** (`map:ngrrd.catalog`) do `ReplicationManager` do NGrid — diferente do
  `HIGH_REPLICATION_LAG` global do snapshot operacional, que fica dessincronizado entre líder e
  seguidores (issue #178, ainda aberta) e não serve para decidir se a réplica do catálogo deste nó
  é confiável. `lag = 0` só é significativo quando o high-watermark do líder já é conhecido
  (`leaderHighWatermark > 0`); o campo é `null` num status publicado por um nó anterior a esta
  versão. Ver a coluna `CAT_LAG` (seção 11) e o gate de rebalance (seção 8).

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
  reabre e repete a operação. Desde a 8.4.1, checkpoint, flush e leituras toleram
  `WRONG_OWNER` seguido de `NOT_OPEN`, inclusive novas migrações durante a reabertura,
  compartilhando um único `retryTimeout` com o `OPEN` e a consulta ao líder.
- **`WRONG_OWNER`:** o nó contatado não é (mais) o dono segundo seu catálogo local — normalmente
  logo após uma migração. O cliente invalida o cache de placement, releitura (ou pergunta ao
  líder) e reenfileira.
- **Redirecionamento confirmado no líder (issue #177, desde a 8.7.0).** Um `WRONG_OWNER`/`MIGRATING`
  derivado só da réplica local do catálogo pode estar atrasado — o sintoma em produção era um
  pingue-pongue entre origem e destino de um rebalance durante minutos, cada um apontando para o
  outro. `StorageRequestHandler` confirma no líder, numa única consulta em lote por request
  (`ngrrd.catalog.lookup`, prazo de 2 s), todo redirecionamento que a réplica local geraria, exceto
  quando o próprio nó já é o líder; o líder encontrar outro dono ou nenhum vira a resposta enviada
  ao cliente, uma confirmação positiva fica em cache por 5 s (nunca autoriza criar), e o líder
  inalcançável faz o nó cair no comportamento da 8.6.0 (responder pela réplica local), com cooldown
  de 1 s entre tentativas de confirmar de novo. Do lado do cliente, `WriteDispatcher`/
  `RemoteSeriesHandle` classificam uma dica de `WRONG_OWNER` como **contraditória** — aponta para o
  próprio nó, para um nó já visitado no episódio de redirecionamento, diverge do dono já confirmado
  pelo líder, ou o episódio já tem 4 saltos — e resolvem no líder com uma consulta coalescida antes
  de reenviar, em vez de seguir a dica cegamente; a cadeia legítima de poucos saltos continua sem
  RPC extra. Métricas, ordem de atualização e troubleshooting em
  [`ngrrd-cluster-operacao.md`](ngrrd-cluster-operacao.md#confirmação-de-redirecionamento-no-líder-e-lag-do-catálogo-issue-177).
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
- **`flush`/`checkpoint`:** aguardam as escritas pendentes da série, inclusive as redirecionadas,
  antes de enviar o comando. A espera e a recuperação usam o mesmo `retryTimeout`.
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

### 5.1. Consultar existência e abrir sem criar

Por padrão, `open` posiciona (`ngrrd.place`) e cria a série quando ela ainda não existe no
catálogo. Para um consumidor que percorre um catálogo externo com centenas de milhares de
chaves e precisa saber quais têm dados sem materializar séries vazias, `NgrrdClusterClient`
expõe uma consulta que nunca cria e um `open` que nunca posiciona:

```java
import dev.nishisan.utils.oss.api.Sample;
import dev.nishisan.utils.oss.api.SeriesNotFoundException;
import dev.nishisan.utils.oss.cluster.api.SeriesInfo;

boolean one = client.exists("iface:eth0:in_octets");
Map<String, Boolean> many = client.exists(List.of("iface:eth0:in_octets", "iface:eth1:in_octets"));
Optional<SeriesInfo> info = client.find("iface:eth0:in_octets");

Ngrrd.OpenOptions readOnly = Ngrrd.OpenOptions.defaults().withCreateIfMissing(false);
try (NgrrdHandle handle = client.open(yaml, tags, readOnly)) {
    SeriesResult r = handle.read("in_octets", query);           // leitura normal
    handle.write("in_octets", new Sample(ts, v));               // lança IllegalStateException
} catch (SeriesNotFoundException e) {
    switch (e.reason()) {
        case NOT_PLACED -> { /* não existe no cluster: remover do catálogo externo */ }
        case MISSING_ON_OWNER -> { /* placement sem arquivo: inconsistência, NÃO remover */ }
        default -> throw e;   // ABSENT não ocorre no cluster
    }
}
```

Exemplo de conciliação de um catálogo externo (ex.: Mongo) contra o cluster, usando `verify`
para confirmar fisicamente antes de decidir:

```java
import dev.nishisan.utils.oss.cluster.api.SeriesVerification;

Map<String, SeriesVerification> report = client.verify(externalCatalogKeys);
for (var entry : report.entrySet()) {
    switch (entry.getValue()) {
        case NOT_PLACED -> externalCatalog.remove(entry.getKey());       // ausência confirmada
        case PRESENT -> { /* nada a fazer */ }
        case MISSING_ON_OWNER -> alerting.reportInconsistency(entry.getKey()); // NUNCA remover
        case UNVERIFIED -> { /* falha ao confirmar: reagenda para a próxima rodada */ }
    }
}
```

**Existência = placement no catálogo (`ngrrd.catalog`), não presença física do arquivo.**
`exists`/`find` só consultam o mapa de placement — um hit na réplica local (ou num override
recente, ex.: de um `WRONG_OWNER`) responde sem RPC; um miss é confirmado em lote no líder
(`ngrrd.catalog.lookup`, ver `NgrrdClusterConfig.catalogLookupBatchSize`, default 2000,
1..10000) antes de responder `false`. `MIGRATING` conta como existente. Um placement sem
arquivo (cliente que caiu entre `PLACE` e `OPEN`, ou disco perdido) aparece como `true` em
`exists` — use `verify` para confirmar fisicamente com o dono antes de decidir.

**Contrato de consistência:**

- Depois de um `open`/`PLACE` feito por outro cliente: se a réplica local ainda não o viu, o
  miss é confirmado no líder, que já tem o placement — devolve `true`.
- Durante migração (`MIGRATING`): devolve `true`; `open` sem criar segue o fluxo normal de
  espera/redirect.
- `false` significa "o líder atual não tem placement para a chave no momento da consulta" —
  não é atômico com um `open` concorrente de outro cliente que crie a série logo em seguida.
- `exists` não lê o storage: um placement sem arquivo aparece como `true`, e o `open` sem criar
  dessa série lança `SeriesNotFoundException` com `Reason.MISSING_ON_OWNER`.
- Falha ao confirmar com o líder (sem líder, timeout, falha de transporte, líder antigo sem a
  capacidade `catalog.lookup`) **nunca** vira `false` — sempre `NgrrdClusterException`.

**`verify` é a verificação física**, diferente de `exists`/`find` (só catálogo): confirma no
dono de cada série se o objeto existe de fato no volume. Devolve um `SeriesVerification` por
chave — `PRESENT`, `MISSING_ON_OWNER`, `NOT_PLACED` ou `UNVERIFIED` (não foi possível confirmar
com o dono; nunca interpretado como ausência). É o ponto natural para um relatório de
conciliação sob demanda, mais caro que `exists` (RPC a cada dono, não só ao líder).

**`open` sem criar abre um handle SOMENTE LEITURA.** O cliente nunca chama `ngrrd.place`: o
dono vem do catálogo e o storage recusa abrir série inexistente. `write`, `flush` e
`checkpoint` lançam `IllegalStateException`; o `close()` é local (sem `CLOSE` remoto nem
drenagem de buffers — o storage fecha a série por ociosidade). Série ausente faz o `open` (ou
uma leitura posterior, se ela deixar de existir) lançar `SeriesNotFoundException`; o handle se
fecha e sai do cache do cliente. Um handle somente leitura nunca dispara migração nem recriação
de geometria: o storage abre sem criar sempre com `OnGeometryChange.FAIL`, ignorando o
`onGeometryChange` pedido — se a definição do leitor diverge da geometria gravada, o `open`
falha com erro e o arquivo não é regravado. Se a série já estiver aberta no storage (por um
gravável, por exemplo), o handle existente é reaproveitado e nada muda. A única exceção é um
arquivo presente mas truncado (menor que o header fixo): ele passa na checagem de existência e
o writer o reinicializa como série vazia — caso raro, descrito em
[`doc/oss/ngrrd.md`](ngrrd.md#abrir-sem-criar-e-consultar-existência).

No máximo um handle principal por `seriesKey` fica em cache. As combinações entre abrir com e
sem criação:

- **com criação + gravável já em cache:** devolve o mesmo handle (compartilhado);
- **com criação + somente leitura já em cache:** abre um gravável NOVO com as opções de quem
  pediu criar e o coloca no lugar do somente leitura no cache — o somente leitura antigo segue
  válido e lendo para quem já o tinha, `close()` dele continua local, sem afetar o gravável;
- **sem criar + gravável já em cache:** devolve uma VISTA somente leitura nova a CADA chamada —
  leituras delegam ao gravável, escrita lança `IllegalStateException`, `close()` da vista fecha
  só a vista (nunca o gravável, que segue escrevendo);
- **sem criar + somente leitura já em cache:** devolve o existente (compartilhado).

O `close()` de um gravável compartilhado fecha o handle para **todos** os chamadores que o
obtiveram (contrato desde a 8.5.0); um somente leitura compartilhado tem o mesmo
comportamento entre quem o recebeu, mas uma vista aberta sobre um gravável é sempre exclusiva
de quem a pediu.

**Como o consumidor deve reagir:**

| Resultado | Ação |
|---|---|
| `exists` → `false`, ou `SeriesNotFoundException` com `NOT_PLACED` | Série confirmada ausente no cluster — seguro remover do catálogo externo. |
| `SeriesNotFoundException` com `MISSING_ON_OWNER`, ou `verify` → `MISSING_ON_OWNER` | Inconsistência do cluster (placement sem arquivo) — **nunca** remover; investigar. |
| `NgrrdClusterException` (inclusive `UNSUPPORTED_BY_NODE`), ou `verify` → `UNVERIFIED` | Falha ao consultar — nenhuma decisão sobre o catálogo externo; retentar depois. |

`open` sem criar exige que o dono anuncie `open.createIfMissing` no status publicado (e
`exists`/`find`/`verify` exigem `catalog.lookup` no líder); um storage de versão anterior faz a
operação falhar com `NgrrdClusterException` de código `ErrorCode.UNSUPPORTED_BY_NODE` **antes**
de qualquer RPC — nunca arriscando criar a série ou devolver `false` por engano. Atualize todos
os storages antes de usar `exists`, `find`, `verify` ou `open` sem criar nos clientes (ordem
detalhada no [guia de operação](ngrrd-cluster-operacao.md)).

**Mudanças no caminho que cria (8.6.0).** Mesmo quem não usa as APIs novas percebe três
diferenças em relação à 8.5.0:

- `open` durante um `close()` lento do mesmo handle abre um handle **novo** — a 8.5.0 devolvia
  o handle que estava fechando.
- A reabertura automática de uma série no storage (depois de um fechamento por ociosidade/LRU)
  **nunca cria**: se o objeto sumiu nesse meio-tempo, o storage responde `NOT_OPEN` e o `OPEN`
  do cliente, que decide `createIfMissing` por si, recria a série — um round-trip a mais.
- `PLACE` de séries novas aguarda `placementGraceAfterLeadership` (3 s por padrão) também depois
  que o **primeiro** líder do boot assume: a criação das primeiras séries logo após subir o
  cluster atrasa até esse prazo (o cliente retenta `NOT_LEADER` dentro do `retryTimeout`).

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
    maxDestinationCatalogLag: 1000     # opcional, default 1000 — issue #177; -1 desliga, 0 exige réplica em dia
  reconcile:
    interval: 10m                      # opcional, default 10m
    orphanGrace: 5m                    # opcional, default 5m
  quota:                               # opcional; 0/omitido = sem limite (issue #167, item 3)
    maxSeries: 200000
    maxBytes: 68719476736
  placement:
    rules:                             # avaliadas em ordem; a primeira que casa vence
      - name: tems-core
        definition: ifaceStats         # metadata.name da definição (opcional)
        keyPrefix: "br-sp/"            # prefixo da seriesKey (opcional) — ao menos um dos dois
        pin: [storage-1, storage-2]
      - name: no-lab-on-3
        keyPrefix: "lab/"
        exclude: [storage-3]           # exatamente um de pin/exclude
```

**Cota e regras de placement (issue #167, item 3).** `ngrrd.quota.maxSeries`/`maxBytes` são a cota
**dura** deste nó como destino (`0`/omitido = sem limite; negativo falha o boot): o líder não coloca
nem migra para cá uma série que a ultrapasse, e o próprio nó recusa `MIGRATE_PREPARE` além dela
(seções 7 e 8). `ngrrd.placement.rules` é uma lista ordenada de regras — cada uma com `name`
(único), ao menos um critério (`definition` = `metadata.name` da definição ngrrd; `keyPrefix` =
prefixo da chave da série; os dois juntos valem em E) e exatamente um efeito não vazio (`pin`: a
série só vive nos nós listados; `exclude`: em qualquer nó menos os listados). A primeira regra que
casa vence; sem casamento a série é irrestrita. Uma regra malformada ou um nome duplicado falha o
boot com `ngrrd.placement.rules[i]: …`. As regras são **configuração uniforme**: todo storage node
carrega a mesma lista (mesmo precedente de `distribution.mode`) e o líder aplica a cópia dele; cada
nó publica o fingerprint da sua lista em `StorageNodeStatus.placementRulesHash` (SHA-256 do texto
canônico, 16 hex) e o líder loga `NGRRD_PLACEMENT_RULES divergent leader=<hash> nodes=<id>(<hash|->),…`
em `WARNING` (uma vez por mudança) quando um nó `ACTIVE` alcançável diverge — as regras nunca são
descartadas por divergência. Ver [operação](ngrrd-cluster-operacao.md#cotas-e-regras-de-placement-issue-167-item-3).

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
  respondidos normalmente, sem passar por essa janela. A mesma janela vale para os misses de
  `ngrrd.catalog.lookup` (`exists`/`find`/`verify`), que também respondem `NOT_LEADER` enquanto
  a réplica do líder ainda sincroniza um mandato anterior. A janela é marcada antes de qualquer
  outro trabalho da posse, e também no primeiro líder eleito durante o boot.

### 6.3. `SeriesHandleRegistry`

Mapa `seriesKey → NgrrdHandle` aberto localmente via `Ngrrd.open(volume, locator, yaml, options)`.
Fecha handles ociosos após `handleIdleTtl` (LRU) e limita `maxOpenHandles`. Toda operação passa por
`withHandle`, que serializa contra fechamento concorrente; `withHandleSelfHealing` tenta
`reopenIfKnown` antes de devolver `NOT_OPEN` — mas **não** reabre uma série fechada por `CLOSE`
explícito do cliente: o `CLOSE` descarta a definição em cache da série (mesmo que o handle já
tenha sido fechado por ociosidade), e sem ela `reopenIfKnown` não reabre; só um novo `open` a
devolve. Nenhuma marca por série fechada fica em memória. Migração de uma série faz
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
2. **Cota e regras (issue #167, item 3):** depois da guarda de capacidade, `DestinationEligibility`
   descarta o candidato cuja cota dura não comporta mais uma série —
   `quota_series(<seriesCount + pendentes + 1>/<maxSeries>)` ou
   `quota_bytes(<usados + max(reservados, pendentes) + pedidos>/<maxBytes>)` — e depois o que a
   primeira regra casada pela série (`seriesKey` + `definitionName` vindos do `PlaceRequest`) não
   admite: `rule_pinned_elsewhere(<regra>)` ou `rule_excluded(<regra>)`. Uma série cujo
   `definitionName` é desconhecido (placement legado, cliente anterior a este campo) só casa regras
   sem o critério `definition`. Um `pin` **nunca transborda**: sem nó fixado elegível a decisão é
   vazia mesmo havendo outros nós livres. Quando este filtro esvazia um conjunto que tinha
   candidatos, o líder loga `NGRRD_PLACEMENT_NO_CANDIDATE series=<chave> excluded=<id>(<motivo>),…`.
   A ordem completa de elegibilidade é: `ACTIVE` → alcançável → capacidade (95 %) → cota → regras →
   frescor → dono preferido.
3. Se `preferredOwnerNodeId` (adoção do `LocalReconciler`, ou reafirmação de um dono já existente)
   sobreviver aos filtros acima, ele vence direto, sem passar pelo desempate — e **ignora cota e
   regras** (log `FINE`): a série já existe naquele volume; o rebalance corrige depois (fase 0).
4. Caso contrário, desempate em ordem: **(a)** menor carga efetiva dividida pelo peso (`COUNT` usa peso 1). A carga efetiva soma `seriesCount` reportado +
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

**Cota dura e regras de placement no rebalance (issue #167, item 3).** Cota e regras gateiam só
**destinos**; um nó que já está acima da própria cota (adoção, dono preferido, cota reduzida) é
tratado como **fonte**: em `COUNT` uma fonte com `carga > maxSeries` sempre cede, sem esperar o
limiar de `minDelta`/`tolerance`. No planejador (`CapacityAwarePlanner`):

- **Fase 0**, antes da drenagem: toda série cujo dono `ACTIVE` as regras não admitem vai para o
  primeiro destino elegível que caiba, mesmo com o cluster equilibrado (limitada por
  `maxMovesPerCycle`) — é o que corrige uma adoção, um dono preferido ou uma regra nova.
- Um nó com `carga >= maxSeries` sai da lista de destinos; um destino cuja cota de bytes não
  comporta `usados + entrando + bytes da série` não recebe; um destino que a regra da série exclui
  é pulado tanto na drenagem quanto no balanceamento.
- Em `CAPACITY`/`WEIGHT` os alvos são calculados por *water-filling*: o alvo de um nó é limitado à
  sua `maxSeries` e o excedente é redistribuído por peso entre os nós sem teto, até nenhum estourar
  (determinístico, ids ordenados).
- `Rebalancer.excludedDestinations` (devolvido por `ngrrd.admin.rebalance`) passa a listar também
  os motivos de nó `quota_series(<n>/<max>)`/`quota_bytes(<n>/<max>)`, além dos de réplica
  atrasada; `MigrationCoordinator` recheca cota e regra do destino quando a migração vai começar
  (`SKIPPED` com `destino <dst> inelegível: <motivo>`), e o **próprio destino** recusa
  `MIGRATE_PREPARE` com `MigrateStatus.QUOTA_EXCEEDED` quando `entradas do volume + alvos de
  migração abertos >= maxSeries` ou `usados + reservados + totalBytes > maxBytes` da configuração
  local — a origem falha e o líder aborta.

**Destino excluído por réplica do catálogo atrasada (issue #177).** `CatalogLagGate` retira da
lista de destinos elegíveis, em qualquer modo de distribuição, um nó cuja `CatalogReplicaStatus`
publicada (seção 3) tenha lag acima de `ngrrd.rebalance.maxDestinationCatalogLag` (default 1000),
lag desconhecido, sincronização por snapshot em curso ou bootstrap do relay pendente — um destino
nessas condições responde pela réplica durante o corte de dono e alimenta o pingue-pongue de
`WRONG_OWNER` descrito na seção 4. A exclusão vale só para **destino**: o nó continua podendo ser
origem e continua contado no cálculo da distribuição alvo. `-1` desliga a porta; `0` exige a
réplica em dia; um nó sem o campo (rolling upgrade a partir da 8.6.0) é elegível. O líder nunca é
excluído (a réplica dele é a fonte). A condição é reconferida quando cada migração vai começar de
fato (`MigrationCoordinator`) — se o destino deixou de ser elegível nesse meio-tempo, o movimento
é `SKIPPED`; migrações já em andamento retomadas por um novo líder (`resumeInFlight()`) não passam
por essa checagem. `NGRRD_REBALANCE_DEST_EXCLUDED` sai em `INFO` só quando o conjunto de exclusões
muda de um ciclo para o outro (evita repetir a mesma linha a cada ciclo).

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
- **Origem depois do `finish`:** a origem marca a série como esquecida (`forget`: solta o handle,
  descarta a definição em cache) e apaga a cópia local. Enquanto a réplica local do catálogo dela
  ainda disser `ACTIVE(src)`, a marca faz toda requisição da série ser decidida pelo líder; quando
  a réplica mostra o novo dono, a marca é descartada (na própria requisição ou no ciclo seguinte do
  `LocalReconciler`). A marca é só em memória: se a origem **reiniciar** logo após o `finish` com a
  réplica ainda atrasada, um `open` com criação encontra `ACTIVE(src)` e nenhum objeto no volume — e
  aí o storage confirma o dono no líder antes de criar, respondendo `WRONG_OWNER(dst)` em vez de
  recriar a série vazia na origem (issue #174). Custo em
  [operação](ngrrd-cluster-operacao.md#confirmação-do-dono-antes-de-criar-issue-174).
- **Cliente durante `MIGRATING`:** vê o status por poucos segundos (série típica ≈ 1,6 MiB em rede
  local) e retenta com backoff — ver seção 5.

## 9. Drenagem e manutenção

- `NgrrdClusterAdminCli --seed host:port drain <nodeId>` (seção 11): o líder marca o nó `DRAINING`
  (idempotente); a `LeastLoadedPlacementPolicy` para de escolhê-lo para séries novas; o próximo ciclo do
  `Rebalancer` (disparado imediatamente pelo próprio comando) começa a esvaziá-lo. Ao atingir
  `seriesCount == 0` **e** nenhuma migração em curso como origem, o `Rebalancer` promove o nó a
  `DRAINED` (nunca antes disso, mesmo que o status pareça vazio momentaneamente). Aí sim o
  operador pode parar o processo.
- **Drenagem sob cota e regras (issue #167, item 3):** uma série presa por `pin` (nenhum nó fixado
  `ACTIVE`, alcançável, com cota e capacidade) ou sem destino admitido pela sua regra **não sai**
  do nó — o drain fica pendente, o nó permanece `DRAINING` (nunca é promovido a `DRAINED` com séries)
  e o líder loga `NGRRD_DRAIN_PENDING reason=no_admissible_destination_or_confirmed_geometry_or_quota_or_rules
  rulesSkipped=<n>` (`n` = séries sem destino por causa das regras). Cabe ao operador ampliar o
  `pin`/cota (reinício coordenado) ou reativar o nó.
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
`reconcileOrphansDeleted`/`reconcileUnplaced`/`reconcileMissing`/`reconcileLastDurationMs`
e, desde a 8.6.0, `leaderConfirmations`/`leaderConfirmationLatency` (leituras fortes no líder para
confirmar o dono de uma série sem objeto no volume antes de criá-la ou de responder `NOT_FOUND`) e,
desde a 8.7.0, `redirectConfirmations`/`redirectOverrides`/`redirectConfirmationFailures`/
`redirectCacheHits` — confirmações no líder de um redirecionamento (`WRONG_OWNER`/`MIGRATING`)
derivado da réplica local do catálogo (issue #177): `redirectConfirmations` conta as séries
confirmadas numa consulta ao líder, `redirectOverrides` quantas vezes o líder divergiu da réplica
local, `redirectConfirmationFailures` quantas vezes a resposta caiu para a réplica local por falha
ou indisponibilidade do líder, e `redirectCacheHits` quantas foram respondidas por uma confirmação
recente em cache (TTL de 5 s) sem nova consulta.

`ClientMetricsSnapshot` (no cliente): `samplesEnqueued`/`samplesSent`/`samplesFailed`,
`batchesSent`, `retriesByStatus`, `bufferedSamples` por nó de destino, `openHandles`,
`rpcLatency` (agregada, não quebrada por comando), `placeCount` e, desde a 8.7.0, `ownerLookups`
(consultas ao líder feitas pelo `WriteDispatcher` para desempatar uma dica de dono contraditória) e
`redirectCycles` (dicas de `WRONG_OWNER` classificadas como contraditórias — apontam para o próprio
nó, para um nó já visitado no episódio, divergem do dono confirmado, ou excedem 4 saltos).

Integração opcional via `NgrrdClusterMetricsListener` (storage node e cliente); log periódico
`NGRRD_NODE_STATUS` (marker de log do projeto — base para futuros Docker ITs; inclui
`leaderConfirmations`/`leaderConfirmationP99us`, as confirmações de dono no líder antes de criar
uma série ou responder `NOT_FOUND`, `catalogLag=` — forma curta do lag da réplica local do
catálogo, seção 3 — e `redirectConfirmations`/`redirectOverrides`/`redirectConfirmationFailures`/
`redirectCacheHits`), `NGRRD_REBALANCE`/
`NGRRD_REBALANCE_MOVE` a cada ciclo de rebalanceamento, `NGRRD_REBALANCE_DEST_EXCLUDED` quando o
conjunto de destinos excluídos por lag do catálogo muda de um ciclo para o outro (issue #177),
`NGRRD_NODE_DRAINED` quando o `Rebalancer`
promove um nó a `DRAINED`, `NGRRD_RECONCILE`/`NGRRD_RECONCILE_ORPHAN_DELETED`/`RECONCILE_UNPLACED`
a cada ciclo do reconciliador (com `forgottenPruned`, marcas de série esquecida descartadas no
ciclo), `NGRRD_OWNER_REDIRECT_OVERRIDE` (nível `FINE`, por série, quando o líder diverge da réplica
local ao confirmar um redirecionamento), e `NGRRD_STORAGE_NODE_STARTED` (processo pronto, emitido por
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

`status` traz, desde a issue #167 (item 3), a linha `REGRAS: <hash|-> (<n> regras)` (fingerprint e
contagem das regras do **líder**) e, depois de `RESERVED`, as colunas `QUOTA`
(`<maxSeries|->/<maxBytes|->`, `-` = sem limite) e `RULES` (8 primeiros hex do fingerprint do nó ou
`-`, com sufixo `!` quando difere do fingerprint do líder — nó reiniciado com outro YAML, ou ainda
não reiniciado após uma mudança). `rebalance` lista também os destinos excluídos por cota
(`quota_series(<n>/<max>)`/`quota_bytes(<n>/<max>)`).

`status` traz, desde a 8.7.0, a coluna `CAT_LAG` — forma curta do `CatalogReplicaStatus` do nó
(seção 3): `lider`, o lag numérico, `sync` (sincronizando por snapshot), `boot` (bootstrap do relay
pendente), `?` (lag desconhecido, HWM do líder ainda não visto) ou `-` (nó anterior à 8.7.0, campo
ausente no status). `rebalance` imprime `rebalanceamento disparado: planejados=N iniciados=M`
seguido de uma linha `destino excluído: <nó> (<motivo>)` por nó excluído do ciclo pela réplica do
catálogo atrasada (issue #177; motivo é `lag desconhecido`, `sincronizando`, `bootstrap pendente`
ou `lag=<N>><limite>`); sem contagens conhecidas (implementação de cliente sem acesso a elas),
imprime só a confirmação do disparo, como antes.

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
  (`AdminStatusClusterTest`, `AdminCliClusterTest`); destino com réplica do catálogo atrasada
  confirmando no líder, issue #177 (`StaleReplicaRedirectClusterTest`, via um gancho de teste que
  decora o `PlacementLookup` do storage para congelar a réplica local — o core não permite pausar a
  replicação diretamente).
- **Fora do CI hospedado, por desenho** — mesmo motivo e mesmo padrão da suíte de resiliência do
  NGrid (`doc/testes-vermelhos-conhecidos.md`): os `*ClusterTest` deste módulo são sensíveis a
  tempo e recursos do executor e não passam de forma confiável em runner hospedado. `pr-validation.yml`
  constrói este módulo e roda apenas dois deles escolhidos a dedo
  (`CheckpointAfterMigrationClusterTest` e `ContinuousIngestionRebalanceClusterTest`, este com
  `-Djdk.virtualThreadScheduler.parallelism=2`); o restante roda localmente com
  `mvn -pl nishi-utils-ngrrd-cluster verify -Pngrrd-cluster`.
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
- **Cota e regras de placement (issue #167, item 3).** (a) `SeriesPlacement.definitionName` só é
  conhecido a partir do `PLACE` de um cliente desta versão: um placement legado (ou criado por
  adoção do `LocalReconciler`) fica com `null` e **só casa regras sem o critério `definition`**; o
  próximo `PLACE` do cliente para a série (todo `open` que não a encontra no cache local) preenche o
  nome oportunisticamente — **não há backfill em lote**, então uma regra por `definition` só passa a
  valer para as séries antigas depois que os clientes as reabrirem (ou, se for preciso, use
  `keyPrefix`). (b) As regras são uniformes por configuração e mudam com **reinício coordenado** de
  todos os storages: o líder aplica a cópia dele, divergência só gera `WARNING` e `!` na CLI. (c) A
  cota é rechecada pelo destino, mas o placement decide com o status mais recente: uma série pode
  escapar pela janela entre o `PLACE` e o próximo status (o rebalance corrige, tratando o nó como
  fonte acima da cota). (d) `MigrateStatus.QUOTA_EXCEEDED` fica no fim do enum: uma origem 8.7.0 não
  o conhece e falha ao decodificar a resposta (aborta a migração) — aceitável, versões mistas não
  são suportadas.
- **`ngrrd.admin.status`/`ngrrd.admin.drain`/`ngrrd.admin.activate`/`ngrrd.admin.rebalance`
  bloqueiam sem líder.** São comandos do líder (`LEADER_COMMANDS`); sem maioria elegível eleita
  (seção 2), eles simplesmente não respondem até haver líder — não há timeout curto dedicado além
  do `requestTimeout` do RPC. O tick do `NodeStatusReporter` (`putNodeStatus` → `invokeLeader`)
  tem o mesmo comportamento: pode ficar bloqueado por até ~28 s sem líder eleito. Esse
  comportamento é **pré-existente no core** (não introduzido por este módulo) e já está
  documentado como tal desde o M4.

### 13.2. Vermelho conhecido deste módulo

Nenhum desde a 8.8.0. `LeaderFailoverDuringMigrationClusterTest` falhava intermitentemente
(~1 em 4 execuções) por duas causas do core, ambas corrigidas na 8.8.0 (issue #178 e revisão do
NGrid; ver `doc/CHANGELOG.md`): a rota PROXY para o líder morto que nunca voltava a DIRECT e
ganhava graça de evicção indevida, e o `op-log append failed … write not durable` no líder
moribundo (ordem de fechamento do `NGridNode`). Além disso, o novo líder só retoma migrações
`MIGRATING` depois do **fence do catálogo** (seção 8): a réplica local do tópico `map:ngrrd.catalog`
precisa ter drenado o relay e alcançado a maior fronteira anunciada pelos peers elegíveis
(`TopicReplicationStatus.maxPeerFrontier`), com prazo de 30 s e `NGRRD_RESUME_FENCE_TIMEOUT` se
vencer; e o storage declara `priorityTopics(map:ngrrd.catalog)` ao NGrid, para que, entre dois
sobreviventes com fronteiras incomparáveis, o catálogo decida quem lidera.

### 13.3. Trabalhos futuros — pontos abertos no core (fora do escopo deste módulo)

Encontrados durante o desenvolvimento do ngrrd cluster, mas pertencem ao NGrid (`nishi-utils-core`)
e afetam qualquer usuário do NGrid, não só este módulo:

- **Ressincronização de mapa não persistente após restart.** Um `DistributedMap` configurado sem
  persistência (`NMapPersistenceMode.DISABLED`) não recupera seu conteúdo automaticamente ao
  reingressar num cluster já convergido após reiniciar — depende de o líder reenviar um snapshot
  completo por outro gatilho.
- **Higiene do `TcpTransport`.** Resolvido na 8.8.0 (revisão do NGrid, onda B): backoff exponencial
  com cache negativo de discagem, relay só com link vivo ao destino (e nunca um cliente inelegível
  nem o próprio destino), rota PROXY que volta a DIRECT quando o relay não entrega, `sendAndAwait`
  sem discagem na thread do chamador e pendentes que falham quando o próximo salto cai. Ver
  `doc/CHANGELOG.md`.
- **`NMapPersistence` falha em silêncio para valor não serializável.** Um valor sem `Serializable`
  gravado num `DistributedMap` persistente falha o append do WAL sem propagar a exceção — o mapa
  parece persistir, mas não persiste (a causa raiz do bug de catálogo vazio corrigido no M1c; ver
  seção 3). Deveria falhar alto (exceção explícita), não em silêncio.
- **Reclaim de liderança cego à linhagem.** Mitigado na 8.8.0 (issue #178): os gates comparam a
  fronteira aplicada **por tópico** (vetor no heartbeat) em vez de um contador escalar, o epoch do
  líder passou a ser persistido e o handback re-ancora todos os tópicos. A ordenação epoch-aware de
  linhagem no protocolo (o follow-up referenciado como PR #142) continua pendente: sequências de
  linhagens divergentes ainda são numericamente comparáveis.
- **Janela de bootstrap padrão e logs de handoff.** Desde a 8.7.0 o NGrid tem saída graciosa
  (`LEAVE`, ver `doc/ngrid/arquitetura.md`): clientes e a CLI administrativa são esquecidos pelos
  storages assim que saem, e o `LEAVE` de um storage (votante) confirma a saída na hora, sem o grace
  de disconnect — ele segue conhecido como votante. Continuam em aberto o default de janela de
  bootstrap do próprio core (fora do `bootDiscoveryWindow` específico do `NGridNodeBuilder` usado
  por este módulo), que contribui para o churn de bootstrap citado em 13.1, e o nível de detalhe
  dos logs de handoff de liderança. **Ainda não confirmado em código** — item registrado a partir
  de observação de campo; os demais pontos desta seção foram confirmados nos artefatos de
  planejamento (`checkpoint.md`) e no `doc/CHANGELOG.md`.

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
