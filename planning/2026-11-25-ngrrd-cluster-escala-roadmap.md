# Roadmap de escala do cluster ngrrd (8.15 → 9.0)

> **Status:** decisões do Lucas registradas em 25/11 (ver "Decisões do Lucas"). Desenho detalhado de cada release ainda precisa de spec própria antes de implementar.
> **Base:** código da 8.14.2. Os fatos vêm de `path:linha` em `nishi-utils-ngrrd-cluster/src/main/java/dev/nishisan/utils/oss/cluster/` (abreviado como `cl/`) e em `nishi-utils-oss/src/main/java/dev/nishisan/utils/oss/` (abreviado como `oss/`).
> **Alvo de planejamento:** ~3 M séries, ~15 mil rec/s, mais de um cliente escritor, 3+ storage nodes.

Princípio: cada item vira capacidade genérica da lib. Nada é chaveado para um consumidor específico.

---

## R1 — Custo do cliente por série

**1. O que existe na 8.14.2**
- O cliente guarda vários objetos por série e **não tem limite nem evicção** de handle. Não existe chave de config para isso.
  - `SeriesHandleCache` guarda o `RemoteSeriesHandle` (`cl/client/SeriesHandleCache.java:51`).
  - `WriteDispatcher.routes` guarda uma `SeriesRoute` com lock, condition, 2 deques e 2 sets (`cl/client/WriteDispatcher.java:145,1518-1560`).
  - `PlacementResolver.overrides` e `deletedGenerations` (`cl/client/PlacementResolver.java:72-73`).
  - O cliente também guarda a **réplica completa do catálogo**, porque ele é membro NGrid (`cl/catalog/CatalogService.java:98`).
- `evictions` não existe como métrica da lib. O 0 observado pelo consumidor vem de outra camada.
- **Medição na lib (JDK 21, 50 mil handles reais, GC forçado):**
  - ~2,2 KB por handle quando todas as séries compartilham a mesma `String` YAML;
  - ~4,8 KB por handle com uma `String` YAML própria por série (YAML de teste de 2,5 KB);
  - mais ~0,4 KB por entrada da réplica do catálogo.
  - Total: **~2,7 a 5,2 KB por série. Isso não explica os 26 KB medidos.**
- **Defeitos encontrados:**
  - a `SeriesRoute` nunca é removida, nem no `close()` (não existe `routes.remove`). Com séries mudando de cliente, isso vaza memória;
  - cada handle refaz hash, parse do YAML e `GeometryDescriptor` (`cl/client/DefaultNgrrdClusterClient.java:463-475`), sem compartilhar a definição entre séries.

**2. Proposta**
- **(a) Correções imediatas (patch):**
  - remover a `SeriesRoute` no close ou quando ela fica ociosa;
  - internar definição e geometria por hash, para que N séries da mesma definição compartilhem um único objeto.
- **(b) Cache de handles com teto e reabertura preguiçosa.** Proposta de config: `client.handles.maxOpen` e `client.handles.idleTtl`. Ao ser despejado, o handle vira só `seriesKey`; a próxima escrita reabre o handle. Métricas: `handles.open`, `handles.evicted` e `handles.reopened`. Isso depende do item R5(a) (nó reabre a série sozinho), senão cada despejo custa um OPEN remoto.
- **(c) Escrita sem handle por série (rota compacta).** É a forma de ter memória independente do número de séries.
  ```java
  DefinitionRef def = client.definition(yaml);           // interna e publica por hash, uma vez
  client.write(def, seriesKey, tags, ds, ts, value);     // rota seriesKey→nó resolvida por cache limitado
  ```
  Exige que o nó crie e reabra a série pela definição publicada no cluster (`ngrrd.geometries` já é um mapa replicado) e que a rota seja um cache limitado `seriesKey → (nodeId, generation)`.
- **(d) Cliente sem réplica completa do catálogo ("cliente leve").** A 3 M séries, a réplica no cliente fica em ~1,3 GB. O cliente deixaria de ser réplica do catálogo e passaria a consultar o líder com `CATALOG_LOOKUP` em lote, com cache limitado. É mudança de arquitetura. **Aprovado (ver "Decisões do Lucas").**

**3. Esforço e release**
- (a): pequeno, entra na 8.15.
- (b): médio, entra na 8.16, junto com R5(a).
- (c) e (d): grandes; release própria depois da 8.16.

**4. Compatibilidade**
- (a) e (b) são só no cliente. Em (b), a config nova tem default que preserva o comportamento atual (sem teto).
- (c) exige nós com a capacidade nova (detectada por `NodeCapabilities`). A ordem é **nós primeiro, depois clientes**.

**5. Medida pedida ao consumidor**
- `jmap -histo:live` do coordenador (top 40 classes, por bytes e por instâncias).
- O YAML da definição mais usada, e quantas definições distintas existem.
- Se o consumidor passa uma `String` YAML nova por série ou reusa a mesma instância.

---

## R2 — Vários clientes escrevendo ao mesmo tempo

**1. O que existe na 8.14.2**
- **Identidade:**
  - `client.id` vira o `NodeId` NGrid. Se faltar, o default é `ngrrd-client-<uuid8>`.
  - O cliente entra no NGrid com o papel `client`, **inelegível a líder** (`cl/client/DefaultNgrrdClusterClient.java:160-165`), e não conta na maioria de votantes.
  - Cliente morto não derruba a eleição desde a #179. Ele **conta**, porém, em `minClusterSize`.
  - Cada cliente precisa de `client.id` e `dataDir` próprios. Com `dataDir` nulo, a lib cria um diretório temporário que é apagado no close.
- **Posse:** **não há lease, epoch nem cerca por escritor.**
  - O `WRITE_BATCH` leva `(seriesKey, ds, ts, valor, generationId)`. O nó só confere a geração da série, não quem escreveu.
  - O OPEN do segundo cliente reaproveita o handle que já está aberto no nó.
  - A ordem é decidida pelo timestamp na engine. Amostra atrasada segue a `lateSamplePolicy`. Counter com timestamp que não avança vira NaN.
  - Resultado: com A e B escrevendo a mesma série, o efeito é o de quem escreveu por último, sem nenhum aviso. Isso já estava fora de escopo na 8.12 (`planning/v8.12.0-marcas-de-escrita.md:31`).
- **`mark()` é por cliente.** Ele fotografa o que *este* cliente admitiu até aquele ponto. Uma marca conclui quando todas essas escritas têm resposta final do dono. A resposta é um ACK sem fsync.
- **Entrega limpa:** `handle.close()` faz flush da série e manda CLOSE ao nó. Não existe API para liberar **um conjunto** de séries.

**2. Proposta**
- **(a) Protocolo de entrega documentado + API de liberação (recomendado primeiro):**
  ```java
  CompletableFuture<ReleaseResult> client.release(Collection<String> seriesKeys);
  // drena as escritas admitidas dessas séries, espera a resposta final do dono,
  // opcionalmente faz CHECKPOINT, fecha os handles e remove rotas e estado local.
  ```
  Uso no rebalance de partições: em `onPartitionsRevoked`, A chama `release(séries da partição).join()` e só então faz o commit do offset. B abre as séries depois que recebe a partição. Para partições disjuntas, isso basta.
- **(b) Cerca opcional por série, contra "zumbi":**
  - o OPEN aceita um `writerEpoch` (long) fornecido pelo chamador. Pode ser, por exemplo, a época da partição;
  - o nó grava o maior epoch visto e responde `FENCED` às escritas de epoch menor;
  - o cliente expõe o `FENCED` como falha de escrita (marca ou `ErrorCode.FENCED`).

  Isso protege contra A ainda escrever depois de perder a partição (pausa de GC, rede). É uma mudança de contrato (status novo e campo novo no OPEN). **Aprovado (ver "Decisões do Lucas").**

**3. Esforço e release:** (a) pequeno a médio, entra na 8.15 junto com a remoção da `SeriesRoute` de R1(a). (b) é médio e pode entrar na 8.16.

**4. Compatibilidade:** (a) é só no cliente. (b) exige nós primeiro; sem `writerEpoch`, o comportamento atual se mantém.

**5. Medida pedida ao consumidor**
- O protocolo de rebalance do consumidor Kafka (eager ou cooperative).
- Quanto tempo o consumidor tem entre o `revoke` e o `assign`.
- Quantas séries existem por partição.

---

## R3 — Placement e rebalance

**1. O que existe na 8.14.2**
- **Modos de distribuição:**
  - `ngrrd.distribution.mode` aceita `COUNT` (padrão), `CAPACITY` e `WEIGHT` (`cl/placement/DistributionMode.java`).
  - Em `CAPACITY`, o peso de cada nó é só a `capacityBytes` declarada (`cl/placement/DistributionWeights.java:50`), e o balanceamento é por **contagem de séries**.
  - **RAM, page cache e blob mapeado não entram em nenhuma decisão.** O status do nó nem tem esses campos (`cl/catalog/StorageNodeStatus.java:54-66`).
  - Os 57% no nó A provavelmente são a proporção da `capacityBytes` declarada. Não confirmei sem a config.
- **Mitigações que existem hoje:**
  - `ngrrd.quota.maxSeries` por nó, com a regra do doc `maxSeries ≈ page cache / regionBytes` (`doc/oss/ngrrd-cluster-operacao.md:623-628`). O planner respeita a cota por water-filling;
  - ou `mode: WEIGHT` com `ngrrd.weight` manual.
- **Rebalance:**
  - É online: a origem continua aceitando escrita, a lib copia por chunks, faz catch-up, congela e envia um delta final de até 256 KiB, depois commit com SHA-256 (`cl/rebalance/MigrationExecutor.java`).
  - Limites: `maxBytesPerSecond` de 16 MiB/s por origem, `maxConcurrentMigrations` = 2 e `maxSeriesBytes` = 64 MiB, com a imagem inteira em memória.
  - A retomada é parcial. Se o líder cai, `resumeInFlight` continua a migração e `healStuckMigrations` cura as órfãs. Mas **a cópia recomeça do zero**: o progresso dos chunks não é persistido.
  - A série migrando pausa só aquela série no cliente.

**2. Proposta**
- **(a) Imediato, sem código:** aplicar `quota.maxSeries` no nó A (ou passar para `WEIGHT`) para parar de concentrar séries novas nela. Depois, rebalance gradual.
- **(b) Placement por recurso dominante.** Cada nó reporta seus recursos e o peso efetivo dele passa a ser o do recurso mais escasso:

  | Recurso | Origem |
  |---|---|
  | `capacityBytes` | já existe |
  | `memoryBudgetBytes` | config; o default vem da RAM física × fração |
  | `seriesCount` × `regionBytes` (working set) | já medido |

  Novo modo `distribution.mode: RESOURCES`, ou `CAPACITY` passa a ser multirrecurso. O planner balanceia **bytes de working set**, não contagem.
- **(c) Migração retomável e com vazão adaptativa:**
  - o destino persiste o progresso dos chunks e a retomada pede só o que falta;
  - a imagem deixa de ficar inteira em memória e passa a ser transmitida em fluxo;
  - a vazão cai automaticamente quando a `writeBatchLatency` p99 do nó passa de um limite (contrapressão).

**3. Esforço e release:** (a) nenhum código. (b) médio. (c) grande. Ambos (b e c) numa release própria, a 8.18.

**4. Compatibilidade:** (b) é config nova, e o modo atual continua sendo o default. (c) muda o protocolo de migração: com nós em versões mistas, ela cai para o caminho antigo. A ordem é nós primeiro.

**5. Medida pedida ao consumidor**
- O bloco `ngrrd.volume`, `ngrrd.distribution` e `ngrrd.quota` de cada nó.
- O `regionBytes` (ou o tamanho por série) das definições principais.
- RAM e `Cached` do `free -b` de cada nó.
- Logs da última tentativa de rebalance que estourou o tempo (#169).

---

## R4 — Crescimento do blob e bytes por série

**1. O que existe na 8.14.2**
- **`maxFillRatio`:** é o maior `used/capacity` **entre os shards**, onde `capacity` é o tamanho atual do arquivo (`oss/storage/blob/BlobStorage.java:686-694`).
- **Shard em 1,0:** o shard **cresce sozinho**, de `segmentBytes` em `segmentBytes` (1 GiB, `oss/storage/blob/BlobStorage.java:431-446`). Por isso esse valor anda em dente de serra e, perto de 1,0, é normal. Não é alarme.
  - O crescimento roda sob o `structuralLock` e bloqueia criação de série no volume enquanto acontece.
- **Limites reais de admissão:** são por nó. A lib recusa novas séries a 95% da `capacityBytes` declarada (`oss/storage/blob/CapacityBudget.java`) e quando o filesystem fica sem espaço.
- **Doc errada:** `doc/oss/ngrrd-blob-volume.md` §8 promete *linear probing* para outro shard. Isso não existe no código.
- **Bytes por série:**
  - a imagem é fixa e o NaN é materializado na criação e na virada de linha (`oss/format/SeriesFileCodec.java:79-82`, `oss/writer/NgrrdWriter.java:722-749`);
  - ~430 KB ≈ 53 mil células de 8 B, compatível com uma geometria intermediária;
  - **não há nenhum trabalho de formato v2 ou NaN implícito.** Nem plano nem código.

**2. Proposta**
- **(a)** Corrigir a doc (crescimento automático, sem probing). Expor `volume.fillOfDeclaredCapacity` (used/declarada), que é o indicador que importa para alerta, e documentar os limiares: 80% aviso, 90% crítico, 95% recusa.
- **(b)** Medir o custo do crescimento sob `structuralLock` (tempo do `grow`) e, se pesar, pré-crescer em segundo plano quando o fill passar de ~0,9.
- **(c)** Formato v2 (alocação esparsa de RRA, NaN implícito por bitmap de linha). É mudança incompatível de formato (`formatVersion=2`), com conversão online e efeito no tier frio. **Aprovado (ver "Decisões do Lucas").** Antes disso, a alavanca mais barata é revisar a geometria (RRAs e linhas), e isso o consumidor faz sem a lib.

**3. Esforço e release:** (a) pequeno, entra na 8.15. (b) pequeno a médio. (c) grande, numa linha 9.x.

**4. Compatibilidade:** (a) e (b) são transparentes. (c) exige nós que leiam v1 e v2 antes de qualquer gravação em v2.

**5. Medida pedida ao consumidor:** a geometria (YAML) das 3 definições com mais séries e quantos % das células ficam NaN no tier frio, se der para calcular.

---

## R5 — Manutenção sem janela e depois HA

**1. O que existe na 8.14.2**
- **No cliente, a escrita já sobrevive a um nó fora:**
  - o buffer é **por nó**, com reenfileiramento na frente e backoff de 100 ms a 2 s, sem prazo;
  - os outros nós seguem escrevendo normalmente.
- **O que derruba o chamador:**
  1. **Buffer do nó cheio.** `maxBufferedSamplesPerNode` tem default de 100 mil amostras. Com ~12 mil amostras/s por nó, enche em **~8 s**. Aí `bufferFullPolicy: BLOCK` **trava a thread que chama `write()`**, mesmo que ela escreva em outros nós.
  2. **Operações síncronas.** `checkpoint`, `flush`, `open` e `read` usam `retryTimeout` (5 min) e depois lançam `TIMEOUT`.
  3. **Tempestade de reabertura no restart.** O nó não reabre as séries ao subir: a definição só vive em memória (`cl/node/SeriesHandleRegistry.java:413,821`). Cada série ativa responde `NOT_OPEN`, e o cliente reabre todas com um pool de **4 threads** (`cl/client/WriteDispatcher.java:154`). São 285 a 490 mil OPENs por nó.
- **Admin:** `drain`/`activate`/`forget-node` existem e drenam por migração. Não existe "pausa para manutenção" curta.
- **Réplica (RF>1):** **não existe e não está planejada.** O desenho atual diz explicitamente *"Nunca. Cada série vive em exatamente um nó"* (`planning/ngrrd-cluster.md:20`).

**2. Proposta**
- **(a) O nó reabre a série sozinho.**
  - Ao abrir uma série, o nó persiste o hash da definição no journal de lifecycle (o WAL já existe).
  - As definições passam a ficar num mapa replicado do cluster, ao lado de `ngrrd.geometries`.
  - Depois do restart, `reopenIfKnown` reabre a série a partir do disco. Isso acaba com a tempestade de `NOT_OPEN` e é a base de R1(b) e R1(c).
- **(b) Nó em manutenção:**
  - comando admin `maintenance <node>`, com estado `MAINTENANCE` no catálogo;
  - o cliente, ao ver esse estado, segura a escrita daquele nó sem log de WARN por tentativa;
  - `checkpoint` e `flush` das séries daquele nó viram **pendentes**: concluem quando o nó volta, ou falham só depois de `maintenanceTimeout`;
  - `activate` devolve o nó ao estado normal.
- **(c) Buffer por nó dimensionável por bytes ou tempo** (`client.write.maxBufferedPerNode`, aceitando "60s" ou bytes) e política `SPILL`. Quando o buffer enche, o que transborda vai para o `dataDir` em disco (NQueue), em vez de bloquear o chamador.
- **(d) Rolling upgrade documentado.** A ordem é: um nó por vez, `maintenance`, parar, atualizar, subir, `activate`, esperar `NOT_OPEN` = 0 e depois seguir para o próximo nó.
- **(e) RF>1:** é mudança da premissa central do desenho. Exige réplica por série (stream de escrita do primário para N secundários, confirmação com quorum mínimo, promoção no failover, reconciliação). É um projeto do tamanho de uma major. **Aprovado como configurável, default 1 (ver "Decisões do Lucas").**

**3. Esforço e release:** (a) médio, entra na 8.16. (b) e (c) médios, também na 8.16. (d) é documentação, na 8.16. (e) muito grande, só depois de aprovada.

**4. Compatibilidade:** (a) só no nó, com journal compatível (campo novo opcional). (b) exige nó e cliente novos, e o cliente antigo ignora o estado novo. Ordem: nós e depois clientes.

**5. Medida pedida ao consumidor**
- Stack e mensagem exatas da falha do cliente no restart de um nó.
- Duração de um restart de nó: do stop até `NGRRD_STORAGE_NODE_STARTED`, e daí até `NOT_OPEN` parar de subir.
- Os valores de `maxBufferedSamplesPerNode` e `bufferFullPolicy` em uso.

---

## R6 — Metadados da série consultáveis no cluster

**1. O que existe na 8.14.2**
- **Catálogo:** guarda dono, estado, geração, geometria e `updatedAtEpochMs` (a última **transição**, não a última escrita). **Não tem `lastWriteTs`** (`cl/catalog/SeriesPlacement.java:47-55`).
- **Última escrita no nó:** o nó tem dois valores aproximados:
  - `receivedThrough` no journal de lifecycle, arredondado para cima na granularidade de 1 h (`ngrrd.series.receiptIntervalMillis`);
  - `last_up` no cabeçalho do `.ngrr`.

  Nenhum dos dois é exposto por RPC.
- **Varredura:** **não há listagem por prefixo, paginada nem em fluxo.** As consultas `exists`, `find` e `verify` são todas por chave.

**2. Proposta**
```java
SeriesScan scan = SeriesScan.builder()
        .prefix("iface/")                 // opcional
        .lastWriteBefore(instant)         // opcional
        .lastWriteAfter(instant)          // opcional
        .pageSize(10_000)
        .build();
Stream<SeriesMeta> client.scanSeries(scan);   // em fluxo, paginado por nó, sem montar tudo em memória
long client.countSeries(scan);
record SeriesMeta(String seriesKey, String ownerNodeId, long lastWriteEpochMs, String definitionName, long sizeBytes) {}
```
- Cada nó varre as séries de que é dono e responde por página, com cursor por nó. O cliente intercala as páginas dos nós.
- `lastWriteEpochMs` vem de um valor exato mantido em memória pelo nó e persistido no checkpoint ou no journal. A precisão precisa ser declarada no contrato.

**3. Esforço e release:** médio, numa release de leitura e metadados (8.17), junto com R8.

**4. Compatibilidade:** comando novo; o cliente detecta a capacidade do nó. Ordem: nós e depois clientes.

**5. Pergunta ao consumidor:** que precisão de `lastWriteTs` basta: segundos, minutos ou 1 h? Com 1 h, a proposta fica mais barata, porque reaproveita o journal.

---

## R7 — Caminho de escrita em escala

**1. O que existe na 8.14.2**
- **Disparo do checkpoint local:** é feito por **uma thread** que espalha as séries sujas em 80% do intervalo (`cl/node/LocalCheckpointer.java:78,151-175`).
- **Execução:**
  - o trabalho vai para o `ngrrd-writer-pool`, que é **compartilhado com a ingestão** e tem `max(2, nCPU)` threads (`oss/writer/WriterScheduler.java:46`);
  - o paralelismo é limitado por `ngrrd.checkpoint.maxInFlight`, com default `min(8, max(2, nCPU))`;
  - cada série faz o próprio `force` (msync).
- **Custo por ciclo:** a lib varre **todas** as entradas do registry para achar as sujas (`cl/node/SeriesHandleRegistry.java:342-348`), e o custo é **um force por série**.
- **Botões existentes:** `ngrrd.checkpoint.interval` e `ngrrd.checkpoint.maxInFlight`. Não existe prioridade nem pool dedicado.
- **Verificar no consumidor:** `maxOpenHandles` no nó tem **default de 10.000** e `handleIdleTtl` de 15 min (`cl/node/StorageNodeConfig.java:1095-1096`). Com 285 a 490 mil séries por nó, se isso não foi sobrescrito, há churn de fechar e reabrir handles. Isso explicaria parte dos `NOT_OPEN` do nó C.
- **Resposta direta:** como está, o checkpoint **não escala para ~1,5 M de séries por nó**. O custo é linear em séries sujas e o fsync é feito série a série.

**2. Proposta**
- **(a) Botões (8.15):**
  - pool dedicado de checkpoint (`ngrrd.checkpoint.threads`), separado da ingestão;
  - `spreadFraction` configurável;
  - índice de séries sujas mantido incrementalmente, no lugar da varredura completa;
  - métricas: `localCheckpoint.queueWaitMs` e `forceMs` p99.
- **(b) Force agrupado por shard (8.16 ou 8.18):** o checkpoint grava o live-state de N séries do mesmo shard e faz **um único force/fdatasync por shard por lote**, no lugar de um por série. É a mudança que muda a escala. Ela exige reorganizar `checkpointAndForce` em `oss/writer/NgrrdWriter.java:775-815`.
- **(c) mmap × pwrite:**
  - a escolha não muda durabilidade. O pwrite reduz bytes gravados em disco (folio inteiro × blocos de 4 KB, ~290 KB por série por virada, medido na 8.13), mas serializa por inode;
  - **critério de A/B:** 24 h em cada modo no mesmo nó, ou em dois nós com carga parecida, comparando:

    | Métrica | Fonte |
    |---|---|
    | MB/s escritos e `w_await` | `/proc/diskstats` / `iostat -x` |
    | `writeBatchLatency` p99 | métrica do nó |
    | `localCheckpoint.lastCycleMs` e `overruns` | métrica do nó |
    | iowait | sistema |

  - **Critério de decisão:** pwrite vence se reduzir ≥30% dos bytes em disco sem piorar o p99 de escrita em mais de 10%;
  - recomendação: começar pelo nó B, que tem o maior fill, em XFS/ext4 com kernel ≥ 6.6.

**3. Esforço e release:** (a) pequeno a médio, 8.15. (b) médio a grande. (c) sem código; só o guia de medição na doc.

**4. Compatibilidade:** tudo interno ao nó; config nova com default igual ao comportamento atual.

**5. Medida pedida ao consumidor**
- `maxOpenHandles`, `handleIdleTtl` e `checkpoint.*` de cada nó.
- nCPU de cada nó, kernel e filesystem.
- `iostat -x 60` durante um ciclo de checkpoint.

---

## R8 — Leitura em massa

**1. O que existe na 8.14.2**
- **Leitura:** `ngrrd.read` e `ngrrd.readPreset` leem **uma série por chamada**. Não há lote nem fluxo.
- **API do cliente:** a leitura passa por `open()` + `handle.read(...)`. Com `createIfMissing=false`, o handle aberto é só de leitura.
- **Limite do frame de transporte:** 64 MB (`nishi-utils-core/.../TcpTransport.java:2101-2104`).
- **Vazão observada:** 537 mil séries em 4 h 40 min é ~32 séries/s. Isso indica que cada leitura paga ida e volta, OPEN e lookup de forma serial.

**2. Proposta**
- **(a) Leitura em lote agrupada por nó, em fluxo:**
  ```java
  client.readMany(Collection<String> seriesKeys, ReadSpec spec,   // preset ou ds+janela+passo
                  ReadOptions opts /* paralelismo por nó, tamanho de página */)
        → Stream<SeriesReadResult>   // ou callback; sem OPEN, sem createIfMissing
  ```
  O cliente agrupa as chaves por dono e manda páginas limitadas por bytes, abaixo do frame. Cada nó lê localmente e responde em fluxo.
- **(b) Exportação da imagem nativa** para o tier frio: fluxo da imagem `.ngrr` por série, o mesmo caminho dos chunks de migração. Em vez de decodificar pontos, o tier frio guarda o arquivo ngrrd, que a lib já sabe ler. Fica muito mais barato quando o objetivo é arquivar.

**3. Esforço e release:** (a) médio, (b) médio. Ambos na 8.17, junto com R6.

**4. Compatibilidade:** comandos novos; o cliente detecta a capacidade do nó. Ordem: nós e depois clientes.

**5. Medida pedida ao consumidor**
- Como o exportador para o tier frio lê hoje: paralelismo, se abre handle, se usa preset ou ds+janela.
- O que o tier frio precisa guardar (pontos ou a série completa).

---

## R9 — Métricas estranhas

**1. Explicações (pelo código; falta confirmar com logs)**
- **`seriesDelete.ERROR` = 275.782 no nó C.**
  - O contador fica no nó que **conduz** o delete, que é o líder, e conta **tentativas**, não o resultado final (`cl/node/SeriesLifecycleService.java:66-68`).
  - Vira ERROR, entre outros casos, em:
    - `NO_LEADER` ou líder não sincronizado;
    - falha de `placementStrong`;
    - timeout de RPC em prepare, commit, apply ou finish;
    - reserva obsoleta.

    (`cl/node/SeriesDeleteHandler.java:194-232`)
  - Depois de um timeout após o commit, o `recover()` (a cada 1 s) conclui a remoção. O retry do cliente então vê `NOT_FOUND`/`DELETED` e não registra erro.
  - Por isso a purga do cliente marca 0 erros. O número é alto demais para ser normal, então a causa dominante precisa sair dos logs.
- **`REFUSED_RECENT_WRITE` = 4.613:** a série recebeu escrita depois do corte `lastWriteBefore`. O `receivedThrough` tem margem de até 1 h, então séries "quase mortas" são recusadas (`cl/node/SeriesLifecycleService.java:201-203`). Isso é esperado e seguro.
- **`errorsByStatus.NOT_OPEN` = 113.725:** é a escrita, leitura ou checkpoint numa série sem handle e sem definição no nó. As causas são:
  - restart do nó (~1 por série ativa);
  - fechamento por `handleIdleTtl` ou `maxOpenHandles` (default 10 mil);
  - migração.

  É autocurado pelo cliente, mas cada ocorrência custa um OPEN. Veja R5(a) e R7.
- **`errorsByStatus.NOT_LEADER` = 128:** provavelmente vem da confirmação de dono no líder (#177), quando ela falha durante uma troca de liderança (`cl/node/StorageRequestHandler.java:463,663,729`). É baixo e transitório.
- **`rpcLatency` p99 104 ms / máx. 15 s:**
  - é **um único histograma para todos os comandos** (PLACE, OPEN, WRITE_BATCH, CHECKPOINT, READ…), por tentativa e incluindo falhas;
  - o p99 vem das últimas 1.024 amostras;
  - o máximo é acumulado **desde o início do processo**. Um único evento de reconexão ou timeout de 15 s fica para sempre (`cl/client/MetricsTrackingClusterRpc.java:56-98`, `cl/metrics/LatencyHistogram.java`).

**2. Proposta (8.15)**
- **`rpcLatency`:** histograma por comando e máximo por janela, além do máximo de toda a vida do processo.
- **`seriesDelete`:** contar o **resultado final** por operação e a **causa** de cada tentativa com falha (`seriesDelete.attemptFailed.{NO_LEADER,TIMEOUT,PARTICIPANT,STALE}`).
- **`errorsByStatus.NOT_OPEN`:** separar por causa (`restart`, `idleClose`, `evicted`, `migrated`).

**3. Esforço:** pequeno, 8.15.

**4. Compatibilidade:** métricas novas; as antigas são mantidas por uma versão e documentadas como obsoletas.

**5. Medida pedida ao consumidor**
- Os WARN de `SeriesDeleteHandler` do nó C, de uma janela de purga.
- Qual nó era líder nessa janela.
- Se o nó C reiniciou nas últimas 24 h.

---

## Decisões do Lucas (25/11)

1. **RF>1: sim, configurável.**
   - O fator de réplica passa a ser opção por cluster ou por definição, e o default continua 1, como hoje.
   - A escrita confirma quando o mínimo de réplicas configurado confirma.
   - A premissa "cada série vive em exatamente um nó" deixa de ser absoluta e vira o comportamento com RF=1.
2. **Formato v2 / NaN implícito: sim, com migração preguiçosa.**
   - O nó lê v1 e v2 para sempre.
   - A conversão acontece na hora, na próxima reescrita da série (virada de linha, checkpoint ou migração entre nós), ou por varredura em segundo plano com vazão limitada.
   - Nunca existe passo de conversão em massa obrigatório.
3. **Cerca por escritor: sim.** O status `FENCED` entra no contrato e também vai servir de base para caches do lado do consumidor.
4. **Cliente leve e escrita sem handle por série: sim.**
5. **Ordem das releases: fica a critério da lib.** O trabalho é estruturante e a 8.14.x está estável. A ordem abaixo foi escolhida para cada etapa construir sobre a anterior, sem retrabalho.

## Sequência de releases

| Release | Conteúdo | Por que nesta ordem |
|---|---|---|
| **8.15.0** | R1(a) vazamento de `SeriesRoute` e definição internada · R2(a) `release(keys)` · R4(a) doc e métrica de fill · R7(a) pool dedicado de checkpoint, índice de séries sujas e `spreadFraction` · R9 métricas | Alívio imediato e compatível; dá as medidas que as etapas seguintes vão usar |
| **8.16.0** | R5(a) definição persistida e reabertura no nó · R2(b) `writerEpoch` / `FENCED` · R5(b) manutenção · R5(c) buffer por tempo/bytes e `SPILL` · R5(d) rolling upgrade | Base no nó: sem definição persistida não há cliente leve, e a cerca usa o mesmo registro por série |
| **8.17.0** | R1(b)(c)(d) cliente leve: escrita sem handle por série, cache de rotas limitado e cliente sem réplica do catálogo | Depende da 8.16; memória do cliente fica independente do número de séries |
| **8.18.0** | R6 `scanSeries`/`countSeries` · R8 `readMany` e exportação nativa | Usam o `lastWrite` exato e os cursores por nó |
| **8.19.0** | R3(b) placement por recurso dominante · R3(c) migração retomável em fluxo · R7(b) force agrupado por shard | Migração retomável vira a base da conversão v2 e da réplica |
| **9.0.0** | R4(c) formato v2 com migração preguiçosa · R5(e) RF configurável (default 1) | Major: formato novo e semântica de réplica; reaproveita o fluxo de migração da 8.19 |

**Regra de upgrade em todas as releases:** storage nodes primeiro (rolling, um por vez), depois os clientes. Cada comando novo é anunciado como capacidade do nó (`NodeCapabilities`), e um cliente novo com nó antigo usa o caminho antigo.

**Fica com o consumidor:** partições do tópico, líder para jobs únicos, catálogo e inventário próprios. O catálogo próprio do consumidor pode ser reduzido ou eliminado com R6.

## Medidas recebidas do consumidor (25/11, 20:40, só leitura)

**R1**
- **Os 26 KB por série estavam errados.** O valor tinha sido calculado sobre o `heapUsed`, que inclui lixo.
- Depois de cada coleta Young, sobram 7,6 a 8,1 GB vivos, ou seja, **≤ ~9,4 KB por série** já contando os caches do consumidor.
- O YAML é uma única `String` por definição (10 definições), o que corresponde ao caso medido de ~2,2 KB por handle.
- O R1 deixa de ser urgente. A correção do vazamento da `SeriesRoute` e do parse feito a cada handle continua na 8.15.

**R3**
- Todos os nós estão em `CAPACITY`, sem `quota` e com `rebalance.enabled: false`. Usam `maxConcurrentMigrations: 8`.

| Nó | CPU | RAM | Cached | capacityBytes | Séries | Blob usado |
|---|---|---|---|---|---|---|
| A | 72 | 189 GiB | 133 GiB | 2,9 TB | 488 mil | 218 GB |
| B | 56 | 189 GiB | 133 GiB | 1,6 TB | 269 mil | 116 GB |
| C | 40 | 126 GiB | 103 GiB | 700 GB | 118 mil | 51 GB |

- **Confirmado:** 2,9/(2,9+1,6+0,7) = 55,8%, o que explica os 57% do nó A só pela proporção de `capacityBytes`.
- O blob do nó A fica acima do page cache (218 GB contra 133 GiB). Com mmap, porém, o working set real é a linha corrente de cada RRA mais o live-state, e não o blob inteiro. O que indica falta de cache é falha de página maior (major fault) e leitura em disco no nó A, e não a comparação blob × RAM.
- Média de ~447 KB por série no blob.

**R7**
- `maxOpenHandles` ficou acima do número de séries em todos os nós, então não há despejo por teto.
- `handleIdleTtl` está no padrão de 15 min.
- O `checkpoint.maxInFlight` é 32 só no nó A.
- Os três nós rodam kernel 6.8, XFS sobre LVM, com readahead em 0.
- **NOT_OPEN do nó C:** o `errorsByStatus` acumula desde a subida do processo, e 113.725 ≈ 118 mil séries do nó C. Tudo indica que o nó C reiniciou e houve a tempestade de reabertura (R5(a)), e não fechamento por ociosidade. Os nós A e B estão zeradas.
- **Risco genérico confirmado na leitura:** o `idleTtl` de 15 min é igual ao `stepSec` de 900 de uma das definições. Uma série que é escrita no ritmo do passo pode ser fechada entre duas escritas. Isso entra na 8.15: o TTL efetivo por série passa a ser `max(idleTtl, k × stepSec)`.
- O nó C grava mais em disco (~5,2 mil w/s, 20 MB/s) que o nó B (10 MB/s), mesmo tendo menos séries. Hipótese: é a líder (catálogo com fsync, condução de deletes). Falta confirmar.

**R5**
- Configuração do cliente: `maxBufferedSamplesPerNode` 100 mil com `BLOCK`, `retryTimeout` 2 min, `requestTimeout` 20 s.
- O buffer de um nó parado enche em ~8 s.
- Faltam a stack da falha do cliente no restart e os tempos de restart. Só dá para obter isso numa janela de manutenção.

**R6**
- O consumidor pede **precisão de 1 min** para o `lastWrite`, o que basta para a UI ("sem coleta") e para a purga.
- Proposta da lib: o nó mantém o `lastWrite` exato em memória (um `long` por série), persiste no checkpoint ou no journal, e o contrato declara precisão ≤ 1 min.

**R8**
- O exportador para o tier frio tem uma thread só, sem limite de vazão efetivo: ~30 ms por série, uma série de cada vez.
- Faz uma leitura por par DS × CF, com `maxPoints = cells + 2`.
- Ações possíveis antes da 8.18, sem mudar a lib:
  - paralelizar o exportador (N threads, com N limitado pelo `requestTimeout`);
  - usar um preset que traga todos os DS numa chamada só.

**R2**
- O tópico tem uma partição hoje. A estratégia de atribuição de partições ainda vai ser verificada pelo consumidor.

## Medidas de página e I/O (25/11, 20:42–20:43, 60 s simultâneos nos 3 nós)

Medido com `sar -B 60` e `iostat -dxm 60`.

| Nó | majflt/s | Leitura em disco | Escrita em disco | %util | Reclaim |
|---|---|---|---|---|---|
| A | 24 | ~0 MB/s | ~18 MB/s (`w_await` 2,2 ms) | ≤5% | nenhum |
| B | 13,5 | 0,03 MB/s | ~10 MB/s | ~3% | `pgsteal` 56/s |
| C (líder) | 6,8 | ~0 | ~7,5 MB/s | ~3,4% | `pgscank` 1.473/s, `pgsteal` 2.945/s |

**R3**
- O nó A **não está sem page cache**, porque falta de página e leitura em disco estão desprezíveis. Quota nela não é necessária.
- O nó C é a líder, divide a máquina com um broker Kafka e outros processos, e é o único nó com reclaim contínuo.
- O que isso muda no placement por recurso da 8.19:
  - o sinal de memória deve ser a memória **disponível** (`MemAvailable` e pressão de reclaim), não a RAM física;
  - o papel de líder (catálogo com fsync, condução de deletes) deve poder ter um desconto configurável no peso do nó.

**R7**
- O disco está folgado (≤5% de uso).
- `lastCycleMs ≈ 240 s` de 300 s **é o valor esperado pelo desenho**: o ciclo espalha as séries sujas em 80% do intervalo (`SPREAD_FRACTION = 0.8`). Esse número não indica teto.
- O sinal real de saturação é `overruns`, que está em 2 no nó A.
- A pergunta "escala para 1,5 M de séries por nó" continua de pé por causa do custo de um force por série. O pool dedicado, a métrica `forceMs` (8.15) e o force agrupado por shard (8.19) seguem como planejado.

## Decisões de desenho aprovadas na onda 1 de specs (25/11)

As specs da 8.15, 8.16 e 8.18 passaram por revisão independente e foram revisadas. O Lucas aprovou estas decisões:

1. **Definições sem mapa replicado.**
   - O nó persiste localmente a definição das suas séries, e a migração transporta o vínculo e a definição.
   - Na 8.17, quando o nó responde "definição desconhecida", o cliente reenvia o YAML do `DefinitionRef`.
   - Isto substitui o mapa replicado previsto em R5(a).
2. **`REJECT` como padrão.** Escrita sem época numa série cercada é recusada. Um cliente antigo recebe `ERROR` com a mensagem `FENCED`, em vez de um status desconhecido.
3. **TTL de ociosidade por série.** O TTL efetivo é `max(handleIdleTtl, 2 × stepSec)`. O fator `0` restaura o comportamento antigo.
4. **Métricas e campos obsoletos.** Na 8.x são só depreciados; a remoção fica para a 9.0. Nenhum componente sai de um record público numa minor.
5. **`lastWrite` (8.18) em índice próprio do nó.**
   - Não fica no checkpoint nem no journal.
   - Cada valor carrega uma qualidade: `EXACT`, `RECOVERED`, `ESTIMATED` ou `UNKNOWN`.
   - A promessa de atraso é "intervalo + atraso" (~30–45 s).
6. **API da 8.18.**
   - `SeriesMeta` ganha `lastWriteQuality` e `generationId`.
   - A varredura é exata por padrão (`STRICT`), com opção por chamada para aceitar resultado parcial com aviso.
7. **`recover()` do delete.** Na 8.15, passa a usar um índice de exclusões pendentes com backoff, em vez de copiar o catálogo inteiro.
8. **Rollback e época.** A garantia da época do escritor não sobrevive a um rollback 8.16 → 8.15. Depois de um rollback, os escritores precisam subir a época.
9. **Época fornecida pela lib.** A 8.16 entrega `acquireWriterEpoch(scope)`: monotônica, durável no cluster e incrementada pelo líder, para que o consumidor não precise de uma fonte própria de época.
10. **Exportação nativa `.ngrr` sai da 8.18.** Os clientes precisam de leitura de pontos em lote para o tier frio. A exportação nativa fica para uma release futura (backup, DR, migração entre clusters).
11. **Teto do spill (8.16).** O padrão passa a ser derivado de taxa × prazo de manutenção, ou ao menos documentado com essa conta: 1 GiB não cobre 15 min num nó com ~20 mil amostras/s.
