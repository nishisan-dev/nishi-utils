# AGENTS.md

## Visão rápida do repositório
- Monorepo Maven com `pom.xml` agregando `nishi-utils-core` e `ngrid-test`.
- O código de runtime fica em `nishi-utils-core/src/main/java/dev/nishisan/utils/{map,queue,ngrid,stats}`.
- O workflow de release (`.github/workflows/publish.yml`) publica apenas `nishi-utils-core`; trate `ngrid-test` como módulo de suporte/teste, não como artefato de release.

## Arquitetura que vale entender antes de editar
- `NGrid` é a parte mais acoplada: `NGridNode` integra `TcpTransport`, `ClusterCoordinator`, `ReplicationManager`, `QueueClusterService` e `MapClusterService` (`nishi-utils-core/.../ngrid/structures/NGridNode.java`).
- **Facade simplificada**: `NGrid` é o ponto de entrada fluente — `NGrid.local(n)` cria clusters em memória (dev/testes via `NGridLocalBuilder` → `NGridCluster`), `NGrid.node(host, port)` cria nós individuais (produção via `NGridNodeBuilder`). A API de baixo nível (`NGridConfig.Builder` + `NGridNode`) continua disponível e é usada internamente.
- As APIs públicas distribuídas são `DistributedQueue` e `DistributedMap`; o backend local é `NQueue` para filas e `NMap`/estado em memória para mapas.
- O fluxo de escrita é leader-based + quorum: followers encaminham via `CLIENT_REQUEST`/`CLIENT_RESPONSE`, o líder replica via `REPLICATION_REQUEST`/`REPLICATION_ACK`, e recuperação usa `SYNC_*` e `SEQUENCE_RESEND_*` (`MessageType.java`, `ReplicationManager.java`, `doc/ngrid/arquitetura.md`).
- O transporte TCP implementa handshake, gossip de peers e proxy routing por RTT; alterações de rede quase sempre impactam coordenação, replicação e testes de resiliência ao mesmo tempo (`doc/ngrid/arquitetura.md`, `cluster/transport/*`).
- **Codec composto**: `CompositeMessageCodec` despacha entre `BinaryFrameCodec` (frames binários compactos para HEARTBEAT/PING) e `JacksonMessageCodec` (JSON para os demais). A distinção é pelo primeiro byte do frame (`0x01`/`0x02` = binário, `0x00` = JSON com marker, `0x7B` = JSON legado). Alterações em serialização devem considerar ambos os caminhos (`cluster/transport/codec/*`).
- Filas são multi-tenant por nome: comandos incluem o nome no qualifier (`queue.offer:{fila}`, `queue.poll:{fila}`), e `NGridNode` cria `QueueClusterService` por fila configurada.
- `TypedQueue<T>` oferece descritores type-safe em tempo de compilação para acessar filas — `node.getQueue(TypedQueue)` evita erros de nome/tipo solto.
- Consumo distribuído de fila é consumer-first: para leituras estáveis entre nós, prefira `DistributedQueue.openConsumer(groupId, consumerId)` que retorna `DistributedQueueConsumer<T>` com `poll()`/`peek()`/`seek()`; os métodos `poll()`/`peek()` na `DistributedQueue` existem como caminho legado/conveniência (`DistributedQueue.java`, `queue/QueueConsumerCursor.java`).
- **NQueue headers**: `NQueueHeaders` carrega metadados imutáveis (key-value) por registro sem embutir no payload. Útil para tracing, routing keys e content-type (`queue/NQueueHeaders.java`).
- **Staging em memória**: `MemoryStager` absorve bursts de produção durante compactação do NQueue; o drain para disco é FIFO e não interfere no lock principal (`queue/MemoryStager.java`).
- Quando `queueDirectory` não é configurado, offsets de fila são persistidos no mapa interno `_ngrid-queue-offsets`, que é forçado a persistência mesmo se mapas do usuário estiverem desabilitados (`NGridNode#createQueueService`, `createMapService`).
- **Consistência de leitura para mapas**: configurada via `Consistency.STRONG`, `Consistency.EVENTUAL` ou `Consistency.bounded(maxLag)` (`ConsistencyLevel.java`, `Consistency.java`).
- **DeploymentProfile** (`DEV`/`STAGING`/`PRODUCTION`): `NGridConfig.Builder.build()` no modo `PRODUCTION` impõe guardrails (strictConsistency=true, replicationFactor≥2, persistência habilitada para mapas). Em `DEV`/`STAGING` não há validações extras.
- **Observabilidade (pacote `metrics`)**: `NGridNode` integra `RttMonitor` (ping periódico), `LeaderReelectionService` (broadcast de write-rate + sugestão de troca de líder), `NGridAlertEngine` (avaliação periódica de regras com alertas tipados: `PERSISTENCE_FAILURE`, `HIGH_REPLICATION_LAG`, `LEADER_LEASE_EXPIRED`, `LOW_QUORUM`, `HIGH_GAP_RATE`, `SNAPSHOT_FALLBACK_SPIKE`) e `NGridDashboardReporter` (dump YAML periódico do snapshot operacional). O snapshot é capturado via `NGridOperationalSnapshot` (record).
- **NMapHealthListener**: callback para falhas de persistência em `NMap`, conectável ao `NGridAlertEngine` para disparo de `PERSISTENCE_FAILURE`.
- **LeaseExpiredException**: lançada quando uma escrita é rejeitada porque o lease do líder expirou (indica nó isolado) (`cluster/coordination/LeaseExpiredException.java`).
- Configuração YAML aceita interpolação `${VAR}` e `${VAR:default}`; no modo autodiscover o cliente busca `cluster/queues/maps` do seed e regrava o YAML local (`NGridConfigLoader.java`, `ngrid-test/config/client-autodiscover.yml`).

## Workflows de build, teste e execução
- Teste padrão do monorepo: `mvn test` (README e CI usam isso como baseline).
- Gate de resiliência in-process: `mvn test -Presilience -Dsurefire.rerunFailingTestsCount=1` (`.github/workflows/resilience.yml`).
- Gate Docker/Testcontainers: primeiro construir `ngrid-test:latest`, depois instalar `nishi-utils-core`, depois rodar `mvn -pl ngrid-test verify -Dngrid.test.docker=true -Dtest='...' -DfailIfNoTests=false` (veja o job `docker-resilience-gate`).
- Profile Maven `docker-resilience`: roda ITs Docker via Failsafe (`**/cluster/*IT.java`), sem surefire, com `ngrid.test.docker=true` e `TESTCONTAINERS_RYUK_DISABLED=true`. Uso: `mvn verify -Pdocker-resilience`.
- Soak test (longa duração): `mvn test -Psoak -Dngrid.soak.durationMinutes=720`. Roda apenas `soak/NGridSoakTest.java`; disponível via `workflow_dispatch` no CI com parâmetro `run_soak=true`.
- Validação de Javadoc: `mvn verify -Pvalidate-javadoc`.
- Script pronto para o subconjunto mínimo de resiliência: `bash doc/ngrid/playbook-automation/run-ci-resilience.sh`; ele também configura logging JUL em `doc/ngrid/playbook-automation/logs/`.
- O módulo `ngrid-test` gera um jar executável com cenários manuais (`server`, `client`, `client-auto`, `scenario-1`) e usa configs em `ngrid-test/config/*.yml`.
- Há um detalhe importante de toolchain: README/workflows falam em Java 21+, mas os POMs e `ngrid-test/Dockerfile` compilam com Java 25. Se aparecer erro de compilação/toolchain, confira isso antes de depurar o código.

## Convenções específicas do projeto
- Nem todo teste de integração usa sufixo `IT`: no core, muitos cenários de cluster ficam em `src/test/java/dev/nishisan/utils/ngrid/**` com nome `*Test.java`; o profile `resilience` inclui esses testes e exclui `testcontainers`/`soak`.
- Em `ngrid-test`, os testes Docker usam `*IT.java` e rodam via Failsafe em `verify`.
- Testes de cluster em memória normalmente estabilizam o ambiente com `ClusterTestUtils.awaitClusterConsensus(...)`; reaproveite esse helper ao mexer em eleição, membership ou conectividade.
- Testes Docker dependem de marcadores de log emitidos por `ngrid-test/src/main/java/dev/nishisan/utils/test/Main.java`, como `CURRENT_LEADER_STATUS`, `ACTIVE_MEMBERS_COUNT` e `REACHABLE_NODES_COUNT`; não renomeie esses textos sem ajustar os wrappers `NGridNodeContainer`/`NGridMapNodeContainer`.
- Para mapas distribuídos tipados, registrar o mapa nos nós participantes antes do tráfego evita hangs e caminhos de erro de handler ausente; veja `DistributedMapPojoReplicationTest` e o `UnknownMapRequestHandler` em `NGridNode`.
- O caminho persistente "novo" usa `dataDirectory/queues/{queue}` para filas; `queueDirectory()` aparece no código como compatibilidade legada.
- Para testes novos envolvendo a facade `NGrid`, prefira `NGrid.local(n)` em vez de montar `NGridConfig.Builder` manualmente; referência: `NGridFacadeLocalTest`, `NGridFacadeNodeTest`.

## Arquivos de referência para navegar rápido
- Arquitetura: `doc/ngrid/arquitetura.md`
- Guia operacional/uso: `doc/ngrid/guia-utilizacao.md`
- Bootstrap e wiring: `nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGridNode.java`
- Facade simplificada: `NGrid.java`, `NGridLocalBuilder.java`, `NGridNodeBuilder.java`, `NGridCluster.java` (todos em `.../ngrid/structures/`)
- Protocolo/mensagens: `nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/common/MessageType.java`
- Codec: `nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/cluster/transport/codec/CompositeMessageCodec.java`
- Observabilidade: `nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/metrics/{NGridAlertEngine,NGridDashboardReporter,NGridOperationalSnapshot,RttMonitor}.java`
- Testes de consenso e resiliência: `nishi-utils-core/src/test/java/dev/nishisan/utils/ngrid/ClusterTestUtils.java`, `QueueCatchUpIntegrationTest.java`, `ngrid-test/src/test/java/dev/nishisan/utils/test/cluster/*`
- Testes da facade: `nishi-utils-core/src/test/java/dev/nishisan/utils/ngrid/structures/NGridFacadeLocalTest.java`, `NGridFacadeNodeTest.java`

