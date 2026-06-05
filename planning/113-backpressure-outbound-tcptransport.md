# Plano — Issue #113: backpressure (bound + drop+catch-up) na fila outbound do TcpTransport

## Context

Sob `quorum=1` + `strictConsistency=false` (escrita best-effort, sem RTT no `put` do líder — o modo de HA que o TEvent Cardinal precisa), os followers não exercem contrapressão sobre o líder. A fila outbound **por conexão** do `TcpTransport` é hoje uma `LinkedBlockingQueue` **unbounded** (`TcpTransport.java:701`) e `send()` usa `offer()` sem bloquear (`:733`). Num *trap storm* (milhares de `EventDto` gordos/s) com WAN lenta, o writer (virtual thread, `drainOutbound` em `:736-758`) não acompanha o produtor → a fila cresce sem limite → **risco de OOM / heap pressure no líder**.

O maintainer **já decidiu a abordagem** (comentário da #113): PR dedicada com política **drop + catch-up** — a política `block` foi descartada porque a fila é compartilhada entre controle (HEARTBEAT) e dados (REPLICATION_REQUEST), e bloquear o `send` represaria o heartbeat → risco de reeleição espúria.

Requisitos: **RF1** capacidade configurável (default = unbounded, compat total); **RF2** drop+catch-up ao encher; **RF3** métrica de ocupação no `operationalSnapshot()`/dashboard.

**Pilar de viabilidade confirmado no código:** o catch-up é **automático e periódico**. `ReplicationManager.checkLagAndSync()` roda a cada 2000ms (`ReplicationManager.java:179`), compara `coordinator.getTrackedLeaderHighWatermark() - lastAppliedSequence` (`:254-277`) e dispara `requestSync` (snapshot) quando o lag passa de `SYNC_THRESHOLD=500`. O high-watermark viaja no HEARTBEAT (`ClusterCoordinator.java:462`, supplier em `NGridNode.java:369`), logo **um REPLICATION_REQUEST dropado é recuperado mesmo que a rajada pare** — não depende de nova operação chegar ao follower.

Execução acordada: **commits atômicos incrementais** em branch dedicada `feature/ngrid-outbound-backpressure`; testes = **unitário determinístico + integração de convergência**.

## Design (recomendado)

### Princípio: limitar apenas o data plane recuperável
O bound conta/descarta **apenas `MessageType.REPLICATION_REQUEST`** — é a fonte do OOM (payloads gordos) e o único tráfego 100% recuperável via gap/snapshot. Todo o resto (HEARTBEAT, PING, SYNC_*, REPLICATION_ACK, CLIENT_REQUEST/RESPONSE, SEQUENCE_RESEND_*) **nunca é dropado nem conta para o limite**. Assim o heartbeat sempre passa (sem represamento → sem reeleição) e o cliente nunca perde request.

### Mecanismo: contador + fila unbounded subjacente (NÃO bounded queue, NÃO PriorityQueue)
Encapsular numa classe nova **`OutboundChannel`** (package-private, `cluster/transport/`), que substitui o campo `outbound` da inner class `Connection`:
- Interno: `LinkedBlockingQueue<ClusterMessage>` unbounded + `AtomicInteger pendingReplication` + `AtomicLong droppedReplication` + `int replicationCapacity` (0 = unbounded).
- `boolean enqueue(ClusterMessage m)`:
  - se `replicationCapacity == 0` → **fast-path**: só `offer`, sem tocar contadores (caminho default byte-a-byte equivalente ao atual → compat total RF1).
  - se `isCountable(m.type())` (só `REPLICATION_REQUEST`) e `pendingReplication.get() >= replicationCapacity` → `droppedReplication.incrementAndGet()`, **retorna false (drop)**. Não enfileira.
  - senão: se countable, `pendingReplication.incrementAndGet()`; `offer(m)`; retorna true.
- `ClusterMessage poll(long, TimeUnit)`: `poll` da fila; **se o item retornado for countable, `pendingReplication.decrementAndGet()` imediatamente** (antes da escrita no socket — semântica "pendente na fila" e sem vazamento em caminho de erro de escrita).
- `int dataDepth()` → `pendingReplication.get()`; `long droppedCount()` → `droppedReplication.get()`.
- `isCountable(MessageType)`: método privado, regra num lugar só.

Limite é **soft** (race benigna check-then-increment: overshoot ≤ nº de produtores concorrentes; nunca unbounded). Documentar.

### Pontos de atenção (do review do Plan agent)
- `Connection.send()` mantém o early-return `if(!isOpen()) return;` **antes** de qualquer contagem.
- A cota é **por enlace físico (Connection)**, não por destino lógico: sob roteamento por proxy (`TcpTransport.send` em `:168-218`) um REPLICATION_REQUEST endereçado a D pode trafegar pela Connection de um proxy P e ser dropado lá — **seguro** (alívio de memória em P, catch-up recupera D). Documentar essa semântica.
- A métrica de drops mede **drop por capacidade**, não perdas por ausência de rota (caminhos de `:186-217` que descartam por "no connection" não passam pelo contador). Documentar.
- Reset do contador na reconexão (Connection trocada em `:430`/`:500`, removida em `:638`) é benigno — a Connection antiga é descartada inteira.
- **Não** corrigir o `@Serial` espúrio de `ClusterPolicyConfig` nesta PR (fora de escopo); apenas não propagar o padrão ao novo campo.

## Arquivos

**Criar:**
- `nishi-utils-core/.../ngrid/cluster/transport/OutboundChannel.java` — fila + política + contadores.
- `nishi-utils-core/src/test/.../ngrid/cluster/transport/OutboundChannelTest.java` — unitário (critério de aceitação primário).
- `nishi-utils-core/src/test/.../ngrid/OutboundBackpressureConvergenceTest.java` — integração com `NGrid.local(n)` (convergência pós-drop).
- `doc/ngrid/outbound-backpressure.md` (pt-BR) + diagrama PlantUML do fluxo drop+catch-up (convenção `doc/diagrams/`).

**Modificar:**
- `cluster/transport/TcpTransport.java` — `Connection` usa `OutboundChannel`; `send`/`drainOutbound` delegam; novos acessores `Map<NodeId,Integer> outboundQueueDepths()` e `Map<NodeId,Long> outboundDropped()` (iteram `connections`, `:77`).
- `cluster/transport/TcpTransportConfig.java` — campo/builder/getter `outboundQueueCapacity` (validação `>= 0`), repassado à `Connection`.
- `structures/NGridConfig.java` — campo/builder/getter `outboundQueueCapacity` (default 0, validação `>= 0`), espelhando o padrão de `transportWorkerThreads`.
- `structures/NGridNode.java` — `startServices` (`:298-304`) passa `config.outboundQueueCapacity()` ao `TcpTransportConfig.Builder`; `operationalSnapshot()` (`:630-684`) popula os 2 campos novos do record.
- `config/ClusterPolicyConfig.java` (inner `TransportConfig`) — campo `outboundQueueCapacity` + getter/setter (sem `@Serial`).
- `config/NGridConfigLoader.java` — binding (`:147-149`).
- `metrics/NGridOperationalSnapshot.java` — record: nova seção "Transport" com `Map<String,Integer> outboundQueueDepthByNode` e `Map<String,Long> outboundDroppedByNode` (+ atualizar os `@param` do Javadoc → necessário para `-Pvalidate-javadoc`).
- `metrics/NGridDashboardReporter.java` — seção "transport" em `buildDashboard`.
- Testes posicionais que quebram junto com o record (mesmo commit): `NGridOperationalSnapshotTest`, `NGridDashboardReporterTest`, `NGridAlertEngineTest`.

## Sequência de commits atômicos

1. **`feat(ngrid): OutboundChannel com política drop+catch-up para replicação`** — `OutboundChannel` + `OutboundChannelTest`. Isolado, compila/testa sozinho.
2. **`refactor(ngrid): Connection delega fila outbound ao OutboundChannel`** — `TcpTransport.Connection` troca a `LinkedBlockingQueue` pelo `OutboundChannel`; capacity ainda 0 (comportamento idêntico ao atual). Sem mudar config.
3. **`feat(ngrid): capacidade outbound configurável no TcpTransportConfig`** — `outboundQueueCapacity` no transport config, repassado à `Connection`.
4. **`feat(ngrid): expõe outboundQueueCapacity no NGridConfig`** — campo no `NGridConfig` + fiação em `NGridNode.startServices` (+ caso de validação `< 0` se houver `NGridConfigValidationTest`).
5. **`feat(ngrid): binding YAML de outboundQueueCapacity`** — `ClusterPolicyConfig.TransportConfig` + `NGridConfigLoader`.
6. **`feat(ngrid): métrica de ocupação outbound no snapshot operacional`** — (commit inevitavelmente largo: record posicional em 4 call sites) campos no record + `NGridNode.operationalSnapshot()` + acessores no `TcpTransport` + `NGridDashboardReporter` + correção dos 3 testes posicionais.
7. **`test(ngrid): convergência pós-drop sob backpressure outbound`** — integração `NGrid.local(n)`, padrão dos catch-up/integration tests existentes.
8. **`docs(ngrid): documenta backpressure outbound (drop+catch-up)`** — `doc/ngrid/outbound-backpressure.md` + diagrama PlantUML + atualização do guia de config YAML.

Cada commit deve compilar e passar `mvn -pl nishi-utils-core test`.

## Verificação (end-to-end)

- **Unitário (gate do critério):** `mvn -pl nishi-utils-core test -Dtest=OutboundChannelTest` — assert: com `capacity=C`, `dataDepth()` nunca passa de C (single-thread exatamente C), `droppedCount() == enfileirados-C`; HEARTBEAT/PING/CLIENT/SYNC sempre `enqueue=true` e não contam; `capacity=0` nunca dropa; após `poll` de REPLICATION_REQUEST a profundidade cai; cenário concorrente (M produtores + consumidor lento) → `dataDepth ≤ C+(M-1)`, drops>0, drenados+dropados = enfileirados.
- **Integração:** `mvn test -Presilience -Dtest=OutboundBackpressureConvergenceTest` — líder com `capacity` pequeno + follower lento/storm sintético → via `operationalSnapshot()`: `outboundDroppedByNode > 0` em algum momento **e** `lastAppliedSequence` converge ao fim (catch-up por snapshot). Polling com timeout generoso; assert em invariantes finais (convergência), não em profundidade instantânea (evita flakiness).
- **Suíte completa:** `mvn -pl nishi-utils-core test` e `mvn test -Presilience`.
- **Build de módulo:** `mvn -pl nishi-utils-core clean install -DskipTests`.
- **Javadoc:** `mvn verify -Pvalidate-javadoc` (record ganhou `@param` novos).
- **Dashboard:** inspecionar `dashboard.yaml` gerado — confirmar seção `transport` com profundidade/drops por nó.
