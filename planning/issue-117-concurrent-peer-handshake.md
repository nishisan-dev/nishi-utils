# Issue #117 — TcpTransport: peers concorrentes não formam conexão direta (proxy-only via hub), quebrando failover

## Context — Veredito

**Bug CONFIRMADO e estrutural**, localizado na **camada de transporte** (`TcpTransport`), não no `ClusterCoordinator`.

Quando dois peers iniciam concorrentemente com configuração full-mesh, ocorre um *simultaneous open* TCP (dois sockets por par). O handshake não tem desempate determinístico: cada lado fecha a própria conexão de saída ao receber o handshake na conexão de entrada, gerando `EOFException`/`Connection reset`. A recuperação depende de um probe defeituoso que nunca reconecta de fato, deixando o par preso em **proxy-only via hub**. Quando o hub morre, os heartbeats entre os sobreviventes (que trafegavam via hub) cessam, eles se despejam mutuamente, perdem quórum e **não elegem novo líder** — o hub vira SPOF.

O comportamento do quórum hoje está **correto**: o que falta é o link direto que nunca se forma. A correção principal é no transporte; o ajuste de quórum é hardening adicional (solicitado pelo usuário).

## Causa raiz (evidências no código)

1. **Dial incondicional no start** — `TcpTransport.start()` linha **134**: `config.initialPeers().forEach(this::ensureConnectionAsync)` disca para *todos* os peers iniciais **ignorando `shouldInitiate`** (linha 608-610). Com full-mesh, todo par abre socket dos dois lados → *simultaneous open*. (`reconnectLoop` linha 382, `handleHandshake` linha 541 e `handlePeerUpdate` linha 601 já respeitam `shouldInitiate` — só o start viola.)

2. **Handshake sem reconciliação determinística** — `handleHandshake()` linhas **511‑516**: faz `connections.put(remoteId, connection)` e fecha a `previous` **incondicionalmente**, sem escolher qual conexão manter. Ambos os lados fecham o próprio outbound → o peer recebe EOF na `readLoop` (linhas 784‑790, log em 807‑811). Resultado: estado assimétrico/zero-conexão.

3. **Probe de recuperação quebrado** — `probeLoop()`/`tryPromoteRoute()` linhas **451‑476**: abre um `Socket` **descartável sem handshake** (linha 468, fechado pelo try-with-resources) — isso (a) gera ainda mais EOF nos peers e (b) chama `router.promoteToDirect()` **sem realmente reestabelecer a `Connection`**. O próximo `send()` chama `ensureConnection()` de novo, colide de novo, `markDirectFailure()` (linha 202) → volta a PROXY. Flapping permanente.

4. **Consequência no coordinator (não é a causa)** — heartbeats são `broadcast()` → `send()`; em proxy-only fluem via hub. Hub morre → `evictDeadMembers()` (linhas 490‑531) despeja o par → `activeMembers = 1 < requiredActiveMembersForLeadership = 2` (linhas 572‑577) → `recomputeLeader()` zera o líder. Quórum correto, link ausente.

## Branch

`fix/ngrid-concurrent-peer-handshake`

## Correção — Transporte (definitivo, sem fallback p/ legado)

Arquivo: `nishi-utils-core/.../cluster/transport/TcpTransport.java`

- **A) Single-initiator no start** — em `start()` (linha 134), só `ensureConnectionAsync` para peers onde `shouldInitiate(peer)` for verdadeiro. Elimina a colisão na origem.

- **B) Reconciliação determinística de *simultaneous open*** — em `handleHandshake()` (substituir a lógica das linhas 511‑516): quando já existe uma conexão **aberta** ao mesmo `remoteId`, manter a conexão **iniciada pelo menor NodeId** e fechar a perdedora; ambos os lados convergem para o mesmo socket físico. Regra unificada usando `Connection.outboundInitiated` (campo existente, linha 715):
  ```
  boolean keepArriving = (localId.compareTo(remoteId) < 0) == connection.outboundInitiated;
  // existente aberta & diferente → se keepArriving: registra nova, fecha existente;
  //                                senão: fecha a recém-chegada e NÃO responde handshake.
  // sem existente aberta → registra normalmente (caso comum, sem colisão).
  ```
  Não responder handshake na conexão descartada (ajustar o `if (firstHandshakeOnThisConnection)` da linha 547). `handleDisconnect` (linhas 651‑659) já ignora o fechamento de conexão não-rastreada — sem regressão em pending responses.

- **C) Auto-promoção de rota** — ao concluir o handshake de uma conexão **direta** (em `handleHandshake`), chamar `router.promoteToDirect(remoteId)` para que um link reestabelecido saia de PROXY automaticamente (self-healing).

- **D) Probe correto** — em `tryPromoteRoute()` (linhas 463‑476) trocar o socket descartável por `ensureConnection(target)` real (handshake completo) **apenas no lado iniciador** (`shouldInitiate`); promover a DIRECT só quando a `Connection` for de fato estabelecida (ou delegar ao `reconnectLoop` + auto-promote do item C e remover o probe defeituoso). Sem `promoteToDirect` "às cegas".

## Correção — Quórum (hardening; **ambos os ajustes**, escolha do usuário)

Arquivo: `nishi-utils-core/.../cluster/coordination/ClusterCoordinator.java`

- **E) Denominador robusto** — em `requiredActiveMembersForLeadership()` (linhas 572‑577), contar apenas peers elegíveis (`info.port() > 0`), excluindo clientes de descoberta/fantasmas do gossip que inflam a maioria exigida.

- **F) Considerar reachability via proxy na atividade** — em `evictDeadMembers()` (linhas 490‑531), conceder *grace* a um membro que perdeu heartbeat porém ainda é `transport.isReachable(id)` (inclui PROXY, ver `TcpTransport.isReachable` linhas 295‑300), evitando despejo durante flaps de link direto enquanto há caminho via proxy.
  - **Trade-off documentado:** contar reachability só-via-proxy pode manter o hub como SPOF (ao morrer o hub, esses membros somem e o quórum cai). Com a correção do transporte a malha é direta, então isso só atua em estados degradados. Registrar o trade-off em Javadoc + `doc/ngrid`.

## Testes (evidência — replicando o cenário)

1. **T1 — core / transporte (evidência primária, determinística)**
   `nishi-utils-core/.../cluster/transport/TcpTransportConcurrentMeshTest.java` (modelo: `ProxyRoutingIntegrationTest`).
   3 `TcpTransport` com config **full-mesh** (cada um conhece os outros dois), iniciados **concorrentemente** (threads + `CountDownLatch`). Após janela de convergência, asserta **malha direta total**: os 6 `isConnected` direcionais verdadeiros e `getRouter().nextHop(peer)` == peer (DIRECT, sem proxy). **Pré-fix falha** (≥1 par fica proxy-only).

2. **T2 — core / failover in-process**
   `NGridLeaderReelectionConcurrentStartTest.java` usando `NGrid.local(3)` (ou 3 `NGridNode` instanciados e iniciados concorrentemente). Estabiliza com `ClusterTestUtils.awaitClusterConsensus`, mata o **primeiro nó** (`node.close()`) e asserta **re-eleição** entre os sobreviventes (novo `leaderInfo()` presente, `activeMembers == 2`). Replica o sintoma de failover quebrado.

3. **T3 — IT Docker (ambiente reportado)**
   `ngrid-test/.../NGridConcurrentMeshFailoverIT.java` (modelo: `NGridLeaderFailoverIT` + `AbstractNGridClusterIT`). Exige **config full-mesh** (hoje os containers usam topologia hub `SEED_HOST` único — não dispara a colisão): adicionar suporte a múltiplos peers (ex.: env `NG_PEERS=a:9000,b:9000,c:9000` em `Main`/`server-config.yml`). Subir os 3 com `Startables.deepStart` (paralelo), validar líder inicial, **parar o primeiro nó** (`docker stop`/SIGTERM) e assertar re-eleição via markers `CURRENT_LEADER_STATUS`/`ACTIVE_MEMBERS_COUNT`. Profile `-Pdocker-resilience`.

## Documentação (DoD)

- Atualizar `doc/ngrid` (transporte/handshake) descrevendo single-initiator + reconciliação determinística e o trade-off do quórum.
- Diagrama PlantUML em `docs/diagrams/` (sequência do *simultaneous open* e desempate por NodeId), incorporado via `uml.nishisan.dev`.

## Verificação

```bash
# Evidenciar a FALHA antes do fix (T1 deve falhar):
mvn -pl nishi-utils-core test -Dtest=TcpTransportConcurrentMeshTest   # pré-fix: RED

# Após implementar o fix:
mvn -pl nishi-utils-core clean install -DskipTests
mvn -pl nishi-utils-core test -Dtest=TcpTransportConcurrentMeshTest,NGridLeaderReelectionConcurrentStartTest
mvn -pl nishi-utils-core test            # suite completa (regressão)
mvn -pl nishi-utils-core test -Presilience

# IT Docker (T3):
mvn -pl ngrid-test verify -Pdocker-resilience
```

Critério de aceite: T1/T2 **falham antes** e **passam depois** do fix; suite completa + `-Presilience` verdes; T3 elege novo líder após a morte do primeiro nó iniciado concorrentemente.

## Observações

- Correção alinhada à diretriz "ir sempre adiante" (sem fallback p/ legado): o probe defeituoso é substituído, não mantido.
- Commits atômicos sugeridos: (1) fix transporte A+B, (2) auto-promote/probe C+D, (3) quórum E+F, (4) T1, (5) T2, (6) T3+config full-mesh, (7) docs/diagrama.
- Se os logs completos dos 3 nós forem necessários para casos de borda do IT Docker, solicitar via comentário na issue #117.
