# Cardinal HA — Failover quebrado: causa-raiz, fixes e validação E2E

**Status:** fixes implementados e validados em E2E (Docker, 3 nós) + testes determinísticos RED→GREEN + regressão.
**Issue:** nishisan-dev/nishi-utils #117 (integração tevent-cardinal, modo cluster 3 nós).
**Escopo:** `nishi-utils-core` (lib). O código da Cardinal **não muda** — só o bump da lib.

---

## 1. Sintoma

Cluster de 3 nós (`card-a` prio 100 / `card-b` 50 / `card-c` 10), boot concorrente, `minClusterSize=2`. Ao matar o líder (`card-a`), o nó seguinte é eleito e cai (cluster sem líder). Reproduz **integrado na Cardinal**, **não** nos testes isolados da lib — a diferença é **timing** (ver §2/§3).

## 2. Causa-raiz #1 — lease não re-armada na eleição (coordenação)

`ClusterCoordinator.leaseExpiresAt` só era inicializada no `start()` e **renovada apenas enquanto o nó é líder** (em `evictDeadMembers`). Um nó que passou minutos como **follower** herda, ao ser eleito no failover, uma lease **já vencida** — e `evictDeadMembers` **checa a expiração ANTES de renovar** (linhas 507 vs 548), então o recém-eleito cai em `stepDown()` no primeiro ciclo de eviction → líder por ~334ms.

**Por que passa na lib e falha na Cardinal:** os testes de failover da lib fazem failover em segundos (lease do boot ainda válida); a Cardinal roda minutos como follower antes do failover → a lease vence. **Timing puro.**

**Fix:** em `updateLeader`, ao transicionar para líder (`isNowLeader && !wasLeader`), re-armar `leaseExpiresAt = now + leaseTimeout`.
**Teste:** `LeaderLeaseRearmOnFailoverTest` — espera a lease do boot vencer antes do failover (cenário que os testes existentes não cobriam). RED→GREEN determinístico.

## 3. Causa-raiz #2 — intermitência: broadcast de heartbeat bloqueante (transporte)

Com o fix #1, o `card-b` segurava a liderança ~14s e **às vezes** caía por perda de quórum (evicção do `card-c`). Causa **provada empiricamente** com instrumentação E2E:

- `broadcast()` era **síncrono** e iterava `knownPeers` (`ConcurrentHashMap`, ordem não-determinística).
- `send() → ensureConnection` faz `socket.connect(connectTimeout=5s)` **bloqueante**; para um peer morto, tenta o dial direto (5s) **e** o fallback de proxy (5s) = **~10s travado**. Medido: `DIAG broadcast send to 00100-card-a took 10079ms`.
- Rodando na thread única do heartbeat: se o peer morto é iterado **antes** do vivo, o heartbeat do vivo atrasa ~10s/ciclo. Com `heartbeatTimeout=6s` + grace de 12s, isso fica na **beira** → passa sem jitter, falha sob carga/jitter (gap > 12s → eviction → perde quórum).
- A **ordem do HashMap** decide antes/depois → **gatilho aleatório por run** = a intermitência.

**Fix:** `broadcast()` faz fan-out **assíncrono** — uma virtual thread por peer (`workerPool.submit`). Um peer morto/lento nunca atrasa o heartbeat de um peer vivo.
**Teste:** `BroadcastNonBlockingTest` — usa o hook `beforeDialHook` (seam de teste) para travar a discagem de um peer e assertar que `broadcast()` retorna < 1s. RED (síncrono: travou 20s) → GREEN (assíncrono: < 1s).

## 4. Validação E2E (Docker, 3 nós, FINE logging)

- **Sem fix:** failover quebra (`card-b` step-down imediato por lease vencida).
- **Com fix da lease:** `card-b` segura 14s, mas cai por evicção do `card-c` (intermitente).
- **Com instrumentação:** gaps de heartbeat do `card-c` no `card-b` de **2.000s cravados** atravessando o failover (vs ~10s sem o fix de broadcast).
- **Com ambos os fixes, 8 ciclos SOB CARGA** (6/12 cores saturados — a condição que disparava a falha original): **8/8 PASS, grace=0, evict=0**.

## 5. Arquivos alterados (lib)

- `cluster/coordination/ClusterCoordinator.java` — re-arm da lease na eleição.
- `cluster/transport/TcpTransport.java` — broadcast assíncrono + seam `beforeDialHook` (test-only).
- Testes: `LeaderLeaseRearmOnFailoverTest`, `BroadcastNonBlockingTest`.

## 6. Pendências

- [ ] Regressão completa verde (`mvn test`).
- [ ] Commits atômicos (1: lease; 2: broadcast async).
- [ ] Release `nishi-utils-core` 4.1.2 + doc/diagrama.
- [ ] Bump da Cardinal para 4.1.2 + E2E final + push/PR da branch `feature/cardinal-ha`.
- [ ] (Opcional/futuro) backoff de discagem para peer morto — evita o connect bloqueante repetido a cada heartbeat (hoje inócuo com o fan-out async, apenas custo de virtual threads).

## 7. Nota histórica

O veredito inicial (Fase 0, adversarial) apontou o gate `shouldInitiate` no `reconnectLoop` (transporte) como causa-raiz. **Estava errado** — o `send→ensureConnection` e o `probeLoop` discam sem esse gate, e a malha b↔c se forma. A causa real só apareceu na **observação E2E integrada** (lease + broadcast bloqueante), não na análise estática. Lição: para bugs de timing/integração, reproduzir no ambiente real (Docker, JVMs separadas, FINE logging) foi decisivo.
