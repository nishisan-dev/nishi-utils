# Checkpoint — ngrrd cluster (sessão pausada em 2026-09-16)

Branch: `feature/ngrrd-cluster` (commit base `5884faa` = spec em `planning/ngrrd-cluster.md`).
Atualizado após a retomada: M0 e M1a estão COMMITADOS; M1b está na árvore sem commit.

## Estado por marco

### M0 — core — COMMITADO (26918ef, 6ac1934, 119c216)
Refuter r3 reprovou a cláusula `port() > 0` em `isLeaderCandidate` (quebrava 25 testes: harnesses
legítimos usam porta 0); corrigido para `!host().isBlank()` (único placeholder real é o de HEARTBEAT).
Suíte completa do core: 556 run / 0 fail / 8 skips pré-existentes. Core instalado no `~/.m2` (8.2.0).
Decisões: nomes de `@Test` no core em inglês (padrão da base); `@DisplayName` segue o arquivo.

### M1a — módulo `nishi-utils-ngrrd-cluster` — COMMITADO (930c85b, fee8f9c, 8982cf6, e9f10c6)
Refuter r2 aprovou (63 testes; ordem total do placement confirmada por probe independente). Acabamentos
baixos foram para a seção 0 da spec do M1b. Decisões: gate JaCoCo do módulo fica para o M1c; versão do
reactor segue 8.2.0 até o M5.

### M1b — rpc + node + StorageNodeClusterTest — COMMITADO (2f5e8a0, 0c47af0, f2155c7, bfac70c)
Três rodadas de Refuter: r1 reprovou deadlock AB-BA no registry de handles, uso de handle após
fechamento por outra thread (falso OK em checkpoint), timeout embrulhado virando REMOTE_ERROR; r2 reprovou
READ sem auto-cura após ociosidade e TOCTOU em open/reopenIfKnown; r3 aprovou. Desenho final do registry:
entrada por série com lock próprio, `withHandle` como único caminho de uso, evict/closeIdle com tryLock
fora de qualquer outro lock, `closedByClient` distingue CLOSE explícito de fechamento por ociosidade,
OpenOptions guardadas por hash. Churn de liderança no bootstrap é ruído padrão do NGrid (A/B do Refuter).
Residuais informativos: `handleClose` não checa dono (limpeza local idempotente); `withHandleSelfHealing`
trata `fn` nulo como "não aberta" — operações novas não podem devolver null.

### M1c — cliente transparente + harness — COMMITADO (6cd744c, ef5a99d, ecc1415, c73a142, 9db1e9c)
Duas rodadas de Refuter + Debugger. Causas raiz encontradas: (1) records do catálogo sem `Serializable`
→ WAL do NMap falhava em silêncio e o nó reiniciado voltava com catálogo vazio (agora `Serializable` +
persistência `ASYNC_WITH_FSYNC` declarada via nova API `NGridNodeBuilder.map(name, mode)` no core);
(2) nó reiniciado respondia `WRONG_OWNER` sem dono e o cliente re-enfileirava em loop silencioso → nó
consulta o líder antes de responder, cliente reabre e loga com rate limit; (3) filtro de frescor de 4 s
descartava o nó que perdeu a liderança → `nodeStatusStaleAfter`, republicação imediata na troca de líder,
fallback para nós alcançáveis; (4) `PlacementResolver` ignorava o líder informado em `NOT_LEADER`;
(5) reroteamento invertia a ordem por série → o backlog da série migra junto. Resultado: 171 testes
unitários (cobertura 80%), `DistributedWriteReadClusterTest` 5/5, `NodeRestartClusterTest` 3/3.
Divergência aceita: distribuição "≥ 1/4 por nó" em vez de "20 ± 2" (churn de bootstrap do NGrid).
Tarefas de core abertas como chips (fora do escopo): ressincronização de mapa não persistente após
restart; higiene do TcpTransport (reconexão infinita, proxy para si mesmo, fast-path em sendAndAwait);
NMapPersistence falhar alto em valor não serializável.

**Para o M3 (registrar na spec):** `RemoteSeriesHandle.owner` não é atualizado quando o `WriteDispatcher`
reroteia por `WRONG_OWNER` com dono conhecido (só `noteOwner`, sem `reopener`) → após migração, `write()`
segue enfileirando no dono antigo e cada lote reroteado é prependido à frente do backlog já movido
(reintroduz inversão de ordem). Também: `connect()` falha se qualquer storage node ativo estiver
inalcançável dentro de `leaderWaitTimeout` (sem disponibilidade parcial).

### M2 — métricas por nó e admin — COMMITADO (2 commits: métricas/admin; orçamento total de close)
Refuter r1 reprovou: `close()` ignorava o orçamento no CLOSE remoto e o dispatcher recebia `requestTimeout` no lugar de
`closeTimeout` (um nó morto custava `requestTimeout` por handle); teste de admin flaky. r2 aprovou: 191 testes,
cobertura de linha 82%, `AdminStatusClusterTest` 7/7 (8-20 s). Trade-off deliberado: com o orçamento esgotado o
dispatcher descarta pendências com log SEVERE e `samplesFailed`. Residuais baixos: `awaitTermination`/`join` do
dispatcher (+3 s) e `node.close()` ficam fora do deadline.
Defeito PRÉ-EXISTENTE confirmado por A/B (falha também sem M2, sob carga): sob churn de liderança o líder pode recolocar
uma série existente e o nó aceita o hint criando uma série vazia no lugar errado → seção 0 obrigatória da spec do M3.

### M3 — migração e rebalanceamento — Builder em andamento (spec `spec-m3.md`, seção 0 primeiro)

## Observações
- `DualLeaderLivelockE2ETest` falhou uma vez sob carga da suíte completa; 4/4 na main e 3/3 isolado na
  branch → não é regressão do M0. `RelayStreamReplicationTest` continua vermelho conhecido (main).
- Fluxo por marco: Builder (sonnet) → Refuter (opus) → commit pelo orquestrador. Sub-agentes não commitam.
