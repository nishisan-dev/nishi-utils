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

### M1c — cliente transparente + harness — Builder em andamento (spec `spec-m1c.md`; inclui gate JaCoCo)
Cliente transparente + harness + `DistributedWriteReadClusterTest`; ligar o gate JaCoCo do módulo.

## Observações
- `DualLeaderLivelockE2ETest` falhou uma vez sob carga da suíte completa; 4/4 na main e 3/3 isolado na
  branch → não é regressão do M0. `RelayStreamReplicationTest` continua vermelho conhecido (main).
- Fluxo por marco: Builder (sonnet) → Refuter (opus) → commit pelo orquestrador. Sub-agentes não commitam.
