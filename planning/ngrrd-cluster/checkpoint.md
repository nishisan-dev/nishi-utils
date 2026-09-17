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

### M1b — rpc + node + StorageNodeClusterTest — implementado, aguardando Refuter
Builder: 106 testes unitários (8 classes) + `StorageNodeClusterTest` 3/3. RPC a si mesmo via despacho local
(`LocalRequestHandler`), pois `TcpTransport` não faz loopback. `NgrrdHandle.close()` já força durabilidade.
Divergências a julgar pelo Refuter: `reopenIfKnown` com `OpenOptions.defaults()`; `READ` não reabre handle.
Builder observou churn de liderança no bootstrap de 3 nós (epoch 1→6, dual-leader detectado) — Refuter
investiga se é ruído padrão do NGrid ou interação com o M0.
**Próximo passo:** veredito do Refuter → correções → commits atômicos (rpc; node; testes/recursos) →
M1c (`planning/ngrrd-cluster/spec-m1c.md`).

### M1c — spec pronta, não iniciado
Cliente transparente + harness + `DistributedWriteReadClusterTest`; ligar o gate JaCoCo do módulo.

## Observações
- `DualLeaderLivelockE2ETest` falhou uma vez sob carga da suíte completa; 4/4 na main e 3/3 isolado na
  branch → não é regressão do M0. `RelayStreamReplicationTest` continua vermelho conhecido (main).
- Fluxo por marco: Builder (sonnet) → Refuter (opus) → commit pelo orquestrador. Sub-agentes não commitam.
