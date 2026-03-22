# Plano de Evolução de Maturidade — NGrid

## Diagnóstico Atual (2026-03-21)

### O que está maduro ✅

| Pilar | Estado | Evidência |
|-------|--------|-----------|
| Modularização | ✅ Sólido | pom.xml multi-módulo (`nishi-utils-core` + `ngrid-test`) |
| Testes unitários | ✅ Forte | 52+ testes em core, cobrindo replicação, metrics, config, queue, map |
| Testes Docker/IT | ✅ Bom | 9 ITs em Testcontainers (eleição, failover, catch-up, map crash/reeleição) |
| Documentação | ✅ Acima da média | arquitetura.md, runbooks (4), playbooks, CHANGELOG, design docs |
| Catch-up/Recovery | ✅ Implementado | Snapshot chunked + reenvio de sequência (`SEQUENCE_RESEND`) |
| Observabilidade (base) | ✅ Implementado | `NGridAlertEngine`, `NGridDashboardReporter`, `NGridOperationalSnapshot` |
| Replay/Duplicatas | ✅ Resolvido | Hybrid Offset Sync, `truncateAndReopen`, flush antes de snapshot |
| Sequenciamento | ✅ Resolvido | Sequência por tópico (bug de deadlock multi-queue eliminado) |
| Leader Lease | ✅ Implementado | Step-down automático em partição, epoch fencing |

### O que ainda precisa de hardening 🟡

| Gap | Impacto | Prioridade |
|-----|---------|------------|
| Testes de partição longa com divergência | Sem cobertura de split-brain write com reconexão | P0 |
| Writes concorrentes no Map durante failover | Sem cobertura automatizada | P0 |
| Guardrails de configuração para produção | Config inválida não bloqueia startup | P1 |
| Soak test em Docker real (não apenas in-process) | Soak atual roda in-process, sem rede real | P1 |
| CI gate de resiliência obrigatório | ITs Docker não rodam no CI por default | P1 |
| Semântica de consumo (`DELETE_ON_CONSUME` vs `TIME_BASED`) | Decisão pendente, ambiguidade na API | P2 |

---

## Proposta: 3 Sprints de Hardening

### Sprint 1 — Cobertura de Cenários Críticos (Queue + Map)

Objetivo: preencher os gaps P0 de cobertura de testes que hoje impedem a confiança em cenários de falha real.

---

#### [NEW] [NGridMapConcurrentWriteFailoverIT.java](file:///home/lucas/Projects/nishisan/nishi-utils/ngrid-test/src/test/java/dev/nishisan/utils/test/cluster/NGridMapConcurrentWriteFailoverIT.java)

Teste Docker IT que valida writes concorrentes de **2 producers** no Map durante failover:
- Dois containers fazem `put()` contínuo em chaves aleatórias
- Líder é killed via SIGKILL
- Após re-eleição, valida:
  - Nenhum `put` confirmado foi perdido
  - Nenhuma chave tem valor inconsistente entre réplicas
  - Producer retomou escrita no novo líder

---

#### [NEW] [NGridPartitionResilienceIT.java](file:///home/lucas/Projects/nishisan/nishi-utils/ngrid-test/src/test/java/dev/nishisan/utils/test/cluster/NGridPartitionResilienceIT.java)

Teste Docker IT para partição de rede longa:
- 5 nós, split em 2 partições (3+2) usando `iptables` ou network disconnect
- Partição dura 15-30s
- Valida:
  - Lado majoritário continua operando (queue offer/poll, map put/get)
  - Lado minoritário faz step-down (lease expira, writes falham)
  - Após reconexão, minority catch-up acontece sem perda

---

#### [NEW] [NGridQueueConcurrentFailoverIT.java](file:///home/lucas/Projects/nishisan/nishi-utils/ngrid-test/src/test/java/dev/nishisan/utils/test/cluster/NGridQueueConcurrentFailoverIT.java)

Teste Docker IT — validação de queue sob failover com producer contínuo:
- Producer fazendo `offer()` contínuo (100 msg/s)
- Consumer fazendo `poll()` contínuo
- Líder killed, re-eleição, validação de:
  - Zero duplicatas
  - Zero perdas de mensagens confirmadas
  - Throughput retoma em < 10s

---

### Sprint 2 — Guardrails e Hardening de Configuração

Objetivo: tornar o comportamento em produção previsível e seguro por padrão.

---

#### [MODIFY] [NGridConfig.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/structures/NGridConfig.java)

Adicionar validação no `build()`:
- Se `profile == PRODUCTION`:
  - `strictConsistency` deve ser `true` → lança `IllegalArgumentException` se `false`
  - `replicationFactor` deve ser `>= 2`
  - `mapPersistenceMode` deve ser `!= DISABLED`
- Novo campo `profile` enum (`DEV`, `STAGING`, `PRODUCTION`) com default `DEV`

---

#### [NEW] [NGridConfigValidationTest.java](file:///home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/src/test/java/dev/nishisan/utils/ngrid/config/NGridConfigValidationTest.java)

Testes unitários para as regras de guardrail:
- `produção com strictConsistency=false rejeita`
- `produção com replicationFactor=1 rejeita`
- `dev sem restrições aceita qualquer config`

---

### Sprint 3 — Soak em Docker + CI Gate

Objetivo: validação prolongada em rede real e gate automatizado no pipeline.

---

#### [NEW] [NGridDockerSoakIT.java](file:///home/lucas/Projects/nishisan/nishi-utils/ngrid-test/src/test/java/dev/nishisan/utils/test/cluster/NGridDockerSoakIT.java)

Port do `NGridSoakTest` existente para Docker real:
- 3 containers Testcontainers
- Duração configurável (default: 5min no CI, extensível para 12h+ local)
- Leader churn via `docker kill` periódico
- Validação: zero duplicatas, zero perdas, throughput sustentado
- Gera relatório markdown em `target/soak-report.md`

---

#### [MODIFY] [publish.yml](file:///home/lucas/Projects/nishisan/nishi-utils/.github/workflows/publish.yml)

- Adicionar job `resilience-gate` que roda os ITs Docker antes do deploy
- Falha no gate bloqueia release

---

## Verificação

### Testes Automatizados

```bash
# Sprint 1 — Novos ITs Docker (requer Docker)
cd /home/lucas/Projects/nishisan/nishi-utils
mvn -pl ngrid-test -Dtest=NGridMapConcurrentWriteFailoverIT verify
mvn -pl ngrid-test -Dtest=NGridPartitionResilienceIT verify
mvn -pl ngrid-test -Dtest=NGridQueueConcurrentFailoverIT verify

# Sprint 2 — Guardrails
mvn -pl nishi-utils-core -Dtest=NGridConfigValidationTest test

# Sprint 3 — Soak Docker (5min default)
mvn -pl ngrid-test -Dtest=NGridDockerSoakIT verify
```

### Verificação Manual
- Após Sprint 2, confirmar que `NGridConfig.builder(...).profile(PRODUCTION).strictConsistency(false).build()` lança exceção
- Após Sprint 3, confirmar que o CI executa o gate de resiliência no workflow de release

---

## User Review Required

> [!IMPORTANT]
> **Definição sobre semântica de consumo** — O projeto tem duas semânticas coexistindo (`DELETE_ON_CONSUME` para legacy e `TIME_BASED` para novo). Preciso de uma decisão de produto sobre qual será o default antes de implementar guardrails que forcem um modo padrão.

> [!WARNING]
> **Sprint 1 requer network manipulation em Docker** — `NGridPartitionResilienceIT` precisa de `iptables` ou `docker network disconnect` dentro dos containers. Isso exige que a imagem Docker tenha privilégios de rede ou que usemos Testcontainers Toxiproxy. Qual abordagem você prefere?
