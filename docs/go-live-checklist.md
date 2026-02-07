# NGrid Go-Live Checklist

> Critérios de aceite para produção crítica, conforme o
> [NGrid Production Maturity Phased Plan](../doc/planning/ngrid-production-maturity-phased-plan.md).

## Critério de Saída Global
- [ ] Todos os gates aprovados por **2 ciclos consecutivos**
- [ ] Sem bugs críticos abertos em consistência/durabilidade

---

## 1. Critérios Funcionais

| # | Critério | Evidência | Status |
|---|---|---|---|
| F1 | Failover do líder preserva dados | `QueueNodeFailoverIntegrationTest.testDataPersistsAfterLeaderFailover` | ☐ |
| F2 | Writes durante failover sem perda | `QueueNodeFailoverIntegrationTest.testWritesDuringFailover` | ☐ |
| F3 | Partição de rede: líder isolado faz step-down | `LeaderLeaseIntegrationTest.leaderStepsDownAndMajorityElectsNewLeader` | ☐ |
| F4 | Epoch monotônico pós-partição | `LeaderLeaseIntegrationTest.epochIsMonotonicallyIncreasingAfterPartition` | ☐ |
| F5 | Crash abrupto: offsets recuperados (sem duplicatas) | `NGridDurabilityTest.shouldRecoverOffsetAfterCrashAndNotReceiveDuplicates` | ☐ |
| F6 | Crash do seed: mensagens recuperadas | `NGridDurabilityTest.shouldRecoverMessagesAfterSeedCrash` | ☐ |
| F7 | Multi-client crash: offsets independentes | `NGridDurabilityTest.shouldMaintainIndependentOffsetsAfterMultiClientCrash` | ☐ |
| F8 | Restart total: zero duplicatas e zero perdas | `NGridTestcontainersSmokeTest` (2 cenários) | ☐ |
| F9 | Consistency levels (STRONG, EVENTUAL, BOUNDED) | `ConsistencyIntegrationTest` | ☐ |
| F10 | Gap recovery + metrics | `SequenceGapRecoveryIntegrationTest` (3 cenários) | ☐ |
| F11 | Catch-up via snapshot sync (gap > threshold) | `SequenceGapRecoveryIntegrationTest.testFollowerCatchUpWithLargeGapViaSnapshotSync` | ☐ |
| F12 | Persistência de queue após restart | `QueueRestartConsistencyTest` (3 cenários) | ☐ |
| F13 | Re-eleição de líder por ingress rate | `LeaderReelectionIntegrationTest` | ☐ |
| F14 | Leader lease: step-down do líder | `LeaderLeaseStepDownTest` | ☐ |
| F15 | Quorum failure: escrita rejeitada sem quorum | `ReplicationQuorumFailureTest` + `ReplicationManagerQuorumFailureTest` | ☐ |
| F16 | Map persistence e recovery | `MapPersistenceTest` + `MapPersistenceRecoveryTest` + `NGridMapPersistenceIntegrationTest` | ☐ |

---

## 2. Critérios Não-Funcionais

| # | Critério | Evidência | Status |
|---|---|---|---|
| NF1 | Soak test 12h+ sem falha | `target/soak-report.md` (via `mvn test -Psoak`) | ☐ |
| NF2 | Zero duplicatas no soak test | Relatório soak: `Duplicates = 0` | ☐ |
| NF3 | Zero perdas no soak test | Relatório soak: `Lost Messages = 0` | ☐ |
| NF4 | P99 de offer latency < SLO definido | Relatório soak: tabela de latência | ☐ |
| NF5 | Replication lag convergente | Snapshots operacionais no relatório | ☐ |
| NF6 | Cluster sobrevive a múltiplos churns de liderança | Relatório soak: tabela de Churn Events | ☐ |

---

## 3. Critérios Operacionais

| # | Critério | Evidência | Status |
|---|---|---|---|
| O1 | Runbook: Partição de rede | `docs/runbooks/network_partition.md` | ☐ |
| O2 | Runbook: Líder instável | `docs/runbooks/unstable_leader.md` | ☐ |
| O3 | Runbook: Recuperação pós-crash | `docs/runbooks/post_crash_recovery.md` | ☐ |
| O4 | Runbook: Consistência pós-incidente | `docs/runbooks/post_incident_consistency.md` | ☐ |
| O5 | Alert engine funcional | `NGridAlertEngineTest` | ☐ |
| O6 | Dashboard reporter funcional | `NGridDashboardReporterTest` | ☐ |
| O7 | Métricas operacionais completas | `NGridOperationalSnapshotTest` | ☐ |
| O8 | Pipeline CI de resiliência ativo | `.github/workflows/resilience.yml` | ☐ |
| O9 | Pipeline CI Docker ativo | Profile `docker-resilience` no `pom.xml` | ☐ |

---

## Aprovação

| Responsável | Data | Assinatura |
|---|---|---|
| Backend/Arquitetura | | |
| SRE/Plataforma | | |
| QA/Qualidade | | |
| Tech Lead | | |
