# Resolução de Falhas na Pipeline de Resiliência (NGrid)

O objetivo desta implementação é corrigir as quebras de build (`ConditionTimeoutException`) que ocorrem nos testes de integração com Docker no GitHub Actions. Em ambientes de CI, a orquestração via Testcontainers e as operações de rede (como eleição de líder e estabilização de quorum) podem sofrer atrasos significativos causados pela contenção de recursos, disparando timeouts curtos configurados localmente.

## User Review Required

Nenhuma alteração de arquitetura será feita, apenas ajustes em configurações de timeout dos testes. Solicito aprovação do tempo estabelecido: limitaremos os métodos a **300 segundos** cada e o tempo máximo de espera das condições (Awaitility) para **120 segundos**.

## Proposed Changes

### Testes de Integração de Resiliência (Docker)

#### [MODIFY] [NGridMapLeaderCrashIT.java](file:///home/lucas/Projects/nishisan/nishi-utils/ngrid-test/src/test/java/dev/nishisan/utils/test/cluster/NGridMapLeaderCrashIT.java)
- Aumentar os valores de `@Timeout` de 120s e 180s para **300s**.
- Alterar as condições do `Awaitility.await().atMost(...)` de `60s` ou `90s` para **120s** abrangendo estabilidade inicial, eleição de novo líder e cura do cluster.

#### [MODIFY] [NGridMapConcurrentWriteFailoverIT.java](file:///home/lucas/Projects/nishisan/nishi-utils/ngrid-test/src/test/java/dev/nishisan/utils/test/cluster/NGridMapConcurrentWriteFailoverIT.java)
- Aumentar os valores de `@Timeout` para **300s**.
- Uniformizar os `Awaitility.await().atMost(...)` para **120s** nas fases de estabilidade, nova eleição após retenção e verificação de retorno de operações do _producer_.

#### [MODIFY] [NGridPartitionResilienceIT.java](file:///home/lucas/Projects/nishisan/nishi-utils/ngrid-test/src/test/java/dev/nishisan/utils/test/cluster/NGridPartitionResilienceIT.java)
- Aumentar os valores de `@Timeout` para **300s**.
- Nas verificações (Awaitility) referentes à separação _majority_ e _minority_ (ex: "majority continues operating", "producer resumes", "cluster converges after heal"), elevar o `.atMost(...)` para **120s**, visto que envolve comandos explícitos de `docker network disconnect/connect` que podem apresentar latência prolongada em CI.

## Verification Plan

### Automated Tests
1. Rodar os testes alvos isoladamente e garantir que ainda passam no ambiente local:
```bash
mvn -B test -Pdocker-resilience -Dtest=NGridMapLeaderCrashIT
mvn -B test -Pdocker-resilience -Dtest=NGridMapConcurrentWriteFailoverIT
mvn -B test -Pdocker-resilience -Dtest=NGridPartitionResilienceIT
```
2. Após commit e push, a Pipeline do GitHub Actions "resilience-tests" deverá executar sem estourar o limite de tempo.

### Manual Verification
- Nenhuma validação manual de UI é necessária, apenas visualização dos status no Github Actions.
