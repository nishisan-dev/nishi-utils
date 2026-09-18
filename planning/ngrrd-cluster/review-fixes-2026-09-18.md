# Correções da revisão de 2026-09-18

Branch: `feature/ngrrd-cluster`. Referência: `docs/review-feature-ngrrd-cluster-2026-09-18.md`.
Preservar as alterações locais anteriores em core, MigrationCoordinator, testes de failover,
changelog e configuração da IDE; não incluí-las nestes commits.

## Sequência de commits

1. Documentação: registrar revisão e este plano.
2. P1 / achado 1: serializar mutações locais de migração por série; rejeitar mensagens de
   migrações abortadas/substituídas e validar o placement forte antes de instalar a imagem.
   Cobrir mensagens tardias, abort antecipado, commit concorrente e cancelamento da origem.
3. P1 / achado 3: serializar admissão e reroteamento por série, preservando backlog e escritas
   concorrentes, inclusive chamadas que capturaram o dono antigo.
4. P1 / achado 2: barreira de conclusão das escritas independente de destino; flush/checkpoint
   só seguem após a confirmação das escritas anteriores, com timeout e erro observáveis.
5. P2 / achado 4: resolver chave física pelo prefixo configurado, sem depender do cache de
   definição; cobrir registry novo, prefixo customizado e migração sucessiva sem OPEN.

## Validação

- Testes de regressão determinísticos nos colaboradores reais, RPC controlado e volumes reais.
- Testes direcionados antes de cada commit de correção.
- Suíte completa do módulo, incluindo `*ClusterTest`, após integração das quatro correções.
- Conferência do diff/index antes de cada commit e dos hashes das alterações preexistentes
  ao final. Nenhum push, merge ou release faz parte deste trabalho.

## Execução

- Achado 1 implementado: autorização forte do destino, locks locais por série, tombstone de
  abort antecipado e cancelamento da transferência sem sobrescrever fases terminais.
  `MigrationExecutorTest,RebalanceClusterTest`: 23 testes aprovados (5 novas regressões).
- Achado 3 implementado: rota canônica e lock por série, backlog transferido antes da
  publicação do dono e revalidação da rota após backpressure.
  `WriteDispatcherTest,RemoteSeriesHandleTest`: 25 testes aprovados (2 novas regressões).
- Achado 2 implementado: barreira por sequência de admissão/conclusão da série, preservada
  em múltiplos redirecionamentos; falhas permanentes impedem confirmação de checkpoint.
  Flush global e fechamento acompanham buffers criados durante reroteamento.
  `WriteDispatcherTest,RemoteSeriesHandleTest,WriteBarrierRegressionTest,CloseBudgetRegressionTest`:
  34 testes aprovados (7 novas regressões, incluindo API pública, timeout e erro de escrita).
- Achado 4 implementado: `MigrationExecutor` recebe o prefixo do storage node e usa
  `StorageKey.series`; origem e FINISH funcionam sem YAML em memória. O destino valida
  que a chave recebida corresponde ao seu prefixo.
  `MigrationExecutorTest,ColdSeriesDrainClusterTest`: 25 testes aprovados, incluindo
  prefixo customizado, reinício e segunda drenagem sem OPEN, com comparação dos bytes.
- Validação integrada concluída: **361 testes, zero falhas, zero erros, zero skips**, BUILD
  SUCCESS em 5 min 40 s (344 casos existentes + 17 novos casos de regressão).
  Comando: `mvn -pl nishi-utils-ngrrd-cluster -am test -Dtest='dev.nishisan.utils.oss.cluster.**.*Test' -Dsurefire.failIfNoSpecifiedTests=false`.
  Inclui adoção de volume, restart, drenagem, rebalanceamento, churn e failover durante migração.
  Não foram executados Docker/Testcontainers, soak nem a suíte inteira do monorepo nesta etapa.
- Hashes dos arquivos que já estavam modificados antes do trabalho: preservados. Essas alterações
  seguem fora dos commits de correção; os commits foram preparados com paths explícitos.

## Commits de correção

- `73599bf`: achado 1 — migrações obsoletas.
- `7aab374`: achado 3 — ordem de admissão/reroteamento.
- `dbb67d4`: achado 2 — barreiras de flush/checkpoint.
- Achado 4: `fix(ngrrd-cluster): migrate persisted series without cached definitions` (esta etapa).

O commit `0b691b2` registra o plano e a revisão original.
