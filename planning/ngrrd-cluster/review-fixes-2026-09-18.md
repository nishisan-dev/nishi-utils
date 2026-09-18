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

Plano registrado; implementação em andamento.
