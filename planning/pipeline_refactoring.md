# Reestruturação das Pipelines CI/CD

Refatoração das ações do GitHub para focar a pipeline de release (`publish.yml`) exclusivamente em empacotamento e publicação, movendo as verificações rigorosas (testes de unidade e testes de resiliência) para serem executadas durante a criação/atualização de Pull Requests (`resilience.yml`). Esta estratégia melhora o tempo de release e aumenta a segurança da branch `main`.

## User Review Required

> [!NOTE]
> Essa mudança não altera o código da aplicação, apenas a infraestrutura de CI.
> Após a aprovação e implementação, será necessário acessar as **"Branch protection rules"** no GitHub (Settings > Branches) e marcar os jobs (ex.: `Unit Tests` e `Resilience Tests`) como verificação obrigatória ("Require status checks to pass before merging") para a branch `main`.

## Proposed Changes

### Workflows do GitHub

#### [MODIFY] .github/workflows/publish.yml
- Removeremos o job `resilience-gate` por completo.
- Removeremos `needs: resilience-gate` do job `release`.
- Como resultado, o fluxo de `release` rodará diretamente quando uma TAG de lançamento for criada, focando puramente no build, deploy/packages e geração da Release no GitHub.

#### [MODIFY] .github/workflows/resilience.yml
- Renomearemos (apenas o `name:` no arquivo) para algo mais representativo, como **"PR Validation & Resilience"**.
- Adicionaremos um job genérico unificado de testes, como um `unit-tests`, ou dependeremos do já existente (`resilience-tests`) com um step para rodar testes padrão primeiro. Executaremos `mvn -B test` para validar as lógicas básicas do sistema além das de resiliência.
- Garantiremos que os jobs de resiliência (`resilience-tests`, `docker-resilience-tests`) rodem na sequência ou de forma paralela garantindo confiabilidade alta em PRs.

## Open Questions

> [!TIP]
> O job de resiliência atual no `publish.yml` testa algumas classes específicas (`NGridLeaderFailoverIT`, etc) enviando propriedades `-Dtest=` via terminal. No `resilience.yml` há um profile `-Pdocker-resilience` configurado que já possui um job `docker-resilience-tests`. Assumo que este profile já engloba todos os testes de estabilidade, e podemos nos apoiar nele ou adicionar o step idêntico ao do `publish.yml`. Qual é a sua preferência? Uso a mesma linha de comando na substituição ou uso o `Pdocker-resilience`?

## Verification Plan

### Testes Automatizados Mapeados
- Inspecionaremos se não há erros de sintaxe nos YAML usando um linter ou rodando um `workflow_dispatch` se aplicável.

### Manual Verification
- Validar se os nomes dos jobs aparecem corretamente na lista de *Checks* do seu próximo Pull Request.
