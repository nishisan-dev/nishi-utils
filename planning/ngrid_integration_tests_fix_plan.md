# Objetivo: Corrigir Falhas nos Testes do NGrid no Docker

O erro atual (`expected: <1> but was: <3>`) indica um cenário de *split-brain*, onde os três containers iniciam isoladamente e cada um se declara líder de si mesmo, pois não conseguem se descobrir na rede.

A causa raiz é que o arquivo `ngrid-test/config/server-config.yml`, utilizado na imagem Docker `ngrid-test:latest`, possui a lista de `seeds` fixada com IPs locais de desenvolvimento (`192.168.5.89:9000` etc), ignorando o alias DNS `seed-1` que a abstração `NGridNodeContainer` tenta injetar via variável de ambiente `SEED_HOST`.

## User Review Required

Nenhuma alteração com risco estrutural profundo. A mudança foca diretamente em utilizar a abstração correta por variável de ambiente para que cada nó Docker enxergue o `seed-1` inicial, de acordo com o padrão do Testcontainers definido em `AbstractNGridClusterIT`.

## Proposed Changes

### Ajuste de Configuração
Substituiremos os IPs fixos pela expressão com a variável de ambiente `$SEED_HOST`. Caso não seja injetada, assumimos o localhost por segurança na execução manual.

#### [MODIFY] [server-config.yml](file:///home/lucas/Projects/nishisan/nishi-utils/ngrid-test/config/server-config.yml)
A lista em `cluster.seeds` no arquivo `server-config.yml`:
```yaml
  seeds:
    - "${SEED_HOST:127.0.0.1:9000}"
```

## Verification Plan

### Automated Tests
Após realizar a alteração no `server-config.yml`, irei:

1. Executar o comando mandatório (segundo as regras de usuário/Java):
   ```bash
   mvn clean install -DskipTests
   ```
2. Reconstruir e rodar os testes falhos de integração NGrid:
   ```bash
   mvn verify -f ngrid-test/pom.xml -Dnishi.utils.version=3.0.3
   ```
3. O objetivo é ver:
   - `NGridLeaderElectionIT` passando (somente o seed ou apenas 1 nó como líder eleito; os 3 nós conectados ao cluster).
   - `NGridLeaderFailoverIT` passando.
   - `NGridCatchUpIT` e `NGridReplicationIT` passando.

Após verificação com sucesso, copiarei este plano para a pasta `/planning` e finalizarei com o arquivo de `walkthrough`.
