# Testes vermelhos conhecidos

Testes que falham na `main` **sem nenhuma alteração local**. Servem para quem
roda `mvn test` no repo inteiro não perder tempo investigando um vermelho que
não é seu, e para não usar "a suite estava vermelha" como desculpa para não
rodar a suite.

Regra: um teste só entra aqui com **prova de que falha na `main` limpa** (a
receita está no fim). Se você consertar um, remova a entrada no mesmo commit.

---

## `RelayStreamReplicationTest.streamConvergesQueueAndMapWithoutNakOrSnapshot`

- **Módulo:** `nishi-utils-core` (NGrid — replicação de cluster)
- **Constatado em:** 2026-09-04, `main` em `52c32cf`
- **Falha:** `follower did not converge via stream: applied=200 target=400`
  após ~31 s de espera (`RelayStreamReplicationTest.java:154`, chamado de `:78`)
- **Consistente**, não intermitente: falhou nas três execuções feitas — duas com
  alterações locais em `nishi-utils-oss` e uma com a `main` limpa via
  `git stash -u`.
- **Escopo:** o vermelho é do RELAY_STREAM da replicação. `nishi-utils-core` não
  depende de `nishi-utils-oss`, então mudanças no ngrrd não têm como alcançá-lo.
- **Efeito prático:** `mvn test` na raiz termina em `BUILD FAILURE` com
  `Tests run: 536, Failures: 1`. O módulo `nishi-utils-oss` fica **verde**
  (234 testes) e é o que importa para quem mexe no ngrrd.
- **Não bloqueia CI nem publicação — por desenho.** A classe vive em
  `dev.nishisan.utils.ngrid.replication`, e o perfil `exclude-ngrid` do `pom.xml`
  corta `**/ngrid/**` do surefire. O `pr-validation.yml` roda justamente com
  `-DexcludeNgrid=true`, com o motivo escrito no próprio workflow: a suíte de
  resiliência do NGrid é sensível a tempo e recursos e *"does not pass reliably
  on hosted runners"*. O `publish.yml` roda com `-DskipTests`.

  Ou seja: este vermelho **só aparece para quem roda a suíte completa localmente**.
  Não é um teste órfão que ninguém viu — é uma suíte que o projeto assume como
  local-only. O que o repo deve a você é dizer isso antes de você perder meia
  hora investigando.
- **Contorno:** valide o módulo que você tocou (`mvn -pl nishi-utils-oss test`).
  Para rodar a suite inteira sem o ruído, use o mesmo interruptor do CI:
  `mvn test -DexcludeNgrid=true`. A suíte de resiliência tem perfil próprio:
  `mvn test -Presilience` / `mvn verify -Pdocker-resilience`.

Convergência parcial (metade dos 400 esperados) com timeout de 30 s tem cara de
janela curta demais para a máquina, ou de o stream parar de aplicar no meio. Não
foi investigado a fundo — é trabalho para quem for mexer na replicação.

---

## `LeaderFailoverDuringMigrationClusterTest`

- **Módulo:** `nishi-utils-ngrrd-cluster` (cluster ngrrd — migração/rebalanceamento)
- **Falha:** intermitente, ~1 em 4 execuções, com duas assinaturas observadas depois de matar o
  líder (3 storage nodes + cliente) no meio de uma migração:
  1. os sobreviventes ficam ~150 s alternando "Direct connection failed / Failing over to proxy
     ngrrd-client-…" tentando alcançar o líder morto, e a nova eleição demora a assentar;
  2. "RELAY_STREAM op-log append failed … write not durable" no líder moribundo, e o novo líder não
     reconverge dentro dos 150 s observados.
- **Dependente do core:** as duas assinaturas apontam para o caminho de failover/replicação do
  NGrid (`nishi-utils-core`), não para a lógica de migração deste módulo em si — o
  `MigrationCoordinator` já resolve corretamente o `resumeInFlight()` quando o novo líder consegue
  assumir; o problema é o **tempo** até essa assunção acontecer sob essas duas condições.
- **Não bloqueia CI nem publicação — por desenho**, mesmo motivo do vermelho do NGrid acima: os
  `*ClusterTest` deste módulo (incluindo este) só rodam com `mvn -pl nishi-utils-ngrrd-cluster
  verify -Pngrrd-cluster`, fora do `pr-validation.yml` e do `publish.yml` (`-DskipTests`).
- **Contorno:** re-execute o teste isoladamente — a falha é intermitente, não determinística.
  Investigação de causa raiz das duas assinaturas é trabalho futuro registrado (fora do escopo da
  documentação do módulo).

---

## Como provar que um vermelho é pré-existente

Não confie em "não toquei nesse módulo". Rode na `main` limpa:

```bash
git stash push -u -m "wip"
mvn -pl <modulo> test -Dtest=<Classe> -DfailIfNoSpecifiedTests=false
git stash pop
```

Confirme o `git stash pop` — é o passo que costuma ser esquecido quando o teste
demora e a atenção vai embora.
