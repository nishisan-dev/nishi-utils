# Testes vermelhos conhecidos

Testes que falham na `main` **sem nenhuma alteração local**. Servem para quem
roda `mvn test` no repo inteiro não perder tempo investigando um vermelho que
não é seu, e para não usar "a suite estava vermelha" como desculpa para não
rodar a suite.

Regra: um teste só entra aqui com **prova de que falha na `main` limpa** (a
receita está no fim). Se você consertar um, remova a entrada no mesmo commit.

---

## Nenhum vermelho conhecido desde a 8.8.0

As entradas anteriores foram resolvidas na 8.8.0 (issue #178 e revisão do NGrid;
ver `doc/CHANGELOG.md`):

- `RelayStreamReplicationTest.streamConvergesQueueAndMapWithoutNakOrSnapshot`
  (`applied=200 target=400`): era um artefato de escala do odômetro — o
  seguidor guardava o **máximo** da sequência por tópico e o teste comparava
  com a soma de dois tópicos. O odômetro passou a ser a soma das fronteiras
  por tópico em todos os papéis, e o teste passou a assertar por tópico e a
  usar portas dinâmicas.
- `LeaderFailoverDuringMigrationClusterTest`: as duas assinaturas apontavam
  para o core — rota PROXY para o líder morto que nunca voltava a DIRECT (com
  graça de evicção indevida) e o `op-log append failed … write not durable` no
  líder moribundo (ordem de fechamento). Ambas corrigidas; além disso o novo
  líder só retoma migrações depois do fence do catálogo.
- `JoinQuiesceReleaseGateTest.rejoinDiscardsStaleProgressFromPreviousSession`
  (intermitente): a reativação de um membro por heartbeat não notificava a
  membership. Corrigido.

Um caso segue **ignorado como root** (não vermelho):
`RelayStreamConcurrentIngestTest.gapRepullFallsBackToSnapshotWhenPurgeFails`
depende de um diretório somente-leitura impedir o purge, o que não acontece
para o root (containers de CI/dev); o teste usa `Assumptions` nesse caso.

---

## Como provar que um vermelho é pré-existente

Não confie em "não toquei nesse módulo". Rode na `main` limpa:

```bash
git stash push -u -m "wip"
mvn -pl <modulo> test -Dtest=<Classe> -DfailIfNoSpecifiedTests=false
git stash pop
```
