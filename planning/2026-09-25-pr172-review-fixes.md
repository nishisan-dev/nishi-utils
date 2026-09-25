# 8.5.1 — Correções da revisão pós-merge da PR #172

Status: aprovado em 2026-09-25 (ciclo paralelo à #171). Base: `9080b69` (8.5.0).
Branch: `fix/pr172-review-findings`, worktree `/home/lucas/Projects/nishisan/nishi-utils-851`.

## Restrições globais

- Testes **sempre** com JDK 21: prefixar Maven com `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64`
  (o padrão da máquina é JDK 25, que mascara pinning de virtual threads). O TEMS roda em JDK 21.
- Identificadores em inglês; Javadoc/comentários/mensagens/docs/commits em PT-BR; nomes de `@Test`
  em PT-BR.
- Commits atômicos (um por achado + um de versão/docs), mensagem `tipo(escopo): descrição` em PT-BR,
  sem menção a IA/agente/Co-Authored-By. Nunca `git add -A`/`git add .`.
- Sem `synchronized` envolvendo I/O bloqueante em código novo.
- **Nunca `mvn install`**: o `~/.m2` é compartilhado com outro worktree e com o TEMS. Sempre builds de
  reactor com `-am` (o módulo resolve core/oss do próprio reactor).
- Escopo: somente os três achados CONFIRMADOS abaixo. Itens PLAUSÍVEIS ficam fora.

## Achado 1 (média) — COMMITTED no destino deve sempre vencer

`nishi-utils-ngrrd-cluster/.../rebalance/MigrationCoordinator.java`, `pollUntilResolved` (~linhas
320-352). Hoje, se `pollStatusQuietly(dst, ...)` devolve `null` (falha de transporte) e, na mesma
iteração, a origem responde erro terminal, o coordenador chama `abort` — mesmo que o destino já esteja
`COMMITTED`. É provável na prática: o destino segura o lock da série durante o commit (fsync) e o
mesmo lock atende `MIGRATE_STATUS`; a origem estoura timeout e marca FAILED.

**Correção:** abortar por erro da origem **somente** quando o poll do destino da mesma iteração
respondeu (não-nulo) e não é `COMMITTED`. Se o poll do destino falhou, continuar o loop (novo poll do
destino na próxima iteração, respeitando o `migrationTimeout`); ao estourar o timeout, antes de abortar
reconsultar o destino uma última vez e completar se `COMMITTED`. Atualizar o comentário na linha ~342.

**Testes (em `MigrationCoordinatorTest`, seguindo os fakes existentes):**
- `commitNoDestinoVenceMesmoComPollDoDestinoFalhandoEOrigemEmErro`: 1º poll do destino lança falha
  de transporte, origem responde erro; 2º poll do destino responde `COMMITTED` → resultado
  completado, nenhum `MIGRATE_ABORT` enviado ao destino, placement `ACTIVE(dst)`.
- `erroDaOrigemComDestinoRespondendoNaoCommittedAborta` (comportamento atual preservado).
- `timeoutReconsultaDestinoAntesDeAbortar`: destino só responde `COMMITTED` na reconsulta final.

## Achado 2 (baixa/média) — patches do cutover final com prioridade na banda

`rebalance/MigrationExecutor.java` (~336-342, `sendPatches` após `markMigrating`) e
`rebalance/MigrationBandwidth.java`. Com a série já congelada (clientes recebem `MIGRATING`), cada
patch final espera na mesma fila dos chunks de 256 KiB das outras cópias. Medido: a 1 MiB/s com 7
transferências concorrentes, um patch de 4 KiB esperou até 252 ms por rodada.

**Decisão:** o delta final continua contando no orçamento (a média de bytes/s por origem é preservada,
como a doc promete), mas é **prioritário**: `MigrationBandwidth` ganha
`void acquireUrgent(int bytes)` que não espera — debita imediatamente, empurrando o próximo slot:
`nextNanos = Math.max(nextNanos, now) + custo(bytes)`, e sinaliza `changed`. Os chunks concorrentes
absorvem o atraso. Só os patches enviados **depois** de `markMigrating` (cutover final) usam
`acquireUrgent`; patches do catch-up (série ainda recebendo escrita) continuam em `acquire`.

**Testes:**
- `MigrationBandwidthTest`: `acquireUrgentNaoEsperaComFilaCheia` (limitador saturado por chunks em
  outra thread; `acquireUrgent` retorna em < 20 ms) e `acquireUrgentDebitaOrcamento` (após um
  `acquireUrgent` de N bytes, o próximo `acquire` espera ≈ N/bytesPerSecond, tolerância generosa).
- Teste no executor (ou onde os patches finais são enviados) garantindo que o caminho pós-freeze usa
  o modo urgente — preferir verificar comportamento (tempo de espera do patch final sob chunks
  concorrentes) a verificar chamada de método.

## Achado 3 (baixa) — `VirtualThreadMigrationTest` inócuo em JDK ≥ 24

`src/test/.../rebalance/VirtualThreadMigrationTest.java:35`: a partir do JDK 24 (JEP 491)
`synchronized` não prende a carrier thread, então o teste passa mesmo com a regressão. Adicionar
`Assumptions.assumeTrue(Runtime.version().feature() < 24, "<motivo em PT-BR>")` no início do teste
(ou da classe), com comentário explicando JEP 491 e que o CI roda em JDK 21.

## Versão e docs

- Versão 8.5.0 → **8.5.1** em todos os `pom.xml`, `README.md` e demais lugares onde a PR #172 mudou a
  versão (`grep -rn "8\.5\.0" --include=pom.xml --include=*.md .`, excluindo CHANGELOG histórico).
- `doc/CHANGELOG.md`: entrada 8.5.1 (correções 1 e 2; nota do teste 3).
- `doc/oss/ngrrd-cluster-operacao.md`: ajustar o texto sobre COMMITTED e sobre patches/banda para
  refletir o comportamento corrigido (delta final prioritário, ainda contabilizado).

## Validação

- `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 mvn -pl nishi-utils-ngrrd-cluster -am verify -DexcludeNgrid=true`
  (unit; relatar contagem).
- `JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 mvn -pl nishi-utils-ngrrd-cluster -am verify -Pngrrd-cluster -DexcludeNgrid=true -Dtest=ContinuousIngestionRebalanceClusterTest,CheckpointAfterMigrationClusterTest,RebalanceClusterTest,LeaderFailoverDuringMigrationClusterTest -Dsurefire.failIfNoSpecifiedTests=false`
  (relatar contagem; falha deve ser comparada rodando o mesmo teste em `9080b69` antes de concluir
  que é regressão).
