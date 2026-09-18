# Revisão de `feature/ngrrd-cluster` — 2026-09-18

## Escopo e conclusão

Base de comparação: `main`, merge-base `1ce8b44eb4842a020653a94bccbcfa13e15d7598`.
HEAD revisado: `bf3f2005ac8de87508de9ab6a4bc164d959f79ef`.
O diff commitado contém 180 arquivos, 30.500 inserções e 132 remoções.
Também foram consideradas as alterações locais já presentes, especialmente em transporte,
coordenação e `MigrationCoordinator`. Os quatro achados abaixo estão em código já commitado
na branch e continuam presentes na árvore de trabalho revisada.

**Recomendação: corrigir os três P1 antes do merge.** Foram confirmados quatro defeitos por
reproduções executáveis isoladas. Não foram alterados código de produção, testes ou configuração.
Este relatório é a única adição da revisão ao repositório.

O foco foi o fluxo cliente → buffer → storage, migração/abort/commit, recuperação e drenagem,
com leitura complementar das mudanças de eleição, replicação, transporte e catálogo.
Não foi feita uma auditoria exaustiva de todas as combinações de falha de rede.

## 1. [P1] Rejeitar commits de migrações abortadas ou substituídas

**Local:** `nishi-utils-ngrrd-cluster/src/main/java/dev/nishisan/utils/oss/cluster/rebalance/MigrationExecutor.java`,
`handleCommit`, linhas 465–466; relacionado a `handleChunk`, linhas 400–401, e `handleAbort`.

O destino valida tamanho e SHA-256, mas não confirma que o `migrationId` ainda corresponde à
migração vigente antes de executar `registry.discard(...)` e `atomicReplace(...)`. Além disso,
um ABORT recebido antes do primeiro chunk não deixa registro, e `handleChunk` pode recriar o
staging. A transferência da origem também não para ao observar ABORTED.

Assim, mensagens atrasadas da migração M1 podem chegar depois de uma migração M2 concluída
para o mesmo destino. O commit de M1 sobrescreve a imagem atual com uma versão antiga, mesmo
com o catálogo já indicando `ACTIVE(destino)`. O comentário que classifica o ABORT antecipado
como produtor apenas de uma cópia órfã não cobre essa sequência: após M2, a cópia sobrescrita
é a ativa.

**Reprodução confirmada**, com volumes reais e os executores do fixture de `MigrationExecutorTest`:

1. Gravar e guardar a imagem I1 de uma série.
2. Entregar `MIGRATE_ABORT(M1)` ao destino antes de seu primeiro chunk.
3. Gravar mais amostras na origem, gerando uma imagem I2 diferente.
4. Executar M2 entre os executores até COMMITTED e publicar `ACTIVE(destino)` no catálogo.
5. Entregar os chunks atrasados de I1 e `MIGRATE_COMMIT(M1)` com seu hash correto.
6. O destino responde OK/COMMITTED e passa a conter I1 em vez de I2.

```text
STALE_MIGRATION chunk=MigrateResponse[status=OK, ...]
commit=MigrateResponse[status=COMMITTED, ..., bytes=346104]
overwroteNewerImage=true
```

**Direção de correção:** vincular a instalação da imagem à migração vigente, com exclusão
por série e proteção contra mensagens antigas; registrar aborts antecipados e interromper
transferências canceladas. Acrescentar teste que intercale M1 abortada e M2 concluída e
verifique que I2 permanece intacta após qualquer mensagem tardia de M1.

## 2. [P1] Aguardar a escrita redirecionada antes de concluir checkpoint/flush

**Local:** `nishi-utils-ngrrd-cluster/src/main/java/dev/nishisan/utils/oss/cluster/client/WriteDispatcher.java`,
linhas 260–267; chamada pública em `RemoteSeriesHandle.java`, linhas 198–201.

`flushNodeSync` captura o buffer do dono inicial e só espera esse buffer esvaziar. Se a resposta
é WRONG_OWNER, o lote passa para outro `NodeBuffer`; o buffer original fica vazio e sem operação
em voo, satisfazendo a condição de término. `RemoteSeriesHandle.checkpoint()` então envia o
CHECKPOINT ao novo dono, sem aguardar a escrita que acabou de ser transferida para ele.
`flush()` usa o mesmo fluxo.

Isso permite confirmar o checkpoint antes das escritas anteriores do próprio handle. Uma leitura
logo depois pode não vê-las; encerrar o processo após essa confirmação pode perder amostras que
a aplicação julgava persistidas.

**Reprodução confirmada:** enfileirar uma escrita em A, iniciar `checkpoint()` e fazer A responder
WRONG_OWNER(B). A resposta da escrita em B é retardada; o CHECKPOINT em B responde imediatamente.
A chamada pública retorna com zero amostras confirmadas. Foi usada a implementação real de
`RemoteSeriesHandle` e `WriteDispatcher`, com RPC controlado para ordenar as respostas.

```text
FLUSH_RETURNED samplesSent=0 buffered={A=0, B=1}
CHECKPOINT_RETURNED commands=[ngrrd.open@A, ngrrd.checkpoint@B] samplesSent=0
```

**Direção de correção:** acompanhar a conclusão das escritas anteriores à barreira por série
ou por sequência, independentemente do nó ao qual forem redirecionadas. Um simples teste de
fila vazia no dono inicial não representa essa garantia.

## 3. [P1] Serializar a troca de dono com o backlog e novas escritas da série

**Local:** `nishi-utils-ngrrd-cluster/src/main/java/dev/nishisan/utils/oss/cluster/client/WriteDispatcher.java`,
linha 524 e linhas 530–539.

No tratamento de WRONG_OWNER, `ownerChanged.accept(...)` publica B como novo dono antes de
extrair e reenfileirar em B o lote antigo e seu backlog. Uma thread produtora pode observar
o novo dono nesse intervalo e enviar a amostra mais recente para B. Como B tem outro buffer
e outra execução de flush, essa amostra pode ser transmitida antes do backlog antigo.
O `addFirst` posterior não recupera a ordem de uma escrita que já saiu do buffer.

O problema independe de a aplicação escrever em múltiplas threads: a thread produtora já
concorre com a thread interna que processa WRONG_OWNER. Séries cuja política descarta amostras
atrasadas podem perder as amostras antigas após essa inversão.

**Reprodução confirmada:** configurar lotes de tamanho 1; A responde WRONG_OWNER(B) para t1.
No ponto em que o callback publica B, permitir que o produtor enfileire t2 em B e aguardar
sua transmissão antes de liberar o restante do reroteamento de t1. O destino recebe:

```text
DELIVERY_ORDER=[2, 1]
```

O probe usa o callback como ponto de sincronização para reproduzir deterministicamente
uma intercalação permitida entre as threads; não depende de sleeps para provocar essa ordem.

**Direção de correção:** estabelecer uma barreira por série que cubra mudança de destino,
transferência do backlog e novas admissões. Mover apenas a atribuição de `owner` para depois
do reenfileiramento não resolve, sozinho, escritores que já capturaram o dono antigo.

## 4. [P2] Resolver o objeto de migração sem depender de OPEN neste processo

**Local:** `nishi-utils-ngrrd-cluster/src/main/java/dev/nishisan/utils/oss/cluster/rebalance/MigrationExecutor.java`,
`resolveStorageKey`, linhas 220–224.

O início de toda migração depende de `registry.cachedYaml(seriesKey)`. Esse cache é somente
em memória e é populado ao abrir a série. Após reiniciar o nó, adotar um volume existente ou
receber uma série por migração, é possível ter o objeto persistido e seu placement válido sem
qualquer OPEN local. Nesses casos, MIGRATE_START falha antes de ler a imagem.

Rebalanceamento e drenagem ficam dependentes de um cliente reabrir cada série afetada. Para
séries inativas isso pode nunca acontecer, impedindo o nó de chegar a zero séries e DRAINED.
O reconciliador atual atualiza placement, mas não popula esse cache de definições.

**Reprodução confirmada:** gravar uma série e fechar seu registry; criar um registry novo
sobre o mesmo volume, sem OPEN, e solicitar MIGRATE_START por um novo executor.

```text
COLD_MIGRATION exists=true response=MigrateResponse[status=ERROR,
message=falha ao resolver a chave de storage de cold:
IllegalStateException: nenhuma definição em cache para cold, bytes=0]
```

**Direção de correção:** resolver a chave pelo prefixo do storage node e pela chave da série,
ou persistir/recuperar os metadados necessários independentemente da abertura por cliente.
Cobrir drenagem após reinício e segunda migração de uma série ainda não aberta no novo dono.

## Validação e limites

- Ambiente: OpenJDK 25.0.4, compilação configurada pelo projeto para Java 21.
- Suíte do módulo, incluindo os cenários de cluster: **344 testes, zero falhas, zero erros,
  zero skips; BUILD SUCCESS**, em 5 min 32 s. `LeaderFailoverDuringMigrationClusterTest`
  passou seus dois testes nesta execução. Isso não elimina o histórico de intermitência
  documentado pelo projeto.
- Core: seleção das 20 classes de teste alteradas no diff commitado, mais
  `ProtocolCompatibilityIntegrationTest` e `UndeliverableRequestIntegrationTest`, que já estavam
  na árvore de trabalho. **52 testes, zero falhas, zero erros, zero skips; BUILD SUCCESS**,
  em 1 min 17 s, usando `mvn -pl nishi-utils-core test -Dtest=<nomes das 22 classes>`.
  Inclui eleição/quórum, propagação de roles, builders, replicação de records e transporte.
- Total das duas execuções: **396 testes aprovados**. Os probes são verificações adicionais
  e não estão incluídos nesse total.
- Cinco probes externos exercitaram os quatro achados: flush, checkpoint público, ordem de
  entrega, registry sem cache e commit antigo. Todos confirmaram o comportamento descrito.
  Migrações usaram `BlobVolume` real; ordenação de RPC foi controlada, sem reproduzir falhas
  físicas de rede. Nenhum teste foi acrescentado ao repositório.
- Não foram executados Docker/Testcontainers, soak, publicação ou o `mvn test` completo
  de todos os módulos.

Comando da suíte do módulo, incluindo os testes `*ClusterTest` selecionados explicitamente:

```sh
mvn -pl nishi-utils-ngrrd-cluster -am test \
  -Dtest='dev.nishisan.utils.oss.cluster.**.*Test' \
  -Dsurefire.failIfNoSpecifiedTests=false
```

Artefatos temporários desta sessão: `/tmp/NgrrdReviewProbe.java`,
`/tmp/ngrrd-review-confirmed.log`, `/tmp/ngrrd-review-unit.log` e `/tmp/ngrrd-review-core.log`.
Os cenários, resultados observados e condições de correção ficam documentados acima para
que a revisão continue útil mesmo após a limpeza desses arquivos temporários.
