# Purga condicional e recuperação de quarentena — 8.9.0

## Deploy e habilitação

1. Atualize todos os storages para 8.9.0; mantenha a purga desligada durante o deploy misto.
2. Atualize os clientes/coordenadores que escrevem para 8.9.0, para transportar gerações.
3. Confirme `series.delete` em todos os storages registrados, inclusive nós drenados que
   conservem cópias. Nós antigos recusam com `UNSUPPORTED_BY_NODE`; um nó indisponível impede
   confirmar a remoção de todas as cópias. Restaure-o ou conclua sua retirada operacional.
4. Faça backup do catálogo e dos volumes, incluindo `series-lifecycle.wal`. Habilite a purga
   somente depois da checagem de capacidades e da atualização dos escritores.

As APIs existentes continuam disponíveis durante o deploy misto. Exclusão, quarentena e
proteção de gerações exigem os participantes atualizados; não habilite purga com escritores antigos.

## Uso pela aplicação

```java
var cutoff = Instant.now().minus(30, ChronoUnit.DAYS).toEpochMilli();
var result = client.deleteSeries(seriesKey, new DeletePrecondition(cutoff));
var results = client.deleteSeriesBatch(Map.of(seriesKey, new DeletePrecondition(cutoff)));
```

O corte usa instante de recebimento, com limite durável arredondado para cima. Igualdade
recusa. Para séries antigas sem marca, o timestamp persistido da amostra é a estimativa inicial;
a ingestão nova atualiza a marca pelo relógio de recebimento mesmo com amostra retroativa.
O intervalo padrão é uma hora; configure `-Dngrrd.series.receiptIntervalMillis=3600000`
no storage para alterá-lo, sempre com valor positivo. As marcas avançadas de um WRITE_BATCH
são persistidas juntas antes da admissão das amostras, com um fsync para o lote.
`REFUSED_RECENT_WRITE` e `REFUSED_MIGRATING` preservam a série. `ERROR` não confirma ausência;
registre a causa e repita a operação. Uma exclusão já confirmada continua na recuperação.
A criação de uma chave sem placement consulta todos os storages registrados para excluir
a possibilidade de dados em quarentena; se algum estiver indisponível, o OPEN dessa chave
falha até que seja possível verificar seu volume. Handles de séries já posicionadas continuam operando.
A partir da 8.10.0, essa falha chega rápido, com `ErrorCode.PLACEMENT_UNAVAILABLE` e os nós em
`NgrrdClusterException.unavailableNodeIds()`, sem retry interno. Trate-a como transitória por série,
com backoff próprio, sem discriminar pela mensagem (ver seção 6.6 de [ngrrd-cluster.md](ngrrd-cluster.md)).

`SERIES_DELETED` exige remover o handle antigo do cache da aplicação e reabrir com criação.
O cliente também invalida seu cache ao receber esse status. Não reutilize buffers ou barreiras
antigas para confirmar a nova geração. `QUARANTINED` exige alerta operacional e recuperação;
não tente recriar a série em loop. A integração do purger e da tela Sistema pertence ao ngrrd-server.

## Operação manual

### Tamanho dos lotes no CTP

Cada página de `deleteSeriesBatch` usa um pedido ao líder, que processa as séries
sequencialmente. O `requestTimeout` limita a espera pela resposta de cada pedido;
`retryTimeout` limita as retentativas. Um `TIMEOUT` não cancela a exclusão em andamento
nem confirma que nenhuma série foi removida. Confira os resultados por série e o
catálogo antes de registrar a conclusão da varredura.

Na ocorrência relatada pelo tems, a página de 500 séries terminou no servidor após
o cliente esgotar a espera. A estimativa de 0,2–0,45 s por série ainda não foi medida
por etapa: 500 séries não caberiam nos 20 s atuais. Use inicialmente páginas de
20–25 séries, com apenas uma chamada em andamento, e acompanhe a duração completa.
Esse tamanho é um ponto de partida, sem garantia em picos. Mantenha o timeout global;
a integração ainda precisa tratar o timeout de abertura sem derrubar o coordenador.
A purga pode continuar com páginas menores, observadas as condições de habilitação
acima e a correção do cache da aplicação.

O tems também relatou uma varredura de aproximadamente 884 mil handles sob o lock
do cache, bloqueando a ingestão por cerca de 70 s. Essa correção pertence ao tems.
Meça separadamente o tempo da varredura, da chamada de exclusão e do lote do poll
Kafka; os aproximadamente 0,9 s observados correspondem ao processamento completo
desse lote, e não demonstram uma espera fixa da biblioteca.

Para escrita, avaliar `batchMaxSamples` de 1000 até 4000 em passos, acompanhando
latência completa, vazão e ocupação das filas. O limite de frame do transporte é
64 MiB; tamanho de frame disponível não garante ganho de vazão nem latência aceitável.
Não misture essa avaliação com a correção do lock do cache ou atribua seu tempo ao fsync.

Use o launcher `ngrrd-admin` do seu deploy, ou `NgrrdClusterAdminCli` com o classpath completo
descrito no guia de execução. Todos os comandos abaixo exigem `--seed host:port`.

```sh
ngrrd-admin --seed storage-1:7101 series-delete 'device:123/iface:eth0' --last-write-before 1791072000000
ngrrd-admin --seed storage-1:7101 metrics storage-1
ngrrd-admin --seed storage-1:7101 reconcile storage-1
ngrrd-admin --seed storage-1:7101 reconcile storage-1 --adopt --series 'device:123/iface:eth0'
ngrrd-admin --seed storage-1:7101 reconcile storage-1 --purge-orphans --series 'device:123/iface:eth0'
```

Sem `--last-write-before`, a CLI usa o instante de sua invocação; o arredondamento pode recusar
séries que receberam dados na hora corrente. Sem `--series`, a resolução percorre toda a
quarentena do nó. `--adopt` e `--purge-orphans` são mutuamente exclusivos. A purga administrativa
é irreversível e atua sem migração em objetos sem placement reconfirmado ou em cópias locais
cujo outro dono foi confirmado fortemente e possui o objeto físico. Nunca apaga a cópia do
dono ativo por esse caminho.
Definições e geometrias compartilhadas não são removidas.

## Catálogo perdido ou restaurado

1. Suspenda a ingestão e a purga. Restaure a maioria, confirme o líder estável e a sincronização
   das réplicas do catálogo. Se houver backup correto do catálogo, restaure-o primeiro.
2. Consulte `ngrrd-admin reconcile <nó>` e `metrics <nó>` **em cada storage**. Verifique os
   objetos em quarentena, o backup e a intenção de recuperação. `QUARANTINED` preserva os dados.
3. Execute `ngrrd-admin reconcile <nó> --adopt` **para todos os nós de storage**, usando o seed.
   A adoção revalida o estado e mantém o objeto físico. Se o catálogo já voltou a apontar para
   o mesmo dono, a adoção explícita libera sua quarentena preservando a marca conhecida.
4. Examine todos os resultados por série. Repita falhas após resolver indisponibilidade,
   conflito de placement ou migração. Cópias duplicadas que já têm outro dono ficam preservadas;
   confira sua procedência e os dados históricos no dono antes de usar `--purge-orphans`
   para eliminar explicitamente as cópias restantes em quarentena.
5. Confira `catalog.lookup`, os donos e a existência física das séries. As métricas de
   quarentena devem zerar nas cópias adotadas. Valide leitura de dados históricos.
6. Retome a ingestão e remova o alarme somente depois dessas verificações. Reative a purga
   por último. Nunca trate catálogo vazio como autorização para apagar volumes.

## Recuperação de exclusão interrompida

Restaure os participantes originais da operação e aguarde o reconciliador de exclusão.
Uma preparação sem commit pode ser cancelada; um commit durável é concluído, inclusive
após restauração de catálogo antigo. Repita `deleteSeries` para obter o resultado final.
Não remova manualmente o journal nem troque os volumes durante a recuperação. Backups do
volume e do journal devem ser consistentes; restaurar somente dados físicos fora da linhagem
não é um procedimento de rollback de exclusão.

## Verificação

```sh
mvn test
mvn test -pl nishi-utils-ngrrd-cluster -am -Pngrrd-cluster -Dtest=SeriesDeleteClusterTest -Dsurefire.failIfNoSpecifiedTests=false
mvn verify -Pvalidate-javadoc -DskipTests
```

`SeriesLifecycleTest` cobre legado, intervalos, fsync agrupado, corrupção, falhas de persistência,
preparação cancelada, exclusão parcialmente aplicada e recuperação com catálogo restaurado.
`SeriesDeleteClusterTest` cobre três storages, todas as cópias, lote misto, espaço liberado,
restart do dono, handles antigos e adoção de catálogo perdido sem alterar os bytes.

O benchmark opcional mede o handler local com writer e checkpoint, em uma JVM, usando
50 séries e lotes de mil amostras. Ele não inclui TCP nem representa a carga real do CTP.

```sh
mvn test -pl nishi-utils-ngrrd-cluster -am -Dtest=SeriesLifecycleTest -Dsurefire.failIfNoSpecifiedTests=false -Dngrrd.delete.benchmark=true
```

Uma execução local com 55 mil amostras por fase produziu:

| Fase | Amostras/s | Fsyncs da marca |
| --- | ---: | ---: |
| Marcas desabilitadas, demais proteções mantidas | 461.657 | 0 |
| Marca habilitada, hora já coberta | 330.641 | 0 |
| Marca habilitada, passagem de hora | 447.343 | 1 |

Esses tempos variam com aquecimento, disco e agendamento; não estimam uma penalidade estável
de vazão. A propriedade verificada é zero fsync adicional na hora já coberta e um fsync
agrupado quando as marcas das séries do lote avançam. Valide a vazão com o tráfego do CTP
antes de habilitar a purga em produção.
