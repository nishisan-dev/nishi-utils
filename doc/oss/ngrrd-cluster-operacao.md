# ngrrd cluster: exemplos e situações de operação

O cluster redistribui séries com os processos em funcionamento. A entrada de um storage
habilita novos placements e pode iniciar migrações de séries existentes, conforme os limites
de rebalanceamento. Durante a transferência, a série fica temporariamente em `MIGRATING` e
o cliente retenta suas operações: operação online não significa latência constante.

Este guia complementa o [quickstart](ngrrd-cluster-quickstart.md) e a
[referência técnica](ngrrd-cluster.md). Os exemplos de terminal usam Bash, a raiz do
repositório e os três processos locais do quickstart. Eles não geram séries automaticamente;
para observar distribuição, é necessário tráfego de uma aplicação usando `NgrrdClusterClient`.

## 1. Preparar os comandos de administração

Depois de compilar e copiar as dependências conforme o quickstart, defina no terminal de
administração:

```bash
NGRRD_CP='nishi-utils-ngrrd-cluster/target/nishi-utils-ngrrd-cluster-8.3.1.jar:nishi-utils-ngrrd-cluster/target/lib/*'
NGRRD_SEED='127.0.0.1:7101'

ngrrd_admin() {
  java -cp "$NGRRD_CP" \
    dev.nishisan.utils.oss.cluster.admin.NgrrdClusterAdminCli \
    --seed "$NGRRD_SEED" "$@"
}

ngrrd_admin status
ngrrd_admin metrics storage-1
```

Ajuste a versão do JAR ao build instalado. O seed precisa estar acessível, mas não precisa
ser o líder. Cada chamada da CLI abre uma conexão como cliente e a encerra ao terminar.
Use estas consultas pontualmente; para monitoramento contínuo, prefira um cliente persistente
com `clusterStatus()`, `nodeMetrics(nodeId)` e `metrics()`.

Os comandos acima são para o laboratório local. A CLI atual anuncia o endereço padrão
`127.0.0.1` e não possui `--host`. Para administração entre máquinas, use a API
`NgrrdClusterClient` com `NgrrdClusterConfig.host` e uma porta acessíveis aos demais membros,
conforme a topologia; trocar apenas o seed da CLI não configura esse endereço.

## 2. Acrescentar um storage com tráfego em andamento

**Situação:** `storage-1`, `storage-2` e `storage-3` estão ativos; queremos acrescentar
`storage-4`, na porta local 7104, sem parar os produtores.

1. Consulte `status`: confirme líder, nós alcançáveis e a distribuição atual.
2. Prepare um ID, porta e diretórios exclusivos para o novo nó. Mantenha o mesmo
   `seriesObjectPrefix` usado pelos outros storages e pelas definições das séries.
3. Inicie o novo processo usando um membro disponível como seed.
4. Aguarde o nó aparecer como `ACTIVE` e `REACHABLE=true` no status do líder.
5. Acompanhe `SERIES`, `MIGRACOES EM CURSO` e as métricas da aplicação.

Crie o YAML do quarto nó:

```bash
mkdir -p target/ngrrd-demo

cat > target/ngrrd-demo/storage-4.yaml <<'YAML'
node:
  id: storage-4
  host: 127.0.0.1
  port: 7104
  dataDir: target/ngrrd-demo/storage-4/ngrid
  seed: 127.0.0.1:7101

ngrrd:
  volume:
    dir: target/ngrrd-demo/storage-4/volumes
    name: ifaceStats
    shardCount: 2
    segmentBytes: 16777216
    initialShardCapacityBytes: 16777216
  seriesObjectPrefix: series
  defaultDurability: FSYNC
  rebalance:
    enabled: true
    interval: 60s
    minDelta: 50
    tolerance: 0.10
    maxConcurrentMigrations: 2
    maxMovesPerCycle: 50
YAML
```

Em um terminal separado, mantenha o novo processo em primeiro plano:

```bash
java -cp 'nishi-utils-ngrrd-cluster/target/nishi-utils-ngrrd-cluster-8.3.1.jar:nishi-utils-ngrrd-cluster/target/lib/*' \
  dev.nishisan.utils.oss.cluster.node.NgrrdStorageNodeMain \
  --config target/ngrrd-demo/storage-4.yaml
```

No terminal de administração:

```bash
ngrrd_admin status
ngrrd_admin metrics storage-4
```

Os caminhos em `target` são apenas para demonstração e são apagados por `mvn clean`.
Em produção, use caminhos absolutos persistentes e endereços acessíveis entre as máquinas.
Não abra o mesmo volume em dois processos. Para reabrir um volume existente, preserve sua
configuração original, incluindo `shardCount`.

### Quando o novo nó começa a receber séries?

- **Séries novas:** assim que estiver elegível no catálogo, o nó participa do placement.
  A política padrão prioriza a menor carga efetiva (`seriesCount` mais placements pendentes),
  com desempate por ocupação e ID. Nós fora de `ACTIVE`, inalcançáveis ou com capacidade
  conhecida ocupada em 95% ou mais não recebem novos placements.
- **Séries existentes:** o líder agenda um ciclo após mudanças de membership, esperando
  5 segundos sem novo evento para agrupar mudanças. Há também um ciclo periódico, com
  intervalo padrão de 60 segundos após o término do ciclo anterior.

Esses tempos são gatilhos, não um prazo de conclusão. O catálogo precisa conhecer o novo nó;
um ciclo que ocorrer antes do seu relatório pode não incluí-lo. O relatório de status tem
intervalo padrão de 10 segundos. Eleições, ciclos já em execução, rede e disco também
influenciam o progresso. O seed não fixa a liderança, que pode mudar durante a expansão.

## 3. Entender por que o rebalanceamento move ou não move séries

O planejador equilibra a **quantidade de séries ACTIVE no catálogo** entre storages
`ACTIVE` e alcançáveis. Não equilibra diretamente bytes, IOPS ou taxa de escrita.
Enquanto houver séries elegíveis e orçamento no ciclo, move uma série inteira do nó mais
carregado para o menos carregado quando:

```text
maior quantidade - menor quantidade > max(minDelta, tolerance × média)
```

Uma série tem um único dono; a migração não a divide nem cria uma réplica permanente.
O objetivo é entrar na tolerância configurada, não necessariamente deixar todos os nós iguais.

Exemplos numéricos, sem novas séries nem migrações anteriores em andamento:

| Séries por nó antes do ciclo | Configuração | Resultado esperado do planejamento |
|---|---|---|
| `20 / 20 / 0` | Padrão: `minDelta=50`, `tolerance=0.10` | Nenhum movimento: diferença 20, limite 50. O nó vazio continua elegível para séries novas. |
| `50 / 50 / 0` | Padrão | Nenhum movimento: diferença igual ao limite também não dispara migração. |
| `1000 / 1000 / 0` | Padrão, até 50 movimentos | Primeiro ciclo planeja 50 movimentos. Se todos concluírem, fica `975 / 975 / 50`; outros ciclos ainda serão necessários. |
| `20 / 20 / 0` | `minDelta=1`, `tolerance=0.0` | Pode chegar a `14 / 13 / 13`, conforme o desempate por ID, se todas as migrações concluírem. |

### Valores padrão

As opções ficam em `ngrrd.rebalance` no YAML de **cada storage node**:

| Opção | Padrão | Efeito operacional |
|---|---|---|
| `enabled` | `true` | Habilita ciclos periódicos e por membership. |
| `interval` | `60s` | Espera entre ciclos periódicos. |
| `minDelta` | `50` | Componente absoluto do limite, em séries. |
| `tolerance` | `0.10` | Componente relativo do limite, como fração da média. |
| `maxConcurrentMigrations` | `2` | Limita migrações simultâneas conduzidas pelo coordenador. |
| `maxMovesPerCycle` | `50` | Limita movimentos planejados por ciclo. |
| `migrationTimeout` | `10m` | Prazo usado pelo coordenador para aguardar a migração. |
| `chunkBytes` | `262144` | Tamanho dos chunks de transferência: 256 KiB. |
| `maxSeriesBytes` | `67108864` | Tamanho máximo aceito para uma imagem de série em migração: 64 MiB. |

O líder usa sua própria configuração para planejar. Mantenha os parâmetros alinhados entre
os storages, pois qualquer um pode assumir a liderança. Os executores também usam seus
limites locais de transferência. Editar o YAML não muda um processo em execução: a configuração
é carregada no startup. Aplique mudanças com reinícios planejados, um nó por vez, mantendo
a maioria de coordenação; durante o reinício, as séries daquele nó ficam indisponíveis.

Para um laboratório com poucas séries, **substitua** o bloco `rebalance` dentro de `ngrrd`
por este fragmento, preservando `node`, `volume` e as demais opções do arquivo:

```yaml
ngrrd:
  rebalance:
    enabled: true
    interval: 10s
    minDelta: 1
    tolerance: 0.0
    maxConcurrentMigrations: 1
    maxMovesPerCycle: 10
```

Esse ajuste torna movimentações pequenas observáveis; não é uma configuração universal de
produção. Para reduzir o impacto de transferências, limite concorrência e movimentos por
ciclo e acompanhe latência e backlog. O planejador de migração atual não aplica a guarda de
95% usada no placement de séries novas: confira espaço disponível nos destinos antes de
rebalancear ou drenar. Contagens parecidas não garantem ocupação de disco parecida.

## 4. O que a aplicação percebe durante uma migração

O fluxo de uma série é:

1. O catálogo marca `MIGRATING`, com origem e destino.
2. A origem bloqueia novas operações locais da série, faz checkpoint e fecha o handle.
3. Transfere a imagem em chunks; o destino valida o SHA-256 e confirma a cópia.
4. O catálogo passa a `ACTIVE` no destino e a origem recebe a ordem de apagar sua cópia.

O cliente trata `MIGRATING` com espera e retentativa; ao receber `WRONG_OWNER`, atualiza
o roteamento. As escritas pendentes são reenfileiradas. Não é necessário reabrir manualmente
todos os handles a cada mudança de dono.

Durante essa janela:

- `write()` aceita a amostra no buffer do cliente; seu retorno não confirma persistência
  no storage. Use as barreiras `flush()`/`checkpoint()` conforme a necessidade da aplicação.
- Leituras e barreiras podem esperar e falhar se o orçamento de retentativa se esgotar.
  O padrão de `retryTimeout` do cliente é 5 minutos; ele é independente do
  `migrationTimeout` de 10 minutos do storage.
- O buffer padrão comporta 100.000 amostras **por nó de destino**. Quando enche, a política
  `BLOCK` bloqueia o produtor; `FAIL` lança `BUFFER_FULL`. O buffer é em memória.
- Como o buffer e seu backoff são por destino, outras séries que compartilham esse destino
  também podem perceber atraso. Acompanhe a aplicação inteira, além das séries migradas.

Uma queda do cliente perde o buffer ainda não enviado. Antes de encerrar produtores em uma
manutenção, pare a entrada de novas amostras e conclua as barreiras necessárias. O `close()`
tem orçamento total limitado (30 segundos por padrão), não uma espera ilimitada por todos
os storages. Para recuperação após falhas, preserve a capacidade de reprocessar a origem
dos dados conforme a estratégia da aplicação.

## 5. Disparar um ciclo manual ou operar em janelas

Para antecipar a próxima avaliação:

```bash
ngrrd_admin rebalance
ngrrd_admin status
```

A mensagem `rebalanceamento disparado` confirma a chamada, **não a conclusão das migrações**.
O comando respeita `minDelta`, `tolerance` e `maxMovesPerCycle`; não força equilíbrio exato
e não escolhe uma série ou destino específicos. Se um ciclo já estiver em execução, não
inicia outro. Observe o progresso antes de repetir.

Para depender de acionamento administrativo, substitua o bloco correspondente em todos os
storages e aplique-o no startup:

```yaml
ngrrd:
  rebalance:
    enabled: false
    minDelta: 50
    tolerance: 0.10
    maxConcurrentMigrations: 1
    maxMovesPerCycle: 10
```

O fragmento não é um arquivo completo. `enabled: false` desliga somente os gatilhos
automáticos: `rebalance`, `drain` e `activate` ainda podem disparar ciclos. Séries novas
continuam sendo distribuídas. Não há comando administrativo de pausa que cancele migrações
em curso; essa configuração não oferece cancelamento imediato.

Com o automático desligado, um conjunto maior que `maxMovesPerCycle` exige novos acionamentos
após cada ciclo, inclusive durante uma drenagem. Uma troca de líder também pode exigir
redisparo para planejar movimentos restantes; a recuperação de migrações já registradas no
catálogo é um mecanismo separado.

## 6. Retirar um storage para manutenção

**Situação:** remover o `storage-4` acrescentado ao laboratório, deixando os demais ativos.

1. Confirme destinos `ACTIVE`, alcançáveis e com espaço para os dados que sairão do nó.
   Planeje a manutenção preservando a maioria dos membros elegíveis à liderança do NGrid.
2. Mantenha o storage de origem em execução e solicite a drenagem:

   ```bash
   ngrrd_admin drain storage-4
   ngrrd_admin status
   ```

3. O nó passa a `DRAINING`, deixa de receber novos placements e suas séries entram na fila
   de saída. A drenagem tem prioridade no planejamento e não depende de superar `minDelta`
   ou `tolerance`; os limites de concorrência e movimentos por ciclo continuam valendo.
4. Repita a consulta conforme o progresso. Aguarde `DRAINED`, confirme `SERIES=0` após os
   relatórios convergirem e verifique as migrações. Para uma janela sem outras movimentações,
   espere também `MIGRACOES EM CURSO: 0`. O log `NGRRD_NODE_DRAINED nodeId=storage-4` marca
   a promoção feita pelo líder. `drain OK` sozinho não autoriza concluir que o nó está vazio.
5. Só então pare o processo (`Ctrl+C` no laboratório ou parada do serviço no deploy).

Se o automático estiver desligado e o nó continuar `DRAINING` após terminar um ciclo,
dispare `ngrrd_admin rebalance` novamente para avançar. Se não houver destino ou a origem
estiver inacessível, a drenagem não consegue transferir os dados; resolva a condição antes
de parar ou apagar o volume.

`DRAINED` é um estado do serviço ngrrd. Não desliga o processo nem remove por si só o membro
da coordenação do NGrid. Drenar dados e manter a maioria de coordenação são verificações
separadas. Clientes não contam como substitutos de storage nodes no quórum de liderança.

### Reativar um nó drenado

Reinicie com o mesmo ID e diretórios, aguarde conectividade e execute:

```bash
ngrrd_admin activate storage-4
ngrrd_admin status
```

O estado `DRAINING`/`DRAINED` é preservado no catálogo; reiniciar não deve ser usado como
substituto de `activate`. A ativação volta a permitir placements e dispara uma avaliação de
rebalanceamento. As séries não necessariamente voltam aos donos anteriores. Se a ativação
for feita durante uma drenagem, ela também não desfaz transferências já concluídas nem
funciona como cancelamento imediato das migrações em curso.

## 7. Reinício, queda inesperada e mudança de máquina

| Situação | Procedimento e resultado esperado |
|---|---|
| Reinício curto, mantendo dados no nó | Pare e reinicie com o mesmo `node.id`, `node.dataDir` e volume/configuração. As séries desse dono ficam indisponíveis no intervalo. Aguarde o retorno e a redução do backlog antes de reiniciar outro nó. |
| Queda sem drenagem | Restaure o processo, conectividade e volume originais. O cluster não transfere automaticamente séries de um nó caído para um nó vazio: elas não têm réplica. |
| Disco/volume perdido | A recuperação depende de backup ou reprocessamento da fonte. Subir um nó vazio com o mesmo ID não recupera o histórico. |
| Mudar host ou porta mantendo o storage | Pare a instância antiga, preserve os diretórios e a identidade, ajuste endereço e seed/peers e inicie a nova instância. O catálogo referencia o ID; o NGrid divulga o novo endereço. Não execute simultaneamente duas instâncias com o mesmo ID/volume. Atualize seeds de clientes que apontavam ao endereço antigo. |
| Líder cai durante migração | Restaure maioria e aguarde a nova eleição. O novo líder consulta o destino: confirma a troca se a cópia estiver commitada ou tenta reverter a migração. Acompanhe catálogo, erros e backlog; não remova cópias manualmente durante a resolução. |

Com três storage nodes, a queda de um permite manter a maioria de coordenação; com dois,
um único indisponível já impede essa maioria. Isso se refere à coordenação: mesmo havendo
líder, as séries de um storage caído continuam indisponíveis até a recuperação do seu dono.

## 8. Acompanhar e diagnosticar

`status` mostra a visão do líder. A coluna `SERIES` usa o último relatório do volume de
cada nó, enquanto o planejador usa placements do catálogo; durante uma migração, essas
contagens podem divergir temporariamente. `MIGRACOES EM CURSO: 0` isoladamente não comprova
equilíbrio: também pode não haver movimentos elegíveis, haver falhas ou faltar novo ciclo.

| Sintoma | O que conferir / próxima ação |
|---|---|
| Novo nó aparece, mas não recebe séries existentes | Confira estado `ACTIVE`, alcançabilidade, `enabled` no líder e a fórmula do limite. `20 / 20 / 0` com defaults é um resultado esperado. |
| `rebalance` respondeu, mas não mudou a distribuição | Veja se já havia ciclo em andamento, se o plano ficou vazio ou se movimentos falharam. Aguarde os relatórios e consulte os logs antes de redisparar. |
| Drenagem permanece em `DRAINING` | Verifique origem/destinos acessíveis, espaço, erros e limite por ciclo. Com `enabled: false`, faça novos acionamentos após cada ciclo. |
| Série não migra e o log cita `maxSeriesBytes` | Compare o tamanho da imagem com o limite padrão de 64 MiB. Ajuste somente após dimensionar memória e transferência nos participantes; uma imagem maior pode impedir a drenagem completa. |
| Latência ou backlog aumentam na expansão | Confira disco/rede, concorrência de migrações, `bufferedSamples` e retentativas do cliente. Reduza a pressão de produção ou planeje limites menores para os próximos ciclos. |
| `MIGRATING` dura além do esperado | Confira logs de origem, destino e líder, conectividade e prazos do storage e cliente. Não há duração fixa garantida por série. |
| Administração não responde durante uma queda | Verifique maioria e eleição. `status`, `drain`, `activate` e `rebalance` dependem do líder. O protocolo de métricas do nó é local, mas a descoberta/conexão da ferramenta também precisa funcionar. |

Métricas úteis por storage: `MIGRATIONS_IN`, `MIGRATIONS_OUT`, `SAMPLES_FAILED`, `SERIES`,
`USED_BYTES`, `CAPACITY_BYTES` e `RECONCILE_MISSING`, expostas por `ngrrd_admin metrics <nodeId>`.
Capacidade omitida/desconhecida não significa disco livre: `FILL%` não substitui o monitoramento
do filesystem. Pela API, `NodeMetricsSnapshot` também inclui histogramas de latência e erros
por status; a CLI imprime apenas um subconjunto.

Na aplicação, acompanhe `client.metrics()`: `bufferedSamples` por destino, `retriesByStatus`,
`samplesSent` e `samplesFailed`. As métricas de um novo cliente administrativo não representam
os buffers dos produtores já em execução.

Nos logs, procure `NGRRD_REBALANCE` (resultado agregado dos ciclos agendados),
`NGRRD_REBALANCE_MOVE` (resultado dos movimentos disparados manualmente; sucesso em nível
`FINE`, falhas/skip em `WARNING`), `NGRRD_NODE_DRAINED` e `NGRRD_NODE_STATUS`.

## Referências de implementação

- [Configuração e defaults](../../nishi-utils-ngrrd-cluster/src/main/java/dev/nishisan/utils/oss/cluster/node/StorageNodeConfig.java).
- [Placement de séries novas](../../nishi-utils-ngrrd-cluster/src/main/java/dev/nishisan/utils/oss/cluster/placement/LeastLoadedPlacementPolicy.java).
- [Gatilhos e ciclos](../../nishi-utils-ngrrd-cluster/src/main/java/dev/nishisan/utils/oss/cluster/rebalance/Rebalancer.java) e [planejamento dos movimentos](../../nishi-utils-ngrrd-cluster/src/main/java/dev/nishisan/utils/oss/cluster/rebalance/RebalancePlanner.java).
- [Comandos administrativos](../../nishi-utils-ngrrd-cluster/src/main/java/dev/nishisan/utils/oss/cluster/admin/NgrrdClusterAdminCli.java) e [API do cliente](../../nishi-utils-ngrrd-cluster/src/main/java/dev/nishisan/utils/oss/cluster/api/NgrrdClusterClient.java).
