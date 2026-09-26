# ngrrd cluster: como executar os storage nodes

Este guia inicia um storage node como processo Java e depois amplia o ambiente para três
processos locais. Os comandos são para Bash em Linux/macOS, executados a partir da raiz do
repositório. Use JDK 21 ou superior e Maven. As portas TCP 7101, 7102 e 7103 precisam estar livres.

O YAML é lido pela classe `NgrrdStorageNodeMain`. O módulo produz um JAR comum: é necessário
disponibilizar suas dependências e indicar a classe principal com `java -cp`.

## 1. Compilar e preparar as dependências

Na raiz do repositório, na branch que contém o módulo:

```bash
java -version
mvn -version

mvn -pl nishi-utils-ngrrd-cluster -am install \
  -DskipTests -Dmaven.javadoc.skip=true -Djacoco.skip=true

mvn -pl nishi-utils-ngrrd-cluster dependency:copy-dependencies \
  -DincludeScope=runtime -DoutputDirectory=target/lib
```

O primeiro comando Maven compila e instala localmente o módulo e os módulos dos quais ele
depende. O segundo copia as dependências para `nishi-utils-ngrrd-cluster/target/lib`.
Os testes são omitidos neste procedimento de preparação; isso não substitui os gates de release.

Os comandos abaixo usam a versão `8.8.0` do POM desta branch. Se a versão mudar, ajuste o nome
do JAR. A pasta `lib` deve conter as dependências do mesmo build; ao trocar versões, prepare
o pacote em um diretório de build limpo para não misturar JARs antigos e novos.

## 2. Criar a configuração do primeiro nó

Copie e execute este bloco, ainda na raiz do repositório:

```bash
mkdir -p target/ngrrd-demo

cat > target/ngrrd-demo/storage-1.yaml <<'YAML'
node:
  id: storage-1
  host: 127.0.0.1
  port: 7101
  dataDir: target/ngrrd-demo/storage-1/ngrid

ngrrd:
  volume:
    dir: target/ngrrd-demo/storage-1/volumes
    name: ifaceStats
    shardCount: 2
    segmentBytes: 16777216
    initialShardCapacityBytes: 16777216
  seriesObjectPrefix: series
  defaultDurability: FSYNC
YAML
```

Este é um ambiente de demonstração com dois shards de 16 MiB iniciais cada. O nó cria os
diretórios necessários. `node.dataDir` guarda o estado do NGrid; os dados das séries ficam
em **`ngrrd.volume.dir/ngrrd.volume.name`**, neste exemplo
`target/ngrrd-demo/storage-1/volumes/ifaceStats`.

Os caminhos relativos são resolvidos a partir do diretório de execução. Em produção, use
caminhos absolutos fora de `target`, pois `mvn clean` apaga essa pasta. Não use este YAML
reduzido para abrir um volume existente: preserve sua configuração, incluindo `shardCount`.

## 3. Iniciar o processo

No terminal 1, na raiz do repositório:

```bash
java -cp 'nishi-utils-ngrrd-cluster/target/nishi-utils-ngrrd-cluster-8.8.0.jar:nishi-utils-ngrrd-cluster/target/lib/*' \
  dev.nishisan.utils.oss.cluster.node.NgrrdStorageNodeMain \
  --config target/ngrrd-demo/storage-1.yaml
```

O processo permanece em primeiro plano. Mantenha esse terminal aberto. Procure no log:

```text
NGRRD_STORAGE_NODE_STARTED nodeId=storage-1 port=7101
```

Esse marcador confirma que a inicialização local terminou. A eleição e a publicação do
status podem levar mais alguns segundos; confirme a descoberta pela CLI no passo seguinte.
Não há um servidor HTTP ou uma página para abrir no navegador: a porta 7101 atende o
protocolo TCP do NGrid.

## 4. Consultar o status

Em outro terminal, também na raiz do repositório:

```bash
java -cp 'nishi-utils-ngrrd-cluster/target/nishi-utils-ngrrd-cluster-8.8.0.jar:nishi-utils-ngrrd-cluster/target/lib/*' \
  dev.nishisan.utils.oss.cluster.admin.NgrrdClusterAdminCli \
  --seed 127.0.0.1:7101 status
```

A saída deve informar `LIDER: storage-1` e uma linha de `storage-1` com estado `ACTIVE` e
`REACHABLE` igual a `true`. Em um volume novo, `SERIES` será zero: iniciar o servidor não
cria séries nem gera amostras. A CLI executa a consulta e encerra.

Para consultar as métricas locais:

```bash
java -cp 'nishi-utils-ngrrd-cluster/target/nishi-utils-ngrrd-cluster-8.8.0.jar:nishi-utils-ngrrd-cluster/target/lib/*' \
  dev.nishisan.utils.oss.cluster.admin.NgrrdClusterAdminCli \
  --seed 127.0.0.1:7101 metrics storage-1
```

## 5. Acrescentar mais dois storage nodes

Mantenha o primeiro processo em execução. Em outro terminal, gere dois arquivos novos:

```bash
for n in 2 3; do
  cat > "target/ngrrd-demo/storage-${n}.yaml" <<YAML
node:
  id: storage-${n}
  host: 127.0.0.1
  port: $((7100 + n))
  dataDir: target/ngrrd-demo/storage-${n}/ngrid
  seed: 127.0.0.1:7101

ngrrd:
  volume:
    dir: target/ngrrd-demo/storage-${n}/volumes
    name: ifaceStats
    shardCount: 2
    segmentBytes: 16777216
    initialShardCapacityBytes: 16777216
  seriesObjectPrefix: series
  defaultDurability: FSYNC
YAML
done
```

No terminal 2, execute:

```bash
java -cp 'nishi-utils-ngrrd-cluster/target/nishi-utils-ngrrd-cluster-8.8.0.jar:nishi-utils-ngrrd-cluster/target/lib/*' \
  dev.nishisan.utils.oss.cluster.node.NgrrdStorageNodeMain \
  --config target/ngrrd-demo/storage-2.yaml
```

No terminal 3, execute:

```bash
java -cp 'nishi-utils-ngrrd-cluster/target/nishi-utils-ngrrd-cluster-8.8.0.jar:nishi-utils-ngrrd-cluster/target/lib/*' \
  dev.nishisan.utils.oss.cluster.node.NgrrdStorageNodeMain \
  --config target/ngrrd-demo/storage-3.yaml
```

Repita a consulta `status` em um quarto terminal após a descoberta e os relatórios de status
(intervalo padrão de 10 segundos). Devem aparecer os três IDs, todos `ACTIVE` e alcançáveis,
e um líder eleito. O líder pode mudar durante a entrada dos nós.

Cada nó precisa de ID, porta local e diretórios próprios. Nunca aponte dois processos para
o mesmo volume. `seed` é o contato inicial para descoberta; não fixa quem será o líder.
Para máquinas distintas, substitua `127.0.0.1` por endereços acessíveis entre elas e configure
os peers/seed conforme a [referência de configuração](ngrrd-cluster.md#61-configuração-yaml-storagenodeconfigfromyaml).
Os exemplos de CLI deste guia foram validados no ambiente local; a CLI atual usa um cliente
com endereço padrão `127.0.0.1` e não expõe uma opção `--host`.

Três nós permitem manter a maioria de coordenação após a queda de um. As séries continuam
com dono único, sem réplica: as séries do nó indisponível aguardam seu retorno.

## 6. Encerrar e reiniciar

Para encerrar o ambiente de demonstração, pressione `Ctrl+C` em cada terminal dos storage
nodes. O shutdown hook fecha o nó e o volume; o log informa `NGRRD_STORAGE_NODE_STOPPING`.
Executar novamente o mesmo comando e YAML reabre os dados persistidos.

Em um cluster em uso, remover um nó permanentemente exige drenagem prévia e confirmação
de `DRAINED`, conforme o [procedimento de manutenção](ngrrd-cluster-operacao.md#6-retirar-um-storage-para-manutenção).

Para continuar este laboratório, o [guia de operação](ngrrd-cluster-operacao.md) mostra como
adicionar um quarto storage, interpretar a distribuição das séries, ajustar o rebalanceamento
para poucas séries e retirar o novo nó com drenagem. Também cobre o impacto nos produtores,
reinícios e diagnóstico de falhas.

## Problemas comuns

| Sintoma | O que conferir |
|---|---|
| Classe principal não encontrada | Execute da raiz do repositório e confira o nome/versão do JAR. |
| `NoClassDefFoundError` | Execute a cópia de dependências e mantenha `target/lib/*` no classpath. |
| `no main manifest attribute` | Use o comando `java -cp` com a classe principal, conforme o passo 3. |
| Porta em uso | Escolha outra porta e atualize o seed dos demais nós e da CLI. |
| YAML ou diretório não encontrado | Confira o diretório de execução e o argumento `--config`. |
| Nó ausente no status | Confira seu log, conectividade e seed; aguarde o próximo relatório de status. |
| Consulta sem resposta durante eleição | Confira se há maioria dos storage nodes disponível e aguarde a eleição. |

O próximo passo para uma aplicação é abrir as séries pelo `NgrrdClusterClient`, como na
[seção 5 do guia técnico](ngrrd-cluster.md#5-cliente-transparente). A adoção de dados existentes
é um procedimento separado, descrito na
[seção 14](ngrrd-cluster.md#14-migração-de-um-ngrrd-single-node-existente).
