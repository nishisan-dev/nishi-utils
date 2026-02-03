# ngrid-test

Teste simples (server/client) para validar um cluster NGrid local.

## Build

```bash
mvn -f ngrid-test/pom.xml -DskipTests package
```

## Rodar (2 terminais)

Terminal 1 (server):

```bash
java -jar ngrid-test/target/ngrid-test-1.0-SNAPSHOT-jar-with-dependencies.jar server
```

Terminal 2 (client):

```bash
java -jar ngrid-test/target/ngrid-test-1.0-SNAPSHOT-jar-with-dependencies.jar client
```

Modo autodiscover (client-auto):

```bash
java -jar ngrid-test/target/ngrid-test-1.0-SNAPSHOT-jar-with-dependencies.jar client-auto
```

## Scenario local (seed + 2 clients + kill/restart)

Executa um cenario automatico com 1 seed e 2 clients na mesma maquina,
fila `global-events` com replication factor 2 e kill/restart do seed.

```bash
java -jar ngrid-test/target/ngrid-test-1.0-SNAPSHOT-jar-with-dependencies.jar scenario-1
```

## Lab (3 hosts com IPs fixos)

Topologia:

- Seed: `192.168.5.89`
- Client 1: `192.168.5.90`
- Client 2: `192.168.5.91`

### Seed (192.168.5.89)

```bash
export NG_NODE_HOST=192.168.5.89
export NG_NODE_PORT=9000
export NG_BASE_DIR=/tmp/ngrid-seed

java -jar ngrid-test/target/ngrid-test-1.0-SNAPSHOT-jar-with-dependencies.jar server
```

### Client 1 (192.168.5.90)

```bash
export NODE_ID=client-1
export HOST=192.168.5.90
export PORT=9000
export BASE_DIR=/tmp/ngrid-client-1
export SEED_HOST=192.168.5.89:9000

java -jar ngrid-test/target/ngrid-test-1.0-SNAPSHOT-jar-with-dependencies.jar client
```

Client 1 (autodiscover):

```bash
export NODE_ID=client-1
export HOST=192.168.5.90
export PORT=9000
export BASE_DIR=/tmp/ngrid-client-1
export SEED_HOST=192.168.5.89:9000

java -jar ngrid-test/target/ngrid-test-1.0-SNAPSHOT-jar-with-dependencies.jar client-auto
```

### Client 2 (192.168.5.91)

```bash
export NODE_ID=client-2
export HOST=192.168.5.91
export PORT=9000
export BASE_DIR=/tmp/ngrid-client-2
export SEED_HOST=192.168.5.89:9000

java -jar ngrid-test/target/ngrid-test-1.0-SNAPSHOT-jar-with-dependencies.jar client
```

Client 2 (autodiscover):

```bash
export NODE_ID=client-2
export HOST=192.168.5.91
export PORT=9000
export BASE_DIR=/tmp/ngrid-client-2
export SEED_HOST=192.168.5.89:9000

java -jar ngrid-test/target/ngrid-test-1.0-SNAPSHOT-jar-with-dependencies.jar client-auto
```

> Dica: libere a porta 9000 nos tres hosts e garanta que os IPs se enxerguem na rede.

## Variaveis de ambiente (opcional)

Server:

- `NG_NODE_ID` (default: `seed-1`)
- `NG_NODE_HOST` (default: `127.0.0.1`)
- `NG_NODE_PORT` (default: `9000`)
- `NG_BASE_DIR` (default: `/tmp/ngrid`)

Client:

- `HOST` (default: `127.0.0.1`)
- `PORT` (default: `9213`)
- `BASE_DIR` (default: `/tmp/client-1`)
- `SEED_HOST` (default: `127.0.0.1:9000`)
- `NODE_ID` (default: `client-1`, persiste offset entre restarts)

## Dependencia do nishi-utils

A versao do `nishi-utils` e controlada via propriedade `nishi.utils.version` no `ngrid-test/pom.xml`.
Altere esse valor quando publicar uma nova versao.

## Modo Autodiscover (opcional)

Se quiser testar o bootstrap automatico via seed, use o arquivo `config/client-autodiscover.yml`.
Ele baixa `cluster/queues/maps` do seed e atualiza o proprio arquivo.

Para usar, substitua o arquivo usado na execucao (copie ou passe o caminho ao jar).
