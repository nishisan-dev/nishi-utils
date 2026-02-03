# Guia de Configuracao (YAML)

O NGrid suporta carregamento de configuracoes a partir de arquivos YAML, facilitando o gerenciamento de ambientes (Dev, Staging, Prod) e deployment em containers (Docker/Kubernetes).

A configuracao atual suporta:

- **Single-queue (legado)** via `queue:`
- **Multi-queue (recomendado)** via `queues:`
- **Diretorio unico de dados** (`node.dirs.base`) que abriga filas, mapas e estado de replicacao

> Dica: use `queues:` para habilitar multiplas filas e retention `TIME_BASED` (log distribuido com offsets).

## Carregando a configuracao

Utilize a classe `NGridConfigLoader` para ler o arquivo YAML e converte-lo para o objeto de configuracao do NGrid.

```java
import dev.nishisan.utils.ngrid.config.NGridConfigLoader;
import dev.nishisan.utils.ngrid.config.NGridYamlConfig;
import dev.nishisan.utils.ngrid.structures.NGridConfig;
import dev.nishisan.utils.ngrid.structures.NGridNode;

import java.nio.file.Path;

public class ConfigLoaderExample {
    public static void main(String[] args) throws Exception {
        // 1. Carregar o YAML
        Path configFile = Path.of("ngrid-config.yaml");
        NGridYamlConfig yamlConfig = NGridConfigLoader.load(configFile);

        // 2. Converter para a configuracao de dominio
        NGridConfig config = NGridConfigLoader.convertToDomain(yamlConfig);

        // 3. Iniciar o no
        try (NGridNode node = new NGridNode(config)) {
            node.start();
            // ...
        }
    }
}
```

## Estrutura do Arquivo YAML

Um arquivo de configuracao tipico possui as secoes: `node`, `cluster`, `queues` e `maps`.

```yaml
node:
  # Identificador unico do no. Se omitido, um ID aleatorio sera gerado.
  id: node-1
  # Endereco IP ou Hostname para bind do servidor TCP
  host: 127.0.0.1
  # Porta TCP
  port: 9000
  dirs:
    # Diretorio base para armazenamento de dados (filas, mapas, estado de replicacao)
    base: ./data

cluster:
  # Nome do cluster (opcional)
  name: my-cluster
  replication:
    # Numero de copias dos dados (fator de replicacao)
    factor: 3
    # Se true, exige quorum estrito para operacoes de escrita (CP).
    # Se false, prioriza disponibilidade (AP).
    strict: false
  transport:
    workers: 4
  # Lista de nos sementes para descoberta inicial (formato host:port)
  seeds:
    - 127.0.0.1:9001
    - 127.0.0.1:9002

# Multiplas filas (recomendado)
queues:
  - name: orders
    retention:
      # Politica suportada no NGrid: TIME_BASED (log/stream)
      policy: TIME_BASED
      duration: 24h
    performance:
      fsync: true
      short-circuit: false
      memory-buffer:
        enabled: true
        size: 10240
    compaction:
      threshold: 0.3
      interval: 10m

  - name: events
    retention:
      policy: TIME_BASED
      duration: 6h

  - name: jobs
    retention:
      policy: TIME_BASED
      duration: 24h

# Mapas (metadados de bootstrap para autodiscover)
maps:
  - name: inventory
    key-type: java.lang.String
    value-type: java.lang.Integer
    persistence: ASYNC_WITH_FSYNC
```

### Estrutura de diretorios (dataDirectory)

Quando `node.dirs.base` esta configurado, o NGrid organiza os dados assim:

```
{base}/
├── queues/
│   ├── orders/
│   ├── events/
│   └── jobs/
├── maps/
│   └── ...
└── replication/
    └── sequence-state.dat
```

## Queue unica (legado)

Se `queues:` estiver ausente, o NGrid aceita a secao `queue:` como configuracao unica. Esse modo existe por compatibilidade e preserva a semantica historica.

```yaml
queue:
  name: main-queue
  retention:
    policy: TIME_BASED
    duration: 24h
  performance:
    fsync: true
```

> Nota: Em configuracoes legadas com `queueDirectory`, o comportamento historico e destrutivo (DELETE_ON_CONSUME).

## Variaveis de Ambiente

O arquivo YAML suporta **interpolacao de variaveis de ambiente**, permitindo injetar valores sensiveis ou dinamicos em tempo de execucao.

### Sintaxe

- `${VAR_NAME}`: Substitui pelo valor da variavel de ambiente `VAR_NAME`. Lanca erro se a variavel nao existir.
- `${VAR_NAME:default_value}`: Usa `default_value` caso a variavel `VAR_NAME` nao esteja definida.

### Exemplo

```yaml
node:
  id: ${POD_NAME:node-default}
  host: ${POD_IP}
  port: ${PORT:9000}
  dirs:
    base: /data/${POD_NAME:node-default}

cluster:
  name: ${CLUSTER_NAME:production-cluster}
```

Neste exemplo:
1. `id` assumira o valor de `POD_NAME` ou "node-default".
2. `host` assumira o valor de `POD_IP` (lanca erro se nao definido).
3. `port` assumira o valor de `PORT` ou 9000.

Isso e especialmente util em ambientes como **Kubernetes**, onde o nome do pod e IP sao atribuidos dinamicamente.
