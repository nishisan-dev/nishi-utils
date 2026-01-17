# Guia de Configuração (YAML)

O NGrid suporta carregamento de configurações a partir de arquivos YAML, facilitando o gerenciamento de ambientes (Dev, Staging, Prod) e deployment em containers (Docker/Kubernetes).

## Carregando a configuração

Utilize a classe `NGridConfigLoader` para ler o arquivo YAML e convertê-lo para o objeto de configuração do NGrid.

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

        // 2. Converter para a configuração de domínio
        NGridConfig config = NGridConfigLoader.convertToDomain(yamlConfig);

        // 3. Iniciar o nó
        try (NGridNode node = new NGridNode(config)) {
            node.start();
            // ...
        }
    }
}
```

## Estrutura do Arquivo YAML

Um arquivo de configuração típico possui as seções: `node`, `cluster` e `queue`.

```yaml
node:
  # Identificador único do nó. Se omitido, um ID aleatório será gerado.
  id: node-1
  # Endereço IP ou Hostname para bind do servidor TCP
  host: 127.0.0.1
  # Porta TCP
  port: 9000
  dirs:
    # Diretório base para armazenamento de dados (filas, mapas)
    base: ./data

cluster:
  # Nome do cluster (opcional)
  name: my-cluster
  replication:
    # Número de cópias dos dados (fator de replicação)
    factor: 3
    # Se true, exige quorum estrito para operações de escrita (CP). 
    # Se false, prioriza disponibilidade (AP).
    strict: false
  transport:
    workers: 4
  # Lista de nós sementes para descoberta inicial (formato host:port)
  seeds:
    - 127.0.0.1:9001
    - 127.0.0.1:9002

queue:
  name: main-queue
  retention:
    # Políticas: DELETE_ON_CONSUME (padrão) ou TIME_BASED
    policy: TIME_BASED
    # Duração da retenção (ex: 10m, 2h, 1d) - Apenas para TIME_BASED
    duration: 24h
  performance:
    # Habilita fsync para durabilidade máxima (true/false)
    fsync: true
    # Otimização para consumidores rápidos
    shortCircuit: true
    memoryBuffer:
      enabled: true
      size: 10240
  compaction:
    # Limiar de desperdício para compactação (0.0 a 1.0)
    threshold: 0.3
    # Intervalo de verificação
    interval: 10m
```

## Variáveis de Ambiente

O arquivo YAML suporta **interpolação de variáveis de ambiente**, permitindo injetar valores sensíveis ou dinâmicos em tempo de execução.

### Sintaxe

- `${VAR_NAME}`: Substitui pelo valor da variável de ambiente `VAR_NAME`. Lança erro se a variável não existir.
- `${VAR_NAME:default_value}`: Usa `default_value` caso a variável `VAR_NAME` não esteja definida.

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
1. `id` assumirá o valor de `POD_NAME` ou "node-default".
2. `host` assumirá o valor de `POD_IP` (lança erro se não definido).
3. `port` assumirá o valor de `PORT` ou 9000.

Isso é especialmente útil em ambientes como **Kubernetes**, onde o nome do pod e IP são atribuídos dinamicamente.
