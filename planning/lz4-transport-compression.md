# Plano: Compressão LZ4 transparente na camada de transporte TCP do NGrid

## Contexto

**Avaliação inicial (o que motivou o plano):** o `ReplicationManager` e toda a cadeia
de serialização/transporte abaixo dele **não usam nenhuma compressão** (nem LZO, GZIP,
LZ4 ou outra). Confirmado rastreando o fluxo ponta a ponta:

- `ReplicationManager` apenas monta `ReplicationPayload` num `ClusterMessage` e chama
  `transport.send(message)` (`ReplicationManager.java:688-700`). Não toca em bytes.
- A serialização real é JSON puro via Jackson em `JacksonMessageCodec.encode()`
  (`writeValueAsBytes`, sem compressão).
- O frame TCP é apenas length-prefix: `writeInt(len)` + `write(data)` (`TcpTransport.java:864-868`).
- A única referência a `java.util.zip` no repo é `CRC32` (checksum) no módulo `oss/ngrrd` — não é compressão e não está no caminho de rede.
- `QueueConfig.compressionEnabled` (`QueueConfig.java:38`) é um **placeholder nunca implementado** em outra camada (storage de fila) — não é o alvo.

**Problema/oportunidade:** além de não comprimir, o Jackson serializa os `byte[]` dos
payloads como **Base64** (~+33% de tamanho). Logo, replicação (`REPLICATION_REQUEST`) e
sobretudo snapshots (`SYNC_RESPONSE`, multi-MB, byte-sliced) trafegam inflados. Há ganho
real em comprimir, com baixo risco se feito na camada certa.

**Resultado pretendido:** compressão LZ4 **transparente** na camada de codec/transporte
(sem tocar o `ReplicationManager`), **configurável (default ligado)** e **compatível com
rolling upgrade** — nós em versões diferentes coexistem, comprimindo apenas para peers
que anunciaram suporte via handshake.

**Decisões do usuário (fixas):** algoritmo **LZ4** (`org.lz4:lz4-java`); compatibilidade
via **negociação no handshake**; ativação **configurável com default ligado**.

## Abordagem

A camada de codec já é extensível por design: `CompositeMessageCodec` discrimina frames
pelo **primeiro byte** (`0x01/0x02` binário, `0x00` JSON, `0x7B` JSON legado) e tem fallback
retrocompatível (`CompositeMessageCodec.java:88-111`). Adicionamos um novo marker
`0x10 = JSON comprimido LZ4`. O decode passa a reconhecê-lo sempre; o encode só comprime
para peers que negociaram suporte.

### 1. Layout do frame comprimido (LZ4 block + tamanho original)
LZ4 block é mais compacto/rápido que o LZ4 Frame format. Como block não guarda o tamanho
descomprimido, o frame carrega o `originalLength`:

```
| offset | size  | campo                                          |
|--------|-------|------------------------------------------------|
| 0      | 1 B   | marker = 0x10 (LZ4_JSON_MARKER)               |
| 1      | 4 B   | originalLength (int)                           |
| 5      | N B   | bytes LZ4 comprimidos do JSON (sem o 0x00)    |
```
`decode()` lê `originalLength`, **valida contra um teto** (evita OOM por valor corrompido),
descomprime com `LZ4FastDecompressor` e entrega ao `JacksonMessageCodec.decode`.

### 2. Threshold e decisão de comprimir (dentro do `encode()`)
1. HEARTBEAT/PING → binário, nunca comprime (`BinaryFrameCodec` intacto).
2. Serializa JSON.
3. Se compressão ligada para a saída **e** `jsonBytes.length >= compressionMinSize` (default 512):
   comprime e **só usa o frame `0x10` se** `5 + compressedLen < 1 + jsonBytes.length`.
4. Senão → frame `0x00` + JSON (comportamento atual). Garante que payload pequeno/incompressível nunca infla.

### 3. Negociação no handshake (compatibilidade / rolling upgrade)
- `HandshakePayload`: novo campo `boolean supportsCompression` com novo `@JsonCreator`;
  construtores legados delegam com `true`. `JacksonMessageCodec` ignora propriedades
  desconhecidas → nó antigo que não envia o campo é lido como `false` (não comprimimos
  para ele); nó antigo ignora o campo extra do nó novo.
- `sendHandshake()` envia `supportsCompression = config.compressionEnabled()`.
- `handleHandshake()` (junto de `setRemote`, `TcpTransport.java:585`) chama
  `connection.setPeerSupportsCompression(...)` e liga a flag de saída do codec.
- **Regra de saída:** comprime se `config.compressionEnabled() && peerSupportsCompression`.
  O **decode descomprime sempre** (incondicional, por robustez).

### 4. Wiring per-connection (resolve codec compartilhado vs decisão per-peer)
Hoje o codec é único e compartilhado (`TcpTransport.java:89`, injetado em `:492`). Como a
decisão de comprimir é por-peer, a `Connection` passa a **criar sua própria instância** de
`CompositeMessageCodec(minSize)`. O codec ganha `volatile boolean compressOutput` (default
`false`) + setter. A flag só liga **após** o handshake confirmar suporte — portanto o
próprio HANDSHAKE nunca vai comprimido (o peer ainda não anunciou). `compressOutput` é
escrito na thread reader (`handleHandshake`) e lido na thread writer (`drainOutbound`):
`volatile` basta. Os sub-codecs e o `LZ4Factory`/compressor/decompressor são stateless e
thread-safe → **singletons estáticos**; só a flag é per-instância.

### 5. Configuração
- `TcpTransportConfig` (Builder): `compressionEnabled` (default `true`), `compressionMinSize`
  (default `512`), espelhando o padrão de `outboundQueueCapacity`.
- `NGridConfig` + Builder: campos/getters equivalentes, propagados.
- `NGridNode` (~`:301-306`): encadear os novos setters no `transportBuilder`.
- YAML: `ClusterPolicyConfig.TransportConfig` ganha POJO aninhado `CompressionConfig`
  (`enabled=true`, `minSize=512`); `NGridConfigLoader.convertToDomain` (~`:156-159`,
  dentro de `if (clusterConfig.getTransport() != null)`) faz o parse null-safe.
  Seção: `cluster.transport.compression.{enabled,minSize}`.
- DeploymentProfile: sem guardrail novo (default ligado em todos os profiles).

### 6. Dependência Maven
- `pom.xml` raiz (`dependencyManagement`): `org.lz4:lz4-java:1.8.0` (versão via property `<lz4.version>`).
- `nishi-utils-core/pom.xml` (`dependencies`): a mesma dependência **sem versão** (herda).

## Arquivos

**Criar:**
- `nishi-utils-core/.../ngrid/cluster/transport/codec/Lz4Compressor.java` — utilitário
  stateless: singleton `LZ4Factory.fastestInstance()`; `wrap(jsonBytes)` produz `0x10|len|lz4`
  e `unwrap(frame)` reverte (com guarda de `originalLength`).

**Modificar:**
- `.../codec/CompositeMessageCodec.java` — marker `LZ4_JSON_MARKER=0x10`; `volatile boolean compressOutput` + `int minSize` + setter; `encode()` com threshold/ganho real; `decode()` reconhece `0x10`; atualizar javadoc.
- `.../cluster/transport/TcpTransport.java` — Connection cria seu `CompositeMessageCodec`; `volatile boolean peerSupportsCompression` + setter; `handleHandshake`/`sendHandshake` negociam e ligam a flag; remover codec compartilhado (`:89`) e ajuste em `:492`.
- `.../common/HandshakePayload.java` — campo `supportsCompression` + `@JsonCreator` + getter; construtores legados delegando `true`.
- `.../cluster/transport/TcpTransportConfig.java` — campos/getters/builder de compressão.
- `.../structures/NGridConfig.java` (+ Builder) e `.../structures/NGridNode.java` — propagação e wiring.
- `.../config/ClusterPolicyConfig.java` — `CompressionConfig` aninhado em `TransportConfig`.
- `.../config/NGridConfigLoader.java` — parse da seção `compression`.
- `pom.xml` (raiz) e `nishi-utils-core/pom.xml` — dependência lz4-java.

## Sequência (commits atômicos — branch `feature/lz4-transport-compression`)
1. `build(deps): adiciona lz4-java ao dependencyManagement e ao core`.
2. `feat(transport): Lz4Compressor (block + originalLength) + testes`.
3. `feat(codec): marker 0x10 LZ4 no CompositeMessageCodec com threshold e ganho real`.
4. `feat(transport): negociação de compressão no handshake (HandshakePayload.supportsCompression)`.
5. `feat(transport): codec per-connection e ativação pós-handshake`.
6. `feat(config): compressionEnabled/minSize em TcpTransportConfig e NGridConfig`.
7. `feat(config): seção YAML cluster.transport.compression`.
8. `test(resilience): replicação ponta-a-ponta com compressão (NGrid.local)`.
9. (Opcional) `docs(ngrid)`: nota sobre a flag e rolling upgrade.

> A ordem garante que o transporte só emite `0x10` (commit 5) depois que codec (3) e
> negociação (4) já existem — nunca há estado intermediário emitindo frame comprimido sem negociação.

## Riscos e mitigações
- **Nó antigo recebendo `0x10`** → derruba conexão (marker desconhecido). Mitigado pela
  negociação: `supportsCompression` default `false` em payload sem o campo; flag de saída
  só liga pós-handshake; HANDSHAKE nunca comprimido.
- **Limite de 64MB do frame** (`TcpTransport.java:885`) aplica-se ao frame comprimido; o
  **descomprimido pode estourar** → validar `originalLength` contra um teto antes de alocar.
- **Thread-safety LZ4** → `LZ4Factory.fastestInstance()` e (de)compressores são thread-safe; usar singletons estáticos.
- **CPU em alta frequência** → HEARTBEAT/PING excluídos; threshold evita frames pequenos; LZ4 fast é barato; impacto concentrado nos snapshots (o alvo).
- **OutboundChannel/backpressure** inalterados (compressão acontece no `drainOutbound`, após `poll`); ordem de escrita preservada (dentro do `synchronized(outputStream)`, 1 writer por conexão).

## Verificação
**Unit (`mvn test`, convenção `*Test.java`):**
- `Lz4CompressorTest`: round-trip puro (incl. tamanho 0/pequeno e multi-MB).
- `CompositeMessageCodecTest` (expandir): round-trip comprimido → 1º byte `0x10` e mensagem idêntica; abaixo do threshold → `0x00`; payload incompressível → cai para `0x00`; `compressOutput=false` → sempre `0x00`; HEARTBEAT/PING permanecem `0x01/0x02`.
- **Interop**: decoder novo lê frames `0x00` e `0x7B` legados; frame `0x10` de um codec é lido por outro codec independente.
- `HandshakePayload`: (de)serialização com `supportsCompression`; JSON **sem** o campo (nó antigo) → `false`.
- `TcpTransportConfig`/`NGridConfig`: defaults (`true`/`512`) e builders; `NGridConfigLoader`: YAML com/sem a seção `compression`.

**Integração (`mvn test -Presilience`, facade `NGrid.local(n)`):**
- Subir cluster pequeno com compressão ligada, gerar `SYNC_RESPONSE` (snapshot) e validar
  convergência de dados entre nós — prova o caminho comprimido ponta a ponta.

**Build:** `mvn clean install` (multi-módulo, por causa da nova dependência). Validar imports e ausência de warnings.

> Nota: a CI não roda a suíte de resiliência ngrid — validar `-Presilience` localmente.
