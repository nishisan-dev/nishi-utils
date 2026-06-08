# Plano — Retenção do binlog do líder (ResendLog) estilo MySQL: rotação por tamanho + máximo de segmentos

## Contexto (por que esta mudança)

Recém implementamos o **RELAY_STREAM**: o líder serve um op-log durável e segmentado (o "binlog"),
o follower puxa por cursor e aplica em ordem. Esse op-log é o `ResendLog`/`ResendLogStore`
(`nishi-utils-core/.../ngrid/replication/`), gravado em segmentos `seg-NNNNNNNNNN.dat` por tópico.

Hoje o `ResendLog` rotaciona segmento por **número de registros** (`segmentMaxEntries`, default 65 536)
e por **idade** (`segmentMaxAge`, default 5 min), e retém por **total de registros**
(`resendLogMaxEntries`, default 10 000 000) e por **tempo** (`replicationLogRetentionTime`, default 0=off).
**Faltam os dois eixos que o operador raciocina no MySQL:**

- **Rotação por tamanho em bytes do arquivo** — equivalente a `max_binlog_size` (no MySQL ~1GB).
- **Retenção por número de arquivos/índices** — "manter no máximo N segmentos", o modelo
  `mysql-bin.000001..00000N` (os "índices" do binlog).

O dono do projeto quer poder expressar retenção como **"10 arquivos de 10GB"** ou
**"10 arquivos com no máximo 10M ops cada"** — ou seja, **limite por-segmento (tamanho OU ops, o que
vier primeiro) + limite de quantidade de segmentos**. Sem teto de espaço total separado
(`binlog_space_limit` fica fora deste escopo por decisão explícita).

**Escopo confirmado:** apenas o **binlog do líder (`ResendLog`)**. O relay do follower
(`RelayStore`/`NQueue`) já se auto-purga por consumo (modelo `relay_log` do slave); a única
evolução pedida lá é **expor via knob** o TTL `expireAfterWrite` que o `NQueue` já suporta mas que o
`RelayStore` hoje não configura.

**Resultado pretendido:** operador define, por builder e por YAML, (a) tamanho-máximo-por-segmento em
bytes, (b) número-máximo-de-segmentos retidos, e (c) TTL opcional do relay do follower — com guardrails
de produção. O backstop de contagem (`resendLogMaxEntries`) e a janela temporal continuam válidos.

---

## Mudança 1 — `ResendLog`: rotação por bytes + retenção por número de segmentos

Arquivo: `nishi-utils-core/src/main/java/dev/nishisan/utils/ngrid/replication/ResendLog.java`

Dois novos parâmetros no construtor (linha 96): `long segmentMaxBytes` (0 = desabilitado) e
`int maxSegments` (0 = desabilitado). Mantém todos os campos atuais.

1. **Rotação por tamanho** — em `append()` (linha 157), estender a condição de roll:
   ```java
   if (active == null || active.count >= segmentMaxEntries
           || active.shouldRollByAge(segmentMaxAgeMillis)
           || active.shouldRollBySize(segmentMaxBytes)) {
       rollSegment();
   }
   ```
   Novo método em `Segment` espelhando `shouldRollByAge` (linha 333), usando o `writePosition` já
   mantido (linha 276):
   ```java
   boolean shouldRollBySize(long maxBytes) {
       return maxBytes > 0 && writePosition >= maxBytes;
   }
   ```
   Semântica idêntica ao MySQL: checa antes do append; um registro pode estourar o teto por uma
   margem de 1 record (o registro nunca é partido).

2. **Retenção por número de segmentos** — em `enforceRetention()` (linha 182), somar a condição:
   ```java
   boolean overCount = totalEntries > maxEntries;
   boolean overSegments = maxSegments > 0 && segments.size() > maxSegments;
   boolean expired = retentionMillis > 0 && (now - oldest.maxTimestamp) > retentionMillis;
   if (!overCount && !overSegments && !expired) break;
   ```
   O loop já é `while (segments.size() > 1)` e nunca dropa o segmento ativo (tail) — preserva a
   garantia de que o líder sempre tem janela para servir o stream; quem cair abaixo de
   `oldestSequence()` recebe `needSnapshot=true` (já implementado no `handleRelayStreamFetch`).

3. **Observabilidade (DoD)** — adicionar acessores `synchronized` para stats:
   `long totalBytes()` (soma de `segment.writePosition` — poucos segmentos, custo desprezível) e
   `int segmentCount()`. Expor no `ResendLogStore` como `diskBytes(topic)`/`segmentCount(topic)`.

## Mudança 2 — `ResendLogStore`: repasse dos novos parâmetros

Arquivo: `.../ngrid/replication/ResendLogStore.java` (construtor linha 53, `open()` linha 68).
Adicionar `segmentMaxBytes` e `maxSegments` como campos finais e repassá-los ao `new ResendLog(...)`.
Adicionar os pass-throughs `diskBytes(topic)`/`segmentCount(topic)` (espelhando `size(topic)`).

## Mudança 3 — Local de construção no `ReplicationManager`

Arquivo: `.../ngrid/replication/ReplicationManager.java` linha 360. Passar os novos getters:
```java
this.resendLogStore = new ResendLogStore(config.dataDirectory().resolve("resend-log"),
        config.resendLogSegmentMaxEntries(), config.resendLogSegmentMaxAge(),
        config.resendLogSegmentMaxBytes(), config.resendLogMaxSegments(),
        config.replicationLogRetentionTime(), config.resendLogMaxEntries(),
        config.relayDurability() == RelayDurability.ALWAYS);
```
(Ordem dos parâmetros a alinhar com a assinatura final do store.)

## Mudança 4 — `ReplicationConfig`: campos, builder, getters, validação

Arquivo: `.../ngrid/replication/ReplicationConfig.java`. Espelhar exatamente o padrão de
`resendLogSegmentMaxEntries`:
- Campos (área linha 44-47): `private final long resendLogSegmentMaxBytes;` e
  `private final int resendLogMaxSegments;`.
- Construtor (61-89), build() (794-797): adicionar os parâmetros.
- Defaults no Builder (412-415): `resendLogSegmentMaxBytes = 0L` (desabilitado),
  `resendLogMaxSegments = 0` (desabilitado) — preserva comportamento atual; o backstop de contagem
  (`resendLogMaxEntries=10M`) continua protegendo out-of-box.
- Builder methods (602-650) com validação: `resendLogSegmentMaxBytes(long)` exige `>= 0`;
  `resendLogMaxSegments(int)` exige `>= 0`.
- Getters (~253) com Javadoc citando os equivalentes MySQL (`max_binlog_size`, número de índices).

## Mudança 5 — `NGridConfig` + `NGridNode`: ponte builder

- `.../ngrid/structures/NGridConfig.java`: campos (área 48), getters (211), Builder fields (493) e
  Builder methods (723) — espelhar `resendLogMaxEntries` (usar `Long`/`Integer` nuláveis para
  "não setado", consistente com o padrão existente do NGridConfig).
- `.../ngrid/structures/NGridNode.java` (linhas 354-371): na ponte para `ReplicationConfig.Builder`,
  adicionar, sob guarda de nulo (como já fazem 361-365):
  ```java
  if (config.resendLogSegmentMaxBytes() != null) {
      replicationBuilder.resendLogSegmentMaxBytes(config.resendLogSegmentMaxBytes());
  }
  if (config.resendLogMaxSegments() != null) {
      replicationBuilder.resendLogMaxSegments(config.resendLogMaxSegments());
  }
  ```

## Mudança 6 — Exposição via YAML

- `.../ngrid/config/ClusterPolicyConfig.java` (inner `ReplicationConfig`, linha 209-272): adicionar
  campos `resendLogSegmentMaxBytes` (long), `resendLogMaxSegments` (int) + getters/setters.
  Aceitar tamanho humano (`"10GB"`, `"512MB"`) via parser de bytes — verificar se já existe util de
  parsing de tamanho no loader; se não, aceitar long puro em bytes nesta primeira versão e documentar.
- `.../ngrid/config/NGridConfigLoader.java` (bloco 144-150): mapear os novos campos para o builder,
  no mesmo `if (clusterConfig.getReplication() != null)`.
- Schema YAML resultante:
  ```yaml
  cluster:
    replication:
      followerIngestMode: RELAY_STREAM
      resendLogSegmentMaxBytes: 10737418240   # 10GB por segmento (max_binlog_size)
      resendLogMaxSegments: 10                # retém no máximo 10 índices
  ```

## Mudança 7 — Knob do TTL do relay do follower (`expireAfterWrite`)

O `NQueue` já suporta `withExpireAfterWrite(Duration)` (NQueue.java:1454, ortogonal à RetentionPolicy),
mas o `RelayStore.open()` (linha 129-143) não o configura. Apenas expor:
- `RelayStore`: novo campo `Duration expireAfterWrite` (construtor linhas 99-119) e, em `open()`,
  `.withExpireAfterWrite(expireAfterWrite)` quando `> 0`.
- `ReplicationManager:344` (`new RelayStore(...)`): passar `config.relayExpireAfterWrite()`.
- `ReplicationConfig` + `NGridConfig` + `NGridNode` + YAML: novo knob `relayExpireAfterWrite`
  (Duration, default `Duration.ZERO` = desabilitado), espelhando o padrão de `replicationLogRetentionTime`.
- **Javadoc/doc com a ressalva:** `expireAfterWrite` descarta no read entradas mais velhas que o TTL
  **mesmo não aplicadas** → se o follower atrasar além do TTL, vira gap → snapshot bootstrap (que já é
  o caminho seguro existente). É a semântica MySQL de relay log expirado; default off e opt-in.

## Mudança 8 — Guardrails por `DeploymentProfile`

Arquivo: `NGridConfig.java`, `validateProductionConfig()` (linha 1017). Validações leves quando
`persistentResendLog || followerIngestMode == RELAY_STREAM`:
- Se `resendLogSegmentMaxBytes` setado, exigir um piso sensato (ex.: `>= 1MB`) para não criar
  rotação patológica de microssegmentos.
- Se `resendLogMaxSegments` setado, exigir `>= 2` (precisa de ao menos ativo + um histórico para
  servir resend/stream).
- Não tornar os knobs obrigatórios em PRODUCTION (o backstop de contagem já garante limite).

---

## Arquivos críticos
- `nishi-utils-core/.../ngrid/replication/ResendLog.java` (núcleo: roll por bytes + retenção por nº segmentos)
- `nishi-utils-core/.../ngrid/replication/ResendLogStore.java` (repasse + acessores de disco)
- `nishi-utils-core/.../ngrid/replication/ReplicationManager.java` (linhas 344 e 360 — construção)
- `nishi-utils-core/.../ngrid/replication/ReplicationConfig.java` (knobs + validação)
- `nishi-utils-core/.../ngrid/replication/RelayStore.java` (knob expireAfterWrite)
- `nishi-utils-core/.../ngrid/structures/NGridConfig.java` + `NGridNode.java` (ponte builder + guardrails)
- `nishi-utils-core/.../ngrid/config/ClusterPolicyConfig.java` + `NGridConfigLoader.java` (YAML)

## Sequência de commits (atômicos, cada um compila e passa)
1. `feat(replication): ResendLog rotaciona por tamanho de segmento e retém por nº de segmentos`
   (Mudanças 1+2+3 + testes de `ResendLogTest`)
2. `feat(config): knobs resendLogSegmentMaxBytes/resendLogMaxSegments (builder + YAML)`
   (Mudanças 4+5+6+8 + testes de `ReplicationConfigTest`/`RelayStreamConfigTest`)
3. `feat(replication): expõe relayExpireAfterWrite TTL do relay do follower`
   (Mudança 7 + teste de `RelayStoreTest`)
4. `docs(replication): documenta retenção do binlog estilo MySQL`

## Testes (perfil padrão `mvn test`; estes são unitários, não exigem `-Presilience`)
Estender os testes existentes do mesmo pacote (`...ngrid/replication/`):
- **`ResendLogTest`**: `rollsNewSegmentWhenByteSizeExceeded` (append grava frames até passar de N
  bytes → conta segmentos); `retainsAtMostMaxSegmentsDroppingOldest` (com `maxSegments=3`, o 4º roll
  evicta o mais antigo e `oldestSequence()` avança); `byteRollAndCountRollWhicheverFirst`;
  `existingTimeAndCountRetentionStillApply` (regressão); `totalBytesAndSegmentCountReported`.
- **`ReplicationConfigTest`** / **`RelayStreamConfigTest`**: builder rejeita valores negativos;
  defaults preservam comportamento (knobs em 0 = desabilitado); plumbing end-to-end builder→config.
- **`RelayStoreTest`**: `expireAfterWriteDropsAgedUnreadEntries` (TTL curto + sleep → entrada some no
  peek); knob default `ZERO` não altera comportamento atual.
- Cobrir o caminho YAML em teste de loader, se houver fixture (`NGridConfigLoader`).

## Verificação end-to-end
- `mvn -pl nishi-utils-core test -Dtest=ResendLogTest,ReplicationConfigTest,RelayStreamConfigTest,RelayStoreTest`
- Suíte completa antes de cada commit: `mvn -pl nishi-utils-core test`
- Resiliência (validação local obrigatória — CI não roda a suíte ngrid):
  `mvn -pl nishi-utils-core test -Presilience -Dsurefire.rerunFailingTestsCount=1`
- Build: `mvn -B -DskipTests -pl nishi-utils-core,nishi-utils-oss -am package`
- Teste manual de retenção: subir `NGrid.local(2)` com `resendLogSegmentMaxBytes` pequeno (ex. 64KB) e
  `resendLogMaxSegments=3`, firehose de ops, e inspecionar `var/.../resend-log/<topic>/seg-*.dat`:
  confirmar que nunca há mais de 3 arquivos e que cada um respeita o teto de bytes.

## Documentação (DoD, pt-BR)
- Atualizar `doc/ngrid/relay-stream-replication.md` com a seção de retenção do binlog: mapear
  `resendLogSegmentMaxBytes`↔`max_binlog_size`, `resendLogMaxSegments`↔nº de índices binlog, e
  `relayExpireAfterWrite`↔relay log TTL; tabela de knobs com defaults e exemplos YAML "10×10GB" e
  "10×10M ops".
- Se houver diagrama de retenção/segmentação aplicável em `doc/diagrams/*.puml`, atualizar e
  incorporar via `uml.nishisan.dev`.
- Atualizar `doc/CHANGELOG.md`.
- Copiar este plano aprovado para `/planning` na execução.
```

