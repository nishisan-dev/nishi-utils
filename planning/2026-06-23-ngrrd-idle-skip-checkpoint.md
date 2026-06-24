# Plano — Idle-skip de checkpoint no ngrrd (sharded blob)

> Camada de cache/debounce anterior ao writer, realizada como **gate idle-skip de
> checkpoint** no `NgrrdWriter`. Branch: `feature/ngrrd-blob-observability`.
> Módulo: `nishi-utils-oss` (`dev.nishisan.utils.oss`).

## Context

A solução de sharded blob hoje **não tem** nenhuma camada de coalescing de escrita.
A investigação do pipeline mostrou que o caminho por-amostra **já é barato**:
amostras dentro do mesmo step base só acumulam um PDP em RAM (`NgrrdWriter.handleWrite`,
`pdp.add` em `:418-420`), e o ring só é tocado em mmap (sem fsync) na virada do step
(`emit(finalEmit=true)`, `:529-543`). A reescrita redundante **cara** está no
`checkpoint()`/`flush()`: `checkpointAndForce()` (`:603-629`) re-emite o CDP **parcial**
de TODOS os RRAs e chama `channel.force()` → fsync no disco / **PUT do objeto inteiro**
no S3 — **mesmo quando nenhuma amostra nova chegou desde o último force**. Com checkpoints
frequentes (~1s) e ingestão mais lenta, isso gera N forces/PUTs idênticos por intervalo.

A ideia do usuário (um cache em memória com a "data da última escrita" por RRA, estilo
Guava, que evita "tocar antes do tempo") está correta. Dado o modelo **single-writer
por série** (1 `NgrrdWriter` por handle/série — confirmado em `Ngrrd.java:187-194`), o
"map com cardinalidade" colapsa para um **campo por writer**: não há lookup, não há
estrutura compartilhada, mesma cardinalidade (1 por série ativa).

**Decisão (confirmada com o usuário):** modo **idle-skip seguro** — pular o
checkpoint/force **somente** quando nenhuma amostra foi aplicada desde o último force.
É **estritamente Pareto-positivo**: leitura idêntica, durabilidade idêntica, e corta
todo force/PUT redundante. NÃO adotar o "debounce por janela" agressivo, porque o slot
em progresso é lido direto da célula persistida (`NgrrdReader.readPoints`, `:144/:156`),
escrita só pela re-emissão parcial (`checkpointAndForce` → `emit(...,false)`, `:613`);
suprimi-la por janela causaria leitura velha/NaN do slot aberto.

**Resultado da verificação adversarial (3 lentes):** núcleo **correto** — um checkpoint
ocioso (`changedSinceForce==false`) não muda nenhum byte do ring nem da live-state (a
re-emissão parcial é byte-idêntica: `cdpValue` é função pura de
`cdpPartial`/`cdpFolded`/`pdp` + geometria estática, **sem wall-clock**, `:501-527`),
logo o reader lê o mesmo valor e o `force()` pulado já seria no-op (canal `dirty==false`).
Veredito: **GO-WITH-ADJUSTMENTS** (1 teste a corrigir + 3 ajustes obrigatórios abaixo).

## Design

Adicionar ao `NgrrdWriter` um único sinal de "mudou desde o último force",
confinado à worker thread (como `ringDirty`, `:84`), e um early-return no topo de
`checkpointAndForce()`.

- **Campo:** `private boolean changedSinceForce = true;` junto de `ringDirty`
  (`NgrrdWriter.java:84`). Init `true` garante o 1º checkpoint (paridade com hoje).
- **Marcação (set):** em `handleWrite`, **incondicionalmente**, logo após o guard de
  amostra atrasada (após `:409`) e antes do bloco `if (cur == -1L)` (`:411`):
  `changedSinceForce = true;`. Cobre todas as mutações de estado arquivado:
  init de slot (`:412`), `advanceColumn` (`:414`), `counterPrev*` em `deriveValue`
  (`:430-431`) e `pdp.add` (`:419`). **Não** condicionar a `!Double.isNaN(derived)`
  nem a `ringDirty`/virada de passo — uma amostra que só acumula no PDP do slot
  corrente também muda o slot in-progress lido pelo reader e o PDP parcial
  materializado no checkpoint; restringi-la ao ramo de ring-advance perderia o último
  PDP parcial no shutdown. (DS-fora-do-appliesTo, que retorna em `:400`, e late-sample,
  que retorna em `:409`, **não** marcam o flag — ver Risco residual 1.)
- **Skip + clear:** em `checkpointAndForce()` (`:603`):
  - No topo, antes do lock: `if (!changedSinceForce) { /* métrica */ return; }`
    (early-return normal, **nunca** via exceção — o `latch.countDown()` está no
    `finally` do chamador `processOne`, `:356/:366`, então o early-return preserva a
    liberação do `flush()`/`checkpoint()`/`Shutdown` bloqueante).
  - O clear `changedSinceForce = false;` vai no **fim** do método, **depois** do bloco
    `if (durability == Durability.FSYNC) channel.force();` (`:622-624`). Assim: em FSYNC,
    se `force()` lançar (ex.: PUT no S3 falha), o flag **permanece `true`** e o próximo
    checkpoint tenta de novo (nunca deixa bytes não-persistidos com flag limpo); em
    OS_CACHE não há force, o clear roda após `persistLiveState()` (`:616`).

Forma final (esqueleto):

```java
private void checkpointAndForce() {
    if (!changedSinceForce) {
        if (metrics != null) metrics.onCheckpointCoalesced(seriesKey);
        return;
    }
    writeLock.lock();
    try {
        // ... loop de emit(...,false) + persistLiveState() — inalterado ...
    } finally {
        writeLock.unlock();
    }
    if (durability == Durability.FSYNC) {
        channel.force();              // pode lançar; se lançar, NÃO chega no clear
    }
    changedSinceForce = false;        // após persistir(OS_CACHE)/forçar(FSYNC) com sucesso
}
```

## Observabilidade (contador de coalescing)

Adicionar um contador por-série seguindo o padrão existente (canônico+legado / `AtomicLong`+forward),
para medir quantos checkpoints redundantes foram coalescidos — alinhado ao tema da branch
e usado também nos testes para provar que o gate disparou.

- `metrics/NgrrdMetricsListener.java`: novo evento canônico
  `default void onCheckpointCoalesced(String seriesKey) { onCheckpointCoalesced(); }`
  + legado `default void onCheckpointCoalesced() {}` (espelha `onLateSample`/`onIngestLag`).
- `metrics/NgrrdMetrics.java`: `AtomicLong checkpointCoalescedCount` + getter
  `checkpointCoalescedCount()` + override canônico (incrementa + forward) + legado (delega
  com `seriesKey==null`).

## Ajuste obrigatório de contrato (doc/Javadoc)

A promessa publicada hoje é durabilidade **incondicional** por checkpoint. Acrescentar a
cláusula "no-op de durabilidade quando nenhuma amostra foi aplicada desde o último force
(a série já está durável naquele estado)" em:
- `NgrrdHandle.java:62-70` (Javadoc de `flush()`/`checkpoint()`).
- `NgrrdWriter.java:243-251` (Javadoc de `flush()`/`checkpoint()`).
- `doc/oss/ngrrd.md` — seção "Durabilidade do checkpoint" (`:576-594`, em especial a linha
  da tabela `FSYNC`, `:585`, "nenhuma (durável a cada checkpoint)").

## Arquivos a modificar

| Arquivo | Mudança |
|---|---|
| `nishi-utils-oss/.../writer/NgrrdWriter.java` | campo `changedSinceForce`; set em `handleWrite` (após `:409`); early-return+clear em `checkpointAndForce`; Javadoc `:243-251` |
| `nishi-utils-oss/.../metrics/NgrrdMetricsListener.java` | evento `onCheckpointCoalesced` (canônico+legado) |
| `nishi-utils-oss/.../metrics/NgrrdMetrics.java` | contador + getter + overrides |
| `nishi-utils-oss/.../NgrrdHandle.java` | Javadoc `:62-70` (no-op idle) |
| `doc/oss/ngrrd.md` | seção durabilidade `:576-594`; tabela de métricas (`checkpoint_coalesced`) |
| `doc/oss/ngrrd-blob-volume.md` | nova subseção em §11 (coalescing de checkpoint) |
| `doc/CHANGELOG.md` | entrada da feature |
| `doc/oss/diagrams/checkpoint_idle_skip.puml` | diagrama de sequência (DoD) |
| `planning/` | cópia deste plano aprovado |

## Testes (`nishi-utils-oss/src/test`)

Regra do projeto: produção sempre com teste; rodar a suíte antes de commitar.

1. **Corrigir o blocker** — `writer/NgrrdWriterDurabilityTest.fsyncForcaPorCheckpoint`
   (`:71-87`): inserir `w.write("in_octets", new Sample(START_MS + 2*STEP_MS, 1_200_000L));`
   **entre** os dois `checkpoint()` (ambos passam a ter dado novo → `+2` mantido, intenção
   "FSYNC força por checkpoint-com-dados" preservada).
2. **Novo** — `checkpointOciosoNaoForcaEmFsync`: `write`→`write`→`checkpoint` (`+1`)→
   `checkpoint` ocioso → assere `forceCount` inalterado (`+1`).
3. **Novo** — equivalência observacional: ingerir uma amostra no slot corrente →
   `checkpoint` → ler o slot em progresso; `checkpoint` ocioso de novo → reler → valores
   **byte-idênticos** (guarda contra futura dependência de wall-clock no `cdpValue`).
4. **Novo** — durabilidade pós-skip: `write`(mesmo slot)→`close`→reabrir confirma o CDP
   parcial persistido; `write`→`checkpoint`(OS_CACHE)→`close`→reabrir lê o último dado.
5. **Métrica** — `metrics/NgrrdMetricsTest`: checkpoint ocioso incrementa
   `checkpointCoalescedCount()` e atribui o `seriesKey` correto (usa `NgrrdMetrics` real,
   espelhando os testes existentes).
6. **Regressão (sem alteração, confirmar verde):** `osCacheNaoForcaPorCheckpoint`,
   `closeDescarregaMesmoEmOsCache`, `NgrrdWriterIncrementalTest`, `QualityPropagationTest`,
   `IfaceTrafficSmokeIT`, `PythonBlobCrossLangIT`.

## Verificação (end-to-end)

```bash
# Unit (writer + métricas + regressões)
mvn -pl nishi-utils-oss test

# IT do blob/S3 (LocalStack via Testcontainers) — confirma o caminho sharded blob
mvn -pl nishi-utils-oss verify -Pngrrd-integration

# Javadoc (a branch valida Javadoc)
mvn -pl nishi-utils-oss verify -Pvalidate-javadoc -DskipTests
```

Critérios de aceite:
- `mvn -pl nishi-utils-oss test` verde, incluindo os novos testes e o `fsyncForcaPorCheckpoint` ajustado.
- Em FSYNC, dois `checkpoint()` consecutivos sem write entre eles produzem **1** force (não 2);
  com write entre eles, **2**.
- Leitura do slot em progresso é byte-idêntica com e sem checkpoint ocioso intermediário.
- `checkpointCoalescedCount()` reflete os checkpoints pulados.
- `OS_CACHE`: `close()` continua descarregando o pendente; visibilidade do CDP parcial preservada.

## Riscos residuais (aceitáveis)

1. `state.lastUpEpochMs` é atualizado em `handleWrite:394-396` **antes** dos early-returns;
   uma amostra descartada (late/fora do appliesTo) avança `lastUpEpochMs` sem marcar o flag.
   Após crash em OS_CACHE, `lastUpEpochMs` pode regredir até o último dado **de fato
   arquivado**. Impacto em `readPoints` é **nulo** (não lê `lastUpEpochMs`). Documentar.
   Se durabilidade exata de `lastUpEpochMs` virar requisito, mover o set do flag para logo
   após `:396`.
2. A equivalência depende de `cdpValue` (`:501-527`) não usar wall-clock — verdadeiro hoje.
   O teste 3 trava isso contra regressão futura (ex.: se o XFF parcial passar a contar slots
   vazios por relógio).
3. `changedSinceForce` não-volátil é seguro **enquanto** mutado/lido **somente** dentro de
   `processOne` (handleWrite/checkpointAndForce). Publicação entre threads do pool dada pelo
   par release/acquire da flag `scheduled` (`AtomicBoolean`, `:309/:326`) — mesmo mecanismo
   que já publica `state`/`ringDirty`. **Nunca** tocar o flag de `write()`/`close()`/reader.

## Ordem de execução

(a) `NgrrdWriter` (flag + set + early-return/clear) e métrica; (b) corrigir
`fsyncForcaPorCheckpoint`; (c) Javadoc/doc/contrato; (d) novos testes; (e) doc/diagrama/
changelog; (f) `mvn -pl nishi-utils-oss test` e `-Pngrrd-integration`. Commits atômicos
por responsabilidade (produção+teste juntos; doc à parte). CI não roda resiliência ngrid —
validar localmente.
