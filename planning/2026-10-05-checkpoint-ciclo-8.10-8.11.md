# Checkpoint do projeto: ciclo 8.10.0 → 8.11.2 (2026-10-04/05)

Registro do estado ao fim do ciclo de incidentes e correções motivado pela integração com o
ngrrd-server (tems) no ambiente CTP.

## Versões publicadas

| Versão | PR | Conteúdo principal |
|---|---|---|
| 8.10.0 | #186 | Gate de criação rápido e `PLACEMENT_UNAVAILABLE` (contrato novo no cliente) |
| 8.10.1 | #187 | Rótulo do snapshot, seguidor à frente do líder, shutdown drenado, snapshot em chunks consistente |
| 8.10.2 | #194 | Hotfix: compactação amortizada do journal de ciclo de vida (OPEN e WRITE_BATCH O(n)) |
| 8.11.0 | #196 | **Retirada** (release apagada, tag mantida): regressões de desempenho e de recuperação |
| 8.11.1 | #197 | Instalação durável dos mapas (#195, #190), journal ACTIVE menor, #191, #192 |
| 8.11.2 | #203 | Handback sem cutover parcial nem reancoragem de tópicos não servidos (#199) |

## Estado do CTP (informado pelo tems)

- Três storages (.209, .79, .217) e o coordenador migrando para a 8.11.2 na janela de 2026-10-05.
- Durabilidade `OS_CACHE`, pedida pelo coordenador (`ngrrd.clusterDurability`), com risco aceito
  pelo usuário do tems (cluster com uma cópia por série).
- `priority` 50 nos três storages durante a mitigação da #199. Voltar um nó a ser o preferido, com
  handback validado na 8.11.2, fica para uma troca planejada.
- Regras operacionais vigentes:
  - coordenador parado e purga pausada em restart de storage (#204, tems#112);
  - diff de valores do catálogo depois de cada restart;
  - bootstrap só com causa clara.

## Decisões de arquitetura deste ciclo

- **Correção primeiro:** o NGrid tem que ser correto com escrita em andamento. Gates operacionais do
  cliente (pausar mutações de catálogo em troca de liderança) são defesa extra, não substitutos.
- **Durabilidade das amostras:**
  - com uma cópia por série, usar `OS_CACHE` e commit do offset do Kafka só até o último checkpoint;
  - os metadados (journal e catálogo) continuam com fsync;
  - documentado em `doc/oss/ngrrd-cluster-operacao.md`, seção "Durabilidade das amostras".
- **Formato NGRR:** a v1 sem recuperação de crash fica documentada (contorno pelo archive). A v2,
  com recuperação inspirada no Kafka (validar na abertura), é a evolução; ver
  `planning/2026-10-05-ngrrd-formato-v2-recuperacao-de-crash.md`.
- **Processo de release:**
  - Builder e revisão adversarial independente;
  - testes no JDK 21 com Maven serializado;
  - "pronto" só depois da revisão;
  - PR de outro agente: revisar; se passar, merge e publicação com autorização; se não passar, comentar na PR.

## Pendências (issues abertas)

| Prioridade | Issue | Tema |
|---|---|---|
| 1 | #204 | Stop gracioso do líder pode perder operação confirmada (step-down antes de drenar os seguidores) |
| 2 | #202 | Latência de checkpoint proporcional aos handles (hipótese: msync/jbd2); medir com `OS_CACHE` |
| 3 | #206 | Formato NGRR v2 com recuperação de crash e a ferramenta de migração v1 → v2 |
| 4 | #201 | Testes dos subpacotes do NGrid fora de todos os profiles (e da CI) |
| 5 | #205 | Ressalvas baixas da 8.11.2 (try/finally na promoção, teste do flag de instalação, backoff) |
| 6 | #200 | `tieBreak` da eleição ignora tópicos prioritários; SYNC/HWM sem handler; backstop |
| 7 | #198 | Pausa do checkpoint do NMap cresce com o backlog da fila |
| — | #188–#193 | Anti-entropia, OFFER duplicado em fila, snapshot não persistido, `DistributedMap.close` |

Do lado do tems: tems#112 (timeout de storage derruba o coordenador), contenção no
`NgrrdHandleCache` e o commit do offset do Kafka acoplado ao checkpoint.

## Intermitentes conhecidos

`NMapConcurrentBenchmarkTest`, `LeaveMembershipTest`, `RelayStreamReplicationTest`,
`WeightedGeometryClusterTest` (relacionado à #204) e `PairModeFailoverTest` (travou uma vez durante
a suspensão do host).
