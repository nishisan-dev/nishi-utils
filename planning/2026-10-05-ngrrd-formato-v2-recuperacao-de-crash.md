# Formato NGRR v2: recuperação de crash (rascunho de desenho)

Issue #206. É um rascunho para revisão do usuário antes de virar plano de implementação.

## Motivação

- Em produção (CTP/tems), a série tem um único dono, e o custo do msync por checkpoint (#202)
  limita a vazão. A decisão foi usar `Durability.OS_CACHE`.
- O formato v1 não é consistente depois de um crash do SO ou de falta de energia sem fsync:
  - o ring é sobrescrito no lugar, e as células não têm CRC;
  - o kernel não garante a ordem de gravação entre as células e o ponteiro;
  - resultado: série perdida (CRC inválido no cabeçalho ou live state) ou valor errado sem sinal.

## Decisões já tomadas

- O produto suporta a **v1** (sem recuperação de crash; documentado, com contorno pelo archive) e a
  **v2** (com recuperação).
- Formato incompatível é aceitável. A v2 é uma evolução, e a migração é feita por uma ferramenta
  dedicada (abaixo).
- **Referência: Kafka.** Não garantir fechamento consistente; validar na abertura e voltar a um
  estado válido. O shutdown limpo é otimização (pula a validação).

## Desenho proposto

1. **Live state duplicado (A/B):** dois slots com número de sequência monotônico e CRC. Cada
   persistência grava no slot alternado; a abertura escolhe o slot válido de maior sequência. O CDP
   em progresso perdido é só um parcial reconstruível.
2. **Linha do ring autodescritiva:**
   - cada linha guarda o timestamp (ou o índice de passo) e um CRC curto dos valores da linha;
   - na leitura e na recuperação, uma linha com timestamp diferente do esperado para a posição, ou
     com CRC inválido, é tratada como **NaN**, a lacuna nativa do RRD;
   - uma escrita interrompida degrada para lacuna, nunca para valor errado.
3. **Marcador de shutdown limpo por volume:** na subida sem o marcador, as séries passam pela
   validação na primeira abertura, ou por uma varredura assíncrona.
4. **Ponto de recuperação:** um checkpoint forçado (fsync) periódico e espaçado (por exemplo,
   10–15 min), com `OS_CACHE` entre um e outro. A sequência do live state forçado é registrada, e só
   o que vem depois dele precisa ser validado na subida suja. É o equivalente ao
   `recovery-point-offset-checkpoint` do Kafka.
5. **Observabilidade:**
   - log com marcador e contagem de linhas invalidadas por série;
   - métricas de séries validadas, reparadas e perdidas.
6. **Compatibilidade:**
   - versão no cabeçalho; o leitor aceita v1 e v2;
   - a escrita usa a versão configurada (padrão v1 até a v2 ser validada);
   - a série v1 continua v1 até ser migrada.

## Custos e riscos a avaliar

- **Espaço:** +8 bytes de timestamp e 4 de CRC por linha (por coluna ou por linha completa, a
  decidir), mais um segundo live state.
- **CPU:** CRC por linha na escrita e na leitura; medir no benchmark do writer.
- **Alcance:** a política de NaN também se aplica a arquivos v2 lidos sem crash (linhas nunca
  escritas já são NaN hoje).
- **Interação com migração, rebalance e cópia** entre storages: copiar bytes v2 sem reinterpretar.

## Ferramenta de migração v1 → v2 (pendência pós-v2)

- **Modos:**
  - offline, por volume;
  - online, por série, num storage em execução (fecha o handle, converte, reabre).
- **Execução:** idempotente e retomável, com o progresso registrado por série; a versão no
  cabeçalho decide o que falta converter.
- **Verificação:** compara as leituras v1 e v2 de cada série convertida.
- **Séries v1 corrompidas:** não são convertidas; ficam listadas para recuperação pelo archive.
- **Convivência com o cluster:** respeita migração, quarentena e geração (não converte série em
  migração ou exclusão).
- **Operação:** métricas, log com marcador e um runbook com backup do volume, uma janela por storage
  e rollback.

## Testes

- Simulação de crash com escritas parciais e fora de ordem:
  - células antes ou depois do ponteiro;
  - live state rasgado;
  - linha rasgada no meio.
- Esperado: nenhum valor errado, só NaN ou o estado anterior válido.
- Leitura de v1 por um leitor v2, e migração v1 → v2 com comparação de leitura.
