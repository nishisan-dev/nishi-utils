# NGrid: referências de Kafka e JGroups para a próxima revisão

Status: proposta de evolução, não uma mudança de protocolo entregue na 8.8.0.
Relacionado à [#184](https://github.com/nishisan-dev/nishi-utils/issues/184).
Fontes consultadas em 2026-09-26.

## O que podemos aproveitar

Os três projetos lidam com falhas, entrada e saída de membros, ordenação e recuperação.
Isso permite reutilizar princípios e cenários de teste; não implica garantias equivalentes.

| Referência | Mecanismo relevante | Aplicação proposta no NGrid |
|---|---|---|
| Kafka, KIP-101 | História de épocas de liderança e offsets para localizar divergência entre logs | Associar a fronteira por tópico à sua linhagem e recuperar a partir de um prefixo comum comprovado |
| Kafka, replicação / ELR | Elegibilidade para liderança ligada à preservação do estado confirmado | Separar prova de aptidão de preferências como afinidade, antiguidade e ID |
| Kafka, KRaft | Quórum de consenso específico para metadados do cluster | Avaliar consenso para catálogo, ownership e decisões de migração, com contrato explícito de confirmação |
| JGroups, GMS | Membership representado por visões e escolha de coordenador na ordem do grupo | Versionar mudanças de membros e invalidar mensagens pertencentes a sessões anteriores |
| JGroups, FD / VERIFY_SUSPECT | Detecção, confirmação da suspeita e alteração de membership em etapas | Evitar que um timeout ou callback atrasado desfaça uma reentrada mais recente |

O KRaft governa metadados; a replicação dos dados das partições tem regras próprias.
O Kafka atual também possui Eligible Leader Replicas (ELR), portanto resumir a eleição
como “somente ISR” não cobre todas as configurações atuais.
Fontes: [KRaft](https://kafka.apache.org/40/operations/kraft/),
[ELR](https://kafka.apache.org/43/operations/eligible-leader-replicas/),
[replicação](https://kafka.apache.org/43/design/design/#replication).

A KIP-101 mostra por que um offset isolado não basta para reconciliar histórias diferentes.
É uma referência especialmente próxima da limitação de linhagem registrada na #184.
Fonte: [KIP-101](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/67634337/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation).

O coordenador de membership do JGroups não equivale automaticamente ao líder de um log
durável da aplicação. A escolha pelo membro mais antigo representa antiguidade no grupo,
não uma prova de que ele contém todas as escritas confirmadas.
Fontes: [manual, visões e GMS](https://www.jgroups.org/manual3/index.html),
[manual, VERIFY_SUSPECT](https://www.jgroups.org/manual/pdf/master.pdf).

## Prioridades propostas

### 1. Definir o contrato de confirmação e de linhagem

Para cada tópico, distinguir explicitamente a posição recebida, aplicada, persistida e
confirmada segundo a política de replicação. Um contador maior não pode compensar a falta
de histórico no catálogo ou provar que duas réplicas têm o mesmo conteúdo.

A evolução deve definir como a autoridade de uma nova época é estabelecida e persistida,
como o histórico de épocas é mantido em snapshots e como identificar o prefixo comum.
Ordenar simplesmente `(época, sequência)` também não prova que o candidato contém o prefixo
confirmado. A versão de protocolo e o comportamento em clusters mistos fazem parte do desenho.

Critérios de aceite:

- Mesma sequência com conteúdos de linhagens diferentes exige reconciliação.
- Líder antigo que retorna não pode confirmar escritas usando uma autoridade já revogada.
- Após queda entre replicação e resposta ao cliente, nenhuma escrita confirmada dentro do
  contrato de durabilidade pode desaparecer na eleição seguinte.
- Se o único detentor de uma escrita aceita com quórum 1 for perdido, documentar a perda
  possível; eleição e uptime não recriam uma cópia inexistente.

### 2. Versionar membership e sessões

Modelar a identidade lógica do nó separadamente da encarnação do processo/conexão e da
versão da visão instalada. Especificar quem pode instalar uma nova visão, como resolver
partições e quais confirmações são necessárias. Apenas acrescentar um número de versão
a um heartbeat não cria acordo entre os participantes.

Critérios de aceite:

- Heartbeat, desconexão e suspeita da sessão anterior não alteram a sessão nova.
- Expiração seguida de reentrada produz duas transições ordenadas e exige progresso fresco.
- Suspeita falsa é reversível antes da exclusão; partição e posterior reconexão convergem
  sem permitir duas autoridades de escrita válidas sob o contrato escolhido.

### 3. Entregar eventos de coordenação em ordem, com callbacks fora do lock

A #184 já registra listeners executando sob `leaderComputationLock` (C10). A correção
da retomada usa `ReentrantLock` para evitar pinning de virtual threads no Java 21, mas
mantém os callbacks dentro da região protegida: C10 continua aberto.

A proposta é produzir eventos imutáveis e sequenciados durante a mudança de estado e
entregá-los por um único fluxo ordenado fora do lock. Antes da implementação, definir
limite de fila, tratamento de falha, encerramento e quando os gates de escrita liberam.
Uma notificação assíncrona não pode liberar escrita antes de seus fences estarem instalados.

Critérios de aceite:

- Callback lento não paralisa heartbeats nem o transporte independente.
- Eventos de saída, entrada e liderança preservam a ordem, mesmo com callback reentrante.
- Falha de callback não perde silenciosamente uma transição necessária à segurança.

### 4. Manter desempate como preferência secundária

Aptidão para assumir vem primeiro: autoridade válida e estado necessário comprovado.
Entre candidatos igualmente aptos, aplicar uma ordem determinística compartilhada.

Se houver interesse em antiguidade, preferir uma posição de entrada na visão acordada,
com semântica explícita para restart, em vez de uptime medido por observadores diferentes.
Uptime do host também não representa o tempo de participação do processo no cluster.
Nenhuma mudança no desempate por afinidade/ID foi introduzida nesta retomada.

## Caminho de implementação

Direção esclarecida pelo mantenedor: estudar as abordagens de Kafka e JGroups, validar
sua adequação ao NGrid e desenvolver uma implementação própria. Esses projetos são
referências de mecanismos, premissas e cenários de falha. A adoção de bibliotecas externas
não faz parte desta proposta.

Com os gates da 8.8.0 concluídos, detalhar o follow-up da #184 nesta sequência:

1. **Mapear garantias e premissas.** Para cada mecanismo estudado, identificar qual problema
   resolve, de quais condições depende e como se aplica ao NGrid. Explicitar confirmação de
   escrita, persistência, quórum, identidade de sessão e comportamento durante partições.
2. **Especificar o protocolo próprio.** Descrever estados, mensagens, transições, dados
   persistidos e regras de autoridade antes de alterar a eleição. Separar elegibilidade,
   desempate, confirmação de escrita e recuperação. Preservar as partes já adequadas do NGrid.
3. **Validar o modelo.** Criar cenários controlados com mensagens atrasadas, duplicadas ou
   reordenadas, reinício, partições, troca de membros e falhas entre persistência e resposta.
   Verificar preservação das escritas confirmadas dentro do contrato, rejeição de autoridades
   antigas e retomada do progresso quando as condições de comunicação e quórum forem atendidas.
   Usar os contraexemplos encontrados para revisar a especificação; testes verdes isolados
   não constituem prova geral de correção do protocolo.
4. **Implementar e integrar em etapas.** Introduzir cada mecanismo validado com testes de
   regressão, observabilidade e uma estratégia explícita de compatibilidade de protocolo e
   dados persistidos. Revalidar as garantias na implementação real e medir custo e desempenho.

O primeiro recorte proposto é linhagem e confirmação do catálogo. Antiguidade como desempate
fica subordinada às regras de aptidão definidas nesse desenho. A decisão de implementação
vem da adequação e das evidências obtidas, não apenas da semelhança com outro projeto.

As propostas acima são inferências para o NGrid a partir das referências; não são uma
afirmação de que Kafka ou JGroups usam a mesma arquitetura interna que este repositório.
