# Posicionamento do NGrid como Infraestrutura Embutida para Microservicos

## Resumo

O NGrid deve ser tratado como uma biblioteca Java embutida para microservicos cooperativos que precisam compartilhar estado pequeno ou medio, eleger lideres/followers e entregar notificacoes confiaveis sem carregar uma plataforma externa pesada.

A tese principal nao e substituir Kafka, Hazelcast, Redis ou uma malha de mensageria corporativa. O valor do projeto esta em oferecer uma primitiva menor, integrada ao dominio dos servicos e adequada para casos onde hoje existe RMI, retry manual, store-and-forward artesanal ou coordenacao ad hoc.

## Posicionamento Recomendado

Uma descricao objetiva para o projeto:

> NGrid e uma biblioteca Java embutida para coordenacao e replicacao leve entre microservicos, com foco em leader/follower, estado compartilhado resiliente e notificacoes confiaveis com store-and-forward.

Esse posicionamento coloca o NGrid no lugar certo:

- coordenacao entre instancias de microservicos que confiam entre si;
- eleicao de lider e acompanhamento por followers;
- replicacao de estado operacional pequeno ou medio;
- notificacoes/eventos internos com entrega resiliente;
- substituicao gradual de RMI com store-and-forward manual;
- operacao simples, sem exigir Kafka, Hazelcast, Redis ou outro servidor dedicado para casos menores.

O NGrid deve ser "menor de proposito". A maturidade do projeto vem mais de clareza de contrato, observabilidade e comportamento previsivel do que de amplitude de features.

## O Que o NGrid Nao Deve Ser

Para proteger o projeto de escopo infinito, vale documentar explicitamente os nao-objetivos:

- nao e Kafka;
- nao e Hazelcast completo;
- nao e Redis Cluster;
- nao e banco de dados distribuido;
- nao e barramento corporativo entre dominios independentes;
- nao promete exactly-once;
- nao deve mirar throughput massivo multi-tenant;
- nao deve implementar partitions, transactions, rebalanceamento sofisticado ou compute grid no v1.

Esses limites nao diminuem o projeto. Eles aumentam a chance de ele ser confiavel no nicho certo.

## Direcao Pos-Relay Log

Com a introducao do relay log no `ReplicationManager`, a recomendacao e congelar features grandes e priorizar maturidade operacional.

Prioridades recomendadas:

- observabilidade forte por fila, mapa e topico;
- controle operacional explicito;
- hardening de recovery, failover e long-running tests;
- clareza de contrato para consumo, replay e ack;
- refatoracao interna gradual do `ReplicationManager`;
- documentacao de limites, SLOs e modos de falha.

Evitar nesta fase:

- novas estruturas distribuidas grandes;
- API de RPC generica;
- semantica exactly-once;
- consumer groups complexos;
- callbacks automaticos com retry/backpressure no core;
- features que aproximem o projeto de um broker completo.

## Modelo Para Substituir RMI

O caso de uso mais forte identificado e substituir microservicos antigos que usam RMI apenas para enviar notificacoes, muitas vezes com store-and-forward implementado manualmente para sobreviver a falhas de conexao.

Nesse cenario, a `NQueue` ja oferece a primitiva correta: persistencia local, replay, compaction e base para entrega resiliente. O refactor ideal e remover o store-and-forward artesanal dos microservicos e concentrar esse comportamento no NGrid.

Contrato recomendado:

- notificacoes/eventos, nao RPC sincrono;
- entrega `at-least-once`;
- idempotencia via `eventId` ou chave de negocio;
- `receive` retorna envelope sem confirmar consumo;
- `ack` explicito confirma processamento;
- "apagado apos consumo" significa delete logico, com limpeza fisica posterior via compaction;
- adapter compativel com o contrato antigo reduz custo de migracao.

Evitar callbacks magicos como API principal por enquanto. Callback parece simples na superficie, mas empurra para o core decisoes sobre thread pool, handler lento, retry, backpressure, shutdown e poison messages. Uma API explicita `send` / `receive` / `ack` e mais previsivel para operacao.

## Estados Operacionais Recomendados

O NGrid deveria expor estados claros para healthchecks, dashboards e automacao:

- `READY`: node convergido e seguro para escrita conforme o modo configurado;
- `SYNCING`: node vivo, mas ainda sincronizando; nao deve aceitar escrita como lider;
- `DEGRADED`: quorum reduzido, lag alto ou resiliencia parcial;
- `READ_ONLY`: node acompanha o cluster, mas nao deve liderar ou aceitar escrita;
- `UNSAFE`: lease expirado, sem quorum, divergencia suspeita ou estado local nao confiavel.

Esses estados ajudam a diferenciar "processo esta vivo" de "node esta seguro para receber trafego".

## Observabilidade Obrigatoria

Observabilidade deve ser tratada como feature de primeira classe. Metricas sugeridas:

- lider atual e epoch;
- quorum esperado vs quorum alcancado;
- tempo restante de leader lease;
- replication lag por topico;
- relay backlog por topico;
- taxa de apply por topico;
- resend count;
- snapshot fallback count;
- tempo medio de convergencia;
- quantidade de eventos recebidos e ainda sem `ack`;
- committed offset por consumidor logico;
- compaction backlog;
- leader churn por janela de tempo;
- erros de handler ou payload poison;
- latencia de fsync e compaction quando aplicavel.

Essas metricas devem aparecer tanto em snapshots operacionais quanto em alertas acionaveis.

## Refatoracao Recomendada do ReplicationManager

O `ReplicationManager` e a peca mais importante e tambem a mais sensivel do sistema. Ele acumula quorum, ordering, resend, sync snapshot, relay log, dedup, recovery, metricas e callbacks de leadership.

Nao e recomendavel reescrever tudo de uma vez. A direcao mais segura e extrair responsabilidades gradualmente, preservando comportamento e testes existentes.

Possiveis componentes internos:

- `QuorumTracker`: controle de acknowledgements, quorum efetivo e timeout de operacao;
- `SequenceGapManager`: buffer ordenado, gaps, resend e fallback para snapshot;
- `SyncCoordinator`: sync de snapshot, chunks, stuck sync e leader sync gate;
- `RelayFollower`: persistencia no relay log e apply loop por topico;
- `LeaderOpLog`: indexacao, retencao e respostas de resend;
- `ReplicationMetrics`: contadores e snapshots especificos de replicacao.

O objetivo dessa refatoracao nao e criar mais abstracao por estetica. E reduzir risco de mudanca, facilitar testes focados e deixar o comportamento distribuido mais auditavel.

## Principio Norteador

O maior risco do NGrid nao e reinventar uma roda. O maior risco e tentar reinventar varias rodas ao mesmo tempo.

A recomendacao central e manter o projeto pequeno de proposito:

- ser excelente para microservicos cooperativos;
- resolver store-and-forward interno melhor do que RMI manual;
- replicar estado operacional com clareza;
- expor saude e controle de forma honesta;
- recusar features que pertencem a brokers, data grids ou bancos distribuidos completos.

Esse recorte torna o NGrid mais facil de operar, mais facil de explicar e mais confiavel para os casos reais que motivaram sua criacao.
