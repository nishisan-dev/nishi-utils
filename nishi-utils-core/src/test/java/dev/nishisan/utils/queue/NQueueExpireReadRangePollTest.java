/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Caracteriza o mecanismo-raiz do wedge do RELAY_STREAM no nível do NQueue e valida a primitiva de
 * correção {@link NQueue#pollRecord()}.
 * <p>
 * Sob {@code withExpireAfterWrite}, {@code readRange(0,N)} (peek) NÃO expira o head, mas {@code poll()}
 * expira o prefixo vencido ANTES de consumir. Logo o par "peek em lote + poll por contagem" diverge: o
 * poll pode descartar silenciosamente o head vencido e consumir o PRÓXIMO registro — sumindo uma
 * entrada de uma fila persistida contígua (é exatamente o que travava o {@code drainRelayOnce}).
 * {@link NQueue#pollRecord()} fecha o buraco: devolve EXATAMENTE o registro removido (sequência real),
 * de modo que o consumidor reconcilia "o que peekou" com "o que removeu" e nunca desincroniza.
 */
class NQueueExpireReadRangePollTest {

    private static NQueue.Options relayLikeOptions(Duration ttl) {
        // Espelha RelayStore.open(...): TIME_BASED + clamp + expireAfterWrite.
        return NQueue.Options.defaults()
                .withFsync(false)
                .withShortCircuit(false)
                .withMemoryBuffer(false)
                .withRetentionPolicy(NQueue.Options.RetentionPolicy.TIME_BASED)
                .withRetentionTime(Duration.ofHours(1))
                .withRetentionClampToConsumer(true)
                .withExpireAfterWrite(ttl)
                .withCompactionInterval(Duration.ofHours(1));
    }

    /**
     * Caracterização (documenta o mecanismo do bug): readRange peeka [1,2,3]; o head "1" venceu o TTL
     * entre o peek e o poll; um único poll descarta "1" e retorna "2", e o head salta para "3" — uma
     * entrada some do ponto de vista de um consumidor que peekou e depois poll-ou por contagem.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void readRangePeekThenPollDivergesWhenHeadExpires() throws Exception {
        Path base = Files.createTempDirectory("nqueue-expire-divergence");
        // TTL de 1s; "1" envelhece 1,2s (vence), "2","3" nascem novos (sobrevivem com folga).
        try (NQueue<String> q = NQueue.open(base, "relay", relayLikeOptions(Duration.ofSeconds(1)))) {
            q.offer("1");          // head recebe timestamp velho
            Thread.sleep(1200);    // só "1" vence o TTL de 1s
            q.offer("2");
            q.offer("3");

            List<String> peeked = q.readRange(0, 3).items();
            assertEquals(List.of("1", "2", "3"), peeked, "readRange enxerga os 3 frames contíguos");

            // Um único poll: por causa do skipExpiredRecordsLocked dentro do poll, descarta "1" e
            // retorna "2"; o head vira "3". Confirma a DIVERGÊNCIA peek↔poll que origina o forward-gap.
            Optional<String> polled = q.poll(2, TimeUnit.SECONDS);
            assertEquals("2", polled.orElse(null),
                    "poll descarta o head vencido ('1') e retorna o próximo ('2') — divergência peek↔poll");
            assertEquals(List.of("3"), q.readRange(0, 3).items(),
                    "o head saltou de '1' para '3': '2' foi consumido fora do passo peekado");
        }
    }

    /**
     * Correção: {@link NQueue#pollRecord()} devolve o registro REALMENTE removido (com sua sequência/
     * índice). Mesmo que o head vença entre decisões, o consumidor sempre sabe qual registro saiu e
     * pode reconciliar — nunca aplica/contabiliza um registro diferente do que removeu. Aqui drenamos a
     * fila inteira via pollRecord() e verificamos que NENHUM índice é pulado nem duplicado, apesar do
     * TTL ativo expirando heads sob o consumidor.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void pollRecordReturnsTheActuallyRemovedHeadSoConsumerNeverDesyncs() throws Exception {
        Path base = Files.createTempDirectory("nqueue-pollrecord");
        // TTL de 1s: "1","2" envelhecem 1,2s (vencem); "3","4","5" nascem novos (sobrevivem com folga
        // mesmo sob carga concorrente de testes).
        try (NQueue<String> q = NQueue.open(base, "relay", relayLikeOptions(Duration.ofSeconds(1)))) {
            q.offer("1");
            q.offer("2");
            Thread.sleep(1200); // "1" e "2" vencem o TTL de 1s
            q.offer("3");
            q.offer("4");
            q.offer("5");

            // Peek de lote (como o apply faz) enxerga todos os 5.
            assertEquals(List.of("1", "2", "3", "4", "5"), q.readRange(0, 5).items());

            // Drena via pollRecord(): cada chamada devolve o índice REAL removido (0-based: "1"=0,
            // ..., "5"=4). Verificamos que os índices ENTREGUES são estritamente crescentes (nenhuma
            // entrada some "por baixo" sem o consumidor saber) e que terminam no último frame ("5"=4).
            long lastIndex = -1;
            int delivered = 0;
            while (true) {
                Optional<NQueueRecord> rec = q.pollRecord();
                if (rec.isEmpty()) {
                    break;
                }
                long idx = rec.get().meta().getIndex();
                assertTrue(idx > lastIndex,
                        "pollRecord deve devolver índices estritamente crescentes (sem pulo silencioso); "
                                + "idx=" + idx + " lastIndex=" + lastIndex);
                lastIndex = idx;
                delivered++;
            }

            // Os vencidos "1","2" (idx 0,1) são descartados pelo TTL dentro do pollRecord ANTES de
            // consumir; os sobreviventes "3","4","5" (idx 2,3,4) são devolvidos um a um. O consumidor VÊ
            // exatamente quais índices saíram, então jamais aplica um registro diferente do que removeu.
            assertEquals(3, delivered, "deve entregar exatamente os 3 sobreviventes ('3','4','5')");
            assertEquals(4L, lastIndex, "o último índice removido (0-based) deve ser o do frame '5' = 4");
            assertEquals(0L, q.size(), "a fila drena completa, sem buraco residual");
        }
    }
}
