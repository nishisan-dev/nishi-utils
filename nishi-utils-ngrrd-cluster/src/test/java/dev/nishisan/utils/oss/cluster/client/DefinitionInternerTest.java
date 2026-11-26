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

package dev.nishisan.utils.oss.cluster.client;

import dev.nishisan.utils.oss.cluster.client.DefinitionInterner.InternedDefinition;
import dev.nishisan.utils.oss.format.DefinitionHash;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre {@link DefinitionInterner}: identidade por conteúdo, teto, concorrência e campos derivados.
 */
class DefinitionInternerTest {

    private static final Path FIXTURE = Path.of("src/test/resources/iface-traffic-blob.yaml");

    @Test
    void teto_padraoEhQuatroMil() {
        assertEquals(4096, DefinitionInterner.MAX_ENTRIES);
    }

    @Test
    void mesmaStringDevolveMesmaInstanciaSemNovoParse() {
        AtomicInteger parses = new AtomicInteger();
        DefinitionInterner interner = new DefinitionInterner(10, countingBuilder(parses));
        String yaml = yamlNamed("alpha");

        InternedDefinition first = interner.intern(yaml);
        InternedDefinition second = interner.intern(yaml);

        assertSame(first, second);
        assertEquals(1, parses.get());
        assertEquals(1, interner.size());
    }

    @Test
    void conteudoIgualEmInstanciasDiferentesDevolveAPublicadaComYamlCanonico() {
        AtomicInteger parses = new AtomicInteger();
        DefinitionInterner interner = new DefinitionInterner(10, countingBuilder(parses));
        String original = yamlNamed("beta");
        String copy = new String(original);
        assertTrue(original != copy, "precisa ser outra instância de String");

        InternedDefinition first = interner.intern(original);
        InternedDefinition second = interner.intern(copy);

        assertSame(first, second);
        assertSame(original, second.yaml(), "o yaml devolvido é a String canônica publicada");
        assertEquals(1, parses.get());
    }

    @Test
    void acimaDoTetoNaoPublicaEContaComoNaoInternada() {
        AtomicInteger parses = new AtomicInteger();
        DefinitionInterner interner = new DefinitionInterner(2, countingBuilder(parses));

        interner.intern(yamlNamed("one"));
        interner.intern(yamlNamed("two"));
        String overflow = yamlNamed("three");
        InternedDefinition third = interner.intern(overflow);

        assertNotNull(third);
        assertEquals(2, interner.size());
        assertEquals(1, interner.notInternedCount());
        assertEquals(3, parses.get());

        // Sem publicação, a mesma definição é recalculada a cada open (comportamento anterior ao interner).
        interner.intern(overflow);
        assertEquals(4, parses.get());
        assertEquals(2, interner.notInternedCount());
        assertEquals(2, interner.size());
    }

    @Test
    void concorrenciaMesmaDefinicaoPublicaUmaVez() throws InterruptedException {
        int threads = 16;
        DefinitionInterner interner = new DefinitionInterner(10, countingBuilder(new AtomicInteger()));
        String yaml = yamlNamed("gamma");
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentHashMap<Integer, InternedDefinition> results = new ConcurrentHashMap<>();
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            int index = i;
            Thread t = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                results.put(index, interner.intern(new String(yaml)));
            });
            workers.add(t);
            t.start();
        }
        start.countDown();
        for (Thread t : workers) t.join(10_000);

        assertEquals(threads, results.size());
        Set<InternedDefinition> distintas = Set.copyOf(results.values());
        assertEquals(1, distintas.size(), "todas as threads recebem a mesma instância publicada");
        assertEquals(1, interner.size());
        assertEquals(0, interner.notInternedCount());
    }

    @Test
    void definicaoRealPreencheCamposDerivadosDoYaml() throws IOException {
        String yaml = Files.readString(FIXTURE, StandardCharsets.UTF_8);
        DefinitionInterner interner = new DefinitionInterner();

        InternedDefinition definition = interner.intern(yaml);

        assertEquals(DefinitionHash.hex(yaml), definition.hashHex());
        assertEquals("device:{deviceId}/iface:{interfaceId}", definition.seriesKeyTemplate());
        assertEquals("iface-traffic-blob", definition.definitionName());
        assertNotNull(definition.geometry());
        assertSame(definition, interner.intern(new String(yaml)));
    }

    @Test
    void yamlInvalidoNaoEhPublicado() {
        DefinitionInterner interner = new DefinitionInterner();

        assertThrows(RuntimeException.class, () -> interner.intern("not: [valid"));
        assertEquals(0, interner.size());
    }

    private static Function<String, InternedDefinition> countingBuilder(AtomicInteger parses) {
        return yaml -> {
            parses.incrementAndGet();
            return new InternedDefinition(yaml, DefinitionHash.hex(yaml), null, "name", "template");
        };
    }

    private static String yamlNamed(String name) {
        try {
            return Files.readString(FIXTURE, StandardCharsets.UTF_8).replace("name: iface-traffic-blob",
                    "name: " + name);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
