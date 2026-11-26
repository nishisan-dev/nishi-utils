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

import dev.nishisan.utils.oss.cluster.catalog.GeometryDescriptor;
import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.format.DefinitionHash;
import dev.nishisan.utils.oss.format.SeriesGeometry;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Internação de definições YAML por conteúdo no cliente do cluster ngrrd.
 *
 * <p>Cada {@code open} recebia o YAML e refazia o parse, o SHA-256 e a
 * {@link GeometryDescriptor}, e cada handle guardava uma {@code String} própria.
 * Este componente faz o trabalho uma vez por conteúdo distinto: handles da mesma
 * definição passam a compartilhar a mesma instância de {@code yaml} e de
 * {@code geometry}.</p>
 *
 * <p>O teto {@link #MAX_ENTRIES} protege contra uma definição gerada por série
 * (abuso). Acima dele a definição é calculada sem ser publicada, como antes, e
 * a ocorrência é contada em {@link #notInternedCount()}. O uso normal tem poucas
 * dezenas de definições.</p>
 *
 * <p>O {@code System.getenv} não muda durante o processo, então internar pelo
 * texto bruto do YAML equivale ao parse com interpolação feito por handle.</p>
 */
final class DefinitionInterner {

    /** Teto de definições publicadas. Acima dele, não se internaliza mais. */
    static final int MAX_ENTRIES = 4096;

    private final int maxEntries;
    private final Function<String, InternedDefinition> builder;
    private final ConcurrentHashMap<String, InternedDefinition> byContent = new ConcurrentHashMap<>();
    /** Quantidade de entradas publicadas; reservada antes do {@code putIfAbsent} para respeitar o teto exato. */
    private final AtomicInteger published = new AtomicInteger();
    private final AtomicLong notInterned = new AtomicLong();

    DefinitionInterner() {
        this(MAX_ENTRIES, DefinitionInterner::build);
    }

    /**
     * Construtor para testes: permite um teto menor e um construtor de definição que conte as chamadas.
     *
     * @param maxEntries quantidade máxima de definições publicadas
     * @param builder    faz o parse e o cálculo derivado de um YAML
     */
    DefinitionInterner(int maxEntries, Function<String, InternedDefinition> builder) {
        if (maxEntries < 0) throw new IllegalArgumentException("maxEntries negativo: " + maxEntries);
        this.maxEntries = maxEntries;
        this.builder = Objects.requireNonNull(builder, "builder");
    }

    /**
     * Devolve a definição internada para este conteúdo YAML, parseando só na primeira vez.
     *
     * @param yaml texto da definição
     * @return a instância publicada, cujo {@code yaml} é a {@code String} canônica compartilhada
     */
    InternedDefinition intern(String yaml) {
        Objects.requireNonNull(yaml, "yaml");
        InternedDefinition existing = byContent.get(yaml);
        if (existing != null) return existing;
        InternedDefinition candidate = builder.apply(yaml);
        if (!reserveSlot()) {
            notInterned.incrementAndGet();
            return candidate;
        }
        InternedDefinition previous = byContent.putIfAbsent(yaml, candidate);
        if (previous != null) {
            // Outra thread publicou o mesmo conteúdo antes: libera a reserva e reutiliza a instância publicada.
            published.decrementAndGet();
            return previous;
        }
        return candidate;
    }

    /** Quantidade de definições publicadas. */
    int size() {
        return byContent.size();
    }

    /** Quantas vezes uma definição foi calculada sem ser publicada, por causa do teto. */
    long notInternedCount() {
        return notInterned.get();
    }

    private boolean reserveSlot() {
        for (;;) {
            int current = published.get();
            if (current >= maxEntries) return false;
            if (published.compareAndSet(current, current + 1)) return true;
        }
    }

    private static InternedDefinition build(String yaml) {
        NgrrdDefinition definition = NgrrdYamlLoader.parse(yaml, System::getenv);
        GeometryDescriptor geometry = GeometryDescriptor.from(new SeriesGeometry(definition));
        return new InternedDefinition(yaml, DefinitionHash.hex(yaml), geometry, definition.metadata().name(),
                definition.spec().identity().seriesKeyTemplate());
    }

    /**
     * Definição já analisada, pronta para os handles.
     *
     * @param yaml              texto canônico da definição
     * @param hashHex           SHA-256 em hexadecimal do texto
     * @param geometry          descritor de geometria derivado da definição
     * @param definitionName    nome do {@code metadata}, enviado ao líder no placement
     * @param seriesKeyTemplate template da chave de série
     */
    record InternedDefinition(String yaml, String hashHex, GeometryDescriptor geometry,
                              String definitionName, String seriesKeyTemplate) {
    }
}
