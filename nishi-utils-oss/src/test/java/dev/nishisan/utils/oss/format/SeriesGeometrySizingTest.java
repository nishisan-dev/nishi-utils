package dev.nishisan.utils.oss.format;

import dev.nishisan.utils.oss.config.NgrrdYamlLoader;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Trava o tamanho em disco ({@code fileTotalBytes}) de uma definição conhecida.
 *
 * <p>O número aqui é o <em>golden</em> que a calculadora de dimensionamento
 * (<code>doc/oss/sizing-calculator.html</code>) replica no seu self-test em
 * JavaScript. Se a geometria do arquivo mudar, este teste falha e sinaliza que o
 * golden do SPA precisa ser atualizado em conjunto — garantindo paridade
 * byte-a-byte entre o engine Java ({@link SeriesGeometry}) e o port JS.</p>
 */
class SeriesGeometrySizingTest {

    @Test
    void fileTotalBytesDoFixtureBlobEhEstavel() throws Exception {
        NgrrdDefinition def;
        try (InputStream in = getClass().getResourceAsStream("/iface-traffic-blob.yaml")) {
            assertNotNull(in, "fixture iface-traffic-blob.yaml deve estar no classpath de teste");
            def = NgrrdYamlLoader.load(in, k -> null);
        }
        SeriesGeometry geo = new SeriesGeometry(def);

        assertEquals(2, geo.columnCount(), "D (colunas) esperado para o fixture");
        assertEquals(3, geo.archiveCount(), "A (archives = rra × cf) esperado para o fixture");
        assertEquals(346104L, geo.fileTotalBytes(),
                "fileTotalBytes mudou — atualize o golden do SPA em doc/oss/sizing-calculator.html");
    }
}
