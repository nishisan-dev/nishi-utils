package dev.nishisan.utils.ngrid.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class NGridConfigLoaderInterpolationTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldInterpolateVariables() throws IOException {
        Path yamlFile = tempDir.resolve("ngrid-interpolated.yaml");
        String yamlContent = 
            "node:\n" +
            "  id: ${NODE_ID}\n" +
            "  host: ${HOST:127.0.0.1}\n" +
            "  port: 9000\n" +
            "  dirs:\n" +
            "    base: /tmp/${APP_NAME}/data\n";
        
        Files.writeString(yamlFile, yamlContent);

        Map<String, String> env = new HashMap<>();
        env.put("NODE_ID", "node-from-env");
        env.put("APP_NAME", "my-app");
        // HOST is missing, should use default

        NGridYamlConfig config = NGridConfigLoader.load(yamlFile, env::get);

        assertEquals("node-from-env", config.getNode().getId());
        assertEquals("127.0.0.1", config.getNode().getHost());
        assertEquals("/tmp/my-app/data", config.getNode().getDirs().getBase());
    }

    @Test
    void shouldFailWhenVariableMissingAndNoDefault() throws IOException {
        Path yamlFile = tempDir.resolve("ngrid-missing.yaml");
        String yamlContent = 
            "node:\n" +
            "  id: ${MISSING_VAR}\n";
        
        Files.writeString(yamlFile, yamlContent);

        Map<String, String> env = new HashMap<>();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            NGridConfigLoader.load(yamlFile, env::get);
        });
        
        assertTrue(exception.getMessage().contains("MISSING_VAR"));
    }
}
