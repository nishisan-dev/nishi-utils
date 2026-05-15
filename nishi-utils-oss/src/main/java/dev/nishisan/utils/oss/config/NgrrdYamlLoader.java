package dev.nishisan.utils.oss.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.function.Function;

/**
 * Carrega definições ngrrd a partir de YAML aplicando interpolação de variáveis
 * de ambiente e validação semântica.
 *
 * <p>Uso típico:</p>
 *
 * <pre>{@code
 * NgrrdDefinition def = NgrrdYamlLoader.load(Path.of("series.yaml"));
 * }</pre>
 *
 * <p>O método entry-point aceita um resolver custom para facilitar testes:</p>
 *
 * <pre>{@code
 * NgrrdDefinition def = NgrrdYamlLoader.load(path, Map.of("ENV", "stg")::get);
 * }</pre>
 */
public final class NgrrdYamlLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory())
            .registerModule(new JavaTimeModule())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private NgrrdYamlLoader() {
    }

    /**
     * Carrega de um arquivo, usando {@link System#getenv(String)} como resolver
     * de variáveis.
     */
    public static NgrrdDefinition load(Path yamlFile) {
        return load(yamlFile, System::getenv);
    }

    /**
     * Carrega de um arquivo com resolver explícito (recomendado em testes).
     */
    public static NgrrdDefinition load(Path yamlFile, Function<String, String> envResolver) {
        Objects.requireNonNull(yamlFile, "yamlFile é obrigatório");
        try {
            String raw = Files.readString(yamlFile, StandardCharsets.UTF_8);
            NgrrdDefinition def = parse(raw, envResolver);
            NgrrdDefinitionValidator.validate(def);
            return def;
        } catch (IOException e) {
            throw new NgrrdDefinitionException("Falha ao ler YAML em " + yamlFile, e);
        }
    }

    /**
     * Carrega de um {@link InputStream}, útil para classpath resources.
     */
    public static NgrrdDefinition load(InputStream input, Function<String, String> envResolver) {
        Objects.requireNonNull(input, "input é obrigatório");
        try {
            String raw = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            NgrrdDefinition def = parse(raw, envResolver);
            NgrrdDefinitionValidator.validate(def);
            return def;
        } catch (IOException e) {
            throw new NgrrdDefinitionException("Falha ao ler YAML do InputStream", e);
        }
    }

    /**
     * Faz parse a partir de uma string já em memória. Não aplica validação
     * semântica — útil para testes unitários do parser.
     */
    public static NgrrdDefinition parse(String yaml, Function<String, String> envResolver) {
        Function<String, String> resolver = envResolver != null ? envResolver : System::getenv;
        String interpolated = VariableInterpolator.interpolate(yaml, resolver);
        try {
            return MAPPER.readValue(interpolated, NgrrdDefinition.class);
        } catch (IOException e) {
            throw new NgrrdDefinitionException("YAML inválido: " + e.getMessage(), e);
        }
    }
}
