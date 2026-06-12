package dev.nishisan.utils.oss.definition;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Metadados da definição (nome lógico, descrição livre, revisão de schema).
 *
 * <p>{@code schemaRevision} é a disciplina que governa o gate de mudança de
 * geometria: um rewrite (MIGRATE/RECREATE) só dispara quando a revisão da
 * definition é maior que a gravada na série. Ausente no YAML desserializa como
 * {@code null}, tratado como {@code 0}. Persistido no campo de 16 bits do
 * cabeçalho da série, logo deve caber em {@code [0, 65535]}.</p>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record NgrrdMetadata(
        @JsonProperty("name") String name,
        @JsonProperty("description") String description,
        @JsonProperty("schemaRevision") Integer schemaRevision
) {

    /** Revisão de schema efetiva: {@code 0} quando ausente no YAML. */
    public int schemaRevisionOrDefault() {
        return schemaRevision == null ? 0 : schemaRevision;
    }
}
