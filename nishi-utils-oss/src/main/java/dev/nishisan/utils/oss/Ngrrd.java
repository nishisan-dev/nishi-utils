package dev.nishisan.utils.oss;

/**
 * Façade pública do formato <strong>ngrrd</strong>.
 *
 * <p>Esta classe é um marco de scaffolding na fase F0 do roteiro de implementação.
 * Os métodos de fábrica e a API completa de leitura/escrita serão introduzidos na
 * fase F7, após a engine, formato binário, backends de storage e writer/reader
 * estarem prontos.</p>
 *
 * <p>Veja o arquivo {@code planning/ngrrd-design.md} para o plano completo.</p>
 */
public final class Ngrrd {

    private Ngrrd() {
        // Façade estática; construtor privado por design.
    }

    /**
     * Identificador do formato persistido em headers de bloco e manifestos.
     *
     * @return string fixa "ngrrd/v1"
     */
    public static String apiVersion() {
        return "ngrrd/v1";
    }
}
