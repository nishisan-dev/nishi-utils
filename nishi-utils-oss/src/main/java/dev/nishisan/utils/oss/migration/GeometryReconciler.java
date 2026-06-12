package dev.nishisan.utils.oss.migration;

import dev.nishisan.utils.oss.api.OnGeometryChange;
import dev.nishisan.utils.oss.format.NgrrdFormatException;
import dev.nishisan.utils.oss.format.SeriesFileCodec;
import dev.nishisan.utils.oss.format.SeriesGeometry;
import dev.nishisan.utils.oss.format.SeriesHeader;
import dev.nishisan.utils.oss.storage.NgrrdStorage;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

/**
 * Reconcilia a geometria gravada de uma série com a geometria nova <strong>antes</strong>
 * de o writer abrir o objeto para escrita. Idempotente: opera sobre a imagem
 * completa via {@link NgrrdStorage#get}/{@link NgrrdStorage#atomicReplace} (tudo
 * ou nada) e, após retornar, o objeto está com a geometria nova — o writer apenas
 * reidrata.
 *
 * <p>Decisão:</p>
 * <ul>
 *   <li>série inexistente → não faz nada (o writer cria do zero);</li>
 *   <li>geometria idêntica → não faz nada;</li>
 *   <li>geometria divergente, mas {@code schemaRevision} não incrementado →
 *       falha sempre (mudança tratada como não intencional);</li>
 *   <li>geometria divergente com revisão incrementada → aplica
 *       {@link OnGeometryChange} (FAIL/RECREATE/MIGRATE).</li>
 * </ul>
 */
public final class GeometryReconciler {

    private GeometryReconciler() {
    }

    public static void reconcile(NgrrdStorage storage, String storageKey, SeriesGeometry newGeo,
                                 byte[] newHash, int newRevision, OnGeometryChange policy) {
        Objects.requireNonNull(storage, "storage é obrigatório");
        Objects.requireNonNull(policy, "onGeometryChange é obrigatório");

        Optional<byte[]> existing = storage.get(storageKey);
        if (existing.isEmpty() || existing.get().length < SeriesFileCodec.FIXED_HEADER_BYTES) {
            return; // série inexistente: o writer fará createFresh.
        }
        byte[] image = existing.get();

        SeriesHeader header;
        try {
            header = SeriesFileCodec.decodeFixedHeader(image);
        } catch (NgrrdFormatException e) {
            // Arquivo ilegível: não há geometria antiga para migrar.
            if (policy == OnGeometryChange.RECREATE) {
                storage.atomicReplace(storageKey,
                        SeriesFileCodec.buildInitialImage(newGeo, newHash, newRevision));
                return;
            }
            throw new NgrrdGeometryChangeException("Arquivo de série ilegível em " + storageKey
                    + ": " + e.getMessage() + ". Use onGeometryChange: RECREATE para recriar a "
                    + "série (perde a história).", e);
        }

        boolean sameGeometry = header.fileTotalBytes() == newGeo.fileTotalBytes()
                && Arrays.equals(header.definitionHash(), newHash);
        if (sameGeometry) {
            return; // geometria idêntica: nada a reconciliar.
        }

        SeriesGeometry oldGeo = SeriesGeometry.fromPersisted(header, image);
        GeometryDiff diff = GeometryDiff.between(oldGeo, newGeo);
        String report = GeometryChangeReport.render(storageKey, diff,
                header.schemaRevision(), newRevision, newGeo.fileTotalBytes());

        // Gate por revisão: rewrite só dispara com incremento explícito.
        if (newRevision <= header.schemaRevision()) {
            throw new NgrrdGeometryChangeException(report
                    + "\nA geometria mudou mas metadata.schemaRevision não foi incrementado (arquivo="
                    + header.schemaRevision() + ", definition=" + newRevision + "). Incremente "
                    + "schemaRevision para confirmar a mudança intencional.");
        }

        switch (policy) {
            case FAIL -> throw new NgrrdGeometryChangeException(report
                    + "\nonGeometryChange=FAIL: abortando sem tocar na série. Use MIGRATE para "
                    + "preservar a história, RECREATE para recomeçar, ou rode ngrrd-migrate "
                    + "--dry-run para um relatório do dataset.");
            case RECREATE -> storage.atomicReplace(storageKey,
                    SeriesFileCodec.buildInitialImage(newGeo, newHash, newRevision));
            case MIGRATE -> {
                if (!diff.structurallyMigratable()) {
                    throw new NgrrdGeometryChangeException(report
                            + "\nonGeometryChange=MIGRATE: a mudança exige reamostragem (step base "
                            + "ou stepSec/rows de archive) e não é migrável sem perda. Use RECREATE "
                            + "para recomeçar a série.");
                }
                storage.atomicReplace(storageKey,
                        GeometryMigrator.migrate(oldGeo, newGeo, image, newHash, newRevision));
            }
        }
    }
}
