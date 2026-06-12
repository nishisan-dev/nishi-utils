package dev.nishisan.utils.oss.migration;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Renderiza um relatório legível de mudança de geometria a partir de um
 * {@link GeometryDiff} e do contexto da série — o relatório impresso na política
 * {@code FAIL} e agregado pelo CLI {@code ngrrd-migrate}.
 */
public final class GeometryChangeReport {

    private GeometryChangeReport() {
    }

    /**
     * @param seriesKey    chave da série
     * @param diff         diferença estrutural calculada
     * @param fileRevision schemaRevision gravado no arquivo
     * @param defRevision  schemaRevision declarado na definição
     * @param seriesBytes  tamanho (pré-alocado) da série na geometria nova
     */
    public static String render(String seriesKey, GeometryDiff diff,
                                int fileRevision, int defRevision, long seriesBytes) {
        StringBuilder sb = new StringBuilder();
        sb.append("Geometria divergente para a série '").append(seriesKey).append("'\n");

        if (diff.baseStepChanged()) {
            sb.append("  baseStep: ").append(diff.oldBaseStepSec()).append("s -> ")
                    .append(diff.newBaseStepSec()).append("s (ALTERADO)\n");
        } else {
            sb.append("  baseStep: ").append(diff.newBaseStepSec()).append("s (inalterado)\n");
        }

        sb.append("  dataSources:\n");
        for (GeometryDiff.ColumnRef c : diff.addedColumns()) {
            sb.append("    + ").append(c.name()).append(" (").append(c.type()).append(") [nova]\n");
        }
        for (GeometryDiff.ColumnRef c : diff.removedColumns()) {
            sb.append("    - ").append(c.name()).append(" (").append(c.type()).append(") [removida]\n");
        }
        if (!diff.keptColumns().isEmpty()) {
            sb.append("      mantidas: ").append(columnNames(diff.keptColumns())).append('\n');
        }

        sb.append("  archives:\n");
        for (GeometryDiff.ArchiveRef a : diff.addedArchives()) {
            sb.append("    + ").append(archive(a)).append(" [novo]\n");
        }
        for (GeometryDiff.ArchiveRef a : diff.removedArchives()) {
            sb.append("    - ").append(archive(a)).append(" [removido]\n");
        }
        for (GeometryDiff.ArchiveRef a : diff.resampledArchives()) {
            sb.append("    ~ ").append(archive(a)).append(" [reamostragem — não migrável]\n");
        }
        if (!diff.keptArchives().isEmpty()) {
            sb.append("      mantidos: ").append(diff.keptArchives().size()).append(" archive(s)\n");
        }

        sb.append("  schemaRevision: arquivo=").append(fileRevision)
                .append(", definition=").append(defRevision).append('\n');
        sb.append("  Migração estrutural: ")
                .append(diff.structurallyMigratable()
                        ? "POSSÍVEL (use onGeometryChange: MIGRATE para preservar a história)"
                        : "NÃO (requer reamostragem ou mudança de baseStep)")
                .append('\n');
        sb.append("  Tamanho da série: ~").append(humanBytes(seriesBytes));
        return sb.toString();
    }

    private static String columnNames(List<GeometryDiff.ColumnRef> cols) {
        return cols.stream().map(GeometryDiff.ColumnRef::name).collect(Collectors.joining(", "));
    }

    private static String archive(GeometryDiff.ArchiveRef a) {
        return a.rraName() + "/" + a.cf() + " (step=" + a.stepSec() + " rows=" + a.rows() + ")";
    }

    /** Tamanho humano (B/KB/MB/GB), com ponto decimal estável (Locale.ROOT). */
    public static String humanBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        double kb = bytes / 1024.0;
        if (kb < 1024) {
            return String.format(Locale.ROOT, "%.1f KB", kb);
        }
        double mb = kb / 1024.0;
        if (mb < 1024) {
            return String.format(Locale.ROOT, "%.1f MB", mb);
        }
        return String.format(Locale.ROOT, "%.1f GB", mb / 1024.0);
    }
}
