package dev.nishisan.utils.oss.format;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataSourceType;
import dev.nishisan.utils.oss.definition.AppliesTo;
import dev.nishisan.utils.oss.definition.ArchiveSpec;
import dev.nishisan.utils.oss.definition.DataSourceDef;
import dev.nishisan.utils.oss.definition.NgrrdDefinition;
import dev.nishisan.utils.oss.definition.RraDef;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Geometria determinística do arquivo de série NGRR derivada da
 * {@link NgrrdDefinition}. É a autoridade sobre a ordem das colunas (DS
 * derivados), a lista de archives ({@code rra × cf}) e <strong>todos os offsets
 * binários</strong> do layout descrito em {@link SeriesFileCodec}.
 *
 * <p>Como o tamanho do arquivo é totalmente determinado pela definição, a mesma
 * definição sempre produz a mesma geometria — o que permite escrita in-place em
 * offsets fixos e pré-alocação do arquivo.</p>
 */
public final class SeriesGeometry {

    /** Coluna de dado: um DS derivado e o DS raw que o origina. */
    public record Column(String derivedName, String rawName, DataSourceType rawType) {
    }

    /** Archive físico: um par {@code (rra, cf)} com seu ring buffer próprio. */
    public record Archive(String rraName, ConsolidationFunction cf, int stepSec, int rows,
                          double xff, int groupSize, long ringBaseOffset) {
    }

    private final int baseStepSec;
    private final List<Column> columns;
    private final List<Archive> archives;
    private final Map<String, Integer> colIndexByDerivedName;

    private final long staticSectionBytes;
    private final long liveStateOffset;
    private final long liveStateBytes;
    private final long ringDataOffset;
    private final long fileTotalBytes;

    public SeriesGeometry(NgrrdDefinition definition) {
        Objects.requireNonNull(definition, "definition é obrigatório");
        this.baseStepSec = definition.spec().time().baseStepSec();
        if (baseStepSec <= 0) {
            throw new IllegalArgumentException("baseStepSec deve ser > 0: " + baseStepSec);
        }

        // Colunas: um por DS, na ordem de declaração, filtradas por
        // archives.appliesTo (include vazio ⇒ todas). O nome da coluna é o DS
        // derivado quando há derive.output; senão o próprio nome do DS — paridade
        // RRD para GAUGE/COUNTER/DERIVE/ABSOLUTE sem bloco derive.
        AppliesTo appliesTo = definition.spec().archives() == null
                ? null : definition.spec().archives().appliesTo();
        Set<String> include = appliesTo == null || appliesTo.include() == null
                ? Set.of() : Set.copyOf(appliesTo.include());
        List<Column> cols = new ArrayList<>();
        Map<String, Integer> colIndex = new HashMap<>();
        for (DataSourceDef ds : definition.spec().dataSources()) {
            String derivedName = ds.derive() != null && ds.derive().output() != null
                    ? ds.derive().output().name()
                    : ds.name();
            if (!include.isEmpty() && !include.contains(derivedName)) {
                continue;
            }
            colIndex.put(derivedName, cols.size());
            cols.add(new Column(derivedName, ds.name(), ds.type()));
        }
        this.columns = List.copyOf(cols);
        this.colIndexByDerivedName = Map.copyOf(colIndex);
        int d = columns.size();

        // Archives: produto (rra × cf) achatado na ordem de declaração.
        ArchiveSpec archiveSpec = definition.spec().archives();
        List<RraDef> rras = archiveSpec == null ? List.of() : archiveSpec.rras();
        List<int[]> archGeom = new ArrayList<>(); // [stepSec, rows]
        List<String> archRra = new ArrayList<>();
        List<ConsolidationFunction> archCf = new ArrayList<>();
        List<Double> archXff = new ArrayList<>();
        for (RraDef rra : rras) {
            for (ConsolidationFunction cf : rra.cf()) {
                archRra.add(rra.name());
                archCf.add(cf);
                archGeom.add(new int[]{rra.stepSec(), rra.rows()});
                archXff.add(rra.xff());
            }
        }
        int a = archRra.size();

        // Offsets: seção estática (header fixo + dicionários), live-state e rings.
        long dictBytes = 0;
        for (Column c : columns) {
            dictBytes += 2 + utf8Len(c.derivedName()) + 2 + utf8Len(c.rawName()) + 1;
        }
        long archBytes = 0;
        for (String rraName : archRra) {
            archBytes += 2 + utf8Len(rraName) + 1 + 4 + 4 + 8;
        }
        this.staticSectionBytes = SeriesFileCodec.FIXED_HEADER_BYTES + dictBytes + archBytes;
        this.liveStateOffset = align8(staticSectionBytes);
        this.liveStateBytes = liveStateBytes(d, a);
        this.ringDataOffset = align8(liveStateOffset + liveStateBytes);

        // Bases dos rings e tamanho total.
        List<Archive> built = new ArrayList<>(a);
        long ringCursor = ringDataOffset;
        for (int i = 0; i < a; i++) {
            int stepSec = archGeom.get(i)[0];
            int rows = archGeom.get(i)[1];
            int groupSize = stepSec / baseStepSec;
            built.add(new Archive(archRra.get(i), archCf.get(i), stepSec, rows,
                    archXff.get(i), groupSize, ringCursor));
            ringCursor += (long) rows * d * Double.BYTES;
        }
        this.archives = List.copyOf(built);
        this.fileTotalBytes = ringCursor;
    }

    /** Tamanho da seção de live-state para {@code d} colunas e {@code a} archives. */
    public static long liveStateBytes(int d, int a) {
        long cp = (long) d * (Double.BYTES + Long.BYTES);                 // counterPrev
        long pdp = (long) d * (Double.BYTES + Integer.BYTES + Double.BYTES
                + Double.BYTES + Double.BYTES + Integer.BYTES + Long.BYTES); // PDP + slot
        long ptr = (long) a * (Integer.BYTES + Long.BYTES);              // ring ptr
        long cdp = (long) a * d * (Double.BYTES + Integer.BYTES + Integer.BYTES); // CDP prep
        return Long.BYTES + cp + pdp + ptr + cdp + Integer.BYTES;        // lastUp + ... + crc
    }

    public int baseStepSec() {
        return baseStepSec;
    }

    public List<Column> columns() {
        return columns;
    }

    public List<Archive> archives() {
        return archives;
    }

    public int columnCount() {
        return columns.size();
    }

    public int archiveCount() {
        return archives.size();
    }

    /** Índice da coluna do DS derivado, ou {@code -1} se não houver. */
    public int columnIndex(String derivedName) {
        Integer i = colIndexByDerivedName.get(derivedName);
        return i == null ? -1 : i;
    }

    /** Índice do archive {@code (rraName, cf)}, ou {@code -1} se não houver. */
    public int archiveIndex(String rraName, ConsolidationFunction cf) {
        for (int i = 0; i < archives.size(); i++) {
            Archive a = archives.get(i);
            if (a.rraName().equals(rraName) && a.cf() == cf) {
                return i;
            }
        }
        return -1;
    }

    public long staticSectionBytes() {
        return staticSectionBytes;
    }

    public long liveStateOffset() {
        return liveStateOffset;
    }

    public long liveStateBytes() {
        return liveStateBytes;
    }

    public long ringDataOffset() {
        return ringDataOffset;
    }

    public long fileTotalBytes() {
        return fileTotalBytes;
    }

    /** Offset absoluto da célula {@code (archive, row, col)} na região de rings. */
    public long cellOffset(int archiveIndex, int row, int col) {
        Archive a = archives.get(archiveIndex);
        return a.ringBaseOffset() + ((long) row * columnCount() + col) * Double.BYTES;
    }

    /**
     * Hash SHA-256 sobre os campos que definem a geometria física (step base,
     * colunas e archives). Gravado no cabeçalho do arquivo; divergência força a
     * recriação (clean cut) pois os offsets mudaram.
     */
    public byte[] geometryHash() {
        StringBuilder sb = new StringBuilder();
        sb.append("base=").append(baseStepSec).append(";cols=");
        for (Column c : columns) {
            sb.append(c.derivedName()).append('|').append(c.rawName())
                    .append('|').append(c.rawType().ordinal()).append(',');
        }
        sb.append(";arch=");
        for (Archive a : archives) {
            sb.append(a.rraName()).append('|').append(a.cf().ordinal()).append('|')
                    .append(a.stepSec()).append('|').append(a.rows()).append('|')
                    .append(a.xff()).append(',');
        }
        return DefinitionHash.sha256(sb.toString());
    }

    static long align8(long v) {
        return (v + 7L) & ~7L;
    }

    private static int utf8Len(String s) {
        return s.getBytes(StandardCharsets.UTF_8).length;
    }
}
