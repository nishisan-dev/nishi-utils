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

    /**
     * Definição de archive sem offsets ({@code groupSize}/{@code ringBaseOffset}
     * são derivados): forma de entrada para construir a geometria a partir da
     * definição ou de um arquivo persistido.
     */
    public record ArchiveDef(String rraName, ConsolidationFunction cf, int stepSec, int rows, double xff) {
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
        this(baseStepOf(definition), columnsOf(definition), archiveDefsOf(definition));
    }

    /**
     * Construtor canônico: calcula todos os offsets a partir do step base, das
     * colunas e das definições de archive — usado tanto pela definição quanto
     * pela geometria reconstruída de um arquivo ({@link #fromPersisted}).
     */
    private SeriesGeometry(int baseStepSec, List<Column> columns, List<ArchiveDef> archiveDefs) {
        if (baseStepSec <= 0) {
            throw new IllegalArgumentException("baseStepSec deve ser > 0: " + baseStepSec);
        }
        this.baseStepSec = baseStepSec;
        this.columns = List.copyOf(columns);
        Map<String, Integer> colIndex = new HashMap<>();
        for (int i = 0; i < this.columns.size(); i++) {
            colIndex.put(this.columns.get(i).derivedName(), i);
        }
        this.colIndexByDerivedName = Map.copyOf(colIndex);
        int d = this.columns.size();
        int a = archiveDefs.size();

        // Offsets: seção estática (header fixo + dicionários), live-state e rings.
        long dictBytes = 0;
        for (Column c : this.columns) {
            dictBytes += 2 + utf8Len(c.derivedName()) + 2 + utf8Len(c.rawName()) + 1;
        }
        long archBytes = 0;
        for (ArchiveDef ad : archiveDefs) {
            archBytes += 2 + utf8Len(ad.rraName()) + 1 + 4 + 4 + 8;
        }
        this.staticSectionBytes = SeriesFileCodec.FIXED_HEADER_BYTES + dictBytes + archBytes;
        this.liveStateOffset = align8(staticSectionBytes);
        this.liveStateBytes = liveStateBytes(d, a);
        this.ringDataOffset = align8(liveStateOffset + liveStateBytes);

        // Bases dos rings e tamanho total.
        List<Archive> built = new ArrayList<>(a);
        long ringCursor = ringDataOffset;
        for (ArchiveDef ad : archiveDefs) {
            int groupSize = ad.stepSec() / baseStepSec;
            built.add(new Archive(ad.rraName(), ad.cf(), ad.stepSec(), ad.rows(),
                    ad.xff(), groupSize, ringCursor));
            ringCursor += (long) ad.rows() * d * Double.BYTES;
        }
        this.archives = List.copyOf(built);
        this.fileTotalBytes = ringCursor;
    }

    /**
     * Reconstrói a geometria a partir de um objeto de série persistido (cabeçalho
     * + seção estática), <strong>sem</strong> a definição YAML. Base do diff e da
     * migração de geometria.
     */
    public static SeriesGeometry fromPersisted(SeriesHeader header, byte[] image) {
        SeriesFileCodec.DecodedStatic decoded = SeriesFileCodec.decodeStaticSection(header, image);
        return new SeriesGeometry(header.baseStepSec(), decoded.columns(), decoded.archiveDefs());
    }

    private static int baseStepOf(NgrrdDefinition definition) {
        Objects.requireNonNull(definition, "definition é obrigatório");
        return definition.spec().time().baseStepSec();
    }

    /**
     * Colunas: um por DS, na ordem de declaração, filtradas por
     * {@code archives.appliesTo} (include vazio ⇒ todas). O nome da coluna é o DS
     * derivado quando há {@code derive.output}; senão o próprio nome do DS —
     * paridade RRD para GAUGE/COUNTER/DERIVE/ABSOLUTE sem bloco derive.
     */
    private static List<Column> columnsOf(NgrrdDefinition definition) {
        AppliesTo appliesTo = definition.spec().archives() == null
                ? null : definition.spec().archives().appliesTo();
        Set<String> include = appliesTo == null || appliesTo.include() == null
                ? Set.of() : Set.copyOf(appliesTo.include());
        List<Column> cols = new ArrayList<>();
        for (DataSourceDef ds : definition.spec().dataSources()) {
            String derivedName = ds.derive() != null && ds.derive().output() != null
                    ? ds.derive().output().name()
                    : ds.name();
            if (!include.isEmpty() && !include.contains(derivedName)) {
                continue;
            }
            cols.add(new Column(derivedName, ds.name(), ds.type()));
        }
        return cols;
    }

    /** Archives: produto {@code (rra × cf)} achatado na ordem de declaração. */
    private static List<ArchiveDef> archiveDefsOf(NgrrdDefinition definition) {
        ArchiveSpec archiveSpec = definition.spec().archives();
        List<RraDef> rras = archiveSpec == null ? List.of() : archiveSpec.rras();
        List<ArchiveDef> defs = new ArrayList<>();
        for (RraDef rra : rras) {
            for (ConsolidationFunction cf : rra.cf()) {
                defs.add(new ArchiveDef(rra.name(), cf, rra.stepSec(), rra.rows(), rra.xff()));
            }
        }
        return defs;
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
