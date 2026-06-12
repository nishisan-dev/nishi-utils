package dev.nishisan.utils.oss.format;

import dev.nishisan.utils.oss.api.ConsolidationFunction;
import dev.nishisan.utils.oss.api.DataSourceType;
import dev.nishisan.utils.oss.engine.PrimaryDataPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * Codec do arquivo de série <strong>NGRR</strong>: um único objeto binário por
 * série, em paridade com o arquivo {@code .rrd} do RRDtool (geometria fixa,
 * pré-alocado, ring buffers in-place). Stateless e thread-safe.
 *
 * <p>Layout (big-endian), com offsets determinísticos vindos de
 * {@link SeriesGeometry}:</p>
 *
 * <pre>
 * === SEÇÃO ESTÁTICA (imutável após create) ===
 * 0  MAGIC "NGRR" | 4 version u16 | 6 schemaRevision u16 | 8 baseStepSec i32
 * 12 definitionHash SHA-256 (32) | 44 D i32 | 48 A i32
 * 52 staticSectionBytes i64 | 60 liveStateOffset i64 | 68 liveStateBytes i64
 * 76 ringDataOffset i64 | 84 fileTotalBytes i64 | 92 headerCrc32 i32  (CRC sobre [0..92))
 * 96 DS_DICT[D]   { nameLen u16, name, originRawLen u16, originRaw, dsType u8 }
 *    ARCH_TABLE[A]{ rraNameLen u16, rraName, cf u8, stepSec i32, rows i32, xff f64 }
 *    (pad 8-align até liveStateOffset)
 *
 * === LIVE-STATE (tamanho fixo, sobrescrito in-place) ===
 * lastUpEpochMs i64
 * D × { prevValue f64, prevTsEpochMs i64 }                       -- last_ds
 * D × { sum f64, count i32, min f64, max f64, last f64, missing i32, slotSec i64 }
 * A × { curRow i32, curRowEpochSec i64 }                         -- rra_ptr
 * A*D × { cdpPartial f64, foldedCount i32, missingCount i32 }    -- cdp_prep
 * liveStateCrc32 i32   (CRC sobre os bytes anteriores da live-state)
 *
 * === RING-DATA (8-align, sobrescrito in-place) ===
 * por archive a: rows × D doubles (8 bytes), row-major (row,col); NaN = missing
 * </pre>
 */
public final class SeriesFileCodec {

    /** Magic ASCII {@code "NGRR"} = {@code 0x4E475252}. */
    public static final int MAGIC = 0x4E475252;
    /** Versão atual do formato de série. */
    public static final int CURRENT_VERSION = 1;
    /** Tamanho do cabeçalho fixo (sem dicionários). */
    public static final int FIXED_HEADER_BYTES = 96;
    /** Offset do CRC32 do cabeçalho fixo (cobre {@code [0..92)}). */
    public static final int HEADER_CRC_OFFSET = 92;

    private SeriesFileCodec() {
    }

    /** Limite do campo de revisão de schema (16 bits no cabeçalho). */
    public static final int MAX_SCHEMA_REVISION = 0xFFFF;

    /**
     * Constrói a imagem completa pré-alocada de uma série vazia: seção estática +
     * live-state vazia + rings preenchidos com {@code NaN}.
     */
    public static byte[] buildInitialImage(SeriesGeometry geo, byte[] definitionHash, int schemaRevision) {
        long total = geo.fileTotalBytes();
        if (total > Integer.MAX_VALUE) {
            throw new NgrrdFormatException("Série excede o tamanho máximo suportado: " + total + " bytes");
        }
        byte[] image = new byte[(int) total];

        byte[] staticSection = encodeStaticSection(geo, definitionHash, schemaRevision);
        System.arraycopy(staticSection, 0, image, 0, staticSection.length);

        byte[] live = encodeLiveState(geo, new SeriesLiveState(geo.columnCount(), geo.archiveCount()));
        System.arraycopy(live, 0, image, (int) geo.liveStateOffset(), live.length);

        // Rings: NaN em todas as células.
        ByteBuffer buf = ByteBuffer.wrap(image).order(ByteOrder.BIG_ENDIAN);
        for (int pos = (int) geo.ringDataOffset(); pos + Double.BYTES <= image.length; pos += Double.BYTES) {
            buf.putDouble(pos, Double.NaN);
        }
        return image;
    }

    /** Serializa a seção estática (cabeçalho fixo + dicionários + pad até live-state). */
    public static byte[] encodeStaticSection(SeriesGeometry geo, byte[] definitionHash, int schemaRevision) {
        Objects.requireNonNull(definitionHash, "definitionHash é obrigatório");
        if (definitionHash.length != DefinitionHash.BYTES) {
            throw new IllegalArgumentException("definitionHash deve ter " + DefinitionHash.BYTES + " bytes");
        }
        if (schemaRevision < 0 || schemaRevision > MAX_SCHEMA_REVISION) {
            throw new IllegalArgumentException(
                    "schemaRevision deve estar em [0, " + MAX_SCHEMA_REVISION + "]: " + schemaRevision);
        }
        byte[] bytes = new byte[(int) geo.liveStateOffset()];
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

        buf.putInt(MAGIC);
        buf.putShort((short) CURRENT_VERSION);
        buf.putShort((short) schemaRevision); // revisão de schema (16 bits)
        buf.putInt(geo.baseStepSec());
        buf.put(definitionHash);
        buf.putInt(geo.columnCount());
        buf.putInt(geo.archiveCount());
        buf.putLong(geo.staticSectionBytes());
        buf.putLong(geo.liveStateOffset());
        buf.putLong(geo.liveStateBytes());
        buf.putLong(geo.ringDataOffset());
        buf.putLong(geo.fileTotalBytes());
        buf.putInt(0); // headerCrc placeholder @ HEADER_CRC_OFFSET

        // DS dictionary.
        for (SeriesGeometry.Column c : geo.columns()) {
            putUtf8(buf, c.derivedName());
            putUtf8(buf, c.rawName());
            buf.put((byte) c.rawType().ordinal());
        }
        // Archive table.
        for (SeriesGeometry.Archive a : geo.archives()) {
            putUtf8(buf, a.rraName());
            buf.put((byte) a.cf().ordinal());
            buf.putInt(a.stepSec());
            buf.putInt(a.rows());
            buf.putDouble(a.xff());
        }

        int crc = crc(bytes, 0, HEADER_CRC_OFFSET);
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(HEADER_CRC_OFFSET, crc);
        return bytes;
    }

    /** Decodifica o cabeçalho fixo (96 bytes) validando MAGIC, versão e CRC. */
    public static SeriesHeader decodeFixedHeader(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes é obrigatório");
        if (bytes.length < FIXED_HEADER_BYTES) {
            throw new NgrrdFormatException("Cabeçalho de série truncado: " + bytes.length + " bytes");
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        int magic = buf.getInt();
        if (magic != MAGIC) {
            throw new NgrrdFormatException(
                    String.format("MAGIC inválido: esperado 0x%08X, lido 0x%08X", MAGIC, magic));
        }
        int version = Short.toUnsignedInt(buf.getShort());
        if (version != CURRENT_VERSION) {
            throw new NgrrdFormatException("Versão de série não suportada: " + version);
        }
        int schemaRevision = Short.toUnsignedInt(buf.getShort());
        int baseStepSec = buf.getInt();
        byte[] defHash = new byte[DefinitionHash.BYTES];
        buf.get(defHash);
        int d = buf.getInt();
        int a = buf.getInt();
        long staticSectionBytes = buf.getLong();
        long liveStateOffset = buf.getLong();
        long liveStateBytes = buf.getLong();
        long ringDataOffset = buf.getLong();
        long fileTotalBytes = buf.getLong();
        int storedCrc = buf.getInt();

        int actualCrc = crc(bytes, 0, HEADER_CRC_OFFSET);
        if (actualCrc != storedCrc) {
            throw new NgrrdFormatException(String.format(
                    "CRC32 de cabeçalho divergente: armazenado=0x%08X calculado=0x%08X", storedCrc, actualCrc));
        }
        return new SeriesHeader(version, schemaRevision, baseStepSec, defHash, d, a,
                staticSectionBytes, liveStateOffset, liveStateBytes, ringDataOffset, fileTotalBytes);
    }

    /**
     * Colunas e definições de archive decodificadas da seção estática de um
     * arquivo persistido (sem a definição YAML).
     */
    public record DecodedStatic(List<SeriesGeometry.Column> columns,
                                List<SeriesGeometry.ArchiveDef> archiveDefs) {
    }

    /**
     * Decodifica o dicionário de DS ({@code DS_DICT}) e a tabela de archives
     * ({@code ARCH_TABLE}) que seguem o cabeçalho fixo, reconstruindo as colunas
     * e archives sem a definição. Espelho de {@link #encodeStaticSection}.
     *
     * @param header cabeçalho fixo já decodificado (fornece {@code D} e {@code A})
     * @param image  imagem do objeto da série (ao menos a seção estática)
     */
    public static DecodedStatic decodeStaticSection(SeriesHeader header, byte[] image) {
        Objects.requireNonNull(header, "header é obrigatório");
        Objects.requireNonNull(image, "image é obrigatório");
        if (image.length < header.staticSectionBytes()) {
            throw new NgrrdFormatException("Seção estática truncada: " + image.length
                    + " < " + header.staticSectionBytes());
        }
        ByteBuffer buf = ByteBuffer.wrap(image).order(ByteOrder.BIG_ENDIAN);
        buf.position(FIXED_HEADER_BYTES);

        int d = header.columnCount();
        List<SeriesGeometry.Column> columns = new ArrayList<>(d);
        for (int i = 0; i < d; i++) {
            String derivedName = getUtf8(buf);
            String rawName = getUtf8(buf);
            int typeOrdinal = Byte.toUnsignedInt(buf.get());
            columns.add(new SeriesGeometry.Column(derivedName, rawName, enumValue(
                    DataSourceType.values(), typeOrdinal, "dsType")));
        }
        int a = header.archiveCount();
        List<SeriesGeometry.ArchiveDef> archiveDefs = new ArrayList<>(a);
        for (int i = 0; i < a; i++) {
            String rraName = getUtf8(buf);
            int cfOrdinal = Byte.toUnsignedInt(buf.get());
            int stepSec = buf.getInt();
            int rows = buf.getInt();
            double xff = buf.getDouble();
            archiveDefs.add(new SeriesGeometry.ArchiveDef(rraName,
                    enumValue(ConsolidationFunction.values(), cfOrdinal, "cf"), stepSec, rows, xff));
        }
        return new DecodedStatic(columns, archiveDefs);
    }

    /** Serializa a live-state (tamanho {@code geo.liveStateBytes()}) com CRC32 no fim. */
    public static byte[] encodeLiveState(SeriesGeometry geo, SeriesLiveState st) {
        int d = geo.columnCount();
        int a = geo.archiveCount();
        byte[] bytes = new byte[(int) geo.liveStateBytes()];
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);

        buf.putLong(st.lastUpEpochMs);
        for (int i = 0; i < d; i++) {
            buf.putDouble(st.counterPrevValue[i]);
            buf.putLong(st.counterPrevTsMs[i]);
        }
        for (int i = 0; i < d; i++) {
            PrimaryDataPoint.Memento m = st.pdp[i].snapshot();
            buf.putDouble(m.sum());
            buf.putInt(m.count());
            buf.putDouble(m.min());
            buf.putDouble(m.max());
            buf.putDouble(m.last());
            buf.putInt(m.missing());
            buf.putLong(st.pdpSlotSec[i]);
        }
        for (int x = 0; x < a; x++) {
            buf.putInt(st.curRow[x]);
            buf.putLong(st.curRowEpochSec[x]);
        }
        for (int x = 0; x < a * d; x++) {
            buf.putDouble(st.cdpPartial[x]);
            buf.putInt(st.cdpFolded[x]);
            buf.putInt(st.cdpMissing[x]);
        }

        int crc = crc(bytes, 0, bytes.length - 4);
        buf.putInt(crc);
        return bytes;
    }

    /** Decodifica a live-state validando o CRC32. Lança em divergência. */
    public static SeriesLiveState decodeLiveState(SeriesGeometry geo, byte[] bytes) {
        int d = geo.columnCount();
        int a = geo.archiveCount();
        int expected = (int) geo.liveStateBytes();
        if (bytes.length != expected) {
            throw new NgrrdFormatException(
                    "Live-state com tamanho inesperado: " + bytes.length + " (esperado " + expected + ")");
        }
        int storedCrc = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt(bytes.length - 4);
        int actualCrc = crc(bytes, 0, bytes.length - 4);
        if (actualCrc != storedCrc) {
            throw new NgrrdFormatException(String.format(
                    "CRC32 de live-state divergente: armazenado=0x%08X calculado=0x%08X", storedCrc, actualCrc));
        }

        SeriesLiveState st = new SeriesLiveState(d, a);
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        st.lastUpEpochMs = buf.getLong();
        for (int i = 0; i < d; i++) {
            st.counterPrevValue[i] = buf.getDouble();
            st.counterPrevTsMs[i] = buf.getLong();
        }
        for (int i = 0; i < d; i++) {
            double sum = buf.getDouble();
            int count = buf.getInt();
            double min = buf.getDouble();
            double max = buf.getDouble();
            double last = buf.getDouble();
            int missing = buf.getInt();
            long slot = buf.getLong();
            st.pdp[i] = PrimaryDataPoint.restore(new PrimaryDataPoint.Memento(sum, count, min, max, last, missing));
            st.pdpSlotSec[i] = slot;
        }
        for (int x = 0; x < a; x++) {
            st.curRow[x] = buf.getInt();
            st.curRowEpochSec[x] = buf.getLong();
        }
        for (int x = 0; x < a * d; x++) {
            st.cdpPartial[x] = buf.getDouble();
            st.cdpFolded[x] = buf.getInt();
            st.cdpMissing[x] = buf.getInt();
        }
        return st;
    }

    /** Serializa um único valor de célula de ring (8 bytes big-endian). */
    public static byte[] doubleBytes(double value) {
        return ByteBuffer.allocate(Double.BYTES).order(ByteOrder.BIG_ENDIAN).putDouble(value).array();
    }

    /** Lê um valor double big-endian de um array de 8 bytes. */
    public static double readDouble(byte[] cell) {
        return ByteBuffer.wrap(cell).order(ByteOrder.BIG_ENDIAN).getDouble();
    }

    private static void putUtf8(ByteBuffer buf, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        buf.putShort((short) b.length);
        buf.put(b);
    }

    private static String getUtf8(ByteBuffer buf) {
        int len = Short.toUnsignedInt(buf.getShort());
        byte[] b = new byte[len];
        buf.get(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static <E extends Enum<E>> E enumValue(E[] values, int ordinal, String field) {
        if (ordinal < 0 || ordinal >= values.length) {
            throw new NgrrdFormatException(field + " inválido na seção estática: " + ordinal);
        }
        return values[ordinal];
    }

    private static int crc(byte[] bytes, int off, int len) {
        CRC32 crc = new CRC32();
        crc.update(bytes, off, len);
        return (int) crc.getValue();
    }
}
