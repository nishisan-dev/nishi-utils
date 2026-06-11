package dev.nishisan.utils.oss.writer;

import dev.nishisan.utils.oss.engine.PrimaryDataPoint;
import dev.nishisan.utils.oss.format.NgrrdFormatException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * Codec binário do {@link SeriesWriterState}. Stateless e thread-safe.
 *
 * <p>Layout (big-endian), CRC32 cobre todos os bytes anteriores ao próprio CRC:</p>
 *
 * <pre>
 * MAGIC(4)="NGST" version(2) flags(2)
 * blockStartEpochSec(8) baseStepSec(4) blockSizeSec(4)
 * counterPrevCount(4) [ nameLen(2) name(utf8) value(8) tsEpochMs(8) ]*
 * bucketCount(4)      [ nameLen(2) name(utf8) slotCount(4)
 *                       ( sum(8) count(4) min(8) max(8) last(8) missing(4) ){slotCount} ]*
 * CRC32(4)
 * </pre>
 */
public final class SeriesStateCodec {

    /** Magic ASCII {@code "NGST"}. */
    public static final int MAGIC = 0x4E475354;
    public static final int CURRENT_VERSION = 1;

    private static final int SLOT_BYTES = Double.BYTES + Integer.BYTES + Double.BYTES
            + Double.BYTES + Double.BYTES + Integer.BYTES; // 40

    private SeriesStateCodec() {
    }

    public static byte[] encode(SeriesWriterState state) {
        Objects.requireNonNull(state, "state é obrigatório");

        // Pré-serializa nomes para dimensionar o buffer.
        Map<String, byte[]> cpNames = utf8Keys(state.counterPrev().keySet());
        Map<String, byte[]> bkNames = utf8Keys(state.buckets().keySet());

        int size = 4 + 2 + 2 + 8 + 4 + 4; // header fixo
        size += 4; // counterPrevCount
        for (Map.Entry<String, BlockWindow.CounterPrev> e : state.counterPrev().entrySet()) {
            size += 2 + cpNames.get(e.getKey()).length + 8 + 8;
        }
        size += 4; // bucketCount
        for (Map.Entry<String, PrimaryDataPoint.Memento[]> e : state.buckets().entrySet()) {
            size += 2 + bkNames.get(e.getKey()).length + 4 + e.getValue().length * SLOT_BYTES;
        }
        size += 4; // CRC32

        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(MAGIC);
        buf.putShort((short) CURRENT_VERSION);
        buf.putShort((short) 0); // flags
        buf.putLong(state.blockStartEpochSec());
        buf.putInt(state.baseStepSec());
        buf.putInt(state.blockSizeSec());

        buf.putInt(state.counterPrev().size());
        for (Map.Entry<String, BlockWindow.CounterPrev> e : state.counterPrev().entrySet()) {
            byte[] name = cpNames.get(e.getKey());
            buf.putShort((short) name.length);
            buf.put(name);
            buf.putDouble(e.getValue().value());
            buf.putLong(e.getValue().tsEpochMs());
        }

        buf.putInt(state.buckets().size());
        for (Map.Entry<String, PrimaryDataPoint.Memento[]> e : state.buckets().entrySet()) {
            byte[] name = bkNames.get(e.getKey());
            buf.putShort((short) name.length);
            buf.put(name);
            PrimaryDataPoint.Memento[] slots = e.getValue();
            buf.putInt(slots.length);
            for (PrimaryDataPoint.Memento m : slots) {
                buf.putDouble(m.sum());
                buf.putInt(m.count());
                buf.putDouble(m.min());
                buf.putDouble(m.max());
                buf.putDouble(m.last());
                buf.putInt(m.missing());
            }
        }

        byte[] bytes = buf.array();
        int crc = computeCrc(bytes, bytes.length - 4);
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(bytes.length - 4, crc);
        return bytes;
    }

    public static SeriesWriterState decode(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes é obrigatório");
        if (bytes.length < 4 + 2 + 2 + 8 + 4 + 4 + 4 + 4 + 4) {
            throw new NgrrdFormatException("Estado de série truncado: " + bytes.length + " bytes");
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        int magic = buf.getInt();
        if (magic != MAGIC) {
            throw new NgrrdFormatException(
                    String.format("MAGIC de estado inválido: esperado 0x%08X, lido 0x%08X", MAGIC, magic));
        }
        int version = Short.toUnsignedInt(buf.getShort());
        if (version != CURRENT_VERSION) {
            throw new NgrrdFormatException("Versão de estado não suportada: " + version);
        }
        buf.getShort(); // flags (reservado)

        int storedCrc = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt(bytes.length - 4);
        int actualCrc = computeCrc(bytes, bytes.length - 4);
        if (actualCrc != storedCrc) {
            throw new NgrrdFormatException(
                    String.format("CRC32 de estado divergente: armazenado=0x%08X calculado=0x%08X",
                            storedCrc, actualCrc));
        }

        long blockStartEpochSec = buf.getLong();
        int baseStepSec = buf.getInt();
        int blockSizeSec = buf.getInt();

        int cpCount = buf.getInt();
        Map<String, BlockWindow.CounterPrev> counterPrev = new LinkedHashMap<>();
        for (int i = 0; i < cpCount; i++) {
            String name = readName(buf);
            double value = buf.getDouble();
            long tsEpochMs = buf.getLong();
            counterPrev.put(name, new BlockWindow.CounterPrev(value, tsEpochMs));
        }

        int bucketCount = buf.getInt();
        Map<String, PrimaryDataPoint.Memento[]> buckets = new LinkedHashMap<>();
        for (int i = 0; i < bucketCount; i++) {
            String name = readName(buf);
            int slotCount = buf.getInt();
            PrimaryDataPoint.Memento[] slots = new PrimaryDataPoint.Memento[slotCount];
            for (int s = 0; s < slotCount; s++) {
                double sum = buf.getDouble();
                int count = buf.getInt();
                double min = buf.getDouble();
                double max = buf.getDouble();
                double last = buf.getDouble();
                int missing = buf.getInt();
                slots[s] = new PrimaryDataPoint.Memento(sum, count, min, max, last, missing);
            }
            buckets.put(name, slots);
        }

        return new SeriesWriterState(blockStartEpochSec, baseStepSec, blockSizeSec, counterPrev, buckets);
    }

    private static String readName(ByteBuffer buf) {
        int len = Short.toUnsignedInt(buf.getShort());
        byte[] name = new byte[len];
        buf.get(name);
        return new String(name, StandardCharsets.UTF_8);
    }

    private static Map<String, byte[]> utf8Keys(Iterable<String> keys) {
        Map<String, byte[]> out = new LinkedHashMap<>();
        for (String k : keys) {
            out.put(k, k.getBytes(StandardCharsets.UTF_8));
        }
        return out;
    }

    private static int computeCrc(byte[] bytes, int length) {
        CRC32 crc = new CRC32();
        crc.update(bytes, 0, length);
        return (int) crc.getValue();
    }
}
