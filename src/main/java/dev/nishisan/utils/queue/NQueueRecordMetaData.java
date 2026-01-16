/*
 *  Copyright (C) 2020-2025 Lucas Nishimura <lucas.nishimura at gmail.com>
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>
 */

package dev.nishisan.utils.queue;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * Represents the metadata for an NQueue record. This class encapsulates
 * information regarding the header structure and its associated properties,
 * such as its length, payload details, and class name.
 *
 * The structure of the metadata header is as follows:
 * [ MAGIC(4 bytes) ][ VERSION(1 byte) ][ HEADER_LEN(4 bytes) ]
 * [ INDEX(8 bytes) ][ PAYLOAD_LEN(4 bytes) ][ CLASSNAME_LEN(2 bytes) ][ CLASSNAME(N bytes) ]
 * After the header, the payload of PAYLOAD_LEN bytes follows.
 *
 * Core functionalities include serialization, deserialization, and size-related calculations
 * for both the prefix and the full header.
 */
public class NQueueRecordMetaData {
    // Layout do registro:
    // [ MAGIC(4) ][ VER(1) ][ HEADER_LEN(4) ][ INDEX(8) ][ PAYLOAD_LEN(4) ][ CLASSNAME_LEN(2) ][ CLASSNAME(N) ]
    // (Depois vem o PAYLOAD de PAYLOAD_LEN bytes)

    public static final int MAGIC = 0x4E_51_4D_44; // 'NQMD'
    public static final byte VERSION_1 = 0x01;
    public static final byte VERSION_2 = 0x02;
    public static final byte CURRENT_VERSION = VERSION_2;

    private static final int MIN_HEADER_LEN = 8 + 4 + 2;
    private static final int MAX_HEADER_LEN = 4096;
    private static final int MAX_CLASSNAME_LEN = 1024;

    private int headerLen;      // tamanho do bloco de header após HEADER_LEN
    private long index;         // índice sequencial
    private long timestamp;     // timestamp de criação (epoch millis)
    private int payloadLen;     // tamanho do payload
    private String className;   // nome da classe (UTF-8)
    private int classNameLen;   // cache do comprimento do nome

    /**
     * Constructs a new instance of NQueueRecordMetaData.
     *
     * @param index The record index, represented as a long value.
     * @param timestamp The creation timestamp in epoch millis.
     * @param payloadLen The length of the payload, represented as an integer.
     * @param className The name of the class, represented as a String in UTF-8 encoding.
     */
    public NQueueRecordMetaData(long index, long timestamp, int payloadLen, String className) {
        this.index = index;
        this.timestamp = timestamp;
        this.payloadLen = payloadLen;
        this.className = className;
        this.classNameLen = className.getBytes(StandardCharsets.UTF_8).length;
        if (this.classNameLen <= 0 || this.classNameLen > MAX_CLASSNAME_LEN) {
            throw new IllegalArgumentException("className length out of bounds");
        }
        // headerLen = tamanho de [INDEX(8) + TIMESTAMP(8) + PAYLOAD_LEN(4) + CLASSNAME_LEN(2) + CLASSNAME(N)]
        this.headerLen = 8 + 8 + 4 + 2 + this.classNameLen;
        if (this.headerLen < MIN_HEADER_LEN || this.headerLen > MAX_HEADER_LEN) {
            throw new IllegalArgumentException("header length out of bounds");
        }
    }
    
    // Legacy constructor for backward compatibility (defaults timestamp to 0)
    public NQueueRecordMetaData(long index, int payloadLen, String className) {
        this(index, 0L, payloadLen, className);
    }

    private NQueueRecordMetaData() {
        // usado no fromBuffer
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }
    
    public long getTimestamp() {
        return timestamp;
    }

    /** Tamanho fixo do prefixo antes do conteúdo do header. */
    public static int fixedPrefixSize() {
        // MAGIC(4) + VER(1) + HEADER_LEN(4)
        return 4 + 1 + 4;
    }

    /** Tamanho total do header incluindo MAGIC/VER/HEADER_LEN. */
    public int totalHeaderSize() {
        return fixedPrefixSize() + headerLen;
    }

    /** Serializa o header completo (MAGIC, VER, HEADER_LEN, INDEX, TIMESTAMP, PAYLOAD_LEN, CLASSNAME_LEN, CLASSNAME). */
    public ByteBuffer toByteBuffer() {
        ByteBuffer buf = ByteBuffer.allocate(totalHeaderSize());
        buf.putInt(MAGIC);
        buf.put(CURRENT_VERSION);
        buf.putInt(headerLen);
        buf.putLong(index);
        buf.putLong(timestamp);
        buf.putInt(payloadLen);
        buf.putShort((short) classNameLen);
        buf.put(className.getBytes(StandardCharsets.UTF_8));
        buf.flip();
        return buf;
    }

    /** Lê apenas o prefixo fixo para descobrir HEADER_LEN (sem avançar canal). */
    public static HeaderPrefix readPrefix(FileChannel ch, long offset) throws IOException {
        ByteBuffer prefix = ByteBuffer.allocate(fixedPrefixSize());
        int r = ch.read(prefix, offset);
        if (r < fixedPrefixSize()) {
            throw new EOFException("Registro incompleto ao ler prefixo.");
        }
        prefix.flip();
        int magic = prefix.getInt();
        if (magic != MAGIC) throw new IOException("MAGIC inválido: " + Integer.toHexString(magic));
        byte ver = prefix.get();
        if (ver != VERSION_1 && ver != VERSION_2) throw new IOException("Versão de header não suportada: " + ver);
        int headerLen = prefix.getInt();
        if (headerLen < MIN_HEADER_LEN || headerLen > MAX_HEADER_LEN) {
            throw new IOException("HEADER_LEN inválido: " + headerLen);
        }
        return new HeaderPrefix(ver, headerLen);
    }

    /** Lê o header inteiro (após já conhecer headerLen e versão). Retorna meta + bytes lidos. */
    public static NQueueRecordMetaData fromBuffer(FileChannel ch, long offset, int headerLen) throws IOException {
        // Para ler a versão precisamos ler o prefixo novamente ou passar como parametro.
        // Como o método `readPrefix` já foi chamado, vamos assumir que quem chama `fromBuffer` sabe o que está fazendo,
        // mas idealmente `fromBuffer` deveria receber a versão também.
        // Vamos ler o prefixo novamente para pegar a versão, é barato (9 bytes em cache de OS).
        HeaderPrefix prefix = readPrefix(ch, offset);
        
        long headerStart = offset + fixedPrefixSize();
        ByteBuffer hb = ByteBuffer.allocate(headerLen);
        int r = ch.read(hb, headerStart);
        if (r < headerLen) {
            throw new EOFException("Header incompleto.");
        }
        hb.flip();

        NQueueRecordMetaData m = new NQueueRecordMetaData();
        m.headerLen = headerLen;

        m.index = hb.getLong();
        
        if (prefix.version >= VERSION_2) {
             m.timestamp = hb.getLong();
        } else {
             m.timestamp = 0L; // Versão 1 não tem timestamp
        }
        
        m.payloadLen = hb.getInt();

        int nameLen = Short.toUnsignedInt(hb.getShort());
        if (nameLen <= 0 || nameLen > MAX_CLASSNAME_LEN) {
            throw new IOException("CLASSNAME_LEN inválido: " + nameLen);
        }
        
        // Validação básica do tamanho esperado
        int expectedLenV1 = 8 + 4 + 2 + nameLen; // index(8) + pLen(4) + cLen(2) + name
        int expectedLenV2 = 8 + 8 + 4 + 2 + nameLen; // index(8) + time(8) + pLen(4) + cLen(2) + name
        
        if (prefix.version == VERSION_1 && headerLen < expectedLenV1) {
             throw new IOException("Header V1 menor que o esperado: " + headerLen);
        }
        if (prefix.version >= VERSION_2 && headerLen < expectedLenV2) {
             throw new IOException("Header V2 menor que o esperado: " + headerLen);
        }

        m.classNameLen = nameLen;

        byte[] nameBytes = new byte[nameLen];
        hb.get(nameBytes);
        m.className = new String(nameBytes, StandardCharsets.UTF_8);

        return m;
    }


    public int getHeaderLen() {
        return headerLen;
    }

    public int getPayloadLen() {
        return payloadLen;
    }

    public int getClassNameLen() {
        return classNameLen;
    }

    public static final class HeaderPrefix {
        public final byte version;
        public final int headerLen;
        public HeaderPrefix(byte version, int headerLen) {
            this.version = version;
            this.headerLen = headerLen;
        }
    }
}
