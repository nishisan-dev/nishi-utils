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

package dev.nishisan.utils.ngrid.cluster.transport;

import dev.nishisan.utils.ngrid.cluster.transport.codec.JacksonMessageCodec;
import dev.nishisan.utils.ngrid.common.ClusterMessage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bare TCP client speaking the legacy JSON framing (int length + JSON bytes). Lets a test play
 * an arbitrary peer on the wire: send hand-crafted handshakes/PEER_UPDATEs and capture every
 * JSON frame the transport sends back.
 */
final class RawPeer implements AutoCloseable {
    private final JacksonMessageCodec json = new JacksonMessageCodec();
    private final Socket socket;
    private final DataOutputStream out;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final List<ClusterMessage> received = new CopyOnWriteArrayList<>();
    private final Thread reader;

    RawPeer(int port) throws IOException {
        this("localhost", port);
    }

    RawPeer(String host, int port) throws IOException {
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), 5_000);
        out = new DataOutputStream(socket.getOutputStream());
        DataInputStream in = new DataInputStream(socket.getInputStream());
        reader = Thread.ofVirtual().start(() -> {
            try {
                while (true) {
                    int length = in.readInt();
                    byte[] data = in.readNBytes(length);
                    int offset = data.length > 0 && data[0] == 0x00 ? 1 : 0; // JSON-with-marker frames
                    if (data.length - offset > 0 && data[offset] == '{') {
                        received.add(json.decode(Arrays.copyOfRange(data, offset, data.length)));
                    }
                }
            } catch (IOException ignored) {
                // socket closed
            }
        });
    }

    void writeFrame(byte[] jsonBytes) throws IOException {
        writeLock.lock();
        try {
            out.writeInt(jsonBytes.length);
            out.write(jsonBytes);
            out.flush();
        } finally {
            writeLock.unlock();
        }
    }

    void send(ClusterMessage message) throws IOException {
        writeFrame(json.encode(message));
    }

    List<ClusterMessage> received() {
        return received;
    }

    @Override
    public void close() throws IOException {
        socket.close();
        reader.interrupt();
    }
}
