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

package dev.nishisan.utils.oss.cluster.client;

import dev.nishisan.utils.ngrid.common.NodeId;
import dev.nishisan.utils.oss.cluster.metrics.LatencySnapshot;
import dev.nishisan.utils.oss.cluster.protocol.Commands;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Cobre a latência por comando de {@link MetricsTrackingClusterRpc} (F.1). */
class MetricsTrackingClusterRpcTest {

    private static final NodeId TARGET = NodeId.of("storage-0");

    private final RecordingClusterRpc delegate = new RecordingClusterRpc(NodeId.of("client"));
    private final MetricsTrackingClusterRpc rpc = new MetricsTrackingClusterRpc(delegate);

    MetricsTrackingClusterRpcTest() {
        delegate.respondDefault((command, body) -> "ok");
    }

    private void callTimes(String command, int times) {
        for (int i = 0; i < times; i++) {
            rpc.call(TARGET, command, null, String.class);
        }
    }

    @Test
    void latenciaPorComando() {
        callTimes(Commands.PLACE, 3);
        callTimes(Commands.WRITE_BATCH, 5);

        Map<String, LatencySnapshot> byCommand = rpc.latencySnapshotByCommand();

        assertEquals(2, byCommand.size());
        assertEquals(3L, byCommand.get(Commands.PLACE).count());
        assertEquals(5L, byCommand.get(Commands.WRITE_BATCH).count());
        assertEquals(3L, rpc.placeCount());
    }

    @Test
    void comandoDesconhecidoVaiParaOther() {
        for (int i = 0; i < MetricsTrackingClusterRpc.MAX_COMMANDS + 10; i++) {
            callTimes("cmd-" + i, 1);
        }

        Map<String, LatencySnapshot> byCommand = rpc.latencySnapshotByCommand();

        assertEquals(MetricsTrackingClusterRpc.MAX_COMMANDS, byCommand.size());
        assertTrue(byCommand.containsKey(MetricsTrackingClusterRpc.OTHER));
        // 63 nomes próprios + "other"; os 11 nomes restantes (64 + 10 - 63) caem em "other".
        assertEquals(11L, byCommand.get(MetricsTrackingClusterRpc.OTHER).count());
        assertFalse(byCommand.containsKey("cmd-" + (MetricsTrackingClusterRpc.MAX_COMMANDS + 9)));

        // Comando já conhecido continua na sua própria entrada depois do teto.
        callTimes("cmd-0", 1);
        assertEquals(2L, rpc.latencySnapshotByCommand().get("cmd-0").count());
    }

    @Test
    void agregadoContinuaPreenchido() {
        callTimes(Commands.PLACE, 2);
        callTimes(Commands.CHECKPOINT, 4);
        // Falha também é medida, no agregado e no comando.
        delegate.respondDefault((command, body) -> {
            throw new IllegalStateException("falha de proposito");
        });
        assertThrows(IllegalStateException.class, () -> rpc.call(TARGET, Commands.WRITE_BATCH, null, String.class));

        assertEquals(7L, rpc.latencySnapshot().count());
        assertEquals(1L, rpc.latencySnapshotByCommand().get(Commands.WRITE_BATCH).count());
    }
}
