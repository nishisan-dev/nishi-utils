package dev.nishisan.utils.ngrid.common;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;

/**
 * Simple heartbeat payload carrying a timestamp from the sender.
 */
public final class HeartbeatPayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final long epochMilli;

    public HeartbeatPayload(long epochMilli) {
        this.epochMilli = epochMilli;
    }

    public static HeartbeatPayload now() {
        return new HeartbeatPayload(Instant.now().toEpochMilli());
    }

    public long epochMilli() {
        return epochMilli;
    }
}
