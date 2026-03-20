package dev.nishisan.utils.ngrid.common;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

public final class QueueNotifyPayload implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final String queueName;

    public QueueNotifyPayload(String queueName) {
        this.queueName = Objects.requireNonNull(queueName, "queueName");
    }

    public String queueName() {
        return queueName;
    }
}
