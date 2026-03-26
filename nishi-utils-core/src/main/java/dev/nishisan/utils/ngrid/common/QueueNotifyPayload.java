package dev.nishisan.utils.ngrid.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class QueueNotifyPayload {

    private final String queueName;

    @JsonCreator
    public QueueNotifyPayload(@JsonProperty("queueName") String queueName) {
        this.queueName = Objects.requireNonNull(queueName, "queueName");
    }

    public String queueName() {
        return queueName;
    }
}
