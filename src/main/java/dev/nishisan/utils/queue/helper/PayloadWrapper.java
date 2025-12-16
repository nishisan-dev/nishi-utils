package dev.nishisan.utils.queue.helper;

import java.io.Serializable;

public class PayloadWrapper<T extends Serializable> {
    private T payload;
    private long seq;

    public PayloadWrapper(T payload, long seq) {
        this.payload = payload;
        this.seq = seq;
    }

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }
}
