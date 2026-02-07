package dev.nishisan.utils.queue.helper;

import java.io.Serializable;

/**
 * Wraps a payload with a sequence number for ordered processing.
 *
 * @param <T> the payload type
 */
public class PayloadWrapper<T extends Serializable> {
    private T payload;
    private long seq;

    /**
     * Creates a new payload wrapper.
     *
     * @param payload the payload
     * @param seq     the sequence number
     */
    public PayloadWrapper(T payload, long seq) {
        this.payload = payload;
        this.seq = seq;
    }

    /**
     * Returns the wrapped payload.
     *
     * @return the payload
     */
    public T getPayload() {
        return payload;
    }

    /**
     * Sets the wrapped payload.
     *
     * @param payload the payload
     */
    public void setPayload(T payload) {
        this.payload = payload;
    }

    /**
     * Returns the sequence number.
     *
     * @return the sequence number
     */
    public long getSeq() {
        return seq;
    }

    /**
     * Sets the sequence number.
     *
     * @param seq the sequence number
     */
    public void setSeq(long seq) {
        this.seq = seq;
    }
}
