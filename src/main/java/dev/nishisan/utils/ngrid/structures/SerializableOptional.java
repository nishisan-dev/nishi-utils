package dev.nishisan.utils.ngrid.structures;

import java.io.Serial;
import java.io.Serializable;
import java.util.Optional;

/**
 * Serializable representation of an optional value used in RPC style responses.
 */
public final class SerializableOptional<T extends Serializable> implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private final boolean present;
    private final T value;

    private SerializableOptional(boolean present, T value) {
        this.present = present;
        this.value = value;
    }

    public static <T extends Serializable> SerializableOptional<T> of(T value) {
        return new SerializableOptional<>(true, value);
    }

    public static <T extends Serializable> SerializableOptional<T> empty() {
        return new SerializableOptional<>(false, null);
    }

    public Optional<T> toOptional() {
        return present ? Optional.ofNullable(value) : Optional.empty();
    }

    public boolean isPresent() {
        return present;
    }

    public T value() {
        return value;
    }
}
