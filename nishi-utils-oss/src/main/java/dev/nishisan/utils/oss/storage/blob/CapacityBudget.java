package dev.nishisan.utils.oss.storage.blob;

/** Shared admission arithmetic for declared volume capacity. */
public final class CapacityBudget {
    /** Maximum admitted fraction of a known capacity. */
    public static final double FILL_RATIO = 0.95;

    private CapacityBudget() { }

    /** Exact floor of 95 percent, without overflowing a long. */
    public static long limit(long capacity) {
        return capacity <= 0 ? Long.MAX_VALUE : capacity / 100 * 95 + capacity % 100 * 95 / 100;
    }

    /** Whether an additional allocation fits, including outstanding reservations. */
    public static boolean fits(long capacity, long used, long reserved, long additional) {
        if (used < 0 || reserved < 0 || additional < 0) {
            return false;
        }
        long limit = limit(capacity);
        if (used > limit || reserved > limit - used) {
            return false;
        }
        long available = limit - used - reserved;
        return additional <= available && (capacity <= 0 || used + reserved < limit);
    }
}
