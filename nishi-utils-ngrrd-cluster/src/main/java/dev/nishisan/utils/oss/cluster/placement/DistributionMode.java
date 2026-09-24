package dev.nishisan.utils.oss.cluster.placement;

/** Opt-in distribution strategies shared by placement and rebalance. */
public enum DistributionMode {
    /** Equal series counts (legacy behavior). */
    COUNT,
    /** Series counts proportional to declared capacity. */
    CAPACITY,
    /** Series counts proportional to explicitly configured node weights. */
    WEIGHT
}
