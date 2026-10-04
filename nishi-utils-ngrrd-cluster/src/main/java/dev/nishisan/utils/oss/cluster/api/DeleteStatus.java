package dev.nishisan.utils.oss.cluster.api;

/** Per-series outcome of a conditional deletion. */
public enum DeleteStatus { DELETED, NOT_FOUND, REFUSED_RECENT_WRITE, REFUSED_MIGRATING, ERROR }
