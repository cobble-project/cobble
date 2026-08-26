package io.cobble;

/** Storage policy used when importing files during bucket expansion. */
public enum ExpandStorageMode {
    ADOPT_ASYNC,
    REFERENCE_PERSISTENT,
    REFERENCE_PERSISTENT_WITH_CACHE
}
