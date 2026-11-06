package io.cobble.structured;

/** File ownership policy for new-database and bucket-expansion imports. */
public enum ImportStorageMode {
    ADOPT_ASYNC,
    REFERENCE_PERSISTENT,
    REFERENCE_PERSISTENT_WITH_CACHE
}
