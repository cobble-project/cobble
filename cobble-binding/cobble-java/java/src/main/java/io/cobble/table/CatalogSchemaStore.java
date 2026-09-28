package io.cobble.table;

/**
 * Metadata persistence supplied by an external catalog backend. Callbacks may run concurrently on
 * attached JVM threads, so implementations must be thread-safe. Detached writer plans do not call
 * this store.
 */
public interface CatalogSchemaStore {
    /** Returns an immutable, previously published schema version. */
    CatalogSchemaVersion loadSchemaVersion(long tableId, long catalogSchemaId);

    /** Records a shard mapping idempotently so a failed materialization can be retried. */
    void recordShardSchemaMapping(ShardSchemaMapping mapping);
}
