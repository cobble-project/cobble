package io.cobble.table;

import io.cobble.Config;
import io.cobble.ImportStorageMode;
import io.cobble.NativeLoader;
import io.cobble.NativeObject;
import io.cobble.RescaleOptions;
import io.cobble.ShardSnapshot;

import java.util.Objects;
import java.util.function.LongFunction;

/**
 * Owns one writable database shard shared by multiple typed tables.
 *
 * <p>Snapshots and bucket changes apply to the entire store. Close every table, projection, and
 * scanner from this store before expanding or shrinking, then reopen tables to pick up the new
 * state. Closing a table releases its handle without closing the store.
 */
public final class TableStore extends NativeObject {
    private boolean inNativeCall;

    private TableStore(long nativeHandle) {
        super(nativeHandle);
    }

    /** Opens a store for the full configured bucket range with a generated ID. */
    public static TableStore open(Config config) {
        return open(config, null, null, null);
    }

    /** Opens a store for one inclusive bucket range with a generated ID. */
    public static TableStore open(Config config, int rangeStartInclusive, int rangeEndInclusive) {
        return open(config, new int[] {rangeStartInclusive}, new int[] {rangeEndInclusive}, null);
    }

    /**
     * Opens a store with an optional ID. Pass two null range arrays for the full configured range.
     */
    public static TableStore open(
            Config config, int[] rangeStartsInclusive, int[] rangeEndsInclusive, String dbId) {
        Objects.requireNonNull(config, "config");
        NativeLoader.load();
        long handle = openNative(config.toJson(), rangeStartsInclusive, rangeEndsInclusive, dbId);
        if (handle == 0L) throw new IllegalStateException("failed to open table store");
        return new TableStore(handle);
    }

    /** Returns the database shard ID. */
    public String id() {
        return withHandle(TableStore::idNative);
    }

    /** Creates a table or opens it when its persisted schema is identical. */
    public Table createTable(String name, TableSchema schema) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(schema, "schema");
        return withHandle(handle -> createTableNative(handle, name, TableJson.toJson(schema)));
    }

    /** Opens a named table from the current store schema. */
    public Table openTable(String name) {
        Objects.requireNonNull(name, "name");
        return withHandle(handle -> openTableNative(handle, name));
    }

    /** Captures all tables at one shard boundary and waits for the manifest to be published. */
    public ShardSnapshot snapshot() {
        return withHandle(handle -> ShardSnapshot.fromJson(snapshotNative(handle)));
    }

    /** Returns metadata for an already published shard snapshot. */
    public ShardSnapshot getShardSnapshot(long snapshotId) {
        return withHandle(
                handle -> ShardSnapshot.fromJson(getShardSnapshotNative(handle, snapshotId)));
    }

    /** Expands from a source manifest and takes both immediate snapshots. */
    public long expandBucketFromManifest(
            String sourceDbId,
            String sourceManifestPath,
            int[] rangeStartsInclusive,
            int[] rangeEndsInclusive,
            ImportStorageMode storageMode) {
        return expandBucketFromManifest(
                sourceDbId,
                sourceManifestPath,
                rangeStartsInclusive,
                rangeEndsInclusive,
                storageMode,
                new RescaleOptions());
    }

    /**
     * Imports buckets from a source manifest and returns its snapshot ID.
     *
     * <p>Empty range arrays import all source ranges. The source metadata volume must be configured
     * (READONLY is supported). Release table, projection, and scanner handles before calling this
     * method.
     */
    public long expandBucketFromManifest(
            String sourceDbId,
            String sourceManifestPath,
            int[] rangeStartsInclusive,
            int[] rangeEndsInclusive,
            ImportStorageMode storageMode,
            RescaleOptions options) {
        Objects.requireNonNull(sourceDbId, "sourceDbId");
        Objects.requireNonNull(sourceManifestPath, "sourceManifestPath");
        Objects.requireNonNull(rangeStartsInclusive, "rangeStartsInclusive");
        Objects.requireNonNull(rangeEndsInclusive, "rangeEndsInclusive");
        Objects.requireNonNull(storageMode, "storageMode");
        Objects.requireNonNull(options, "options");
        return withHandle(
                handle ->
                        expandBucketFromManifestNative(
                                handle,
                                sourceDbId,
                                sourceManifestPath,
                                rangeStartsInclusive,
                                rangeEndsInclusive,
                                storageMode.ordinal(),
                                options.snapshotBefore,
                                options.snapshotAfter));
    }

    /** Removes the requested buckets and takes both immediate snapshots. */
    public long shrinkBucket(int[] rangeStartsInclusive, int[] rangeEndsInclusive) {
        return shrinkBucket(rangeStartsInclusive, rangeEndsInclusive, new RescaleOptions());
    }

    /**
     * Removes the requested buckets after all table, projection, and scanner handles are released.
     *
     * @return the pre-operation snapshot ID, or -1 when no pre-operation snapshot was created
     */
    public long shrinkBucket(
            int[] rangeStartsInclusive, int[] rangeEndsInclusive, RescaleOptions options) {
        Objects.requireNonNull(rangeStartsInclusive, "rangeStartsInclusive");
        Objects.requireNonNull(rangeEndsInclusive, "rangeEndsInclusive");
        Objects.requireNonNull(options, "options");
        return withHandle(
                handle ->
                        shrinkBucketNative(
                                handle,
                                rangeStartsInclusive,
                                rangeEndsInclusive,
                                options.snapshotBefore,
                                options.snapshotAfter));
    }

    /** Waits for asynchronous adoption of imported files. */
    public void waitForImportAdoption(long timeoutMillis) {
        withHandle(
                handle -> {
                    waitForImportAdoptionNative(handle, timeoutMillis);
                    return null;
                });
    }

    /** Closes the shared database. Close scanners first to let active reads finish. */
    @Override
    public synchronized void close() {
        if (isDisposed()) return;
        withHandle(
                handle -> {
                    closeNative(handle);
                    super.close();
                    return null;
                });
    }

    private synchronized <T> T withHandle(LongFunction<T> operation) {
        // The Java monitor is reentrant; a filesystem callback must not access
        // or close the same native store while an operation borrows it.
        if (inNativeCall) throw new IllegalStateException("reentrant table store operation");
        inNativeCall = true;
        try {
            return operation.apply(nativeHandle);
        } finally {
            inNativeCall = false;
        }
    }

    @Override
    protected void disposeInternal(long nativeHandle) {
        disposeNative(nativeHandle);
    }

    private static native long openNative(
            String configJson, int[] rangeStartsInclusive, int[] rangeEndsInclusive, String dbId);

    private static native String idNative(long handle);

    private static native Table createTableNative(long handle, String name, String schemaJson);

    private static native Table openTableNative(long handle, String name);

    private static native String snapshotNative(long handle);

    private static native String getShardSnapshotNative(long handle, long snapshotId);

    private static native long expandBucketFromManifestNative(
            long handle,
            String sourceDbId,
            String sourceManifestPath,
            int[] rangeStartsInclusive,
            int[] rangeEndsInclusive,
            int storageMode,
            boolean snapshotBefore,
            boolean snapshotAfter);

    private static native long shrinkBucketNative(
            long handle,
            int[] rangeStartsInclusive,
            int[] rangeEndsInclusive,
            boolean snapshotBefore,
            boolean snapshotAfter);

    private static native void waitForImportAdoptionNative(long handle, long timeoutMillis);

    private static native void closeNative(long handle);

    private static native void disposeNative(long handle);
}
