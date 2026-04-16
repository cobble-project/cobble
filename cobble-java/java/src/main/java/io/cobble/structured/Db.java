package io.cobble.structured;

import io.cobble.Config;
import io.cobble.NativeLoader;
import io.cobble.NativeObject;
import io.cobble.ReadOptions;
import io.cobble.ScanOptions;
import io.cobble.ShardSnapshot;
import io.cobble.WriteOptions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * A structured database with typed column support (Bytes and List).
 *
 * <p>Wraps an underlying sharded Cobble DB and provides typed read/write interfaces that
 * automatically encode/decode column values according to the schema.
 *
 * <p>Example:
 *
 * <pre>{@code
 * Db db = Db.open(config);
 * db.updateSchema()
 *     .addListColumn(1, ListConfig.of(100, ListRetainMode.LAST))
 *     .commit();
 *
 * // Write bytes to column 0
 * db.put(0, key, 0, ColumnValue.ofBytes(data));
 *
 * // Write list elements to column 1
 * db.put(0, key, 1, ColumnValue.ofList(new byte[][] { elem1, elem2 }));
 *
 * // Merge (append) to list
 * db.merge(0, key, 1, ColumnValue.ofList(new byte[][] { elem3 }));
 *
 * // Read typed row
 * Row row = db.get(0, key);
 * byte[] bytesVal = row.getBytes(0);
 * byte[][] listVal = row.getList(1);
 * }</pre>
 */
public final class Db extends NativeObject {

    private Db(long nativeHandle) {
        super(nativeHandle);
    }

    // ── open / restore / resume ───────────────────────────────────────────

    /** Open a structured DB from a config file path. */
    public static Db open(String configPath) {
        NativeLoader.load();
        long h = openHandle(configPath);
        if (h == 0L) {
            throw new IllegalStateException("failed to open structured db");
        }
        return new Db(h);
    }

    /** Open a structured DB from Java {@link Config}. */
    public static Db open(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long h = openHandleFromJson(config.toJson());
        if (h == 0L) {
            throw new IllegalStateException("failed to open structured db from config json");
        }
        return new Db(h);
    }

    public Schema currentSchema() {
        return Schema.fromJson(currentSchemaJson(nativeHandle));
    }

    public StructuredSchemaBuilder updateSchema() {
        long h = createSchemaBuilder(nativeHandle);
        if (h == 0L) {
            throw new IllegalStateException("failed to create structured schema builder");
        }
        return new StructuredSchemaBuilder(h);
    }

    /** Restore a structured DB from a snapshot. Schema is auto-loaded from the snapshot. */
    public static Db restore(String configPath, long snapshotId, String dbId) {
        NativeLoader.load();
        long h = openFromSnapshotHandle(configPath, snapshotId, dbId);
        if (h == 0L) {
            throw new IllegalStateException("failed to restore structured db from snapshot");
        }
        return new Db(h);
    }

    /** Restore a structured DB from a snapshot. Schema is auto-loaded from the snapshot. */
    public static Db restore(Config config, long snapshotId, String dbId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long h = openFromSnapshotHandleFromJson(config.toJson(), snapshotId, dbId);
        if (h == 0L) {
            throw new IllegalStateException(
                    "failed to restore structured db from snapshot config json");
        }
        return new Db(h);
    }

    /** Resume a structured DB from existing folder state. Schema is auto-loaded. */
    public static Db resume(String configPath, String dbId) {
        NativeLoader.load();
        long h = resumeHandle(configPath, dbId);
        if (h == 0L) {
            throw new IllegalStateException("failed to resume structured db");
        }
        return new Db(h);
    }

    /** Resume a structured DB from existing folder state. Schema is auto-loaded. */
    public static Db resume(Config config, String dbId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long h = resumeHandleFromJson(config.toJson(), dbId);
        if (h == 0L) {
            throw new IllegalStateException("failed to resume structured db from config json");
        }
        return new Db(h);
    }

    // ── typed write operations ────────────────────────────────────────────

    /**
     * Put a typed column value for a key.
     *
     * <p>For bytes columns, use {@link ColumnValue#ofBytes(byte[])}. For list columns, use {@link
     * ColumnValue#ofList(byte[][])}.
     */
    public void put(int bucket, byte[] key, int column, ColumnValue value) {
        if (value.isBytes()) {
            putBytes(nativeHandle, bucket, key, column, value.asBytes());
        } else {
            putList(nativeHandle, bucket, key, column, value.asList());
        }
    }

    /** Put a typed column value for a key in a specific column family. */
    public void put(int bucket, byte[] key, String columnFamily, int column, ColumnValue value) {
        try (WriteOptions options = WriteOptions.withColumnFamily(columnFamily)) {
            putWithOptions(bucket, key, column, value, options);
        }
    }

    /** Convenience: put a bytes value for a column. */
    public void put(int bucket, byte[] key, int column, byte[] value) {
        putBytes(nativeHandle, bucket, key, column, value);
    }

    /** Convenience: put a bytes value for a column in a specific column family. */
    public void put(int bucket, byte[] key, String columnFamily, int column, byte[] value) {
        try (WriteOptions options = WriteOptions.withColumnFamily(columnFamily)) {
            putWithOptions(bucket, key, column, ColumnValue.ofBytes(value), options);
        }
    }

    /** Put a typed column value with explicit write options. */
    public void putWithOptions(
            int bucket, byte[] key, int column, ColumnValue value, WriteOptions options) {
        long woh = options == null ? 0L : options.getNativeHandle();
        if (value.isBytes()) {
            putBytesWithOptions(nativeHandle, bucket, key, column, value.asBytes(), woh);
        } else {
            putListWithOptions(nativeHandle, bucket, key, column, value.asList(), woh);
        }
    }

    /**
     * Merge a typed column value for a key.
     *
     * <p>For list columns, merge appends the given elements to the list.
     */
    public void merge(int bucket, byte[] key, int column, ColumnValue value) {
        if (value.isBytes()) {
            mergeBytes(nativeHandle, bucket, key, column, value.asBytes());
        } else {
            mergeList(nativeHandle, bucket, key, column, value.asList());
        }
    }

    /** Merge a typed column value for a key in a specific column family. */
    public void merge(int bucket, byte[] key, String columnFamily, int column, ColumnValue value) {
        try (WriteOptions options = WriteOptions.withColumnFamily(columnFamily)) {
            mergeWithOptions(bucket, key, column, value, options);
        }
    }

    /** Convenience: merge a bytes value for a column. */
    public void merge(int bucket, byte[] key, int column, byte[] value) {
        mergeBytes(nativeHandle, bucket, key, column, value);
    }

    /** Convenience: merge a bytes value for a column in a specific column family. */
    public void merge(int bucket, byte[] key, String columnFamily, int column, byte[] value) {
        try (WriteOptions options = WriteOptions.withColumnFamily(columnFamily)) {
            mergeWithOptions(bucket, key, column, ColumnValue.ofBytes(value), options);
        }
    }

    /** Merge a typed column value with explicit write options. */
    public void mergeWithOptions(
            int bucket, byte[] key, int column, ColumnValue value, WriteOptions options) {
        long woh = options == null ? 0L : options.getNativeHandle();
        if (value.isBytes()) {
            mergeBytesWithOptions(nativeHandle, bucket, key, column, value.asBytes(), woh);
        } else {
            mergeListWithOptions(nativeHandle, bucket, key, column, value.asList(), woh);
        }
    }

    /** Delete one column value for a key. */
    public void delete(int bucket, byte[] key, int column) {
        deleteWithOptions(bucket, key, column, null);
    }

    /** Delete one column value for a key in a specific column family. */
    public void delete(int bucket, byte[] key, String columnFamily, int column) {
        try (WriteOptions options = WriteOptions.withColumnFamily(columnFamily)) {
            deleteWithOptions(bucket, key, column, options);
        }
    }

    /** Delete one column value for a key with explicit write options. */
    public void deleteWithOptions(int bucket, byte[] key, int column, WriteOptions options) {
        long woh = options == null ? 0L : options.getNativeHandle();
        deleteWithOptions(nativeHandle, bucket, key, column, woh);
    }

    // ── typed read operations ─────────────────────────────────────────────

    /**
     * Get all columns for a key as a typed {@link Row}.
     *
     * @return typed row, or null if key does not exist
     */
    public Row get(int bucket, byte[] key) {
        Object[] raw = getTyped(nativeHandle, bucket, key);
        return Row.fromRawColumns(key, raw);
    }

    /** Get all columns for a key from a specific column family. */
    public Row get(int bucket, byte[] key, String columnFamily) {
        try (ReadOptions options = new ReadOptions().clearColumns().columnFamily(columnFamily)) {
            return getWithOptions(bucket, key, options);
        }
    }

    /**
     * Get columns for a key with read options (supports projection).
     *
     * @return typed row, or null if key does not exist
     */
    public Row getWithOptions(int bucket, byte[] key, ReadOptions options) {
        long roh = options == null ? 0L : options.getNativeHandle();
        Object[] raw = getTypedWithOptions(nativeHandle, bucket, key, roh);
        return Row.fromRawColumns(key, raw);
    }

    /**
     * Open a structured scan cursor within [startKeyInclusive, endKeyExclusive).
     *
     * <p>Each batch from the cursor yields typed {@link Row} objects.
     */
    public ScanCursor scan(int bucket, byte[] startKeyInclusive, byte[] endKeyExclusive) {
        return scanWithOptions(bucket, startKeyInclusive, endKeyExclusive, null);
    }

    /** Open a structured scan cursor in a specific column family. */
    public ScanCursor scan(
            int bucket, byte[] startKeyInclusive, byte[] endKeyExclusive, String columnFamily) {
        try (ScanOptions options = new ScanOptions().columnFamily(columnFamily)) {
            return scanWithOptions(bucket, startKeyInclusive, endKeyExclusive, options);
        }
    }

    /** Open a structured scan cursor with explicit scan options. */
    public ScanCursor scanWithOptions(
            int bucket, byte[] startKeyInclusive, byte[] endKeyExclusive, ScanOptions options) {
        long soh = options == null ? 0L : options.getNativeHandle();
        long h =
                openStructuredScanCursor(
                        nativeHandle, bucket, startKeyInclusive, endKeyExclusive, soh);
        if (h == 0L) {
            throw new IllegalStateException("failed to open structured scan cursor");
        }
        return new ScanCursor(h);
    }

    // ── metadata / time ───────────────────────────────────────────────────

    /** Return the DB runtime id. */
    public String id() {
        return id(nativeHandle);
    }

    /** Return current time in seconds from the DB's time provider. */
    public int nowSeconds() {
        return nowSeconds(nativeHandle);
    }

    /** Set manual time provider seconds (only effective when config uses manual time). */
    public void setTime(int nextSeconds) {
        if (nextSeconds < 0) {
            throw new IllegalArgumentException("nextSeconds must be >= 0");
        }
        setTime(nativeHandle, nextSeconds);
    }

    // ── snapshot lifecycle ────────────────────────────────────────────────

    /** Trigger snapshot creation asynchronously and return a future of shard snapshot. */
    public Future<ShardSnapshot> asyncSnapshot() {
        CompletableFuture<String> snapshotJsonFuture = new CompletableFuture<>();
        asyncSnapshot(nativeHandle, snapshotJsonFuture);
        return snapshotJsonFuture.thenApply(ShardSnapshot::fromJson);
    }

    /** Trigger snapshot creation and block until manifest is materialized. */
    public ShardSnapshot snapshot() {
        try {
            return asyncSnapshot().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("snapshot interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new IllegalStateException("snapshot failed: " + cause.getMessage(), cause);
        }
    }

    /** Expire a snapshot and release related references. */
    public boolean expireSnapshot(long snapshotId) {
        return expireSnapshot(nativeHandle, snapshotId);
    }

    /** Retain a snapshot to avoid auto-expiration. */
    public boolean retainSnapshot(long snapshotId) {
        return retainSnapshot(nativeHandle, snapshotId);
    }

    /** Build a {@link ShardSnapshot} from a DB snapshot id. */
    public ShardSnapshot getShardSnapshot(long snapshotId) {
        return ShardSnapshot.fromJson(getShardSnapshotJson(nativeHandle, snapshotId));
    }

    // ── native methods ────────────────────────────────────────────────────

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long openHandle(String configPath);

    private static native long openHandleFromJson(String configJson);

    private static native String currentSchemaJson(long nativeHandle);

    private static native long createSchemaBuilder(long nativeHandle);

    private static native long openFromSnapshotHandle(
            String configPath, long snapshotId, String dbId);

    private static native long openFromSnapshotHandleFromJson(
            String configJson, long snapshotId, String dbId);

    private static native long resumeHandle(String configPath, String dbId);

    private static native long resumeHandleFromJson(String configJson, String dbId);

    // bytes put/merge
    private static native void putBytes(
            long nativeHandle, int bucket, byte[] key, int column, byte[] value);

    private static native void putBytesWithOptions(
            long nativeHandle,
            int bucket,
            byte[] key,
            int column,
            byte[] value,
            long writeOptionsHandle);

    private static native void mergeBytes(
            long nativeHandle, int bucket, byte[] key, int column, byte[] value);

    private static native void mergeBytesWithOptions(
            long nativeHandle,
            int bucket,
            byte[] key,
            int column,
            byte[] value,
            long writeOptionsHandle);

    // list put/merge
    private static native void putList(
            long nativeHandle, int bucket, byte[] key, int column, byte[][] elements);

    private static native void putListWithOptions(
            long nativeHandle,
            int bucket,
            byte[] key,
            int column,
            byte[][] elements,
            long writeOptionsHandle);

    private static native void mergeList(
            long nativeHandle, int bucket, byte[] key, int column, byte[][] elements);

    private static native void mergeListWithOptions(
            long nativeHandle,
            int bucket,
            byte[] key,
            int column,
            byte[][] elements,
            long writeOptionsHandle);

    private static native void deleteWithOptions(
            long nativeHandle, int bucket, byte[] key, int column, long writeOptionsHandle);

    // typed get: returns Object[] where each element is null | byte[] | byte[][]
    private static native Object[] getTyped(long nativeHandle, int bucket, byte[] key);

    private static native Object[] getTypedWithOptions(
            long nativeHandle, int bucket, byte[] key, long readOptionsHandle);

    // typed scan
    private static native long openStructuredScanCursor(
            long nativeHandle,
            int bucket,
            byte[] startKeyInclusive,
            byte[] endKeyExclusive,
            long scanOptionsHandle);

    private static native String id(long nativeHandle);

    private static native int nowSeconds(long nativeHandle);

    private static native void setTime(long nativeHandle, int nextSeconds);

    private static native void asyncSnapshot(
            long nativeHandle, CompletableFuture<String> snapshotJsonFuture);

    private static native boolean expireSnapshot(long nativeHandle, long snapshotId);

    private static native boolean retainSnapshot(long nativeHandle, long snapshotId);

    private static native String getShardSnapshotJson(long nativeHandle, long snapshotId);
}
