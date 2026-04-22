package io.cobble;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Java binding for writable Cobble database.
 *
 * <p>Use {@link #open(String)} or {@link #open(Config)} to create a DB instance, then call
 * put/get/delete operations. Remember to close with try-with-resources.
 */
public final class Db extends NativeObject {
    private Db(long nativeHandle) {
        super(nativeHandle);
    }

    /**
     * Open a writable DB from a config file path.
     *
     * @param configPath path to a JSON/YAML/TOML/INI config file
     * @return opened DB handle
     */
    public static Db open(String configPath) {
        NativeLoader.load();
        long nativeHandle = openHandle(configPath);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open db");
        }
        return new Db(nativeHandle);
    }

    /** Open a writable DB from a config file path with an explicit bucket range. */
    public static Db open(String configPath, int rangeStartInclusive, int rangeEndInclusive) {
        NativeLoader.load();
        long nativeHandle = openHandleWithRange(configPath, rangeStartInclusive, rangeEndInclusive);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open db with bucket range");
        }
        return new Db(nativeHandle);
    }

    /**
     * Open a writable DB from Java {@link Config}.
     *
     * @param config Java-side config serialized to JSON and parsed by Rust
     * @return opened DB handle
     */
    public static Db open(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = openHandleFromJson(config.toJson());
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open db from config json");
        }
        return new Db(nativeHandle);
    }

    /** Open a writable DB from Java {@link Config} with an explicit bucket range. */
    public static Db open(Config config, int rangeStartInclusive, int rangeEndInclusive) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle =
                openHandleFromJsonWithRange(
                        config.toJson(), rangeStartInclusive, rangeEndInclusive);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open db from config json with bucket range");
        }
        return new Db(nativeHandle);
    }

    /**
     * Restore a writable DB from one snapshot.
     *
     * @param configPath db config path
     * @param snapshotId snapshot id
     * @param dbId source db id
     * @return restored writable db
     */
    public static Db restore(String configPath, long snapshotId, String dbId) {
        NativeLoader.load();
        long nativeHandle = openFromSnapshotHandle(configPath, snapshotId, dbId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to restore db from snapshot");
        }
        return new Db(nativeHandle);
    }

    /**
     * Restore a writable DB from one snapshot.
     *
     * @param config Java config
     * @param snapshotId snapshot id
     * @param dbId source db id
     * @return restored writable db
     */
    public static Db restore(Config config, long snapshotId, String dbId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = openFromSnapshotHandleFromJson(config.toJson(), snapshotId, dbId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to restore db from snapshot config json");
        }
        return new Db(nativeHandle);
    }

    /**
     * Resume a writable DB from existing folder state.
     *
     * @param configPath db config path
     * @param dbId db id
     * @return resumed writable db
     */
    public static Db resume(String configPath, String dbId) {
        NativeLoader.load();
        long nativeHandle = resumeHandle(configPath, dbId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to resume db");
        }
        return new Db(nativeHandle);
    }

    /**
     * Resume a writable DB from existing folder state.
     *
     * @param config Java config
     * @param dbId db id
     * @return resumed writable db
     */
    public static Db resume(Config config, String dbId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = resumeHandleFromJson(config.toJson(), dbId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to resume db from config json");
        }
        return new Db(nativeHandle);
    }

    /**
     * Put one column value for a key.
     *
     * @param bucket bucket id
     * @param key logical user key
     * @param column column index
     * @param value raw bytes
     */
    public void put(int bucket, byte[] key, int column, byte[] value) {
        put(nativeHandle, bucket, key, column, value);
    }

    /** Put one column value for a key in a specific column family. */
    public void put(int bucket, byte[] key, String columnFamily, int column, byte[] value) {
        try (WriteOptions options = WriteOptions.withColumnFamily(columnFamily)) {
            putWithOptions(bucket, key, column, value, options);
        }
    }

    /** Put one column value for a key with explicit write options (native handle mode). */
    public void putWithOptions(
            int bucket, byte[] key, int column, byte[] value, WriteOptions options) {
        long writeOptionsHandle = options == null ? 0L : options.nativeHandle;
        putWithOptions(nativeHandle, bucket, key, column, value, writeOptionsHandle);
    }

    /** Merge one column value for a key. */
    public void merge(int bucket, byte[] key, int column, byte[] value) {
        merge(nativeHandle, bucket, key, column, value);
    }

    /** Merge one column value for a key in a specific column family. */
    public void merge(int bucket, byte[] key, String columnFamily, int column, byte[] value) {
        try (WriteOptions options = WriteOptions.withColumnFamily(columnFamily)) {
            mergeWithOptions(bucket, key, column, value, options);
        }
    }

    /** Merge one column value for a key with explicit write options (native handle mode). */
    public void mergeWithOptions(
            int bucket, byte[] key, int column, byte[] value, WriteOptions options) {
        long writeOptionsHandle = options == null ? 0L : options.nativeHandle;
        mergeWithOptions(nativeHandle, bucket, key, column, value, writeOptionsHandle);
    }

    /** Get one column by index. */
    public byte[] get(int bucket, byte[] key, int column) {
        try (ReadOptions options = ReadOptions.forColumn(column)) {
            return singleColumnOrNull(get(nativeHandle, bucket, key, options.nativeHandle));
        }
    }

    /** Get one column by index from a specific column family. */
    public byte[] get(int bucket, byte[] key, String columnFamily, int column) {
        try (ReadOptions options = ReadOptions.forColumnInFamily(columnFamily, column)) {
            return singleColumnOrNull(get(nativeHandle, bucket, key, options.nativeHandle));
        }
    }

    /** Get default selected columns for one key (same as null/default read options). */
    public byte[][] get(int bucket, byte[] key) {
        return get(nativeHandle, bucket, key, 0L);
    }

    /** Get selected columns for one key using reusable native-backed read options. */
    public byte[][] getWithOptions(int bucket, byte[] key, ReadOptions options) {
        long readOptionsHandle = options == null ? 0L : options.nativeHandle;
        return get(nativeHandle, bucket, key, readOptionsHandle);
    }

    /** Open a high-throughput native scan cursor within [startKeyInclusive, endKeyExclusive). */
    public ScanCursor scan(int bucket, byte[] startKeyInclusive, byte[] endKeyExclusive) {
        return scanWithOptions(bucket, startKeyInclusive, endKeyExclusive, null);
    }

    /** Open a high-throughput native scan cursor in a specific column family. */
    public ScanCursor scan(
            int bucket, byte[] startKeyInclusive, byte[] endKeyExclusive, String columnFamily) {
        try (ScanOptions options = new ScanOptions().columnFamily(columnFamily)) {
            return scanWithOptions(bucket, startKeyInclusive, endKeyExclusive, options);
        }
    }

    /** Open a high-throughput native scan cursor with explicit options handle mode. */
    public ScanCursor scanWithOptions(
            int bucket, byte[] startKeyInclusive, byte[] endKeyExclusive, ScanOptions options) {
        long scanOptionsHandle = options == null ? 0L : options.nativeHandle;
        long handle =
                openScanCursor(
                        nativeHandle,
                        bucket,
                        startKeyInclusive,
                        endKeyExclusive,
                        scanOptionsHandle);
        if (handle == 0L) {
            throw new IllegalStateException("failed to open scan cursor");
        }
        return new ScanCursor(handle);
    }

    /**
     * Delete one column value for a key.
     *
     * @param bucket bucket id
     * @param key logical user key
     * @param column column index
     */
    public void delete(int bucket, byte[] key, int column) {
        delete(nativeHandle, bucket, key, column);
    }

    /** Delete one column value for a key with explicit write options (native handle mode). */
    public void deleteWithOptions(int bucket, byte[] key, int column, WriteOptions options) {
        long writeOptionsHandle = options == null ? 0L : options.nativeHandle;
        deleteWithOptions(nativeHandle, bucket, key, column, writeOptionsHandle);
    }

    /** Delete one column value for a key in a specific column family. */
    public void delete(int bucket, byte[] key, String columnFamily, int column) {
        try (WriteOptions options = WriteOptions.withColumnFamily(columnFamily)) {
            deleteWithOptions(bucket, key, column, options);
        }
    }

    /** Set manual time provider seconds (only effective when config uses manual time provider). */
    public void setTime(int nextSeconds) {
        if (nextSeconds < 0) {
            throw new IllegalArgumentException("nextSeconds must be >= 0");
        }
        setTime(nativeHandle, nextSeconds);
    }

    /** Expand bucket ownership by importing data from another shard snapshot. */
    public long expandBucket(
            String sourceDbId,
            long snapshotId,
            int[] rangeStartsInclusive,
            int[] rangeEndsInclusive) {
        return expandBucket(
                nativeHandle, sourceDbId, snapshotId, rangeStartsInclusive, rangeEndsInclusive);
    }

    /** Expand bucket ownership from the latest retained snapshot of the source DB. */
    public long expandBucket(
            String sourceDbId, int[] rangeStartsInclusive, int[] rangeEndsInclusive) {
        return expandBucket(
                nativeHandle, sourceDbId, -1L, rangeStartsInclusive, rangeEndsInclusive);
    }

    /** Shrink bucket ownership by removing the given ranges from the current DB. */
    public long shrinkBucket(int[] rangeStartsInclusive, int[] rangeEndsInclusive) {
        return shrinkBucket(nativeHandle, rangeStartsInclusive, rangeEndsInclusive);
    }

    /**
     * Return DB runtime id.
     *
     * @return unique db id string
     */
    public String id() {
        return id(nativeHandle);
    }

    /**
     * Return current time in seconds from the DB's time provider.
     *
     * @return current epoch seconds (u32 range)
     */
    public int nowSeconds() {
        return nowSeconds(nativeHandle);
    }

    /** Trigger snapshot creation asynchronously and return a future of shard snapshot payload. */
    public CompletableFuture<ShardSnapshot> asyncSnapshot() {
        CompletableFuture<String> snapshotJsonFuture = new CompletableFuture<>();
        asyncSnapshot(nativeHandle, snapshotJsonFuture);
        return snapshotJsonFuture.thenApply(ShardSnapshot::fromJson);
    }

    /**
     * Trigger snapshot creation and block until snapshot manifest is materialized.
     *
     * @return shard snapshot payload
     */
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

    /**
     * Expire a snapshot and release related references.
     *
     * @param snapshotId target snapshot id
     * @return true if expired in this call
     */
    public boolean expireSnapshot(long snapshotId) {
        return expireSnapshot(nativeHandle, snapshotId);
    }

    /**
     * Retain a snapshot to avoid auto-expiration.
     *
     * @param snapshotId snapshot id
     * @return true if retain succeeded
     */
    public boolean retainSnapshot(long snapshotId) {
        return retainSnapshot(nativeHandle, snapshotId);
    }

    /** Return the current schema snapshot. */
    public Schema currentSchema() {
        return Schema.fromJson(currentSchemaJson(nativeHandle));
    }

    /** Start a schema update transaction. Commit with {@link SchemaBuilder#commit()}. */
    public SchemaBuilder updateSchema() {
        long builderHandle = createSchemaBuilder(nativeHandle);
        if (builderHandle == 0L) {
            throw new IllegalStateException("failed to create schema builder");
        }
        return new SchemaBuilder(builderHandle);
    }

    /**
     * Build one {@link ShardSnapshot} from a DB snapshot id.
     *
     * <p>The returned object is pure Java data (no native handle) and can be passed to {@link
     * DbCoordinator#materializeGlobalSnapshot(int, long, java.util.List)}.
     *
     * @param snapshotId local db snapshot id
     * @return shard snapshot input payload
     */
    public ShardSnapshot getShardSnapshot(long snapshotId) {
        return ShardSnapshot.fromJson(getShardSnapshotJson(nativeHandle, snapshotId));
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long openHandle(String configPath);

    private static native long openHandleWithRange(
            String configPath, int rangeStartInclusive, int rangeEndInclusive);

    private static native long openHandleFromJson(String configJson);

    private static native long openHandleFromJsonWithRange(
            String configJson, int rangeStartInclusive, int rangeEndInclusive);

    private static native long openFromSnapshotHandle(
            String configPath, long snapshotId, String dbId);

    private static native long openFromSnapshotHandleFromJson(
            String configJson, long snapshotId, String dbId);

    private static native long resumeHandle(String configPath, String dbId);

    private static native long resumeHandleFromJson(String configJson, String dbId);

    private static native void put(
            long nativeHandle, int bucket, byte[] key, int column, byte[] value);

    private static native void putWithOptions(
            long nativeHandle,
            int bucket,
            byte[] key,
            int column,
            byte[] value,
            long writeOptionsHandle);

    private static native void merge(
            long nativeHandle, int bucket, byte[] key, int column, byte[] value);

    private static native void mergeWithOptions(
            long nativeHandle,
            int bucket,
            byte[] key,
            int column,
            byte[] value,
            long writeOptionsHandle);

    private static native byte[][] get(
            long nativeHandle, int bucket, byte[] key, long readOptionsHandle);

    private static byte[] singleColumnOrNull(byte[][] columns) {
        if (columns == null) {
            return null;
        }
        if (columns.length != 1) {
            throw new IllegalStateException(
                    "expected exactly one selected column, got " + columns.length);
        }
        return columns[0];
    }

    private static native long openScanCursor(
            long nativeHandle,
            int bucket,
            byte[] startKeyInclusive,
            byte[] endKeyExclusive,
            long scanOptionsHandle);

    private static native void delete(long nativeHandle, int bucket, byte[] key, int column);

    private static native void deleteWithOptions(
            long nativeHandle, int bucket, byte[] key, int column, long writeOptionsHandle);

    private static native void setTime(long nativeHandle, int nextSeconds);

    private static native String id(long nativeHandle);

    private static native int nowSeconds(long nativeHandle);

    private static native void asyncSnapshot(
            long nativeHandle, CompletableFuture<String> snapshotJsonFuture);

    private static native boolean expireSnapshot(long nativeHandle, long snapshotId);

    private static native boolean retainSnapshot(long nativeHandle, long snapshotId);

    private static native String getShardSnapshotJson(long nativeHandle, long snapshotId);

    private static native long expandBucket(
            long nativeHandle,
            String sourceDbId,
            long snapshotId,
            int[] rangeStartsInclusive,
            int[] rangeEndsInclusive);

    private static native long shrinkBucket(
            long nativeHandle, int[] rangeStartsInclusive, int[] rangeEndsInclusive);

    static native String currentSchemaJson(long nativeHandle);

    static native long createSchemaBuilder(long nativeHandle);
}
