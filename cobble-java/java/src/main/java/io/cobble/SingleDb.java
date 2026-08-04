package io.cobble;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/** Java binding for cobble SingleDb. */
public final class SingleDb extends NativeObject {
    private SingleDb(long nativeHandle) {
        super(nativeHandle);
    }

    public static SingleDb open(String configPath) {
        NativeLoader.load();
        long nativeHandle = openHandle(configPath);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open single db");
        }
        return new SingleDb(nativeHandle);
    }

    public static SingleDb open(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = openHandleFromJson(config.toJson());
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open single db from config json");
        }
        return new SingleDb(nativeHandle);
    }

    /** Resume a single-node DB from an existing global snapshot id. */
    public static SingleDb resume(String configPath, long globalSnapshotId) {
        NativeLoader.load();
        long nativeHandle = openFromGlobalSnapshotHandle(configPath, globalSnapshotId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to resume single db from global snapshot");
        }
        return new SingleDb(nativeHandle);
    }

    /** Resume a single-node DB from an existing global snapshot id. */
    public static SingleDb resume(Config config, long globalSnapshotId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = openFromGlobalSnapshotHandleFromJson(config.toJson(), globalSnapshotId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException(
                    "failed to resume single db from global snapshot config json");
        }
        return new SingleDb(nativeHandle);
    }

    public void put(int bucket, byte[] key, int column, byte[] value) {
        put(nativeHandle, bucket, key, column, value);
    }

    public void put(int bucket, byte[] key, String columnFamily, int column, byte[] value) {
        try (WriteOptions options = WriteOptions.withColumnFamily(columnFamily)) {
            putWithOptions(bucket, key, column, value, options);
        }
    }

    public void putWithOptions(
            int bucket, byte[] key, int column, byte[] value, WriteOptions options) {
        long writeOptionsHandle = options == null ? 0L : options.nativeHandle;
        putWithOptions(nativeHandle, bucket, key, column, value, writeOptionsHandle);
    }

    public void merge(int bucket, byte[] key, int column, byte[] value) {
        merge(nativeHandle, bucket, key, column, value);
    }

    public void merge(int bucket, byte[] key, String columnFamily, int column, byte[] value) {
        try (WriteOptions options = WriteOptions.withColumnFamily(columnFamily)) {
            mergeWithOptions(bucket, key, column, value, options);
        }
    }

    public void mergeWithOptions(
            int bucket, byte[] key, int column, byte[] value, WriteOptions options) {
        long writeOptionsHandle = options == null ? 0L : options.nativeHandle;
        mergeWithOptions(nativeHandle, bucket, key, column, value, writeOptionsHandle);
    }

    public byte[] get(int bucket, byte[] key, int column) {
        try (ReadOptions options = ReadOptions.forColumn(column)) {
            return singleColumnOrNull(get(nativeHandle, bucket, key, options.nativeHandle));
        }
    }

    public byte[] get(int bucket, byte[] key, String columnFamily, int column) {
        try (ReadOptions options = ReadOptions.forColumnInFamily(columnFamily, column)) {
            return singleColumnOrNull(get(nativeHandle, bucket, key, options.nativeHandle));
        }
    }

    public byte[][] get(int bucket, byte[] key) {
        return get(nativeHandle, bucket, key, 0L);
    }

    public byte[][] getWithOptions(int bucket, byte[] key, ReadOptions options) {
        long readOptionsHandle = options == null ? 0L : options.nativeHandle;
        return get(nativeHandle, bucket, key, readOptionsHandle);
    }

    /**
     * Read several keys in one batch from a consistent database-state snapshot.
     *
     * <p>{@code buckets} and {@code keys} must have the same length. The returned array has the
     * same length as the input; each element is {@code null} (key not found) or a {@code byte[][]}
     * of column values (identical to {@link #get(int, byte[])}).
     */
    public byte[][][] multiGet(int[] buckets, byte[][] keys) {
        return multiGet(nativeHandle, buckets, keys, 0L);
    }

    /** Batch multi-get with explicit native-backed read options. */
    public byte[][][] multiGetWithOptions(int[] buckets, byte[][] keys, ReadOptions options) {
        long readOptionsHandle = options == null ? 0L : options.nativeHandle;
        return multiGet(nativeHandle, buckets, keys, readOptionsHandle);
    }

    /** Open a high-throughput native scan cursor within [startKeyInclusive, endKeyExclusive). */
    public ScanCursor scan(int bucket, byte[] startKeyInclusive, byte[] endKeyExclusive) {
        return scanWithOptions(bucket, startKeyInclusive, endKeyExclusive, null);
    }

    public ScanCursor scan(
            int bucket, byte[] startKeyInclusive, byte[] endKeyExclusive, String columnFamily) {
        try (ScanOptions options = new ScanOptions().columnFamily(columnFamily)) {
            return scanWithOptions(bucket, startKeyInclusive, endKeyExclusive, options);
        }
    }

    /** Open a high-throughput native scan cursor with explicit options. */
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

    public void delete(int bucket, byte[] key, int column) {
        delete(nativeHandle, bucket, key, column);
    }

    public void deleteWithOptions(int bucket, byte[] key, int column, WriteOptions options) {
        long writeOptionsHandle = options == null ? 0L : options.nativeHandle;
        deleteWithOptions(nativeHandle, bucket, key, column, writeOptionsHandle);
    }

    public void delete(int bucket, byte[] key, String columnFamily, int column) {
        try (WriteOptions options = WriteOptions.withColumnFamily(columnFamily)) {
            deleteWithOptions(bucket, key, column, options);
        }
    }

    public void setTime(int nextSeconds) {
        if (nextSeconds < 0) {
            throw new IllegalArgumentException("nextSeconds must be >= 0");
        }
        setTime(nativeHandle, nextSeconds);
    }

    /**
     * Switch the active memtable type used by future active memtables in this process.
     *
     * <p>This is a runtime-only setting: it does not modify the persisted {@link Config}. When
     * {@code flushCurrent} is {@code false}, the active memtable is left untouched and the target
     * type applies at its next natural rotation. When {@code flushCurrent} is {@code true}, a
     * non-empty active memtable is rotated through the normal manual-flush and auto-snapshot path
     * (the call returns once rotation is scheduled, not after the data reaches disk), while an
     * empty active table is immediately replaced when its implementation differs.
     *
     * @param memtableType target memtable type (hash, skiplist, or vec)
     * @param flushCurrent whether to rotate the current memtable before switching
     */
    public void switchMemtableType(Config.MemtableType memtableType, boolean flushCurrent) {
        if (memtableType == null) {
            throw new IllegalArgumentException("memtableType must not be null");
        }
        switchMemtableType(nativeHandle, memtableType.name().toLowerCase(Locale.ROOT), flushCurrent);
    }

    /** Trigger snapshot creation asynchronously and return future of global snapshot payload. */
    public CompletableFuture<GlobalSnapshot> asyncSnapshot() {
        CompletableFuture<String> snapshotJsonFuture = new CompletableFuture<>();
        asyncSnapshot(nativeHandle, snapshotJsonFuture);
        return snapshotJsonFuture.thenApply(GlobalSnapshot::fromJson);
    }

    /** Trigger snapshot creation and block until global snapshot manifest is materialized. */
    public GlobalSnapshot snapshot() {
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

    /** Retain one global snapshot and related local shard snapshot(s). */
    public boolean retainSnapshot(long snapshotId) {
        return retainSnapshot(nativeHandle, snapshotId);
    }

    /** Expire one global snapshot and related local shard snapshot(s). */
    public boolean expireSnapshot(long snapshotId) {
        return expireSnapshot(nativeHandle, snapshotId);
    }

    /**
     * Mark every currently referenced READONLY file for asynchronous loading into primary storage.
     *
     * @return number of READONLY files marked by this call
     */
    public long loadReadonlyFilesToPrimary() {
        return loadReadonlyFilesToPrimary(nativeHandle);
    }

    /** List global snapshots materialized by this single-node coordinator. */
    public List<GlobalSnapshot> listSnapshots() {
        return GlobalSnapshot.listFromJson(listSnapshotsJson(nativeHandle));
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long openHandle(String configPath);

    private static native long openHandleFromJson(String configJson);

    private static native long openFromGlobalSnapshotHandle(
            String configPath, long globalSnapshotId);

    private static native long openFromGlobalSnapshotHandleFromJson(
            String configJson, long globalSnapshotId);

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

    private static native byte[][][] multiGet(
            long nativeHandle, int[] buckets, byte[][] keys, long readOptionsHandle);

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

    private static native void switchMemtableType(
            long nativeHandle, String memtableType, boolean flushCurrent);

    private static native void asyncSnapshot(
            long nativeHandle, CompletableFuture<String> snapshotJsonFuture);

    private static native boolean retainSnapshot(long nativeHandle, long snapshotId);

    private static native boolean expireSnapshot(long nativeHandle, long snapshotId);

    private static native long loadReadonlyFilesToPrimary(long nativeHandle);

    private static native String listSnapshotsJson(long nativeHandle);

    static native String currentSchemaJson(long nativeHandle);

    static native long createSchemaBuilder(long nativeHandle);

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
}
