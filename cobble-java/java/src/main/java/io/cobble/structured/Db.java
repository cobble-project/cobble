package io.cobble.structured;

import io.cobble.Config;
import io.cobble.NativeLoader;
import io.cobble.NativeObject;
import io.cobble.ShardSnapshot;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private static volatile DirectBufferPool directBufferPool = DirectBufferPool.defaults();

    private Db(long nativeHandle) {
        super(nativeHandle);
    }

    /**
     * Configure pooled direct IO buffers used by direct read methods.
     *
     * <p>Only pool expansion is allowed. Shrinking either buffer size or max pool size returns
     * {@code false}.
     */
    public static synchronized boolean configureDirectBufferPool(
            int bufferSizeBytes, int maxPoolSize) {
        DirectBufferPool current = directBufferPool;
        if (bufferSizeBytes < current.bufferSizeBytes() || maxPoolSize < current.maxPoolSize()) {
            return false;
        }
        if (bufferSizeBytes == current.bufferSizeBytes() && maxPoolSize == current.maxPoolSize()) {
            return true;
        }
        directBufferPool = new DirectBufferPool(bufferSizeBytes, maxPoolSize);
        return true;
    }

    private static void initializeDirectBufferPool(long nativeHandle) {
        int[] resolved = directBufferPoolConfig(nativeHandle);
        if (resolved == null || resolved.length != 2) {
            throw new IllegalStateException("failed to load direct buffer pool config from db");
        }
        if (!configureDirectBufferPool(resolved[0], resolved[1])) {
            throw new IllegalStateException(
                    "direct buffer pool config can only grow: requested "
                            + resolved[0]
                            + " / "
                            + resolved[1]
                            + ", current "
                            + directBufferPool.bufferSizeBytes()
                            + " / "
                            + directBufferPool.maxPoolSize());
        }
    }

    private static Db wrapOpenedDb(long nativeHandle) {
        Db db = new Db(nativeHandle);
        try {
            initializeDirectBufferPool(nativeHandle);
            return db;
        } catch (RuntimeException e) {
            try {
                db.close();
            } catch (RuntimeException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    // ── open / restore / resume ───────────────────────────────────────────

    /** Open a structured DB from a config file path. */
    public static Db open(String configPath) {
        NativeLoader.load();
        long h = openHandle(configPath);
        if (h == 0L) {
            throw new IllegalStateException("failed to open structured db");
        }
        return wrapOpenedDb(h);
    }

    /** Open a structured DB from a config file path with an explicit bucket range. */
    public static Db open(String configPath, int rangeStartInclusive, int rangeEndInclusive) {
        NativeLoader.load();
        long h = openHandleWithRange(configPath, rangeStartInclusive, rangeEndInclusive);
        if (h == 0L) {
            throw new IllegalStateException("failed to open structured db with bucket range");
        }
        return wrapOpenedDb(h);
    }

    /** Open a structured DB from Java {@link Config}. */
    public static Db open(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        String configJson = config.toJson();
        long h = openHandleFromJson(configJson);
        if (h == 0L) {
            throw new IllegalStateException("failed to open structured db from config json");
        }
        return wrapOpenedDb(h);
    }

    /** Open a structured DB from Java {@link Config} with an explicit bucket range. */
    public static Db open(Config config, int rangeStartInclusive, int rangeEndInclusive) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        String configJson = config.toJson();
        long h = openHandleFromJsonWithRange(configJson, rangeStartInclusive, rangeEndInclusive);
        if (h == 0L) {
            throw new IllegalStateException(
                    "failed to open structured db from config json with bucket range");
        }
        return wrapOpenedDb(h);
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
        return restore(configPath, snapshotId, dbId, false);
    }

    /**
     * Restore a structured DB from a snapshot. Schema is auto-loaded from the snapshot.
     *
     * <p>Set {@code newDbId} to {@code true} to restore from the source snapshot into a fresh db
     * identity and start a new snapshot chain.
     */
    public static Db restore(String configPath, long snapshotId, String dbId, boolean newDbId) {
        NativeLoader.load();
        long h = restoreHandle(configPath, snapshotId, dbId, newDbId);
        if (h == 0L) {
            throw new IllegalStateException(
                    newDbId
                            ? "failed to restore structured db from snapshot with fresh db id"
                            : "failed to restore structured db from snapshot");
        }
        return wrapOpenedDb(h);
    }

    /** Restore a structured DB from a snapshot. Schema is auto-loaded from the snapshot. */
    public static Db restore(Config config, long snapshotId, String dbId) {
        return restore(config, snapshotId, dbId, false);
    }

    /**
     * Restore a structured DB from a snapshot. Schema is auto-loaded from the snapshot.
     *
     * <p>Set {@code newDbId} to {@code true} to restore from the source snapshot into a fresh db
     * identity and start a new snapshot chain.
     */
    public static Db restore(Config config, long snapshotId, String dbId, boolean newDbId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        String configJson = config.toJson();
        long h = restoreHandleFromJson(configJson, snapshotId, dbId, newDbId);
        if (h == 0L) {
            throw new IllegalStateException(
                    newDbId
                            ? "failed to restore structured db from snapshot config json with fresh db id"
                            : "failed to restore structured db from snapshot config json");
        }
        return wrapOpenedDb(h);
    }

    /**
     * Restore a structured DB from an explicit source manifest path. Schema is auto-loaded from
     * that source manifest.
     *
     * <p>The returned DB always gets a fresh db id and starts a new snapshot chain.
     */
    public static Db restoreWithManifest(String configPath, String manifestPath) {
        NativeLoader.load();
        long h = restoreWithManifestHandle(configPath, manifestPath);
        if (h == 0L) {
            throw new IllegalStateException("failed to restore structured db from manifest path");
        }
        return wrapOpenedDb(h);
    }

    /**
     * Restore a structured DB from an explicit source manifest path. Schema is auto-loaded from
     * that source manifest.
     *
     * <p>The returned DB always gets a fresh db id and starts a new snapshot chain.
     */
    public static Db restoreWithManifest(Config config, String manifestPath) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        String configJson = config.toJson();
        long h = restoreWithManifestHandleFromJson(configJson, manifestPath);
        if (h == 0L) {
            throw new IllegalStateException(
                    "failed to restore structured db from manifest path config json");
        }
        return wrapOpenedDb(h);
    }

    /** Resume a structured DB from existing folder state. Schema is auto-loaded. */
    public static Db resume(String configPath, String dbId) {
        NativeLoader.load();
        long h = resumeHandle(configPath, dbId);
        if (h == 0L) {
            throw new IllegalStateException("failed to resume structured db");
        }
        return wrapOpenedDb(h);
    }

    /** Resume a structured DB from existing folder state. Schema is auto-loaded. */
    public static Db resume(Config config, String dbId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        String configJson = config.toJson();
        long h = resumeHandleFromJson(configJson, dbId);
        if (h == 0L) {
            throw new IllegalStateException("failed to resume structured db from config json");
        }
        return wrapOpenedDb(h);
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
     * Put one bytes column value with direct ByteBuffer input.
     *
     * <p>The key/value buffers are caller-owned and can be reused by the caller across invocations.
     */
    public void putDirectWithOptions(
            int bucket,
            ByteBuffer keyBuffer,
            int keyLength,
            int column,
            ByteBuffer valueBuffer,
            int valueLength,
            WriteOptions options) {
        if (keyBuffer == null || !keyBuffer.isDirect()) {
            throw new IllegalArgumentException("keyBuffer must be a direct ByteBuffer");
        }
        if (valueBuffer == null || !valueBuffer.isDirect()) {
            throw new IllegalArgumentException("valueBuffer must be a direct ByteBuffer");
        }
        if (keyLength < 0 || keyLength > keyBuffer.capacity()) {
            throw new IllegalArgumentException("keyLength out of range: " + keyLength);
        }
        if (valueLength < 0 || valueLength > valueBuffer.capacity()) {
            throw new IllegalArgumentException("valueLength out of range: " + valueLength);
        }
        long woh = options == null ? 0L : options.getNativeHandle();
        putBytesDirectWithOptions(
                nativeHandle,
                bucket,
                directAddress(keyBuffer),
                keyBuffer.capacity(),
                keyLength,
                column,
                directAddress(valueBuffer),
                valueBuffer.capacity(),
                valueLength,
                woh);
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
     * Get columns for a key through direct ByteBuffer IO.
     *
     * <p>The input key and encoded row payload share one IO buffer. The binding keeps a reusable
     * direct buffer pool (default 2KB) for this path, and returns a temporary larger direct buffer
     * when the encoded row does not fit in the pooled one.
     *
     * @return direct row, or null if key does not exist
     */
    public DirectRow getDirectWithOptions(int bucket, byte[] key, ReadOptions options) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        long roh = options == null ? 0L : options.getNativeHandle();
        DirectBufferPool pool = directBufferPool;
        ByteBuffer pooled = pool.acquire();
        ByteBuffer ioBuffer = pooled;
        boolean usingPooledForResult = false;
        boolean pooledReleased = false;

        try {
            if (key.length > ioBuffer.capacity()) {
                ioBuffer = ByteBuffer.allocateDirect(key.length);
            }
            writeKey(ioBuffer, key);
            int encodedLength =
                    getEncodedDirectWithOptions(
                            nativeHandle,
                            bucket,
                            directAddress(ioBuffer),
                            ioBuffer.capacity(),
                            key.length,
                            roh);
            if (encodedLength == 0) {
                if (!pooledReleased) {
                    pool.release(pooled);
                    pooledReleased = true;
                }
                return null;
            }
            ByteBuffer resultBuffer = resolveEncodedBuffer(ioBuffer, encodedLength);
            int decodedLength = Math.abs(encodedLength);
            if (resultBuffer == pooled) {
                usingPooledForResult = true;
            } else {
                pool.release(pooled);
                pooledReleased = true;
            }

            if (!usingPooledForResult) {
                return DirectRow.decode(resultBuffer, decodedLength, null);
            }

            AtomicBoolean released = new AtomicBoolean(false);
            Runnable releaser =
                    () -> {
                        if (released.compareAndSet(false, true)) {
                            pool.release(pooled);
                        }
                    };
            return DirectRow.decode(resultBuffer, decodedLength, releaser);
        } catch (RuntimeException e) {
            if (!usingPooledForResult && !pooledReleased) {
                pool.release(pooled);
            }
            throw e;
        }
    }

    /**
     * Get columns for a key via caller-owned direct key buffer.
     *
     * <p>Key bytes are read from {@code keyBuffer[0..keyBuffer.limit())}.
     */
    public DirectRow getDirectWithOptions(int bucket, ByteBuffer keyBuffer, ReadOptions options) {
        if (keyBuffer == null || !keyBuffer.isDirect()) {
            throw new IllegalArgumentException("keyBuffer must be a direct ByteBuffer");
        }
        return getDirectWithOptions(bucket, keyBuffer, ((Buffer) keyBuffer).limit(), options);
    }

    /** Compatibility overload that treats {@code keyBuffer[0..keyLength)} as key bytes. */
    public DirectRow getDirectWithOptions(
            int bucket, ByteBuffer keyBuffer, int keyLength, ReadOptions options) {
        if (keyBuffer == null || !keyBuffer.isDirect()) {
            throw new IllegalArgumentException("keyBuffer must be a direct ByteBuffer");
        }
        if (keyLength < 0 || keyLength > keyBuffer.capacity()) {
            throw new IllegalArgumentException("keyLength out of range: " + keyLength);
        }
        long roh = options == null ? 0L : options.getNativeHandle();
        DirectBufferPool pool = directBufferPool;
        ByteBuffer pooled = pool.acquire();
        ByteBuffer ioBuffer = pooled;
        boolean usingPooledForResult = false;
        boolean pooledReleased = false;

        try {
            if (keyLength > ioBuffer.capacity()) {
                ioBuffer = ByteBuffer.allocateDirect(keyLength);
            }
            writeKey(ioBuffer, keyBuffer, keyLength);
            int encodedLength =
                    getEncodedDirectWithOptions(
                            nativeHandle,
                            bucket,
                            directAddress(ioBuffer),
                            ioBuffer.capacity(),
                            keyLength,
                            roh);
            if (encodedLength == 0) {
                pool.release(pooled);
                pooledReleased = true;
                return null;
            }
            ByteBuffer resultBuffer = resolveEncodedBuffer(ioBuffer, encodedLength);
            int decodedLength = Math.abs(encodedLength);
            if (resultBuffer == pooled) {
                usingPooledForResult = true;
                AtomicBoolean released = new AtomicBoolean(false);
                Runnable releaser =
                        () -> {
                            if (released.compareAndSet(false, true)) {
                                pool.release(pooled);
                            }
                        };
                return DirectRow.decode(resultBuffer, decodedLength, releaser);
            }
            pool.release(pooled);
            pooledReleased = true;
            return DirectRow.decode(resultBuffer, decodedLength, null);
        } catch (RuntimeException e) {
            if (!usingPooledForResult && !pooledReleased) {
                pool.release(pooled);
            }
            throw e;
        }
    }

    private static void writeKey(ByteBuffer buffer, byte[] key) {
        ((Buffer) buffer).clear();
        buffer.put(key);
    }

    private static void writeKey(ByteBuffer buffer, ByteBuffer keyBuffer, int keyLength) {
        ByteBuffer keyView = keyBuffer.duplicate();
        ((Buffer) keyView).clear();
        ((Buffer) keyView).limit(keyLength);
        ((Buffer) buffer).clear();
        buffer.put(keyView);
    }

    static long directAddress(ByteBuffer buffer) {
        return ((sun.nio.ch.DirectBuffer) buffer).address();
    }

    static ByteBuffer resolveEncodedBuffer(ByteBuffer ioBuffer, int encodedLength) {
        if (encodedLength > 0) {
            return ioBuffer;
        }
        ByteBuffer overflow = getLastDirectOverflowBuffer();
        if (overflow == null) {
            throw new IllegalStateException("missing overflow direct buffer from JNI");
        }
        return overflow;
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

    /**
     * Open a structured direct scan cursor with caller-owned direct range buffers.
     *
     * <p>The direct cursor keeps batch payloads in pooled direct buffers so scan results can be
     * consumed without JNI byte[] materialization.
     */
    public DirectScanCursor scanDirectWithOptions(
            int bucket,
            ByteBuffer startKeyInclusive,
            int startKeyLength,
            ByteBuffer endKeyExclusive,
            int endKeyLength,
            ScanOptions options) {
        if (startKeyInclusive == null || !startKeyInclusive.isDirect()) {
            throw new IllegalArgumentException("startKeyInclusive must be a direct ByteBuffer");
        }
        if (endKeyExclusive == null || !endKeyExclusive.isDirect()) {
            throw new IllegalArgumentException("endKeyExclusive must be a direct ByteBuffer");
        }
        if (startKeyLength < 0 || startKeyLength > startKeyInclusive.capacity()) {
            throw new IllegalArgumentException("startKeyLength out of range: " + startKeyLength);
        }
        if (endKeyLength < 0 || endKeyLength > endKeyExclusive.capacity()) {
            throw new IllegalArgumentException("endKeyLength out of range: " + endKeyLength);
        }
        long soh = options == null ? 0L : options.getNativeHandle();
        long h =
                openStructuredDirectScanCursor(
                        nativeHandle,
                        bucket,
                        directAddress(startKeyInclusive),
                        startKeyLength,
                        directAddress(endKeyExclusive),
                        endKeyLength,
                        soh);
        if (h == 0L) {
            throw new IllegalStateException("failed to open structured direct scan cursor");
        }
        return new DirectScanCursor(h, directBufferPool);
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
    public CompletableFuture<ShardSnapshot> asyncSnapshot() {
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

    private static native long openHandleWithRange(
            String configPath, int rangeStartInclusive, int rangeEndInclusive);

    private static native int[] directBufferPoolConfig(long nativeHandle);

    private static native long openHandleFromJson(String configJson);

    private static native long openHandleFromJsonWithRange(
            String configJson, int rangeStartInclusive, int rangeEndInclusive);

    private static native String currentSchemaJson(long nativeHandle);

    private static native long createSchemaBuilder(long nativeHandle);

    private static native long restoreHandle(
            String configPath, long snapshotId, String dbId, boolean newDbId);

    private static native long restoreHandleFromJson(
            String configJson, long snapshotId, String dbId, boolean newDbId);

    private static native long restoreWithManifestHandle(String configPath, String manifestPath);

    private static native long restoreWithManifestHandleFromJson(
            String configJson, String manifestPath);

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

    private static native void putBytesDirectWithOptions(
            long nativeHandle,
            int bucket,
            long keyAddress,
            int keyCapacity,
            int keyLength,
            int column,
            long valueAddress,
            int valueCapacity,
            int valueLength,
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

    private static native int getEncodedDirectWithOptions(
            long nativeHandle,
            int bucket,
            long ioAddress,
            int ioCapacity,
            int keyLength,
            long readOptionsHandle);

    static native ByteBuffer getLastDirectOverflowBuffer();

    // typed scan
    private static native long openStructuredScanCursor(
            long nativeHandle,
            int bucket,
            byte[] startKeyInclusive,
            byte[] endKeyExclusive,
            long scanOptionsHandle);

    private static native long openStructuredDirectScanCursor(
            long nativeHandle,
            int bucket,
            long startKeyAddress,
            int startKeyLength,
            long endKeyAddress,
            int endKeyLength,
            long scanOptionsHandle);

    private static native String id(long nativeHandle);

    private static native int nowSeconds(long nativeHandle);

    private static native void setTime(long nativeHandle, int nextSeconds);

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
}
