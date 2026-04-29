package io.cobble;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Java binding for writable Cobble database.
 *
 * <p>Use {@link #open(String)} or {@link #open(Config)} to create a DB instance, then call
 * put/get/delete operations. Remember to close with try-with-resources.
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
        return wrapOpenedDb(nativeHandle);
    }

    /** Open a writable DB from a config file path with an explicit bucket range. */
    public static Db open(String configPath, int rangeStartInclusive, int rangeEndInclusive) {
        NativeLoader.load();
        long nativeHandle = openHandleWithRange(configPath, rangeStartInclusive, rangeEndInclusive);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open db with bucket range");
        }
        return wrapOpenedDb(nativeHandle);
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
        return wrapOpenedDb(nativeHandle);
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
        return wrapOpenedDb(nativeHandle);
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
        return restore(configPath, snapshotId, dbId, false);
    }

    /**
     * Restore a writable DB from one snapshot.
     *
     * <p>Set {@code newDbId} to {@code true} to restore from the source snapshot into a fresh db
     * identity and start a new snapshot chain.
     *
     * @param configPath db config path
     * @param snapshotId snapshot id
     * @param dbId source db id
     * @param newDbId whether to restore into a fresh db identity
     * @return restored writable db
     */
    public static Db restore(String configPath, long snapshotId, String dbId, boolean newDbId) {
        NativeLoader.load();
        long nativeHandle = restoreHandle(configPath, snapshotId, dbId, newDbId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException(
                    newDbId
                            ? "failed to restore db from snapshot with fresh db id"
                            : "failed to restore db from snapshot");
        }
        return wrapOpenedDb(nativeHandle);
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
        return restore(config, snapshotId, dbId, false);
    }

    /**
     * Restore a writable DB from one snapshot.
     *
     * <p>Set {@code newDbId} to {@code true} to restore from the source snapshot into a fresh db
     * identity and start a new snapshot chain.
     *
     * @param config Java config
     * @param snapshotId snapshot id
     * @param dbId source db id
     * @param newDbId whether to restore into a fresh db identity
     * @return restored writable db
     */
    public static Db restore(Config config, long snapshotId, String dbId, boolean newDbId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = restoreHandleFromJson(config.toJson(), snapshotId, dbId, newDbId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException(
                    newDbId
                            ? "failed to restore db from snapshot config json with fresh db id"
                            : "failed to restore db from snapshot config json");
        }
        return wrapOpenedDb(nativeHandle);
    }

    /**
     * Restore a writable DB from an explicit source manifest path.
     *
     * <p>The manifest path is treated as restore input, and the returned DB always gets a fresh db
     * id and starts a new snapshot chain.
     */
    public static Db restoreWithManifest(String configPath, String manifestPath) {
        NativeLoader.load();
        long nativeHandle = restoreWithManifestHandle(configPath, manifestPath);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to restore db from manifest path");
        }
        return wrapOpenedDb(nativeHandle);
    }

    /**
     * Restore a writable DB from an explicit source manifest path.
     *
     * <p>The manifest path is treated as restore input, and the returned DB always gets a fresh db
     * id and starts a new snapshot chain.
     */
    public static Db restoreWithManifest(Config config, String manifestPath) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = restoreWithManifestHandleFromJson(config.toJson(), manifestPath);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to restore db from manifest path config json");
        }
        return wrapOpenedDb(nativeHandle);
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
        return wrapOpenedDb(nativeHandle);
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
        return wrapOpenedDb(nativeHandle);
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

    /**
     * Put one column value using caller-owned direct buffers.
     *
     * <p>Both {@code keyBuffer} and {@code valueBuffer} must be direct ByteBuffers. Only bytes in
     * range {@code [0, keyLength)} / {@code [0, valueLength)} are consumed.
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
        long writeOptionsHandle = options == null ? 0L : options.nativeHandle;
        putDirectWithOptions(
                nativeHandle,
                bucket,
                DirectIoUtils.directAddress(keyBuffer),
                keyBuffer.capacity(),
                keyLength,
                column,
                DirectIoUtils.directAddress(valueBuffer),
                valueBuffer.capacity(),
                valueLength,
                writeOptionsHandle);
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

    /**
     * Get selected columns for one key as an encoded zero-copy direct row view.
     *
     * <p>The returned payload references either the pooled IO buffer or an overflow direct buffer.
     * Close the returned view when done so pooled buffers can be reused.
     */
    public DirectEncodedRow getDirectEncodedRowWithOptions(
            int bucket, byte[] key, ReadOptions options) {
        EncodedDirectResult encoded = readEncodedDirectResult(bucket, key, options);
        return encoded == null
                ? null
                : new DirectEncodedRow(encoded.address, encoded.length, encoded.releaser);
    }

    /**
     * Get selected columns for one key as an encoded zero-copy direct row view via caller-owned
     * direct key buffer.
     */
    public DirectEncodedRow getDirectEncodedRowWithOptions(
            int bucket, ByteBuffer keyBuffer, ReadOptions options) {
        if (keyBuffer == null || !keyBuffer.isDirect()) {
            throw new IllegalArgumentException("keyBuffer must be a direct ByteBuffer");
        }
        return getDirectEncodedRowWithOptions(
                bucket, keyBuffer, ((Buffer) keyBuffer).limit(), options);
    }

    /** Compatibility overload that treats {@code keyBuffer[0..keyLength)} as key bytes. */
    public DirectEncodedRow getDirectEncodedRowWithOptions(
            int bucket, ByteBuffer keyBuffer, int keyLength, ReadOptions options) {
        EncodedDirectResult encoded =
                readEncodedDirectResult(bucket, keyBuffer, keyLength, options);
        return encoded == null
                ? null
                : new DirectEncodedRow(encoded.address, encoded.length, encoded.releaser);
    }

    /** Get selected columns for one key as a zero-copy direct view. */
    public DirectColumns getDirectColumnsWithOptions(int bucket, byte[] key, ReadOptions options) {
        EncodedDirectResult encoded = readEncodedDirectResult(bucket, key, options);
        if (encoded == null) {
            return null;
        }
        try {
            return DirectColumns.decode(encoded.buffer, encoded.length, encoded.releaser);
        } catch (RuntimeException e) {
            encoded.releaser.run();
            throw e;
        }
    }

    /** Get selected columns for one key as a zero-copy direct view via direct key buffer. */
    public DirectColumns getDirectColumnsWithOptions(
            int bucket, ByteBuffer keyBuffer, ReadOptions options) {
        if (keyBuffer == null || !keyBuffer.isDirect()) {
            throw new IllegalArgumentException("keyBuffer must be a direct ByteBuffer");
        }
        EncodedDirectResult encoded =
                readEncodedDirectResult(bucket, keyBuffer, ((Buffer) keyBuffer).limit(), options);
        if (encoded == null) {
            return null;
        }
        try {
            return DirectColumns.decode(encoded.buffer, encoded.length, encoded.releaser);
        } catch (RuntimeException e) {
            encoded.releaser.run();
            throw e;
        }
    }

    /** Compatibility overload that treats {@code keyBuffer[0..keyLength)} as key bytes. */
    public DirectColumns getDirectColumnsWithOptions(
            int bucket, ByteBuffer keyBuffer, int keyLength, ReadOptions options) {
        EncodedDirectResult encoded =
                readEncodedDirectResult(bucket, keyBuffer, keyLength, options);
        if (encoded == null) {
            return null;
        }
        try {
            return DirectColumns.decode(encoded.buffer, encoded.length, encoded.releaser);
        } catch (RuntimeException e) {
            encoded.releaser.run();
            throw e;
        }
    }

    /** Get one key via pooled direct IO and return one direct buffer per selected column. */
    public ByteBuffer[] getDirectWithOptions(int bucket, byte[] key, ReadOptions options) {
        EncodedDirectResult encoded = readEncodedDirectResult(bucket, key, options);
        if (encoded == null) {
            return null;
        }
        try {
            return DirectIoUtils.decodeDirectColumnsCopy(encoded.address, encoded.length);
        } finally {
            encoded.releaser.run();
        }
    }

    /**
     * Get one key via caller-owned direct key buffer and return direct buffers per column.
     *
     * <p>Key bytes are read from {@code keyBuffer[0..keyBuffer.limit())}.
     */
    public ByteBuffer[] getDirectWithOptions(
            int bucket, ByteBuffer keyBuffer, ReadOptions options) {
        if (keyBuffer == null || !keyBuffer.isDirect()) {
            throw new IllegalArgumentException("keyBuffer must be a direct ByteBuffer");
        }
        return getDirectWithOptions(bucket, keyBuffer, ((Buffer) keyBuffer).limit(), options);
    }

    /** Compatibility overload that treats {@code keyBuffer[0..keyLength)} as key bytes. */
    public ByteBuffer[] getDirectWithOptions(
            int bucket, ByteBuffer keyBuffer, int keyLength, ReadOptions options) {
        EncodedDirectResult encoded =
                readEncodedDirectResult(bucket, keyBuffer, keyLength, options);
        if (encoded == null) {
            return null;
        }
        try {
            return DirectIoUtils.decodeDirectColumnsCopy(encoded.address, encoded.length);
        } finally {
            encoded.releaser.run();
        }
    }

    /**
     * Read one key and return an encoded payload buffer.
     *
     * <p>{@code ioBuffer} must be a direct ByteBuffer. Key bytes are read from {@code
     * ioBuffer[0..keyLength)}.
     *
     * <p>JNI returns only encoded length. If payload fits in {@code ioBuffer}, this method returns
     * a view of {@code ioBuffer}. Otherwise JNI writes payload into a cached overflow direct buffer
     * and this method returns a view of that overflow buffer. The returned view always has position
     * 0 and limit set to payload length.
     *
     * @return payload buffer, or null if key is not found
     */
    public ByteBuffer getEncodedDirectWithOptions(
            int bucket, ByteBuffer ioBuffer, int keyLength, ReadOptions options) {
        if (ioBuffer == null || !ioBuffer.isDirect()) {
            throw new IllegalArgumentException("ioBuffer must be a direct ByteBuffer");
        }
        if (keyLength < 0 || keyLength > ioBuffer.capacity()) {
            throw new IllegalArgumentException("keyLength out of range: " + keyLength);
        }
        long readOptionsHandle = options == null ? 0L : options.nativeHandle;
        int encodedLength =
                getEncodedDirectWithOptions(
                        nativeHandle,
                        bucket,
                        DirectIoUtils.directAddress(ioBuffer),
                        ioBuffer.capacity(),
                        keyLength,
                        readOptionsHandle);
        if (encodedLength == 0) {
            return null;
        }
        ByteBuffer encoded =
                DirectIoUtils.resolveEncodedBuffer(
                        ioBuffer, encodedLength, getLastDirectOverflowBuffer());
        int length = Math.abs(encodedLength);
        ByteBuffer view = encoded.duplicate();
        ((Buffer) view).clear();
        ((Buffer) view).limit(length);
        return view;
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
     * Open a direct scan cursor with caller-owned direct range buffers.
     *
     * <p>The direct cursor keeps each batch payload in pooled direct buffers and exposes zero-copy
     * key/value slices.
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
        long scanOptionsHandle = options == null ? 0L : options.nativeHandle;
        long handle =
                openDirectScanCursor(
                        nativeHandle,
                        bucket,
                        DirectIoUtils.directAddress(startKeyInclusive),
                        startKeyLength,
                        DirectIoUtils.directAddress(endKeyExclusive),
                        endKeyLength,
                        scanOptionsHandle);
        if (handle == 0L) {
            throw new IllegalStateException("failed to open direct scan cursor");
        }
        return new DirectScanCursor(handle, directBufferPool);
    }

    private EncodedDirectResult readEncodedDirectResult(
            int bucket, byte[] key, ReadOptions options) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }
        long readOptionsHandle = options == null ? 0L : options.nativeHandle;
        DirectBufferPool pool = directBufferPool;
        ByteBuffer pooled = pool.acquire();
        ByteBuffer ioBuffer = pooled;
        boolean pooledReleased = false;
        boolean usingPooledForResult = false;
        try {
            if (key.length > ioBuffer.capacity()) {
                ioBuffer = ByteBuffer.allocateDirect(key.length);
            }
            DirectIoUtils.copyKey(ioBuffer, key);
            int encodedLength =
                    getEncodedDirectWithOptions(
                            nativeHandle,
                            bucket,
                            DirectIoUtils.directAddress(ioBuffer),
                            ioBuffer.capacity(),
                            key.length,
                            readOptionsHandle);
            if (encodedLength == 0) {
                pool.release(pooled);
                return null;
            }
            ByteBuffer resultBuffer =
                    DirectIoUtils.resolveEncodedBuffer(
                            ioBuffer, encodedLength, getLastDirectOverflowBuffer());
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
                return new EncodedDirectResult(
                        resultBuffer,
                        DirectIoUtils.directAddress(resultBuffer),
                        decodedLength,
                        releaser);
            }
            pool.release(pooled);
            pooledReleased = true;
            return new EncodedDirectResult(
                    resultBuffer,
                    DirectIoUtils.directAddress(resultBuffer),
                    decodedLength,
                    () -> {});
        } catch (RuntimeException e) {
            if (!pooledReleased && !usingPooledForResult) {
                pool.release(pooled);
            }
            throw e;
        }
    }

    private EncodedDirectResult readEncodedDirectResult(
            int bucket, ByteBuffer keyBuffer, int keyLength, ReadOptions options) {
        if (keyBuffer == null || !keyBuffer.isDirect()) {
            throw new IllegalArgumentException("keyBuffer must be a direct ByteBuffer");
        }
        if (keyLength < 0 || keyLength > keyBuffer.capacity()) {
            throw new IllegalArgumentException("keyLength out of range: " + keyLength);
        }
        long readOptionsHandle = options == null ? 0L : options.nativeHandle;
        DirectBufferPool pool = directBufferPool;
        ByteBuffer pooled = pool.acquire();
        ByteBuffer ioBuffer = pooled;
        boolean pooledReleased = false;
        boolean usingPooledForResult = false;
        try {
            if (keyLength > ioBuffer.capacity()) {
                ioBuffer = ByteBuffer.allocateDirect(keyLength);
            }
            DirectIoUtils.copyKey(ioBuffer, keyBuffer, keyLength);
            int encodedLength =
                    getEncodedDirectWithOptions(
                            nativeHandle,
                            bucket,
                            DirectIoUtils.directAddress(ioBuffer),
                            ioBuffer.capacity(),
                            keyLength,
                            readOptionsHandle);
            if (encodedLength == 0) {
                pool.release(pooled);
                return null;
            }
            ByteBuffer resultBuffer =
                    DirectIoUtils.resolveEncodedBuffer(
                            ioBuffer, encodedLength, getLastDirectOverflowBuffer());
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
                return new EncodedDirectResult(
                        resultBuffer,
                        DirectIoUtils.directAddress(resultBuffer),
                        decodedLength,
                        releaser);
            }
            pool.release(pooled);
            pooledReleased = true;
            return new EncodedDirectResult(
                    resultBuffer,
                    DirectIoUtils.directAddress(resultBuffer),
                    decodedLength,
                    () -> {});
        } catch (RuntimeException e) {
            if (!pooledReleased && !usingPooledForResult) {
                pool.release(pooled);
            }
            throw e;
        }
    }

    private static final class EncodedDirectResult {
        private final ByteBuffer buffer;
        private final long address;
        private final int length;
        private final Runnable releaser;

        private EncodedDirectResult(
                ByteBuffer buffer, long address, int length, Runnable releaser) {
            this.buffer = buffer;
            this.address = address;
            this.length = length;
            this.releaser = releaser;
        }
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

    private static native int[] directBufferPoolConfig(long nativeHandle);

    private static native long openHandleFromJson(String configJson);

    private static native long openHandleFromJsonWithRange(
            String configJson, int rangeStartInclusive, int rangeEndInclusive);

    private static native long restoreHandle(
            String configPath, long snapshotId, String dbId, boolean newDbId);

    private static native long restoreHandleFromJson(
            String configJson, long snapshotId, String dbId, boolean newDbId);

    private static native long restoreWithManifestHandle(String configPath, String manifestPath);

    private static native long restoreWithManifestHandleFromJson(
            String configJson, String manifestPath);

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

    private static native void putDirectWithOptions(
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

    private static native int getEncodedDirectWithOptions(
            long nativeHandle,
            int bucket,
            long ioAddress,
            int ioCapacity,
            int keyLength,
            long readOptionsHandle);

    static native ByteBuffer getLastDirectOverflowBuffer();

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

    private static native long openDirectScanCursor(
            long nativeHandle,
            int bucket,
            long startKeyAddress,
            int startKeyLength,
            long endKeyAddress,
            int endKeyLength,
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
