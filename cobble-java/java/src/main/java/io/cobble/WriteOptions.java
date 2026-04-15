package io.cobble;

/** Native-backed write options for put/merge operations. */
public final class WriteOptions extends NativeObject {
    public WriteOptions() {
        super(loadAndCreateHandle());
    }

    /** Set per-write TTL in seconds. */
    public WriteOptions ttlSeconds(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("ttlSeconds must be >= 0");
        }
        setTtlSeconds(nativeHandle, value);
        return this;
    }

    /** Clear per-write TTL; falls back to DB default TTL behavior. */
    public WriteOptions clearTtl() {
        clearTtlSeconds(nativeHandle);
        return this;
    }

    /** Target one column family for subsequent writes. */
    public WriteOptions columnFamily(String columnFamily) {
        if (columnFamily == null || columnFamily.trim().isEmpty()) {
            throw new IllegalArgumentException("columnFamily must not be blank");
        }
        setColumnFamily(nativeHandle, columnFamily);
        return this;
    }

    /** Clear any previously selected column family and fall back to default family. */
    public WriteOptions clearColumnFamily() {
        clearColumnFamily(nativeHandle);
        return this;
    }

    /** Create options with ttl set. */
    public static WriteOptions withTtl(int ttlSeconds) {
        return new WriteOptions().ttlSeconds(ttlSeconds);
    }

    /** Create options with the target column family set. */
    public static WriteOptions withColumnFamily(String columnFamily) {
        return new WriteOptions().columnFamily(columnFamily);
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long createHandle();

    private static native void setTtlSeconds(long nativeHandle, int ttlSeconds);

    private static native void clearTtlSeconds(long nativeHandle);

    private static native void setColumnFamily(long nativeHandle, String columnFamily);

    private static native void clearColumnFamily(long nativeHandle);

    private static long loadAndCreateHandle() {
        NativeLoader.load();
        return createHandle();
    }
}
