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

    /** Create options with ttl set. */
    public static WriteOptions withTtl(int ttlSeconds) {
        return new WriteOptions().ttlSeconds(ttlSeconds);
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long createHandle();

    private static native void setTtlSeconds(long nativeHandle, int ttlSeconds);

    private static native void clearTtlSeconds(long nativeHandle);

    private static long loadAndCreateHandle() {
        NativeLoader.load();
        return createHandle();
    }
}
