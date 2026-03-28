package io.cobble;

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

    public void put(int bucket, byte[] key, int column, byte[] value) {
        put(nativeHandle, bucket, key, column, value);
    }

    public byte[] get(int bucket, byte[] key, int column) {
        try (ReadOptions options = ReadOptions.forColumn(column)) {
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

    public void delete(int bucket, byte[] key, int column) {
        delete(nativeHandle, bucket, key, column);
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long openHandle(String configPath);

    private static native long openHandleFromJson(String configJson);

    private static native void put(
            long nativeHandle, int bucket, byte[] key, int column, byte[] value);

    private static native byte[][] get(
            long nativeHandle, int bucket, byte[] key, long readOptionsHandle);

    private static native void delete(long nativeHandle, int bucket, byte[] key, int column);

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
