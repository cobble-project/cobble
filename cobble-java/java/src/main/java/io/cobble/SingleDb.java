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
        return get(nativeHandle, bucket, key, column);
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

    private static native byte[] get(long nativeHandle, int bucket, byte[] key, int column);

    private static native void delete(long nativeHandle, int bucket, byte[] key, int column);
}
