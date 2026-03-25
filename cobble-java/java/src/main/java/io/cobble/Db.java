package io.cobble;

/** Java binding for cobble Db. */
public final class Db extends NativeObject {
    private Db(long nativeHandle) {
        super(nativeHandle);
    }

    public static Db open(String configPath) {
        NativeLoader.load();
        long nativeHandle = openHandle(configPath);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open db");
        }
        return new Db(nativeHandle);
    }

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
