package io.cobble;

/** Java binding for cobble-data-structure StructuredDb (DataStructureDb). */
public final class StructuredDb extends NativeObject {
    private StructuredDb(long nativeHandle) {
        super(nativeHandle);
    }

    public static StructuredDb open(String configPath) {
        NativeLoader.load();
        long nativeHandle = openHandle(configPath);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open structured db");
        }
        return new StructuredDb(nativeHandle);
    }

    public static StructuredDb open(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = openHandleFromJson(config.toJson());
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open structured db from config json");
        }
        return new StructuredDb(nativeHandle);
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
