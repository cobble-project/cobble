package io.cobble;

/**
 * Java binding for read-only DB view from one snapshot.
 *
 * <p>Use this for direct snapshot reads without write capability.
 */
public final class ReadOnlyDb extends NativeObject {
    private ReadOnlyDb(long nativeHandle) {
        super(nativeHandle);
    }

    /**
     * Open read-only db by config file path.
     *
     * @param configPath db config path
     * @param snapshotId snapshot id to open
     * @param dbId source db id
     * @return read-only db handle
     */
    public static ReadOnlyDb open(String configPath, long snapshotId, String dbId) {
        NativeLoader.load();
        long nativeHandle = openHandle(configPath, snapshotId, dbId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open readonly db");
        }
        return new ReadOnlyDb(nativeHandle);
    }

    /**
     * Open read-only db by Java config object.
     *
     * @param config Java config
     * @param snapshotId snapshot id to open
     * @param dbId source db id
     * @return read-only db handle
     */
    public static ReadOnlyDb open(Config config, long snapshotId, String dbId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = openHandleFromJson(config.toJson(), snapshotId, dbId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open readonly db from config json");
        }
        return new ReadOnlyDb(nativeHandle);
    }

    /**
     * Get one column value from snapshot.
     *
     * @return column bytes, or {@code null} when missing
     */
    public byte[] get(int bucket, byte[] key, int column) {
        return get(nativeHandle, bucket, key, column);
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long openHandle(String configPath, long snapshotId, String dbId);

    private static native long openHandleFromJson(String configJson, long snapshotId, String dbId);

    private static native byte[] get(long nativeHandle, int bucket, byte[] key, int column);
}
