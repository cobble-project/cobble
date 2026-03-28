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

    /** Get one column by index from snapshot. */
    public byte[] get(int bucket, byte[] key, int column) {
        try (ReadOptions options = ReadOptions.forColumn(column)) {
            return singleColumnOrNull(get(nativeHandle, bucket, key, options.nativeHandle));
        }
    }

    /** Get selected columns from snapshot using reusable native-backed read options. */
    public byte[][] get(int bucket, byte[] key, ReadOptions options) {
        long readOptionsHandle = options == null ? 0L : options.nativeHandle;
        return get(nativeHandle, bucket, key, readOptionsHandle);
    }

    /** Open a high-throughput native scan cursor within [startKeyInclusive, endKeyExclusive). */
    public ScanCursor scan(
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
            throw new IllegalStateException("failed to open readonly scan cursor");
        }
        return new ScanCursor(handle);
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long openHandle(String configPath, long snapshotId, String dbId);

    private static native long openHandleFromJson(String configJson, long snapshotId, String dbId);

    private static native byte[][] get(
            long nativeHandle, int bucket, byte[] key, long readOptionsHandle);

    private static native long openScanCursor(
            long nativeHandle,
            int bucket,
            byte[] startKeyInclusive,
            byte[] endKeyExclusive,
            long scanOptionsHandle);

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
