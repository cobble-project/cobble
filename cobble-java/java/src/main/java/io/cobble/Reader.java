package io.cobble;

import java.util.List;

/**
 * Java binding for Reader (read proxy over global snapshots).
 *
 * <p>Supports fixed-snapshot mode and current-pointer auto-refresh mode.
 */
public final class Reader extends NativeObject {
    private Reader(long nativeHandle) {
        super(nativeHandle);
    }

    /**
     * Open reader following current global snapshot pointer.
     *
     * @param configPath config path
     * @return reader in current mode
     */
    public static Reader openCurrent(String configPath) {
        NativeLoader.load();
        long nativeHandle = openCurrentHandle(configPath);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open current reader");
        }
        return new Reader(nativeHandle);
    }

    /**
     * Open reader following current global snapshot pointer.
     *
     * @param config Java config
     * @return reader in current mode
     */
    public static Reader openCurrent(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = openCurrentHandleFromJson(config.toJson());
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open current reader from config json");
        }
        return new Reader(nativeHandle);
    }

    /**
     * Open reader pinned to one global snapshot id.
     *
     * @param configPath config path
     * @param globalSnapshotId global snapshot id
     * @return snapshot-pinned reader
     */
    public static Reader open(String configPath, int globalSnapshotId) {
        NativeLoader.load();
        long nativeHandle = openHandle(configPath, globalSnapshotId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open reader");
        }
        return new Reader(nativeHandle);
    }

    /**
     * Open reader pinned to one global snapshot id.
     *
     * @param config Java config
     * @param globalSnapshotId global snapshot id
     * @return snapshot-pinned reader
     */
    public static Reader open(Config config, int globalSnapshotId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = openHandleFromJson(config.toJson(), globalSnapshotId);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open reader from config json");
        }
        return new Reader(nativeHandle);
    }

    /** Force refresh from global snapshot pointer. */
    public void refresh() {
        refresh(nativeHandle);
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

    /** Get selected columns by key using reusable native-backed read options. */
    public byte[][] get(int bucket, byte[] key) {
        return get(nativeHandle, bucket, key, 0L);
    }

    /** Get selected columns by key using reusable native-backed read options. */
    public byte[][] getWithOptions(int bucket, byte[] key, ReadOptions options) {
        long readOptionsHandle = options == null ? 0L : options.nativeHandle;
        return get(nativeHandle, bucket, key, readOptionsHandle);
    }

    /** Open a high-throughput native scan cursor within [startKeyInclusive, endKeyExclusive). */
    public ScanCursor scan(int bucket, byte[] startKeyInclusive, byte[] endKeyExclusive) {
        return scanWithOptions(bucket, startKeyInclusive, endKeyExclusive, null);
    }

    public ScanCursor scan(
            int bucket, byte[] startKeyInclusive, byte[] endKeyExclusive, String columnFamily) {
        try (ScanOptions options = new ScanOptions().columnFamily(columnFamily)) {
            return scanWithOptions(bucket, startKeyInclusive, endKeyExclusive, options);
        }
    }

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
            throw new IllegalStateException("failed to open reader scan cursor");
        }
        return new ScanCursor(handle);
    }

    /** Return mode string: {@code current} or {@code snapshot}. */
    public String readMode() {
        return readMode(nativeHandle);
    }

    /** Return configured snapshot id in snapshot mode, or -1 in current mode. */
    public long configuredSnapshotId() {
        return configuredSnapshotId(nativeHandle);
    }

    /**
     * List global snapshots visible to this reader.
     *
     * @return snapshots sorted by id
     */
    public List<GlobalSnapshot> listGlobalSnapshots() {
        return GlobalSnapshot.listFromJson(listGlobalSnapshotsJson(nativeHandle));
    }

    /**
     * Return the current global snapshot this reader is using.
     *
     * @return current global snapshot
     */
    public GlobalSnapshot currentGlobalSnapshot() {
        return GlobalSnapshot.fromJson(currentGlobalSnapshotJson(nativeHandle));
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long openCurrentHandle(String configPath);

    private static native long openCurrentHandleFromJson(String configJson);

    private static native long openHandle(String configPath, int globalSnapshotId);

    private static native long openHandleFromJson(String configJson, int globalSnapshotId);

    private static native void refresh(long nativeHandle);

    private static native byte[][] get(
            long nativeHandle, int bucket, byte[] key, long readOptionsHandle);

    private static native long openScanCursor(
            long nativeHandle,
            int bucket,
            byte[] startKeyInclusive,
            byte[] endKeyExclusive,
            long scanOptionsHandle);

    private static native String readMode(long nativeHandle);

    private static native long configuredSnapshotId(long nativeHandle);

    private static native String currentGlobalSnapshotJson(long nativeHandle);

    private static native String listGlobalSnapshotsJson(long nativeHandle);

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
