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

    /** Get one column value from routed shard snapshot. */
    public byte[] get(int bucket, byte[] key, int column) {
        return get(nativeHandle, bucket, key, column);
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

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long openCurrentHandle(String configPath);

    private static native long openCurrentHandleFromJson(String configJson);

    private static native long openHandle(String configPath, int globalSnapshotId);

    private static native long openHandleFromJson(String configJson, int globalSnapshotId);

    private static native void refresh(long nativeHandle);

    private static native byte[] get(long nativeHandle, int bucket, byte[] key, int column);

    private static native String readMode(long nativeHandle);

    private static native long configuredSnapshotId(long nativeHandle);

    private static native String listGlobalSnapshotsJson(long nativeHandle);
}
