package io.cobble;

import java.util.List;

/** Java binding for cobble DbCoordinator (global snapshot manifest coordinator). */
public final class DbCoordinator extends NativeObject {
    private DbCoordinator(long nativeHandle) {
        super(nativeHandle);
    }

    /**
     * Open coordinator from config path.
     *
     * @param configPath path to coordinator config
     * @return opened coordinator
     */
    public static DbCoordinator open(String configPath) {
        NativeLoader.load();
        long nativeHandle = openHandle(configPath);
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open db coordinator");
        }
        return new DbCoordinator(nativeHandle);
    }

    /**
     * Open coordinator from Java config.
     *
     * @param config Java-side config
     * @return opened coordinator
     */
    public static DbCoordinator open(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        NativeLoader.load();
        long nativeHandle = openHandleFromJson(config.toJson());
        if (nativeHandle == 0L) {
            throw new IllegalStateException("failed to open db coordinator from config json");
        }
        return new DbCoordinator(nativeHandle);
    }

    /**
     * Materialize one global snapshot from shard snapshot inputs.
     *
     * <p>This method replaces the old single-shard helper and accepts a list of pure Java {@link
     * ShardSnapshot}.
     *
     * @param totalBuckets total bucket count
     * @param snapshotId target global snapshot id
     * @param shardInputs shard snapshot inputs
     */
    public GlobalSnapshot materializeGlobalSnapshot(
            int totalBuckets, long snapshotId, List<ShardSnapshot> shardInputs) {
        if (shardInputs == null || shardInputs.isEmpty()) {
            throw new IllegalArgumentException("shardInputs must not be empty");
        }
        return GlobalSnapshot.fromJson(
                materializeGlobalSnapshot(
                        nativeHandle,
                        totalBuckets,
                        snapshotId,
                        ShardSnapshot.listToJson(shardInputs)));
    }

    /**
     * Load one global snapshot by id.
     *
     * @param snapshotId global snapshot id
     * @return global snapshot object
     */
    public GlobalSnapshot getGlobalSnapshot(long snapshotId) {
        return GlobalSnapshot.fromJson(getGlobalSnapshotJson(nativeHandle, snapshotId));
    }

    /**
     * List all materialized global snapshots.
     *
     * @return snapshots sorted by id
     */
    public List<GlobalSnapshot> listGlobalSnapshots() {
        return GlobalSnapshot.listFromJson(listGlobalSnapshotsJson(nativeHandle));
    }

    /** Retain one global snapshot to protect it from auto-retention cleanup. */
    public boolean retainSnapshot(long snapshotId) {
        return retainSnapshot(nativeHandle, snapshotId);
    }

    /** Expire one global snapshot (also removes retain protection if present). */
    public boolean expireSnapshot(long snapshotId) {
        return expireSnapshot(nativeHandle, snapshotId);
    }

    @Override
    protected native void disposeInternal(long nativeHandle);

    private static native long openHandle(String configPath);

    private static native long openHandleFromJson(String configJson);

    private static native String materializeGlobalSnapshot(
            long nativeHandle, int totalBuckets, long snapshotId, String shardInputsJson);

    private static native String getGlobalSnapshotJson(long nativeHandle, long snapshotId);

    private static native String listGlobalSnapshotsJson(long nativeHandle);

    private static native boolean retainSnapshot(long nativeHandle, long snapshotId);

    private static native boolean expireSnapshot(long nativeHandle, long snapshotId);
}
