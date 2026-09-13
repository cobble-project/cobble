package io.cobble;

/** Snapshot maintenance helper APIs. */
public final class SnapshotTools {
    static {
        NativeLoader.load();
    }

    private SnapshotTools() {}

    /**
     * Prune one shard snapshot by dbId and snapshotId.
     *
     * <p>This is an out-of-band maintenance API. It does not rely on an existing Db instance.
     */
    public static boolean pruneShardSnapshot(Config config, String dbId, long snapshotId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (dbId == null || dbId.trim().isEmpty()) {
            throw new IllegalArgumentException("dbId must not be empty");
        }
        if (snapshotId < 0) {
            throw new IllegalArgumentException("snapshotId must be >= 0");
        }
        return pruneShardSnapshotFromJson(config.toJson(), dbId, snapshotId);
    }

    /**
     * Load complete shard snapshot metadata without opening a live writer DB.
     *
     * <p>The manifest path must be absolute and accessible through {@code config}'s metadata
     * volume. This call only reads the manifest and captured schema metadata.
     */
    public static ShardSnapshot loadShardSnapshot(Config config, String dbId, String manifestPath) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (dbId == null || dbId.trim().isEmpty()) {
            throw new IllegalArgumentException("dbId must not be empty");
        }
        if (manifestPath == null || manifestPath.trim().isEmpty()) {
            throw new IllegalArgumentException("manifestPath must not be empty");
        }
        return ShardSnapshot.fromJson(
                loadShardSnapshotFromJson(config.toJson(), dbId, manifestPath));
    }

    private static native boolean pruneShardSnapshotFromJson(
            String configJson, String dbId, long snapshotId);

    private static native String loadShardSnapshotFromJson(
            String configJson, String dbId, String manifestPath);
}
