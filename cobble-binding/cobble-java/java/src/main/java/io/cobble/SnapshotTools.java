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

    private static native boolean pruneShardSnapshotFromJson(
            String configJson, String dbId, long snapshotId);
}
