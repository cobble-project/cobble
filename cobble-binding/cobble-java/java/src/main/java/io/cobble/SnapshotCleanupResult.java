package io.cobble;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

/** Counts and dependency-preserved shard snapshots from offline cleanup. */
public final class SnapshotCleanupResult {
    private static final Gson GSON = new Gson();

    /** Removed data, schema, and active-memtable files, excluding manifests and CURRENT. */
    @SerializedName("deleted_files")
    public long deletedFiles;

    @SerializedName("deleted_shard_snapshots")
    public long deletedShardSnapshots;

    @SerializedName("deleted_global_snapshots")
    public long deletedGlobalSnapshots;

    /** Selected shard snapshots retained because surviving snapshots depend on them. */
    @SerializedName("preserved_snapshots")
    public List<ShardSnapshotIdentity> preservedSnapshots = new ArrayList<>();

    static SnapshotCleanupResult fromJson(String json) {
        return GSON.fromJson(json, SnapshotCleanupResult.class);
    }

    /** Identity of one dependency-preserved shard snapshot. */
    public static final class ShardSnapshotIdentity {
        @SerializedName("db_id")
        public String dbId;

        @SerializedName("snapshot_id")
        public long snapshotId;

        @SerializedName("manifest_path")
        public String manifestPath;
    }
}
