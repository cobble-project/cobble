package io.cobble;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;

/** Counts and dependency-preserved shard/global snapshots from offline cleanup. */
public final class SnapshotCleanupResult {
    private static final Gson GSON = new Gson();

    /** Removed data, schema, and active-memtable files, excluding manifests and CURRENT. */
    @SerializedName("deleted_files")
    public long deletedFiles;

    @SerializedName("deleted_shard_snapshots")
    public long deletedShardSnapshots;

    @SerializedName("deleted_global_snapshots")
    public long deletedGlobalSnapshots;

    /** Selected snapshots retained for dependencies or pending shard cleanup. */
    @SerializedName("preserved_snapshots")
    public List<PreservedSnapshot> preservedSnapshots = new ArrayList<>();

    static SnapshotCleanupResult fromJson(String json) {
        return GSON.fromJson(json, SnapshotCleanupResult.class);
    }

    /** Identity and reasons for a preserved snapshot; null dbId denotes a global snapshot. */
    public static final class PreservedSnapshot {
        @SerializedName("db_id")
        public String dbId;

        @SerializedName("snapshot_id")
        public long snapshotId;

        @SerializedName("manifest_path")
        public String manifestPath;

        public List<String> reasons = new ArrayList<>();
    }
}
