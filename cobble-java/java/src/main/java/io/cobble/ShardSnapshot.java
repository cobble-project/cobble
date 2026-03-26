package io.cobble;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Pure Java shard snapshot payload.
 *
 * <p>This object mirrors Rust {@code cobble::ShardSnapshotInput} and is intended for passing
 * snapshot metadata from {@link Db#getShardSnapshot(long)} to {@link
 * DbCoordinator#materializeGlobalSnapshot(int, long, List)}.
 */
public final class ShardSnapshot implements Serializable {
    private static final long serialVersionUID = 874398237491L;
    private static final Gson GSON = new GsonBuilder().create();

    /** Covered bucket ranges for this shard snapshot. */
    @SerializedName("ranges")
    public List<Range> ranges = new ArrayList<Range>();

    /** Source DB id. */
    @SerializedName("db_id")
    public String dbId;

    /** Source snapshot id in the shard DB. */
    @SerializedName("snapshot_id")
    public long snapshotId;

    /** Full manifest path to shard snapshot manifest. */
    @SerializedName("manifest_path")
    public String manifestPath;

    static ShardSnapshot fromJson(String json) {
        return GSON.fromJson(json, ShardSnapshot.class);
    }

    static String listToJson(List<ShardSnapshot> snapshots) {
        return GSON.toJson(snapshots);
    }

    /** Inclusive bucket range. */
    public static final class Range {
        /** Range start bucket. */
        @SerializedName("start")
        public int start;

        /** Range end bucket. */
        @SerializedName("end")
        public int end;
    }
}
