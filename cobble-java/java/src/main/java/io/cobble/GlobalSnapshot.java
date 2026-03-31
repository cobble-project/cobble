package io.cobble;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** Java object for a global snapshot manifest in coordinator storage. */
public final class GlobalSnapshot implements Serializable {
    private static final long serialVersionUID = 534597938344L;
    private static final Gson GSON = new GsonBuilder().create();

    /** Global snapshot id. */
    @SerializedName("id")
    public long id;

    /** Total bucket count covered by this global snapshot. */
    @SerializedName("total_buckets")
    public int totalBuckets;

    /** All shard snapshots included in this global snapshot. */
    @SerializedName("shard_snapshots")
    public List<ShardSnapshot> shardSnapshots = new ArrayList<ShardSnapshot>();

    public static GlobalSnapshot fromJson(String json) {
        return GSON.fromJson(json, GlobalSnapshot.class);
    }

    public static List<GlobalSnapshot> listFromJson(String json) {
        GlobalSnapshot[] snapshots = GSON.fromJson(json, GlobalSnapshot[].class);
        List<GlobalSnapshot> out = new ArrayList<GlobalSnapshot>();
        if (snapshots == null) {
            return out;
        }
        for (GlobalSnapshot snapshot : snapshots) {
            out.add(snapshot);
        }
        return out;
    }
}
