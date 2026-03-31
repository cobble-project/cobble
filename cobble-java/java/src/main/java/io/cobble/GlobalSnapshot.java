package io.cobble;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

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

    static GlobalSnapshot fromJson(String json) {
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

    /**
     * Poll until a global snapshot with the given id appears in the list returned by {@code
     * listSnapshots}. Retries up to 120 times with 50ms intervals.
     *
     * @param snapshotId the expected global snapshot id
     * @param listSnapshots supplier that returns the current list of global snapshots
     * @return the matching GlobalSnapshot
     * @throws IllegalStateException if the snapshot is not found within the retry window
     */
    public static GlobalSnapshot waitForReady(
            long snapshotId, Callable<List<GlobalSnapshot>> listSnapshots) {
        IllegalStateException lastError = null;
        for (int i = 0; i < 120; i++) {
            try {
                List<GlobalSnapshot> snapshots = listSnapshots.call();
                for (GlobalSnapshot snapshot : snapshots) {
                    if (snapshot.id == snapshotId) {
                        return snapshot;
                    }
                }
            } catch (IllegalStateException e) {
                lastError = e;
            } catch (Exception e) {
                lastError = new IllegalStateException(e);
            }
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "interrupted while waiting global snapshot ready", e);
            }
        }
        if (lastError != null) {
            throw lastError;
        }
        throw new IllegalStateException("global snapshot is unavailable: " + snapshotId);
    }
}
