package io.cobble.table;

import io.cobble.Config;
import io.cobble.NativeLoader;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Serializable table scan plan pinned to one committed global snapshot. */
public final class TableScanPlan implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String planJson;
    private final TableSchema schema;
    private final long snapshotId;
    private final int totalBuckets;
    private final long dataSizeBytes;

    private TableScanPlan(
            String planJson,
            TableSchema schema,
            long snapshotId,
            int totalBuckets,
            long dataSizeBytes) {
        this.planJson = planJson;
        this.schema = schema;
        this.snapshotId = snapshotId;
        this.totalBuckets = totalBuckets;
        this.dataSizeBytes = dataSizeBytes;
    }

    /** Plan the current committed snapshot. */
    public static TableScanPlan forCurrentSnapshot(Config config, String tableName) {
        return open(config, tableName, null);
    }

    /** Plan one fixed committed snapshot. */
    public static TableScanPlan forSnapshot(Config config, String tableName, long snapshotId) {
        if (snapshotId < 0L) {
            throw new IllegalArgumentException("snapshotId must be >= 0");
        }
        return open(config, tableName, Long.valueOf(snapshotId));
    }

    public TableSchema schema() {
        return schema;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public int totalBuckets() {
        return totalBuckets;
    }

    public long dataSizeBytes() {
        return dataSizeBytes;
    }

    /** Produce one independently serializable assignment per shard snapshot. */
    public List<TableScanSplit> splits() {
        NativeLoader.load();
        String json = splitsNative(planJson);
        JsonArray values = JsonParser.parseString(json).getAsJsonArray();
        List<TableScanSplit> splits = new ArrayList<TableScanSplit>(values.size());
        for (JsonElement value : values) {
            splits.add(new TableScanSplit(value.toString()));
        }
        return Collections.unmodifiableList(splits);
    }

    private static TableScanPlan open(Config config, String tableName, Long snapshotId) {
        if (config == null) {
            throw new IllegalArgumentException("config must not be null");
        }
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException("tableName must not be empty");
        }
        NativeLoader.load();
        String json =
                snapshotId == null
                        ? openCurrentNative(config.toJson(), tableName)
                        : openSnapshotNative(config.toJson(), tableName, snapshotId.longValue());
        return fromResponseJson(json);
    }

    static TableScanPlan fromResponseJson(String json) {
        JsonObject response = JsonParser.parseString(json).getAsJsonObject();
        return new TableScanPlan(
                response.get("plan").toString(),
                TableSchema.fromJson(response.getAsJsonObject("schema").toString()),
                response.get("snapshot_id").getAsLong(),
                response.get("total_buckets").getAsInt(),
                response.get("data_size_bytes").getAsLong());
    }

    private static native String openCurrentNative(String configJson, String tableName);

    private static native String openSnapshotNative(
            String configJson, String tableName, long snapshotId);

    private static native String splitsNative(String planJson);
}
