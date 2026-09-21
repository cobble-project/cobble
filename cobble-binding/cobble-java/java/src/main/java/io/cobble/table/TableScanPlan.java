package io.cobble.table;

import io.cobble.Config;
import io.cobble.DbCoordinator;
import io.cobble.GlobalSnapshot;
import io.cobble.NativeLoader;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Serializable, format-aware scan plan pinned to one committed global snapshot. */
public final class TableScanPlan implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String formatId;
    private final TableSchema nativeSchema;
    private final TableReadSchema sourceSchema;
    private final TableReadSchema readSchema;
    private final List<TableScanSplit> splits;
    private final int[] projection;
    private final long snapshotId;
    private final int totalBuckets;
    private final long dataSizeBytes;

    private TableScanPlan(
            String formatId,
            TableSchema nativeSchema,
            TableReadSchema sourceSchema,
            TableReadSchema readSchema,
            List<? extends TableScanSplit> splits,
            int[] projection,
            long snapshotId,
            int totalBuckets,
            long dataSizeBytes) {
        this.formatId = TableScanSplit.requireText(formatId, "formatId");
        this.nativeSchema = nativeSchema;
        this.sourceSchema = Objects.requireNonNull(sourceSchema, "sourceSchema");
        this.readSchema = Objects.requireNonNull(readSchema, "readSchema");
        this.splits = Collections.unmodifiableList(new ArrayList<TableScanSplit>(splits));
        for (TableScanSplit split : this.splits) {
            if (!formatId.equals(split.formatId())) {
                throw new IllegalArgumentException("split format differs from plan format");
            }
        }
        this.projection = projection.clone();
        if (snapshotId < 0 || totalBuckets < 1 || totalBuckets > 65536 || dataSizeBytes < 0) {
            throw new IllegalArgumentException(
                    "invalid scan snapshot identity, bucket count or size");
        }
        this.snapshotId = snapshotId;
        this.totalBuckets = totalBuckets;
        this.dataSizeBytes = dataSizeBytes;
    }

    /** Constructs a plugin plan. Read schemas deliberately do not require primary keys. */
    public static TableScanPlan forRead(
            String formatId,
            TableReadSchema schema,
            long snapshotId,
            int totalBuckets,
            long dataSizeBytes,
            List<? extends TableScanSplit> splits) {
        return new TableScanPlan(
                formatId,
                null,
                schema,
                schema,
                splits,
                identityProjection(schema),
                snapshotId,
                totalBuckets,
                dataSizeBytes);
    }

    /** Resolves the format of the current snapshot, then pins that snapshot for every split. */
    public static TableScanPlan forCurrentSnapshot(Config config, String tableName) {
        return open(config, tableName, null);
    }

    /** Resolves the format of one fixed committed snapshot. */
    public static TableScanPlan forSnapshot(Config config, String tableName, long snapshotId) {
        if (snapshotId < 0) throw new IllegalArgumentException("snapshotId must be >= 0");
        return open(config, tableName, snapshotId);
    }

    /** Full native table schema, including write keys; unavailable for external read formats. */
    public TableSchema schema() {
        if (nativeSchema == null) {
            throw new UnsupportedOperationException(
                    "format '" + formatId + "' has no native table schema; use readSchema()");
        }
        return nativeSchema;
    }

    /** Projected logical columns shared by native and plugin formats. */
    public TableReadSchema readSchema() {
        return readSchema;
    }

    public String formatId() {
        return formatId;
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

    public List<TableScanSplit> splits() {
        return splits;
    }

    /** Projects logical rows by name; an empty projection preserves row count. */
    public TableScanPlan project(List<String> fieldNames) {
        Objects.requireNonNull(fieldNames, "fieldNames");
        List<DataField> fields = new ArrayList<DataField>();
        int[] indexes = new int[fieldNames.size()];
        for (int i = 0; i < fieldNames.size(); i++) {
            int index = sourceIndex(fieldNames.get(i));
            for (int j = 0; j < i; j++) {
                if (indexes[j] == index)
                    throw new IllegalArgumentException(
                            "duplicate read field: " + fieldNames.get(i));
            }
            indexes[i] = index;
            fields.add(sourceSchema.fields().get(index));
        }
        return new TableScanPlan(
                formatId,
                nativeSchema,
                sourceSchema,
                new TableReadSchema(fields),
                splits,
                indexes,
                snapshotId,
                totalBuckets,
                dataSizeBytes);
    }

    /** Keeps only the worker's assigned split while preserving schema and projection. */
    public TableScanPlan forSplit(TableScanSplit split) {
        requireAssignedSplit(split);
        return new TableScanPlan(
                formatId,
                nativeSchema,
                sourceSchema,
                readSchema,
                Collections.singletonList(split),
                projection,
                snapshotId,
                totalBuckets,
                dataSizeBytes);
    }

    /** Opens the assigned split using the format plugin installed in the worker JVM. */
    public TableReadProvider<List<Value>, ?> open(Config config, TableScanSplit split)
            throws Exception {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(split, "split");
        if (!formatId.equals(split.formatId()))
            throw new IllegalArgumentException("split format differs from plan format");
        return TableReadProjection.apply(
                TableReadFormatRegistry.resolve(formatId).open(config, split),
                readSchema,
                projection);
    }

    private void requireAssignedSplit(TableScanSplit split) {
        if (!splits.contains(Objects.requireNonNull(split, "split"))) {
            throw new IllegalArgumentException("split is not part of this plan");
        }
    }

    private int sourceIndex(String name) {
        TableScanSplit.requireText(name, "fieldName");
        for (int i = 0; i < sourceSchema.fields().size(); i++) {
            if (name.equals(sourceSchema.fields().get(i).name())) return i;
        }
        throw new IllegalArgumentException("unknown read field: " + name);
    }

    private static int[] identityProjection(TableReadSchema schema) {
        int[] indexes = new int[schema.fields().size()];
        for (int i = 0; i < indexes.length; i++) indexes[i] = i;
        return indexes;
    }

    private static TableScanPlan open(Config config, String tableName, Long id) {
        Objects.requireNonNull(config, "config");
        TableScanSplit.requireText(tableName, "tableName");
        GlobalSnapshot snapshot;
        try (DbCoordinator coordinator = DbCoordinator.open(config)) {
            snapshot =
                    id == null
                            ? coordinator.loadCurrentGlobalSnapshot()
                            : coordinator.getGlobalSnapshot(id);
        }
        if (snapshot == null)
            throw new IllegalArgumentException("no committed snapshot for '" + tableName + "'");
        try {
            return TableReadFormatRegistry.plan(
                    config, TableReadSnapshot.forGlobal(config, snapshot, tableName));
        } catch (RuntimeException error) {
            throw error;
        } catch (Exception error) {
            throw new IllegalStateException("failed to plan format-aware snapshot scan", error);
        }
    }

    // Only the native factory uses this path, avoiding recursive format resolution.
    static TableScanPlan nativeForSnapshot(Config config, String tableName, long snapshotId) {
        NativeLoader.load();
        return fromResponseJson(openSnapshotNative(config.toJson(), tableName, snapshotId));
    }

    static TableScanPlan fromResponseJson(String json) {
        NativeLoader.load();
        JsonObject response = JsonParser.parseString(json).getAsJsonObject();
        TableSchema schema = TableSchema.fromJson(response.getAsJsonObject("schema").toString());
        TableReadSchema readSchema = new TableReadSchema(schema.fields());
        JsonArray values =
                JsonParser.parseString(splitsNative(response.get("plan").toString()))
                        .getAsJsonArray();
        List<TableScanSplit> splits = new ArrayList<TableScanSplit>();
        for (JsonElement value : values) splits.add(new TableScanSplit(value.toString()));
        return new TableScanPlan(
                TableMetadata.FORMAT,
                schema,
                readSchema,
                readSchema,
                splits,
                identityProjection(readSchema),
                response.get("snapshot_id").getAsLong(),
                response.get("total_buckets").getAsInt(),
                response.get("data_size_bytes").getAsLong());
    }

    private static native String openSnapshotNative(
            String configJson, String tableName, long snapshotId);

    private static native String splitsNative(String planJson);
}
