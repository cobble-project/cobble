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
import java.util.Objects;

/** Serializable, format-aware scan plan pinned to one committed global snapshot. */
public final class TableScanPlan implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String formatId;
    private final TableSchema nativeSchema;
    private final TableReadSchema sourceSchema;
    private final TableReadSchema readSchema;
    private final List<TableScanSplit> splits;
    private final TableScanReadContext readContext;
    private final long snapshotId;
    private final int totalBuckets;
    private final long dataSizeBytes;

    private TableScanPlan(
            String formatId,
            TableSchema nativeSchema,
            TableReadSchema sourceSchema,
            TableReadSchema readSchema,
            List<? extends TableScanSplit> splits,
            TableScanReadContext readContext,
            long snapshotId,
            int totalBuckets,
            long dataSizeBytes) {
        this.formatId = TableScanSplit.requireText(formatId, "formatId");
        this.nativeSchema = nativeSchema;
        this.sourceSchema = Objects.requireNonNull(sourceSchema, "sourceSchema");
        this.readSchema = Objects.requireNonNull(readSchema, "readSchema");
        this.readContext = Objects.requireNonNull(readContext, "readContext");
        List<TableScanSplit> assignments = new ArrayList<TableScanSplit>(splits.size());
        for (TableScanSplit split : splits) {
            if (!formatId.equals(split.formatId())) {
                throw new IllegalArgumentException("split format differs from plan format");
            }
            assignments.add(split.withReadContext(readContext));
        }
        this.splits = Collections.unmodifiableList(assignments);
        if (snapshotId < 0 || totalBuckets < 1 || totalBuckets > 65536 || dataSizeBytes < 0) {
            throw new IllegalArgumentException(
                    "invalid scan snapshot identity, bucket count or size");
        }
        this.snapshotId = snapshotId;
        this.totalBuckets = totalBuckets;
        this.dataSizeBytes = dataSizeBytes;
    }

    /** Constructs portable assignments for a fixed external read description. */
    public static TableScanPlan forRead(
            TableReadSnapshot snapshot,
            TableReadSchema schema,
            TableReadCapabilities capabilities,
            long dataSizeBytes) {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(capabilities, "capabilities");
        List<TableScanSplit> splits = new ArrayList<TableScanSplit>();
        for (TableReadSnapshot.ShardDescriptor shard : snapshot.shards()) {
            splits.add(
                    new TableScanSplit(
                            snapshot.formatId(),
                            snapshot.columnFamily(),
                            shard.snapshot(),
                            shard.metadataJson()));
        }
        io.cobble.GlobalSnapshot global = snapshot.globalSnapshot();
        if (global == null) {
            throw new IllegalArgumentException("portable table reads require a global snapshot");
        }
        return new TableScanPlan(
                snapshot.formatId(),
                null,
                schema,
                schema,
                splits,
                new TableScanReadContext(
                        snapshot, capabilities, new TableReadProjection(schema, schema)),
                global.id,
                global.totalBuckets,
                dataSizeBytes);
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
        for (int i = 0; i < fieldNames.size(); i++) {
            int index = sourceIndex(fieldNames.get(i));
            fields.add(sourceSchema.fields().get(index));
        }
        return project(new TableReadSchema(fields));
    }

    /** Projects nested fields by stable ID; leaf types and map keys must remain unchanged. */
    public TableScanPlan project(TableReadSchema schema) {
        if (readSchema.fields().equals(schema.fields())) return this;
        TableReadProjection selected = new TableReadProjection(sourceSchema, schema);
        return new TableScanPlan(
                formatId,
                nativeSchema,
                sourceSchema,
                schema,
                splits,
                readContext.project(nativeSchema, selected),
                snapshotId,
                totalBuckets,
                dataSizeBytes);
    }

    /** Keeps only the worker's assigned split while preserving schema and projection. */
    public TableScanPlan forSplit(TableScanSplit split) {
        requireAssignedSplit(split);
        return withOnlySplit(split);
    }

    /** Partitions an assigned native split at an encoded physical key for exact scan resume. */
    public Partition partitionAfter(TableScanSplit split, int bucket, byte[] encodedKeyInclusive) {
        requireAssignedSplit(split);
        if (readContext.nativeProjection == null) {
            throw new UnsupportedOperationException(
                    "physical-key split partitioning requires a native table");
        }
        TableScanSplit.Partition partition = split.partitionAfter(bucket, encodedKeyInclusive);
        return new Partition(withOnlySplit(partition.before), withOnlySplit(partition.after));
    }

    public static final class Partition {
        public final TableScanPlan before;
        public final TableScanPlan after;

        private Partition(TableScanPlan before, TableScanPlan after) {
            this.before = before;
            this.after = after;
        }
    }

    private TableScanPlan withOnlySplit(TableScanSplit split) {
        return new TableScanPlan(
                formatId,
                nativeSchema,
                sourceSchema,
                readSchema,
                Collections.singletonList(split),
                readContext,
                snapshotId,
                totalBuckets,
                dataSizeBytes);
    }

    /** Opens the assigned split using the format plugin installed in the worker JVM. */
    public TableReadProvider<List<Value>, ?> open(Config config, TableScanSplit split)
            throws Exception {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(split, "split");
        requireAssignedSplit(split);
        return split.withReadContext(readContext).open(config);
    }

    /** Opens a sequential logical cursor across this plan's assigned fixed splits. */
    public TableReadCursor<List<Value>> scan(
            Config config, TableReadRange range, TableReadPosition position) throws Exception {
        return new PlannedCursor(this, config, range, position);
    }

    private void requireAssignedSplit(TableScanSplit split) {
        TableScanSplit requested = Objects.requireNonNull(split, "split");
        for (TableScanSplit assigned : splits) {
            if (sameSplit(assigned, requested)) return;
        }
        throw new IllegalArgumentException("split is not part of this plan");
    }

    private static boolean sameSplit(TableScanSplit left, TableScanSplit right) {
        if (TableMetadata.FORMAT.equals(left.formatId())) {
            return left.sameNativeAssignment(right);
        }
        if (!left.formatId().equals(right.formatId())
                || !left.columnFamily().equals(right.columnFamily())
                || !left.metadataJson().equals(right.metadataJson())) {
            return false;
        }
        io.cobble.ShardSnapshot leftShard = left.shardSnapshot();
        io.cobble.ShardSnapshot rightShard = right.shardSnapshot();
        return leftShard.dbId.equals(rightShard.dbId)
                && leftShard.snapshotId == rightShard.snapshotId
                && leftShard.manifestPath.equals(rightShard.manifestPath)
                && sameRanges(leftShard.ranges, rightShard.ranges);
    }

    private static boolean sameRanges(
            List<io.cobble.ShardSnapshot.Range> left, List<io.cobble.ShardSnapshot.Range> right) {
        if (left.size() != right.size()) return false;
        for (int index = 0; index < left.size(); index++) {
            if (left.get(index).start != right.get(index).start
                    || left.get(index).end != right.get(index).end) return false;
        }
        return true;
    }

    private int sourceIndex(String name) {
        TableScanSplit.requireText(name, "fieldName");
        for (int i = 0; i < sourceSchema.fields().size(); i++) {
            if (name.equals(sourceSchema.fields().get(i).name())) return i;
        }
        throw new IllegalArgumentException("unknown read field: " + name);
    }

    private static final class PlannedCursor implements TableReadCursor<List<Value>> {
        private final TableScanPlan plan;
        private final Config config;
        private final TableReadRange range;
        private TableReadPosition position;
        private boolean resumeLocated;
        private int splitIndex;
        private TableReadProvider<List<Value>, ?> provider;
        private TableReadSession<List<Value>, ?> session;
        private TableReadCursor<List<Value>> cursor;
        private boolean closed;

        private PlannedCursor(
                TableScanPlan plan,
                Config config,
                TableReadRange range,
                TableReadPosition position) {
            this.plan = Objects.requireNonNull(plan, "plan");
            this.config = Objects.requireNonNull(config, "config");
            this.range = Objects.requireNonNull(range, "range");
            this.position = position;
            this.resumeLocated = position == null;
        }

        @Override
        public TableReadEntry<List<Value>> next() throws Exception {
            while (!closed) {
                if (cursor == null && !openNextSplit()) return null;
                TableReadEntry<List<Value>> entry = cursor.next();
                if (entry != null) {
                    position = entry.position();
                    return entry;
                }
                closeCurrent();
            }
            return null;
        }

        @Override
        public void close() {
            closed = true;
            closeCurrent();
        }

        private boolean openNextSplit() throws Exception {
            while (splitIndex < plan.splits.size()) {
                TableScanSplit split = plan.splits.get(splitIndex++);
                if (!intersects(split, range)) continue;
                if (!resumeLocated && !ownsBucket(split, position.bucket())) continue;
                try {
                    provider = plan.open(config, split);
                    session = provider.open();
                    cursor = session.scan(range, resumeLocated ? null : position);
                    resumeLocated = true;
                    return true;
                } catch (Exception error) {
                    closeCurrent();
                    throw error;
                }
            }
            if (!resumeLocated) {
                throw new IllegalArgumentException("resume position is outside the scan plan");
            }
            return false;
        }

        private static boolean ownsBucket(TableScanSplit split, int bucket) {
            for (io.cobble.ShardSnapshot.Range range : split.shardSnapshot().ranges) {
                if (bucket >= range.start && bucket <= range.end) return true;
            }
            return false;
        }

        private static boolean intersects(TableScanSplit split, TableReadRange requested) {
            for (io.cobble.ShardSnapshot.Range range : split.shardSnapshot().ranges) {
                if (range.start <= requested.lastBucket() && range.end >= requested.firstBucket()) {
                    return true;
                }
            }
            return false;
        }

        private void closeCurrent() {
            try {
                if (cursor != null) cursor.close();
            } finally {
                try {
                    if (session != null) session.close();
                } finally {
                    if (provider != null) provider.close();
                    cursor = null;
                    session = null;
                    provider = null;
                }
            }
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
        TableScanReadContext readContext =
                new TableScanReadContext(schema, new TableReadProjection(readSchema, readSchema));
        JsonArray values =
                JsonParser.parseString(splitsNative(response.get("plan").toString()))
                        .getAsJsonArray();
        List<TableScanSplit> splits = new ArrayList<TableScanSplit>();
        for (JsonElement value : values)
            splits.add(new TableScanSplit(value.toString(), readContext));
        return new TableScanPlan(
                TableMetadata.FORMAT,
                schema,
                readSchema,
                readSchema,
                splits,
                readContext,
                response.get("snapshot_id").getAsLong(),
                response.get("total_buckets").getAsInt(),
                response.get("data_size_bytes").getAsLong());
    }

    private static native String openSnapshotNative(
            String configJson, String tableName, long snapshotId);

    private static native String splitsNative(String planJson);
}
