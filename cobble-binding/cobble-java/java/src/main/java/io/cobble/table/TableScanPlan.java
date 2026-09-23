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
    private final TableReadSnapshot snapshot;
    private final TableReadCapabilities capabilities;
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
            TableReadSnapshot snapshot,
            TableReadCapabilities capabilities,
            TableReadSchema sourceSchema,
            TableReadSchema readSchema,
            List<? extends TableScanSplit> splits,
            int[] projection,
            long snapshotId,
            int totalBuckets,
            long dataSizeBytes) {
        this.formatId = TableScanSplit.requireText(formatId, "formatId");
        this.nativeSchema = nativeSchema;
        this.snapshot = snapshot;
        this.capabilities = capabilities;
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
                snapshot,
                capabilities,
                schema,
                schema,
                splits,
                identityProjection(schema),
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
                snapshot,
                capabilities,
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
                snapshot,
                capabilities,
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
        requireAssignedSplit(split);
        if (!formatId.equals(split.formatId()))
            throw new IllegalArgumentException("split format differs from plan format");
        if (snapshot == null) {
            return TableReadProjection.apply(
                    new NativeTableScanReadProvider(config, split), readSchema, projection);
        }
        TableFormatPlugin plugin = TableFormatPluginRegistry.resolve(formatId);
        return new PhysicalProvider(
                config, snapshot, plugin, capabilities, split, readSchema, projection);
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
        {
            throw new IllegalArgumentException("split is not part of this plan");
        }
    }

    private static boolean sameSplit(TableScanSplit left, TableScanSplit right) {
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

    private static int[] identityProjection(TableReadSchema schema) {
        int[] indexes = new int[schema.fields().size()];
        for (int i = 0; i < indexes.length; i++) indexes[i] = i;
        return indexes;
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

    private static final class PhysicalProvider implements TableReadProvider<List<Value>, Void> {
        private final Config config;
        private final TableReadSnapshot snapshot;
        private final TableFormatPlugin plugin;
        private final TableReadCapabilities capabilities;
        private final TableScanSplit split;
        private final TableReadSchema schema;
        private final int[] projection;

        private PhysicalProvider(
                Config config,
                TableReadSnapshot snapshot,
                TableFormatPlugin plugin,
                TableReadCapabilities capabilities,
                TableScanSplit split,
                TableReadSchema schema,
                int[] projection) {
            this.config = config.copy();
            this.snapshot = snapshot;
            this.plugin = plugin;
            this.capabilities = capabilities;
            this.split = split;
            this.schema = schema;
            this.projection = projection.clone();
        }

        @Override
        public TableReadCapabilities capabilities() {
            return capabilities;
        }

        @Override
        public TableReadSession<List<Value>, Void> open() throws Exception {
            TableFormatBinding binding = plugin.bind(snapshot);
            return new SplitSession(
                    new PhysicalTableReadSession(config, snapshot, binding),
                    split,
                    schema,
                    projection);
        }

        @Override
        public void close() {}
    }

    private static final class SplitSession implements TableReadSession<List<Value>, Void> {
        private final PhysicalTableReadSession delegate;
        private final TableScanSplit split;
        private final TableReadSchema schema;
        private final int[] projection;

        private SplitSession(
                PhysicalTableReadSession delegate,
                TableScanSplit split,
                TableReadSchema schema,
                int[] projection) {
            this.delegate = delegate;
            this.split = split;
            this.schema = schema;
            this.projection = projection;
        }

        @Override
        public TableReadSchema schema() {
            return schema;
        }

        @Override
        public TableReadCapabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public TableReadCursor<List<Value>> scan(TableReadRange range, TableReadPosition position)
                throws Exception {
            return TableReadProjection.cursor(
                    delegate.scan(range, position, split.shardSnapshot().ranges), projection);
        }

        @Override
        public java.util.Collection<TableReadEntry<List<Value>>> lookup(Void unused) {
            throw new UnsupportedOperationException("scan assignments do not support lookup");
        }

        @Override
        public void close() {
            delegate.close();
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
                null,
                null,
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
