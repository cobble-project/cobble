package io.cobble.table;

import io.cobble.Config;
import io.cobble.NativeLoader;

import java.util.Objects;

/** Configures and terminally opens one writable table shard. */
public final class TableWriterBuilder {
    private static final int OPEN = 0;
    private static final int RESUME_FROM_SNAPSHOT = 1;

    private final CatalogTable catalogTable;
    private final TableWritePlan plan;
    private final Config runtime;
    private final boolean standalone;
    private String tableName;
    private Integer bucket;

    private TableWriterBuilder(
            CatalogTable catalogTable, TableWritePlan plan, Config runtime, boolean standalone) {
        this.catalogTable = catalogTable;
        this.plan = plan;
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.standalone = standalone;
    }

    static TableWriterBuilder fromCatalog(CatalogTable table, Config runtime) {
        return new TableWriterBuilder(Objects.requireNonNull(table, "table"), null, runtime, false);
    }

    static TableWriterBuilder fromPlan(TableWritePlan plan, Config runtime) {
        return new TableWriterBuilder(null, Objects.requireNonNull(plan, "plan"), runtime, false);
    }

    static TableWriterBuilder fromStandalone(Config runtime) {
        return new TableWriterBuilder(null, null, runtime, true);
    }

    /** Selects exactly one isolated physical bucket for this writer. */
    public TableWriterBuilder bucket(int value) {
        if (value < 0 || value > 65535) {
            throw new IllegalArgumentException("bucket must be in [0, 65535]");
        }
        bucket = Integer.valueOf(value);
        return this;
    }

    /** Sets the physical table name for a standalone writer. */
    public TableWriterBuilder tableName(String value) {
        if (!standalone) {
            throw new IllegalStateException(
                    "catalog-bound TableWriterBuilder cannot change tableName");
        }
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("tableName must not be empty");
        }
        tableName = value;
        return this;
    }

    /** Opens this bucket at its empty baseline, creating that baseline when absent. */
    public Table create(TableSchema schema) {
        if (!standalone) {
            throw new IllegalStateException("catalog-bound TableWriterBuilder requires open()");
        }
        Objects.requireNonNull(schema, "schema");
        if (tableName == null) {
            throw new IllegalStateException("standalone TableWriterBuilder requires tableName");
        }
        int selectedBucket = requireBucket();
        NativeLoader.load();
        return Table.writerCreateNative(
                runtime.toJson(), tableName, TableJson.toJson(schema), selectedBucket);
    }

    /** Opens this catalog bucket at its empty baseline, creating that baseline when absent. */
    public Table open() {
        return open(OPEN, -1L);
    }

    /** Resumes this bucket from its exact committed snapshot boundary. */
    public Table resumeFromSnapshot(long snapshotId) {
        validateSnapshotId(snapshotId);
        if (standalone) return resumeStandalone(snapshotId);
        return open(RESUME_FROM_SNAPSHOT, snapshotId);
    }

    private Table open(int mode, long snapshotId) {
        if (standalone) {
            throw new IllegalStateException(
                    "standalone TableWriterBuilder requires create(schema)");
        }
        int selectedBucket = requireBucket();
        NativeLoader.load();
        if (catalogTable == null) {
            return TableWritePlan.writerOpenNative(
                    plan.nativePlanJson(), runtime.toJson(), mode, snapshotId, selectedBucket);
        }
        synchronized (catalogTable) {
            return CatalogTable.writerOpenNative(
                    catalogTable.nativeHandleForBuilder(),
                    runtime.toJson(),
                    mode,
                    snapshotId,
                    selectedBucket);
        }
    }

    private Table resumeStandalone(long snapshotId) {
        if (tableName == null) {
            throw new IllegalStateException("standalone TableWriterBuilder requires tableName");
        }
        int selectedBucket = requireBucket();
        NativeLoader.load();
        return Table.writerResumeNative(runtime.toJson(), tableName, selectedBucket, snapshotId);
    }

    private int requireBucket() {
        if (bucket == null) throw new IllegalStateException("TableWriterBuilder requires bucket");
        return bucket.intValue();
    }

    private static void validateSnapshotId(long snapshotId) {
        if (snapshotId < 0L) throw new IllegalArgumentException("snapshotId must be >= 0");
    }
}
